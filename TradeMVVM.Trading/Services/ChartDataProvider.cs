using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using TradeMVVM.Domain;

#nullable enable

namespace TradeMVVM.Trading.Services
{
    public class ChartDataProvider
    {
        // cleaned imports and nullable handling — no logic changes
        private readonly Services.Infrastructure.IUnresolvedIsinLogger _unresolvedLogger;
        // track consecutive failure counts per ISIN; log only when threshold is reached
        private static readonly ConcurrentDictionary<string, int> _failureCounts = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        private readonly DatabaseService _dbService;
        private readonly string[] _bnpPriorityKeywords = Array.Empty<string>();
        // initialize with null-forgiving to satisfy nullable analysis; constructors assign a proper instance
        private readonly TradeMVVM.Trading.Services.SettingsService _settingsService = null!;

        // Providers

        public ChartDataProvider(TradeMVVM.Trading.Services.SettingsService settings,
            HttpClient client,
            SemaphoreSlim throttle,
            Services.Infrastructure.IUnresolvedIsinLogger unresolvedLogger)
        {
            _settingsService = settings;
            _ = client ?? throw new ArgumentNullException(nameof(client));
            _ = throttle ?? throw new ArgumentNullException(nameof(throttle));
            // unresolved logger should be injected via DI; if not, create a default one from settings
            _unresolvedLogger = unresolvedLogger ?? ResolveLogger(settings);

            // Database service for persistent counters (fallback to new instance if not registered)
            try { _dbService = (App.Services?.GetService(typeof(DatabaseService)) as DatabaseService) ?? new DatabaseService(); } catch { _dbService = new DatabaseService(); }

            // Load any persisted failure counts
            try
            {
                var persisted = _dbService.LoadFailureCounts("unresolved");
                foreach (var kv in persisted)
                {
                    _failureCounts.AddOrUpdate(kv.Key, kv.Value, (_, __) => kv.Value);
                }
            }
            catch { }

            try
            {
                _bnpPriorityKeywords = (settings?.BnpPriorityKeywords ?? string.Empty)
                    .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim().ToLowerInvariant())
                    .Where(s => !string.IsNullOrWhiteSpace(s))
                    .ToArray();
            }
            catch { }

        }

        // Backwards-compatible constructors - preserve original behavior when DI is not used
        public ChartDataProvider() : this(new TradeMVVM.Trading.Services.SettingsService()) { }

        public ChartDataProvider(TradeMVVM.Trading.Services.SettingsService settings) : this(
            settings ?? new TradeMVVM.Trading.Services.SettingsService(),
            ResolveHttpClient(),
            ResolveThrottle(),
            ResolveLogger(settings ?? new TradeMVVM.Trading.Services.SettingsService()))
        {
            // ctor chaining performed
        }

        private static HttpClient ResolveHttpClient()
        {
            try
            {
                var sp = App.Services;
                var client = sp?.GetService<HttpClient>();
                if (client != null) return client;
            }
            catch { }
            return new HttpClient { Timeout = TimeSpan.FromSeconds(8) };
        }

        private static SemaphoreSlim ResolveThrottle()
        {
            try
            {
                var sp = App.Services;
                var thr = sp?.GetService<SemaphoreSlim>();
                if (thr != null) return thr;
            }
            catch { }
            return new SemaphoreSlim(2);
        }

        // Wallstreet provider removed

        // Lang & Schwarz provider removed



        // DeutscheBoerse provider removed

        private static Services.Infrastructure.IUnresolvedIsinLogger ResolveLogger(TradeMVVM.Trading.Services.SettingsService? settings)
        {
            try
            {
                var sp = App.Services;
                var l = sp?.GetService<Services.Infrastructure.IUnresolvedIsinLogger>();
                if (l != null) return l;
            }
            catch { }
            var folder = settings?.UnresolvedLogFolder ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "Trade", "TradeMVVM.Trading");
            return new Services.Infrastructure.DevFolderUnresolvedIsinLogger(folder);
        }

        public async Task<(double price, double percent, string provider, DateTime? providerTime)> DataProvider(
            string isin,
            TradeMVVM.Domain.StockType type,
            string? name = null,
            CancellationToken token = default)
        {
            var attemptedUrls = new List<string>();

            bool preferBnp = false;
            try
            {
                // determine preferred provider
                var preferred = GetPrimaryProviderForName(name ?? string.Empty);
                preferBnp = string.Equals(preferred, "BNP", StringComparison.OrdinalIgnoreCase);
                Debug.WriteLine($"DataProvider: {isin} preferred={preferred}");
            }
            catch { }

            var bnp = App.Services?.GetService<Providers.BnpProvider>() as Providers.BnpProvider;
            var defaultProvider = App.Services?.GetService<TradeMVVM.Trading.Services.Providers.IPriceProvider>() as TradeMVVM.Trading.Services.Providers.IPriceProvider;

            async Task<(double price, double percent, string provider, DateTime? providerTime)?> TryBnpAsync()
            {
                try
                {
                    if (bnp == null) return null;
                    var r = await bnp.GetPriceAsync(isin, attemptedUrls, token);
                    if (r.HasValue && r.Value.Item1 != 0)
                        return (r.Value.Item1, r.Value.Item2, "BNP", r.Value.Item3);
                }
                catch (Exception ex)
                {
                    try { Console.WriteLine($"TryBnpAsync exception for {isin}: {ex.Message}"); } catch { }
                }
                finally
                {
                    try { if (attemptedUrls != null && attemptedUrls.Count > 0) Debug.WriteLine($"TryBnpAsync attempted URLs for {isin}: {string.Join(';', attemptedUrls)}"); } catch { }
                }
                return null;
            }

            async Task<(double price, double percent, string provider, DateTime? providerTime)?> TryDefaultAsync()
            {
                try
                {
                    if (defaultProvider == null || defaultProvider is Providers.BnpProvider)
                        return null;

                    var r = await defaultProvider.GetPriceAsync(isin, attemptedUrls, token);
                    if (r.HasValue && r.Value.Item1 != 0)
                        return (r.Value.Item1, r.Value.Item2, "Gettex", r.Value.Item3);
                }
                catch (Exception ex)
                {
                    try { Console.WriteLine($"TryDefaultAsync exception for {isin}: {ex.Message}"); } catch { }
                }
                finally
                {
                    try { if (attemptedUrls != null && attemptedUrls.Count > 0) Debug.WriteLine($"TryDefaultAsync attempted URLs for {isin}: {string.Join(';', attemptedUrls)}"); } catch { }
                }
                return null;
            }

            var preferredProvider = preferBnp ? "BNP" : "Gettex";

            if (preferBnp)
            {
                var bnpRes = await TryBnpAsync();
                if (bnpRes.HasValue)
                {
                    // minimal server output: ISIN,Price,Percent,Provider
                    Console.WriteLine($"{isin},{bnpRes.Value.price},{bnpRes.Value.percent},{bnpRes.Value.provider}");
                    return bnpRes.Value;
                }
                // failed — output ISIN,null,null,preferredProvider
                Console.WriteLine($"{isin},null,null,{preferredProvider}");
                return (double.NaN, double.NaN, preferredProvider, null);
            }
            else
            {
                var defRes = await TryDefaultAsync();
                if (defRes.HasValue)
                {
                    Console.WriteLine($"{isin},{defRes.Value.price},{defRes.Value.percent},{defRes.Value.provider}");
                    return defRes.Value;
                }
                Console.WriteLine($"{isin},null,null,{preferredProvider}");
                return (double.NaN, double.NaN, preferredProvider, null);
            }
        }

        // DeutscheBoerse provider removed - segments list deleted

        public TradeMVVM.Domain.StockType DetectStockType(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return TradeMVVM.Domain.StockType.Aktie;

            var lower = name.ToLowerInvariant();

            if (lower.Contains("knockout") ||
                lower.Contains("turbo") ||
                lower.Contains("zertifikat") ||
                lower.Contains("optionsschein") ||
                lower.Contains("warrant"))
                return TradeMVVM.Domain.StockType.Knockout;

            return TradeMVVM.Domain.StockType.Aktie;
        }

        public string GetPrimaryProviderForName(string name)
        {
            var lowerName = (name ?? string.Empty).ToLowerInvariant();

            if (!string.IsNullOrWhiteSpace(lowerName))
            {
                // BNP keywords from preferences take precedence if enabled
                if (_settingsService?.BnpPriorityEnabled == true && _bnpPriorityKeywords.Length > 0)
                {
                    foreach (var kw in _bnpPriorityKeywords)
                        if (!string.IsNullOrWhiteSpace(kw) && lowerName.Contains(kw))
                            return "BNP";
                }

                // knockout default provider from preferences
                if (DetectStockType(name ?? string.Empty) == StockType.Knockout)
                {
                    if (string.Equals(_settingsService?.DefaultProviderForKnockout, "BNP", StringComparison.OrdinalIgnoreCase))
                        return "BNP";
                    return "Gettex";
                }

                // Default to Gettex for everything else
                return "Gettex";
            }

            return "Gettex";
        }

        public void WriteTestLog(string isin = "INVALID-ISIN-TEST-000")
        {
            try
            {
                var attemptedUrls = new List<string>();

                // Create a simple unresolved ISIN test entry
                attemptedUrls.Add(isin);
                _unresolvedLogger?.LogUnresolvedIsin(isin, StockType.Aktie, attemptedUrls);
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"WriteTestLog failed: {ex}");
            }
        }
    }
}