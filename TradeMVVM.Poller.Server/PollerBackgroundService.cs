using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Net.Http;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using TradeMVVM.Poller.Core;
using TradeMVVM.Domain;
using TradeMVVM.Trading.Services;
using TradeMVVM.Trading.DataAnalysis;

internal class PollerBackgroundService : BackgroundService
{
    private readonly ILogger<PollerBackgroundService> _logger;
    private readonly ChartDataProvider _provider;
    private readonly TradeMVVM.Trading.Data.PriceRepository _repo;
    private readonly HttpClient _http;

    public PollerBackgroundService(ILogger<PollerBackgroundService> logger, ChartDataProvider provider, TradeMVVM.Trading.Data.PriceRepository repo, HttpClient http)
    {
        _logger = logger;
        _provider = provider;
        _repo = repo;
        _http = http ?? new HttpClient();
    }

    private async Task<string?> GetStringWithRetriesAsync(string url, CancellationToken token)
    {
        const int maxAttempts = 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                var resp = await _http.GetAsync(url, token).ConfigureAwait(false);
                if (!resp.IsSuccessStatusCode) return null;
                return await resp.Content.ReadAsStringAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { throw; }
            catch (IOException) when (attempt < maxAttempts)
            {
                // transient socket IO error — back off and retry
                try { await Task.Delay(200 * attempt, token).ConfigureAwait(false); } catch { }
                continue;
            }
            catch (HttpRequestException) when (attempt < maxAttempts)
            {
                try { await Task.Delay(200 * attempt, token).ConfigureAwait(false); } catch { }
                continue;
            }
            catch
            {
                return null;
            }
        }
        return null;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("PollerBackgroundService starting");

        // Write an initial heartbeat immediately so the GUI can detect the server quickly.
        try
        {
            var dbInit = new TradeMVVM.Poller.Core.DatabaseService();
            try { dbInit.SetHeartbeat(DateTime.Now); } catch { }
            try { Console.WriteLine($"Poller initial heartbeat (local): {DateTime.Now.ToString("o")} "); } catch { }
        }
        catch { }

        // Start a background heartbeat updater so the GUI sees the server as running quickly
        try
        {
            var hbDb = new TradeMVVM.Poller.Core.DatabaseService();
            Task.Run(async () =>
            {
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var nowUtc = DateTime.UtcNow;
                            hbDb.SetHeartbeat(nowUtc);
                            try { Console.WriteLine($"Poller heartbeat (local): {DateTime.Now.ToString("o")} "); } catch { }
                        }
                        catch (OperationCanceledException)
                        {
                            // cancellation requested
                            break;
                        }
                        catch (Exception ex)
                        {
                            try { _logger?.LogDebug(ex, "Heartbeat write failed"); } catch { }
                        }

                        try
                        {
                            await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // graceful shutdown
                }
                catch (Exception ex)
                {
                    try { _logger?.LogWarning(ex, "Heartbeat background task failed"); } catch { }
                }
            }, stoppingToken);
        }
        catch (Exception ex)
        {
            try { _logger?.LogWarning(ex, "Failed to start heartbeat background task"); } catch { }
        }

        // Kick off self-test and full-scan in background so they do not block the main polling loop or heartbeat.
        try
        {
            Task.Run(async () =>
            {
                try
                {
                    // keep a very short self-test to speed startup
                    await RunSelfTestAsync(new[] { "DE000BASF111" }, stoppingToken).ConfigureAwait(false);
                }
                catch { }

                try
                {
                    var skipFull = Environment.GetEnvironmentVariable("SKIP_POLLER_FULLSCAN");
                    if (string.IsNullOrWhiteSpace(skipFull) || skipFull != "1")
                    {
                        await RunFullScanAsync(stoppingToken).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    try { _logger.LogWarning(ex, "RunFullScanAsync failed"); } catch { }
                }
            }, stoppingToken);
        }
        catch { }

        // instantiate the original GUI polling service so behavior matches the local app polling
        var service = new PricePollingService(_provider, _repo, TimeSpan.FromSeconds(2));
        // attempt to load holdings CSV to map ISIN->Name so preferred provider logic can use the holding name
        var holdingsMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var settings = new TradeMVVM.Trading.Services.SettingsService();
            var csv = settings.HoldingsCsvPath;
            if (!string.IsNullOrWhiteSpace(csv) && File.Exists(csv))
            {
                try
                {
                    var map = HoldingsCalculator.ComputeHoldingsFromCsv(csv);
                    foreach (var kv in map)
                        if (!string.IsNullOrWhiteSpace(kv.Key)) holdingsMap[kv.Key] = kv.Value?.Name ?? string.Empty;
                }
                catch { }
            }
        }
        catch { }

        // load ISINs from DB holdings or fall back to sample list
        var stocks = new List<(string isin, string name, StockType type)>();
        try
        {
            var dbService = new TradeMVVM.Trading.Services.DatabaseService();
            var all = dbService.LoadAll();
            if (all != null && all.Count > 0)
            {
                var distinct = all.Select(p => p.ISIN).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                foreach (var isin in distinct)
                {
                    holdingsMap.TryGetValue(isin, out var nm);
                    stocks.Add((isin, nm ?? string.Empty, StockType.Aktie));
                }
            }
        }
        catch { }

        if (stocks.Count == 0)
        {
            // sample fallback list
            stocks.Add(("DE000BASF111", "BASF", StockType.Aktie));
            stocks.Add(("DE0007664039", "Siemens", StockType.Aktie));
        }

        // attempt a best-effort backfill for existing rows where Price != 0 but Percent is missing/zero
        try
        {
            var dbForBackfill = new TradeMVVM.Trading.Services.DatabaseService();
            var updated = dbForBackfill.BackfillPercentWhereZero();
            try { _logger.LogInformation("BackfillPercentWhereZero updated {count} rows", updated); } catch { }
        }
        catch (Exception ex)
        {
            try { _logger.LogWarning(ex, "BackfillPercentWhereZero failed"); } catch { }
        }

        try
        {
            // run polling loop using two staggered workers (same pattern as GUI Start)
            const int workerCount = 2;
            var t1 = service.StartAsync(stocks, stoppingToken, workerIndex: 0, workerCount: workerCount);
            var t2 = Task.Run(async () =>
            {
                try { await Task.Delay(1500, stoppingToken).ConfigureAwait(false); } catch { }
                if (stoppingToken.IsCancellationRequested) return;
                await service.StartAsync(stocks, stoppingToken, workerIndex: 1, workerCount: workerCount).ConfigureAwait(false);
            }, stoppingToken);

            await Task.WhenAll(t1, t2).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // expected on shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PollerBackgroundService crashed");
        }

        _logger.LogInformation("PollerBackgroundService stopping");
    }

    // helper: run a small self-test of providers and HTTP fetches
    private async Task RunSelfTestAsync(IEnumerable<string> isins, CancellationToken token)
    {
        foreach (var isin in isins)
        {
            if (token.IsCancellationRequested) break;
            try
            {
                var provider = _provider;
                if (provider == null) continue;

                // call DataProvider to populate attempted URLs (ChartDataProvider logs attempted URLs)
                var res = await provider.DataProvider(isin, StockType.Aktie, string.Empty, token).ConfigureAwait(false);
                try { Console.WriteLine($"SelfTest: DataProvider result for {isin}: price={res.price} percent={res.percent} provider={res.provider}"); } catch { }

                // additionally try the BNP URL pattern and dump a small HTML snippet to help debugging
                try
                {
                    var bnpUrl = $"https://derivate.bnpparibas.com/product-details/{isin}/";
                    var html = await GetStringWithRetriesAsync(bnpUrl, token).ConfigureAwait(false);
                    if (html != null)
                    {
                        var snippet = html.Length > 1000 ? html.Substring(0, 1000) : html;
                        try { Console.WriteLine($"SelfTest BNP HTML snippet for {isin}: {snippet.Replace('\n',' ')}"); } catch { }
                    }
                    else
                    {
                        try { Console.WriteLine($"SelfTest BNP fetch failed for {isin}"); } catch { }
                    }
                }
                catch (IOException) { try { Console.WriteLine($"SelfTest BNP fetch IO error for {isin}"); } catch { } }
                catch (Exception ex) { try { Console.WriteLine($"SelfTest BNP fetch failed for {isin}: {ex.Message}"); } catch { } }

                // try Gettex URL as well
                try
                {
                    var gettexUrl = $"https://www.gettex.de/aktie/{isin}/";
                    var html2 = await GetStringWithRetriesAsync(gettexUrl, token).ConfigureAwait(false);
                    if (html2 != null)
                    {
                        var snippet2 = html2.Length > 1000 ? html2.Substring(0, 1000) : html2;
                        try { Console.WriteLine($"SelfTest Gettex HTML snippet for {isin}: {snippet2.Replace('\n',' ')}"); } catch { }
                    }
                    else
                    {
                        try { Console.WriteLine($"SelfTest Gettex fetch failed for {isin}"); } catch { }
                    }
                }
                catch (IOException) { try { Console.WriteLine($"SelfTest Gettex fetch IO error for {isin}"); } catch { } }
                catch (Exception ex) { try { Console.WriteLine($"SelfTest Gettex fetch failed for {isin}: {ex.Message}"); } catch { } }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { try { Console.WriteLine($"SelfTest exception for {isin}: {ex.Message}"); } catch { } }
        }
    }

    // perform a full scan of distinct ISINs from DB; create a timestamped report with provider results and simple HTML snippets for failures
    private async Task RunFullScanAsync(CancellationToken token)
    {
        var sw = Stopwatch.StartNew();
        var db = new TradeMVVM.Trading.Services.DatabaseService();
        var all = db.LoadAll();
        var isins = new List<string>();
        if (all != null && all.Count > 0)
            isins.AddRange(all.Select(p => p.ISIN).Distinct(StringComparer.OrdinalIgnoreCase));
        if (isins.Count == 0)
        {
            isins.AddRange(new[] { "DE000BASF111", "DE0007664039", "IE00B5L8K969" });
        }

        var reportPath = Path.Combine(Environment.CurrentDirectory, $"poller_full_scan_{DateTime.UtcNow:yyyyMMdd_HHmmss}.txt");
        var prefPath = Path.Combine(Environment.CurrentDirectory, $"preferred_providers_{DateTime.UtcNow:yyyyMMdd_HHmmss}.txt");
        using (var swr = new StreamWriter(reportPath, false))
        {
            await swr.WriteLineAsync($"Full scan started {DateTime.UtcNow:O}");
            int checkedCount = 0, failedCount = 0;

            // attempt to load holdings CSV to map ISIN->Name so we pass the name into DataProvider
            var holdingsMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var settings = new TradeMVVM.Trading.Services.SettingsService();
                try
                {
                    var csv = settings.HoldingsCsvPath;
                    if (!string.IsNullOrWhiteSpace(csv) && File.Exists(csv))
                    {
                        var map = HoldingsCalculator.ComputeHoldingsFromCsv(csv);
                        foreach (var kv in map)
                        {
                            if (!string.IsNullOrWhiteSpace(kv.Key))
                                holdingsMap[kv.Key] = kv.Value?.Name ?? string.Empty;
                        }
                    }
                }
                catch { }

                using (var pw = new StreamWriter(prefPath, false))
                {
                    foreach (var i in isins)
                    {
                        try
                        {
                            holdingsMap.TryGetValue(i, out var name);
                            var pref = _provider?.GetPrimaryProviderForName(name ?? string.Empty) ?? "Gettex";
                            await pw.WriteLineAsync($"{i},{pref},{(name ?? string.Empty)}");
                        }
                        catch { await pw.WriteLineAsync($"{i},Gettex,"); }
                    }
                    pw.Flush();
                }
                try { Console.WriteLine($"Preferred providers written to {prefPath}"); } catch { }
            }
            catch { }
            foreach (var isin in isins)
            {
                if (token.IsCancellationRequested) break;
                checkedCount++;
                try
                {
                    holdingsMap.TryGetValue(isin, out var holdingName);
                    var res = await _provider.DataProvider(isin, StockType.Aktie, holdingName ?? string.Empty, token).ConfigureAwait(false);
                    await swr.WriteLineAsync($"ISIN={isin} price={res.price} percent={res.percent} provider={res.provider}");
                    if (double.IsNaN(res.price) || string.IsNullOrWhiteSpace(res.provider))
                    {
                        failedCount++;
                        // try BNP HTML
                        try
                        {
                            var bnpUrl = $"https://derivate.bnpparibas.com/product-details/{isin}/";
                            var html = await GetStringWithRetriesAsync(bnpUrl, token).ConfigureAwait(false);
                            if (html != null)
                            {
                                var snippet = html.Length > 1000 ? html.Substring(0, 1000) : html;
                                await swr.WriteLineAsync($"BNP_OK {bnpUrl} snippet={snippet.Replace('\n',' ')}");
                            }
                            else
                            {
                                await swr.WriteLineAsync($"BNP_FAIL no response");
                            }
                        }
                        catch (IOException ex)
                        {
                            await swr.WriteLineAsync($"BNP_FAIL {ex.Message}");
                        }

                        // try Gettex HTML
                        try
                        {
                            var gettexUrl = $"https://www.gettex.de/aktie/{isin}/";
                            var html2 = await GetStringWithRetriesAsync(gettexUrl, token).ConfigureAwait(false);
                            if (html2 != null)
                            {
                                var snippet2 = html2.Length > 1000 ? html2.Substring(0, 1000) : html2;
                                await swr.WriteLineAsync($"GETTEX_OK {gettexUrl} snippet={snippet2.Replace('\n',' ')}");
                            }
                            else
                            {
                                await swr.WriteLineAsync($"GETTEX_FAIL no response");
                            }
                        }
                        catch (IOException ex)
                        {
                            await swr.WriteLineAsync($"GETTEX_FAIL {ex.Message}");
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    await swr.WriteLineAsync($"ISIN_EXCEPTION {isin} {ex.Message}");
                }
            }

            await swr.WriteLineAsync($"Full scan finished {DateTime.UtcNow:O} elapsed={sw.Elapsed}");
            await swr.WriteLineAsync($"Checked={checkedCount} Failed={failedCount}");
            swr.Flush();
        }

        try { Console.WriteLine($"Full scan report written to {reportPath}"); } catch { }
    }
}
