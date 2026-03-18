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

    // Helper to run fire-and-forget tasks without CS4014 warnings and to observe/log exceptions.
    private void FireAndForget(Task task)
    {
        if (task == null) return;
        task.ContinueWith(t =>
        {
            if (t.IsFaulted)
            {
                try
                {
                    var logger = _logger;
                    if (logger != null && t.Exception != null)
                        logger.LogDebug(t.Exception, "Background fire-and-forget task faulted");
                }
                catch { }
            }
        }, TaskScheduler.Default);
    }

// Helper: sanitize holdings dictionary produced by HoldingsCalculator to skip malformed/empty entries
static class PollerHelpers
{
    public static Dictionary<string, TradeMVVM.Trading.DataAnalysis.Holding> SanitizeHoldings(Dictionary<string, TradeMVVM.Trading.DataAnalysis.Holding> raw, string sourcePath)
    {
        var outDict = new Dictionary<string, TradeMVVM.Trading.DataAnalysis.Holding>(StringComparer.OrdinalIgnoreCase);
        if (raw == null) return outDict;

        foreach (var kv in raw)
        {
            try
            {
                var h = kv.Value;
                if (h == null) continue;
                var isin = (h.ISIN ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                if (string.IsNullOrWhiteSpace(isin))
                {
                    try { Trace.TraceInformation($"Poller: skipping holding with empty ISIN from {sourcePath}"); } catch { }
                    continue;
                }

                // skip holdings with zero net shares (historical-only or empty)
                if (h.Shares == 0)
                {
                    try { Trace.TraceInformation($"Poller: skipping zero-share holding for ISIN {isin} from {sourcePath}"); } catch { }
                    continue;
                }

                outDict[isin] = h;
            }
            catch { }
        }

        return outDict;
    }
}

    // compute Total P/L from holdings CSV and insert into TotalPLHistory (used by heartbeat)
    private async Task ComputeAndInsertTotalPlAsync(CancellationToken token)
    {
        try
        {
            // run quickly and do not block caller; use Task.Run to avoid synchronous work on caller thread
            await Task.Run(() =>
            {
                try
                {
                    var settings = new TradeMVVM.Trading.Services.SettingsService();
                    var converter = new TradeMVVM.Trading.DataAnalysis.CurrencyConverter();
                    double totalPlEur = 0.0;

                        try
                        {
                            var csv = settings.HoldingsCsvPath;
                                if (!string.IsNullOrWhiteSpace(csv) && System.IO.File.Exists(csv))
                            {
                                var holdingsRaw = TradeMVVM.Trading.DataAnalysis.HoldingsCalculator.ComputeHoldingsFromCsv(csv);
                                var holdings = PollerHelpers.SanitizeHoldings(holdingsRaw, csv);
                                if (holdings != null && holdings.Count > 0)
                                {
                                    foreach (var kv in holdings)
                                    {
                                        try
                                        {
                                            var h = kv.Value;
                                            double avgBuy = h.RemainingBoughtShares > 0 ? (h.RemainingBoughtAmount / h.RemainingBoughtShares) : double.NaN;
                                            double lastPrice = double.NaN;
                                            try
                                            {
                                                var rows = _repo.LoadByIsin(h.ISIN);
                                                if (rows != null && rows.Count > 0)
                                                    lastPrice = rows[rows.Count - 1].Price;
                                            }
                                            catch { }

                                            double unrealizedNative = 0.0;
                                            if (!double.IsNaN(avgBuy) && !double.IsNaN(lastPrice) && !double.IsInfinity(lastPrice))
                                            {
                                                unrealizedNative = h.Shares * (lastPrice - avgBuy);
                                            }

                                            try { totalPlEur += converter.ConvertToEur(unrealizedNative, h.Currency ?? "EUR"); } catch { }
                                        }
                                        catch { }
                                    }
                                }
                            }
                        }
                        catch { }

                    try
                    {
                        var db = new TradeMVVM.Trading.Services.DatabaseService();
                        var rounded = Math.Round(totalPlEur, 2);
                        // avoid duplicate inserts: skip if the last stored sample is very recent and has the same rounded value
                        try
                        {
                            var hist = db.LoadTotalPLHistory();
                            if (hist != null && hist.Count > 0)
                            {
                                var last = hist[hist.Count - 1];
                                var lastTime = last.Item1;
                                var lastTotal = last.Item2;
                                if (Math.Abs((DateTime.Now - lastTime).TotalSeconds) < 5 && Math.Abs(lastTotal - rounded) < 0.005)
                                {
                                    // recent identical sample exists — skip insert
                                }
                                else
                                {
                                    db.InsertTotalPLHistory(DateTime.Now, rounded);
                                }
                            }
                            else
                            {
                                db.InsertTotalPLHistory(DateTime.Now, rounded);
                            }
                        }
                        catch { /* best-effort, fall back to direct insert */
                            try { db.InsertTotalPLHistory(DateTime.Now, rounded); } catch { }
                        }
                    }
                    catch { }
                }
                catch { }
            }, token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        catch { }
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
            FireAndForget(Task.Run(async () =>
            {
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var nowUtc = DateTime.UtcNow;
                            hbDb.SetHeartbeat(nowUtc);
                            // also compute and persist Total P/L on every heartbeat (fire-and-forget)
                            try { FireAndForget(ComputeAndInsertTotalPlAsync(stoppingToken)); } catch { }
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
            }, stoppingToken));
        }
        catch (Exception ex)
        {
            try { _logger?.LogWarning(ex, "Failed to start heartbeat background task"); } catch { }
        }

        // Start a background task to compute Total P/L and persist it to TotalPLHistory every 10s.
        try
        {
            FireAndForget(Task.Run(async () =>
            {
                try
                {
                    var settings = new TradeMVVM.Trading.Services.SettingsService();
                    var converter = new TradeMVVM.Trading.DataAnalysis.CurrencyConverter();

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            double totalPlEur = 0.0;
                            try
                            {
                                var csv = settings.HoldingsCsvPath;
                                if (!string.IsNullOrWhiteSpace(csv) && System.IO.File.Exists(csv))
                                {
                                    var holdings = TradeMVVM.Trading.DataAnalysis.HoldingsCalculator.ComputeHoldingsFromCsv(csv);
                                if (holdings != null && holdings.Count > 0)
                                {
                                    var sanitized = PollerHelpers.SanitizeHoldings(holdings, csv);
                                    foreach (var kv in sanitized)
                                    {
                                            try
                                            {
                                                var h = kv.Value;
                                                double avgBuy = h.RemainingBoughtShares > 0 ? (h.RemainingBoughtAmount / h.RemainingBoughtShares) : double.NaN;
                                                double lastPrice = double.NaN;
                                                try
                                                {
                                                    var rows = _repo.LoadByIsin(h.ISIN);
                                                    if (rows != null && rows.Count > 0)
                                                        lastPrice = rows[rows.Count - 1].Price;
                                                }
                                                catch { }

                                                double unrealizedNative = 0.0;
                                                if (!double.IsNaN(avgBuy) && !double.IsNaN(lastPrice) && !double.IsInfinity(lastPrice))
                                                {
                                                    unrealizedNative = h.Shares * (lastPrice - avgBuy);
                                                }

                                                try { totalPlEur += converter.ConvertToEur(unrealizedNative, h.Currency ?? "EUR"); } catch { }
                                            }
                                            catch { }
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                try { _logger?.LogDebug(ex, "Compute TotalPL failed"); } catch { }
                            }

                            try
                            {
                                var db = new TradeMVVM.Trading.Services.DatabaseService();
                                // round to 2 decimal places to match GUI display precision
                                var rounded = Math.Round(totalPlEur, 2);
                                try
                                {
                                    var hist = db.LoadTotalPLHistory();
                                    if (hist != null && hist.Count > 0)
                                    {
                                        var last = hist[hist.Count - 1];
                                        var lastTime = last.Item1;
                                        var lastTotal = last.Item2;
                                        if (Math.Abs((DateTime.Now - lastTime).TotalSeconds) < 5 && Math.Abs(lastTotal - rounded) < 0.005)
                                        {
                                            // recent identical sample exists — skip insert
                                        }
                                        else
                                        {
                                            db.InsertTotalPLHistory(DateTime.Now, rounded);
                                            try { Console.WriteLine($"Poller: Inserted TotalPL {rounded:0.00} at {DateTime.Now:O}"); } catch { }
                                        }
                                    }
                                    else
                                    {
                                        db.InsertTotalPLHistory(DateTime.Now, rounded);
                                        try { Console.WriteLine($"Poller: Inserted TotalPL {rounded:0.00} at {DateTime.Now:O}"); } catch { }
                                    }
                                }
                                catch { try { db.InsertTotalPLHistory(DateTime.Now, rounded); } catch { } }
                            }
                            catch (Exception ex)
                            {
                                try { _logger?.LogWarning(ex, "Failed to insert TotalPLHistory"); } catch { }
                            }
                        }
                        catch (OperationCanceledException) { break; }
                        catch { }

                        try { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    try { _logger?.LogWarning(ex, "TotalPL background task failed"); } catch { }
                }
            }, stoppingToken));
        }
        catch (Exception ex)
        {
            try { _logger?.LogWarning(ex, "Failed to start TotalPL background task"); } catch { }
        }

        // Kick off self-test and full-scan in background so they do not block the main polling loop or heartbeat.
        try
        {
            FireAndForget(Task.Run(async () =>
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
            }, stoppingToken));
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

        // Start a background task that periodically refreshes the stocks list from the DB
        // and updates the shared `stocks` list under lock so the polling service can pick up changes.
        try
        {
                    FireAndForget(Task.Run(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var dbService = new TradeMVVM.Trading.Services.DatabaseService();
                        var allLive = dbService.LoadAll();
                        var newStocks = new List<(string isin, string name, StockType type)>();
                        if (allLive != null && allLive.Count > 0)
                        {
                            var distinctLive = allLive.Select(p => p.ISIN).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                            foreach (var isin in distinctLive)
                            {
                                holdingsMap.TryGetValue(isin, out var nm);
                                newStocks.Add((isin, nm ?? string.Empty, StockType.Aktie));
                            }
                        }

                        if (newStocks.Count > 0)
                        {
                            lock (stocks)
                            {
                                stocks.Clear();
                                foreach (var s in newStocks)
                                    stocks.Add(s);
                            }
                            try { _logger?.LogInformation("Stocks list refreshed from DB: {count} items", newStocks.Count); } catch { }
                        }
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        try { _logger?.LogDebug(ex, "Failed to refresh stocks list"); } catch { }
                    }

                    try { await Task.Delay(TimeSpan.FromSeconds(60), stoppingToken).ConfigureAwait(false); } catch { break; }
                }
            }, stoppingToken));
        }
        catch { }

        //// attempt a best-effort backfill for existing rows where Price != 0 but Percent is missing/zero
        //try
        //{
        //    var dbForBackfill = new TradeMVVM.Trading.Services.DatabaseService();
        //    var updated = dbForBackfill.BackfillPercentWhereZero();
        //    try { _logger.LogInformation("BackfillPercentWhereZero updated {count} rows", updated); } catch { }
        //}
        //catch (Exception ex)
        //{
        //    try { _logger.LogWarning(ex, "BackfillPercentWhereZero failed"); } catch { }
        //}

        try
        {
            // run polling loop using two staggered workers (same pattern as GUI Start)
            const int workerCount = 2;
            var t1 = service.StartAsync(stocks, stoppingToken, workerIndex: 0, workerCount: workerCount);
            FireAndForget(Task.Run(async () =>
            {
                try { await Task.Delay(1500, stoppingToken).ConfigureAwait(false); } catch { }
                if (stoppingToken.IsCancellationRequested) return;
                await service.StartAsync(stocks, stoppingToken, workerIndex: 1, workerCount: workerCount).ConfigureAwait(false);
            }, stoppingToken));
            var t2 = Task.CompletedTask; // ensure t2 exists for Task.WhenAll when FireAndForget used

            await Task.WhenAll(t1, t2).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // expected on shutdown
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "PollerBackgroundService crashed");
        }

        _logger?.LogInformation("PollerBackgroundService stopping");
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
