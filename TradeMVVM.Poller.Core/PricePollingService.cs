using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TradeMVVM.Trading.Data;
using TradeMVVM.Domain;   // <-- für StockPoint
using TradeMVVM.Trading.Services;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Collections.Concurrent;
using System.Globalization;

namespace TradeMVVM.Poller.Core
{
    public class PricePollingServiceCore : IDisposable
    {
        // Raised whenever a full polling cycle over the stock list completed (used as a heartbeat)
        public event Action CycleCompleted;

        private readonly TradeMVVM.Trading.Services.ChartDataProvider _provider;
        // use the application-level PriceRepository to persist points; keep fully-qualified type to avoid ambiguity
        private readonly TradeMVVM.Trading.Data.PriceRepository _repository;
        private readonly TimeSpan _pollInterval;
        private readonly int _maxDegreeOfParallelism;
        private readonly Random _rand;
        // jitter fraction (e.g. 0.5 means +/-50%)
        private readonly double _pollJitterFraction = 0.5;
        // per-ISIN consecutive NaN/invalid-price counter; only log Info after threshold
        private static readonly ConcurrentDictionary<string, int> _nanFailureCounts = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        private readonly TradeMVVM.Trading.Services.DatabaseService _dbService;
        private readonly DatabaseService _coreDbService;
        // Timeout for a single ISIN fetch operation (skip ISIN if exceeded)
        private readonly TimeSpan _perIsinTimeout = TimeSpan.FromSeconds(25);

        public PricePollingServiceCore(
            TradeMVVM.Trading.Services.ChartDataProvider provider,
            TradeMVVM.Trading.Data.PriceRepository repository,
            TimeSpan? pollInterval = null,
            int maxDegreeOfParallelism = 10)
        {
            _provider = provider;
            _repository = repository;
            // default polling interval restored to a less aggressive value
            _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(2000);
            _rand = new Random();
            _maxDegreeOfParallelism = Math.Max(1, maxDegreeOfParallelism);
            try { _dbService = (TradeMVVM.Trading.App.Services?.GetService(typeof(TradeMVVM.Trading.Services.DatabaseService)) as TradeMVVM.Trading.Services.DatabaseService) ?? new TradeMVVM.Trading.Services.DatabaseService(); } catch { _dbService = new TradeMVVM.Trading.Services.DatabaseService(); }
            try { _coreDbService = new DatabaseService(); } catch { _coreDbService = null!; }

            // load persisted nan counts
            try
            {
                var persisted = _dbService.LoadFailureCounts("price_nan");
                foreach (var kv in persisted)
                {
                    _nanFailureCounts.AddOrUpdate(kv.Key, kv.Value, (_, __) => kv.Value);
                }
            }
            catch { }
        }
        public void Dispose()
        {
            // nothing to dispose for ChartDataProvider
        }

        public async Task StartAsync(
         List<(string isin, string name, StockType type)> stocks,
         CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    // take a snapshot of the stocks list under lock so it is consistent
                    // even when UpdateStocksFromHoldings rebuilds it on the UI thread
                    List<(string isin, string name, StockType type)> snapshot;
                    lock (stocks)
                    {
                        snapshot = stocks.ToList();
                    }
                    var knockouts = snapshot.Where(s => s.type == StockType.Knockout).ToList();
                    var others = snapshot.Where(s => s.type != StockType.Knockout).ToList();
                    var currentStocks = new List<(string isin, string name, StockType type)>();
                    int idx = 0;
                    while (idx < others.Count || idx < knockouts.Count)
                    {
                        if (idx < others.Count) currentStocks.Add(others[idx]);
                        if (idx < knockouts.Count) currentStocks.Add(knockouts[idx]);
                        idx++;
                    }

                    var concurrency = new SemaphoreSlim(_maxDegreeOfParallelism);
                    var tasks = new List<Task>();

                    foreach (var stock in currentStocks)
                    {
                        await concurrency.WaitAsync(token);

                        var s = stock;
                    tasks.Add(Task.Run(async () =>
                        {
                            try
                            {
                                // Try fetching with retries and exponential backoff
                                // pass only the outer polling token so cancellation means real app-stop
                                const int maxAttempts = 3;
                                TimeSpan baseDelay = TimeSpan.FromMilliseconds(500);
                                (double price, double percent, string provider, DateTime? providerTime) result = (0, 0, "", null);
                                bool success = false;

                                for (int attempt = 1; attempt <= maxAttempts && !token.IsCancellationRequested; attempt++)
                                {
                                    try
                                    {

                                        // pass through name when available
                                        var name = s.name;
                                        try { Console.WriteLine($"PricePolling: fetching {s.isin} (name='{name}') attempt {attempt}"); } catch { }
                                        result = await _provider.DataProvider(s.isin, s.type, name, token);
                                        try { Console.WriteLine($"PricePolling: received {s.isin} -> price={result.price} percent={result.percent} provider={result.provider}"); } catch { }
                                        success = true;
                                        break;
                                    }
                                    catch (OperationCanceledException)
                                    {
                                        if (token.IsCancellationRequested)
                                        {
                                            success = false;
                                            break;
                                        }

                                        if (attempt < maxAttempts)
                                        {
                                            try { await Task.Delay(TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1)), token); } catch { }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        // log full exception (includes message and any attempted URLs added by provider)
                                        Debug.WriteLine($"PricePolling: attempt {attempt} failed for {s.isin}: {ex}");
                                        if (attempt < maxAttempts)
                                        {
                                            try { await Task.Delay(TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1)), token); } catch { }
                                        }
                                    }
                                }

                                double percent = result.percent;
                                double price = result.price;
                                var providerName = result.provider;
                                DateTime? providerTime = result.providerTime;

                                if (!success)
                                {
                                    return;
                                }

                                // If price is not available, skip storing and continue
                                if (double.IsNaN(price) || double.IsInfinity(price))
                                {
                                    // increment per-ISIN NaN counter and only log Info when threshold is reached
                                    var newCount = _nanFailureCounts.AddOrUpdate(s.isin, 1, (_, old) => old + 1);
                                    try { _dbService.SetFailureCount(s.isin, newCount, "price_nan"); } catch { }
                                    var settings = TradeMVVM.Trading.App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                                    var threshold = settings?.PriceNanFailureThreshold ?? 5;
                                    if (newCount >= threshold)
                                    {
                                        try
                                        {
                                            Trace.TraceInformation($"PricePolling: price NaN for {s.isin}, skipping this cycle with price: {price} (attempt {newCount}/{threshold})");
                                        }
                                        catch { }
                                        // reset after logging so we only log again after another run of failures
                                        _nanFailureCounts.TryRemove(s.isin, out _);
                                        try { _dbService.DeleteFailureCount(s.isin, "price_nan"); } catch { }
                                    }
                                    return;
                                }

                                // successful numeric price -> reset any NaN failure counter for this ISIN
                                _nanFailureCounts.TryRemove(s.isin, out _);
                                try { _dbService.DeleteFailureCount(s.isin, "price_nan"); } catch { }

                                // Wenn Prozent fehlt, ungültig oder 0 obwohl sich der Preis geändert hat:
                                // berechne aus letztem DB-Preis oder setze 0
                                if (double.IsNaN(percent) || double.IsInfinity(percent) || percent == 0.0)
                                {
                                    try
                                    {
                                        var dbRows = _repository.LoadByIsin(s.isin);
                                        if (dbRows != null && dbRows.Count > 0)
                                        {
                                            var lastDbPrice = dbRows[dbRows.Count - 1].Price;
                                            if (lastDbPrice != 0 && !double.IsNaN(price) && !double.IsInfinity(price) && Math.Abs(price - lastDbPrice) > 1e-9)
                                            {
                                                percent = (price - lastDbPrice) / lastDbPrice * 100.0;
                                            }
                                            else
                                            {
                                                // leave as 0.0 when no previous price or no meaningful change
                                                percent = 0.0;
                                            }
                                        }
                                        else
                                        {
                                            percent = 0.0;
                                        }
                                    }
                                    catch
                                    {
                                        percent = 0.0;
                                    }
                                }

                                // compute prediction using recent DB history (moving average)
                                string forecastStr = string.Empty;
                                double predictedPrice = 0.0;
                                try
                                {
                                    var settings = TradeMVVM.Trading.App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                                    int window = settings?.PredictionWindowSize ?? 20;
                                    double threshold = settings?.PredictionThresholdPercent ?? 1.0;
                                    var method = settings?.PredictionMethod ?? "MovingAverage";

                                    if (string.Equals(method, "MovingAverage", StringComparison.OrdinalIgnoreCase))
                                    {
                                        try
                                        {
                                            var past = _repository.LoadByIsin(s.isin);
                                            if (past != null && past.Count >= 1)
                                            {
                                                var recentPrices = past.OrderByDescending(p => p.Time).Take(window).Select(p => p.Price).Where(v => !double.IsNaN(v) && !double.IsInfinity(v)).ToList();
                                                if (recentPrices.Count > 0)
                                                {
                                                    predictedPrice = recentPrices.Average();
                                                    // compute deviation in percent
                                                    double dev = predictedPrice != 0.0 ? (price - predictedPrice) / predictedPrice * 100.0 : 0.0;
                                                    if (Math.Abs(dev) >= threshold)
                                                    {
                                                        forecastStr = dev > 0 ? "Up" : "Down";
                                                    }
                                                    else
                                                    {
                                                        forecastStr = "Neutral";
                                                    }
                                                }
                                            }
                                        }
                                        catch { }
                                    }
                                    else if (string.Equals(method, "ML", StringComparison.OrdinalIgnoreCase))
                                    {
                                        // Placeholder ML: simple linear regression extrapolation for next point
                                        try
                                        {
                                            var past = _repository.LoadByIsin(s.isin);
                                            if (past != null && past.Count >= 3)
                                            {
                                                var recent = past.OrderByDescending(p => p.Time).Take(window).OrderBy(p => p.Time).Select(p => p.Price).Where(v => !double.IsNaN(v) && !double.IsInfinity(v)).ToList();
                                                int n = recent.Count;
                                                if (n >= 3)
                                                {
                                                    double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
                                                    for (int i = 0; i < n; i++)
                                                    {
                                                        double x = i;
                                                        double y = recent[i];
                                                        sumX += x; sumY += y; sumXY += x * y; sumXX += x * x;
                                                    }
                                                    double denom = (n * sumXX - sumX * sumX);
                                                    if (Math.Abs(denom) > 1e-9)
                                                    {
                                                        double slope = (n * sumXY - sumX * sumY) / denom;
                                                        double last = recent[n - 1];
                                                        // predict one step ahead
                                                        predictedPrice = last + slope;
                                                        double dev = predictedPrice != 0.0 ? (price - predictedPrice) / predictedPrice * 100.0 : 0.0;
                                                        if (Math.Abs(dev) >= threshold)
                                                            forecastStr = dev > 0 ? "Up" : "Down";
                                                        else
                                                            forecastStr = "Neutral";
                                                    }
                                                }
                                            }
                                        }
                                        catch { }
                                    }
                                }
                                catch { }

                                var point = new StockPoint
                                {
                                    ISIN = s.isin,
                                    // store repository time as the local insertion time; keep provider-supplied timestamp
                                    Time = DateTime.Now,
                                    Price = price,
                                    Percent = percent,
                                    Provider = providerName,
                                    ProviderTime = providerTime,
                                    Forecast = forecastStr,
                                    PredictedPrice = predictedPrice
                                };

                                try
                                {
                                    _repository.Insert(point, providerName);
                                }
                                catch (Exception ex)
                                {
                                    Debug.WriteLine($"PricePolling: failed to insert point for {s.isin}: {ex.Message}");
                                }

                                // Update heartbeat more frequently so GUI shows activity even during long cycles
                                try
                                {
                                    _coreDbService?.SetHeartbeat(DateTime.UtcNow);
                                }
                                catch { }

                                // successful retrieval and store
                            }
                            catch (Exception ex)
                            {
                                System.Diagnostics.Debug.WriteLine($"Fehler beim Abrufen von {s.isin}: {ex.Message}");
                            }
                            finally
                            {
                                concurrency.Release();
                            }
                        }, token));
                    }

                    try
                    {
                        await Task.WhenAll(tasks);
                    }
                    catch (OperationCanceledException)
                    {
                        // cancellation requested -> proceed to outer loop check
                    }
                    finally
                    {
                        // heartbeat after a full polling cycle completed
                        try { CycleCompleted?.Invoke(); } catch { }
                    }
                }
                catch (OperationCanceledException)
                {
                    // stop requested -> break outer loop
                    break;
                }
                catch (Exception exOuter)
                {
                    // unexpected error in polling loop: reset driver and pause before continuing
                    System.Diagnostics.Debug.WriteLine($"PricePolling: unexpected error in polling loop: {exOuter.Message}");
                    try { await Task.Delay(5000, token); } catch { }
                }

                // write heartbeat and check DB control flag
                try
                {
                    try
                    {
                        // check DB control flag first; if disabled, do not write heartbeat and sleep briefly
                        if (_coreDbService != null && !_coreDbService.IsPollingEnabled())
                        {
                            try { Console.WriteLine($"Poller paused by DB flag at {DateTime.UtcNow:O}"); } catch { }
                            try { await Task.Delay(TimeSpan.FromSeconds(5), token); } catch { }
                            continue;
                        }

                        _coreDbService?.SetHeartbeat(DateTime.UtcNow);
                        try { Console.WriteLine($"Poller heartbeat: {DateTime.UtcNow:O}"); } catch { }
                    }
                    catch { }

                    // add randomized jitter to avoid synchronized polling patterns
                    try
                    {
                        var baseMs = _pollInterval.TotalMilliseconds;
                        var jitter = (_rand.NextDouble() * 2.0 - 1.0) * _pollJitterFraction * baseMs; // +/- fraction
                        var delayMs = Math.Max(100, baseMs + jitter);
                        await Task.Delay(TimeSpan.FromMilliseconds(delayMs), token);
                    }
                    catch
                    {
                        await Task.Delay(_pollInterval, token);
                    }
                }
                catch (TaskCanceledException)
                {
                    // cancellation requested -> exit loop
                    break;
                }
            }

        }

    }
}
