using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TradeMVVM.Domain;   // <-- für StockPoint
using TradeMVVM.Trading.Data;
using TradeMVVM.Trading.Services;

public class PricePollingService : IDisposable
{
    // Raised whenever a full polling cycle over the stock list completed (used as a heartbeat)
    public event Action CycleCompleted;
    // Raised whenever an ISIN is attempted in polling (before provider call)
    public event Action<string> PollAttempted;

    private readonly ChartDataProvider _provider;
    private readonly PriceRepository _repository;
    private readonly TimeSpan _pollInterval;
    // per-ISIN consecutive NaN/invalid-price counter; only log Info after threshold
    private static readonly ConcurrentDictionary<string, int> _nanFailureCounts = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    private readonly TradeMVVM.Trading.Services.DatabaseService _dbService;
    // Timeout for a single ISIN fetch operation (skip ISIN if exceeded)
    private readonly TimeSpan _perIsinTimeout = TimeSpan.FromSeconds(25);

    public PricePollingService(
        ChartDataProvider provider,
        PriceRepository repository,
        TimeSpan? pollInterval = null,
        int maxDegreeOfParallelism = 10)
    {
        _provider = provider;
        _repository = repository;
        // default polling interval restored to a less aggressive value
        _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(3000);
        try { _dbService = TradeMVVM.Trading.App.Services?.GetService(typeof(TradeMVVM.Trading.Services.DatabaseService)) as TradeMVVM.Trading.Services.DatabaseService ?? new TradeMVVM.Trading.Services.DatabaseService(); } catch { _dbService = new TradeMVVM.Trading.Services.DatabaseService(); }

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
     List<(string isin, string name, TradeMVVM.Domain.StockType type)> stocks,
     CancellationToken token,
     int workerIndex = 0,
     int workerCount = 1)
    {
        if (workerCount <= 0) workerCount = 1;
        if (workerIndex < 0) workerIndex = 0;
        if (workerIndex >= workerCount) workerIndex = workerCount - 1;

        while (!token.IsCancellationRequested)
        {
            try
            {
                // simple deterministic cycle: one snapshot, then process all entries in order
                List<(string isin, string name, StockType type)> snapshot;
                lock (stocks)
                {
                    snapshot = stocks
                        .Select(x => (isin: NormalizeIsin(x.isin), name: x.name, type: x.type))
                        .Where(x => !string.IsNullOrWhiteSpace(x.isin))
                        .ToList();
                }

                var currentStocks = workerCount > 1
                    ? snapshot.Where((x, i) => (i % workerCount) == workerIndex).ToList()
                    : snapshot;

                try { Debug.WriteLine($"PricePolling: cycle start — {currentStocks.Count} ISINs: {string.Join(", ", currentStocks.Select(x => x.isin))}"); } catch { }

                foreach (var s in currentStocks)
                {
                    await PollSingleStockAsync(s, token);
                }

                // heartbeat after a full polling cycle completed
                try { CycleCompleted?.Invoke(); } catch { }
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

            try
            {
                await Task.Delay(_pollInterval, token);
            }
            catch (TaskCanceledException) { break; }
        }

    }

    private static string NormalizeIsin(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var chars = value.Where(char.IsLetterOrDigit).ToArray();
        return new string(chars).ToUpperInvariant();
    }

    private async Task PollSingleStockAsync((string isin, string name, StockType type) s, CancellationToken token)
    {
        try
        {
            try { PollAttempted?.Invoke(s.isin); } catch { }
            (double price, double percent, string provider, DateTime? providerTime) result = (0, 0, "", null);
            bool success = false;

            try
            {
                using var perIsinCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                perIsinCts.CancelAfter(_perIsinTimeout);

                // one deterministic provider run per cycle (preferred provider first + fallback inside DataProvider)
                var name = s.name;
                result = await _provider.DataProvider(s.isin, s.type, name, perIsinCts.Token);
                success = true;
            }
            catch (OperationCanceledException)
            {
                if (token.IsCancellationRequested)
                    return;

                Debug.WriteLine($"PricePolling: timeout after {_perIsinTimeout.TotalSeconds}s for {s.isin}");
                return;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"PricePolling: failed for {s.isin}: {ex}");
                return;
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
                }
                else
                {
                    // emit only a brief debug line with attempt count (not visible in release traces)
                    Debug.WriteLine($"PricePolling: price NaN for {s.isin} (attempt {newCount}/{threshold})");
                }
                return;
            }

            // successful numeric price -> reset any NaN failure counter for this ISIN
            _nanFailureCounts.TryRemove(s.isin, out _);

            // Wenn Prozent fehlt oder ungültig: berechne aus letztem DB-Preis oder setze 0
            if (double.IsNaN(percent) || double.IsInfinity(percent))
            {
                try
                {
                    var dbRows = _repository.LoadByIsin(s.isin);
                    if (dbRows != null && dbRows.Count > 0)
                    {
                        var lastDbPrice = dbRows[dbRows.Count - 1].Price;
                        if (lastDbPrice != 0 && !double.IsNaN(price) && !double.IsInfinity(price))
                            percent = (price - lastDbPrice) / lastDbPrice * 100.0;
                        else
                            percent = 0.0;
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

            // StockPoint erzeugen und persistieren
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
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"Fehler beim Abrufen von {s.isin}: {ex.Message}");
        }
    }

}
