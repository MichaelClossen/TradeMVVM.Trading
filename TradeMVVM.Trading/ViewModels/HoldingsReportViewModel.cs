using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Windows.Input;
using TradeMVVM.Trading.DataAnalysis;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using TradeMVVM.Trading.Data;
using TradeMVVM.Domain;
using System.Data.SQLite;

namespace TradeMVVM.Trading.ViewModels
{
    public class HoldingsRow : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;

        public string ISIN { get; set; }
        // Forecast for next 5 days (computed from recent price history)
        public ForecastDirection Forecast { get; set; }
        public DateTime? LastTradeTime { get; set; }
        public DateTime? ProviderTime { get; set; }
        public string Name { get; set; }
        public string Currency { get; set; }
        public double Shares { get; set; }
        public double LastPrice { get; set; }
        public double MarketValueEur { get; set; }
        public double TotalBoughtShares { get; set; }
        public double AvgBuyPrice { get; set; }
        public double TotalSoldShares { get; set; }
        public double AvgSellPrice { get; set; }
        public double TotalTaxes { get; set; }
        public double RealizedPL { get; set; }
        public double UnrealizedPL { get; set; }
        public double TotalPL { get; set; }
        // new columns: native market value and P/L columns (to be filled)
        public double MarketValue { get; set; }
        public double PLPercent { get; set; }
        private double _alertThresholdPercent = 1.0;
        public double AlertThresholdPercent
        {
            get => _alertThresholdPercent;
            set
            {
                if (Math.Abs(_alertThresholdPercent - value) < 1e-9) return;
                _alertThresholdPercent = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(AlertThresholdPercent)));
            }
        }
        private double _trailingStopPercent = 0.0;
        public double TrailingStopPercent
        {
            get => _trailingStopPercent;
            set
            {
                if (Math.Abs(_trailingStopPercent - value) < 1e-9) return;
                _trailingStopPercent = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(TrailingStopPercent)));
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsTrailingStopBreached)));
            }
        }
        private double _trailingStopConfiguredPercent = 0.0;
        public double TrailingStopConfiguredPercent
        {
            get => _trailingStopConfiguredPercent;
            set
            {
                if (Math.Abs(_trailingStopConfiguredPercent - value) < 1e-9) return;
                _trailingStopConfiguredPercent = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(TrailingStopConfiguredPercent)));
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsTrailingStopBreached)));
            }
        }
        public bool IsTrailingStopBreached =>
            TrailingStopConfiguredPercent > 0 &&
            !double.IsNaN(TrailingStopPercent) &&
            !double.IsInfinity(TrailingStopPercent) &&
            TrailingStopPercent <= 0;
        public double PLAmount { get; set; }
        // current market price (to be filled later)
        public double CurrentPrice { get; set; }
        // flag used by the view to hide total rows if present
        public bool IsTotal { get; set; }

        // highlight flag: true for 5 seconds after the row was updated with new DB data
        private bool _isRecentlyUpdated;
        private System.Windows.Threading.DispatcherTimer _highlightTimer;
        // highlight flag: true for 5 seconds after a polling attempt (independent of DB update)
        private bool _isRecentlyPolled;
        private System.Windows.Threading.DispatcherTimer _pollHighlightTimer;

        public bool IsRecentlyUpdated
        {
            get => _isRecentlyUpdated;
            set
            {
                if (_isRecentlyUpdated == value) return;
                _isRecentlyUpdated = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsRecentlyUpdated)));

                if (value)
                {
                    if (_highlightTimer == null)
                    {
                        _highlightTimer = new System.Windows.Threading.DispatcherTimer
                        {
                            Interval = TimeSpan.FromSeconds(1)
                        };
                        _highlightTimer.Tick += (s, e) =>
                        {
                            _highlightTimer.Stop();
                            IsRecentlyUpdated = false;
                        };
                    }
                    _highlightTimer.Stop();
                    _highlightTimer.Start();
                }
            }
        }

        public bool IsRecentlyPolled
        {
            get => _isRecentlyPolled;
            set
            {
                if (_isRecentlyPolled == value) return;
                _isRecentlyPolled = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsRecentlyPolled)));

                if (value)
                {
                    if (_pollHighlightTimer == null)
                    {
                        _pollHighlightTimer = new System.Windows.Threading.DispatcherTimer
                        {
                            Interval = TimeSpan.FromSeconds(1)
                        };
                        _pollHighlightTimer.Tick += (s, e) =>
                        {
                            _pollHighlightTimer.Stop();
                            IsRecentlyPolled = false;
                        };
                    }
                    _pollHighlightTimer.Stop();
                    _pollHighlightTimer.Start();
                }
            }
        }
    }
    public enum ForecastDirection
    {
        Neutral,
        Up,
        Down
    }
    public class HoldingsReportViewModel : BaseViewModel
    {
        private Timer _forecastTimer;
        // path to last backup created by delete operations (used for undo)
        private string _lastBackupPath;

        // event fired after Refresh() completes with the current source holdings
        public event Action<List<Holding>> HoldingsUpdated;

        private string _csvPath;
        private List<Holding> _source;
        private readonly Dictionary<string, double> _isinAlertThresholds = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, double> _isinTrailingStopThresholds = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, double> _isinTrailingStopCurrentThresholds = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, double> _isinTrailingStopLastPlPercents = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, DateTime> _isinTrailingStopSetAtUtc = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private readonly object _isinThresholdLock = new object();
        private double _defaultAlertThresholdPercent = 1.0;
        private bool _trailingStateDirty = false;
        private DateTime _lastTrailingStateSavedUtc = DateTime.MinValue;
        private static readonly TimeSpan TrailingStateSaveInterval = TimeSpan.FromSeconds(10);

        public ObservableCollection<HoldingsRow> Holdings { get; }
        private ICollectionView _holdingsView;
        public ICollectionView HoldingsView => _holdingsView;
        private string _searchText;
        public string SearchText
        {
            get => _searchText;
            set
            {
                _searchText = value;
                OnPropertyChanged();
                ApplyFilter();
            }
        }
        public ICommand RefreshCommand { get; }
        public ICommand ExportCommand { get; }

        private double _totalRealizedPl;
        private double _totalUnrealizedPl;
        private double _totalPl;
        private double _totalTaxes;
        private double _totalGains;
        private double _totalLosses;

        public double TotalRealizedPL { get => _totalRealizedPl; private set { _totalRealizedPl = value; OnPropertyChanged(); } }
        public double TotalUnrealizedPL { get => _totalUnrealizedPl; private set { _totalUnrealizedPl = value; OnPropertyChanged(); } }
        public double TotalPL { get => _totalPl; private set { _totalPl = value; OnPropertyChanged(); } }
        public double TotalTaxes { get => _totalTaxes; private set { _totalTaxes = value; OnPropertyChanged(); } }
        public double TotalGains { get => _totalGains; private set { _totalGains = value; OnPropertyChanged(); } }
        public double TotalLosses { get => _totalLosses; private set { _totalLosses = value; OnPropertyChanged(); } }

        public HoldingsReportViewModel(List<Holding> source, string csvPath)
        {
            _source = source ?? new List<Holding>();
            _csvPath = csvPath;

            try { LoadIsinAlertThresholds(); } catch { }

            // localize headers/labels if needed in future

            Holdings = new ObservableCollection<HoldingsRow>();
            _holdingsView = CollectionViewSource.GetDefaultView(Holdings);
            _holdingsView.Filter = null;
            RefreshCommand = new RelayCommand(Refresh);
            ExportCommand = new RelayCommand(ExportCsv);

            Refresh();

            // start periodic forecast updates (every 5 minutes)
            try
            {
                _forecastTimer = new Timer(_ => Application.Current?.Dispatcher?.BeginInvoke((Action)Refresh), null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
            }
            catch { }

            // start periodic snapshot timer for TotalPL (every minute)
            try
            {
                var snapshotTimer = new System.Threading.Timer(_ =>
                {
                    try
                    {
                        // write snapshot directly to DB to avoid cross-assembly DatabaseService resolution issues
                        try
                        {
                            using (var conn = new SQLiteConnection("Data Source=trading.db"))
                            {
                                conn.Open();
                                var cmd = new SQLiteCommand("INSERT INTO TotalPLHistory (Time, TotalPL) VALUES (@time, @total);", conn);
                                cmd.Parameters.AddWithValue("@time", DateTime.UtcNow);
                                cmd.Parameters.AddWithValue("@total", TotalPL);
                                cmd.ExecuteNonQuery();
                            }
                        }
                        catch { }
                    }
                    catch { }
                }, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            }
            catch { }
        }

        private (double configured, double current) GetTrailingStopValues(string isin)
        {
            try
            {
                var key = NormalizeIsin(isin);
                if (string.IsNullOrWhiteSpace(key))
                    return (0.0, 0.0);

                lock (_isinThresholdLock)
                {
                    _isinTrailingStopThresholds.TryGetValue(key, out var configured);
                    if (configured < 0 || double.IsNaN(configured) || double.IsInfinity(configured))
                        configured = 0.0;

                    var current = configured;
                    if (_isinTrailingStopCurrentThresholds.TryGetValue(key, out var currentSaved)
                        && !double.IsNaN(currentSaved) && !double.IsInfinity(currentSaved))
                    {
                        current = currentSaved;
                    }

                    if (configured <= 0)
                        return (0.0, 0.0);

                    if (current > configured)
                        current = configured;

                    return (configured, current);
                }
            }
            catch { }

            return (0.0, 0.0);
        }

        private void ResetTrailingStopState(string isin, double configured, DateTime setAtUtc)
        {
            try
            {
                var key = NormalizeIsin(isin);
                if (string.IsNullOrWhiteSpace(key))
                    return;

                lock (_isinThresholdLock)
                {
                    if (configured <= 0)
                    {
                        _isinTrailingStopCurrentThresholds.Remove(key);
                        _isinTrailingStopLastPlPercents.Remove(key);
                        _isinTrailingStopSetAtUtc.Remove(key);
                    }
                    else
                    {
                        _isinTrailingStopCurrentThresholds[key] = configured;
                        _isinTrailingStopLastPlPercents.Remove(key);
                        _isinTrailingStopSetAtUtc[key] = setAtUtc;
                    }

                    _trailingStateDirty = true;
                }
            }
            catch { }
        }

        private double UpdateTrailingStopCurrent(string isin, double latestPlPercent, double configured)
        {
            try
            {
                var key = NormalizeIsin(isin);
                if (string.IsNullOrWhiteSpace(key) || configured <= 0)
                    return 0.0;

                lock (_isinThresholdLock)
                {
                    if (!_isinTrailingStopCurrentThresholds.TryGetValue(key, out var current)
                        || double.IsNaN(current) || double.IsInfinity(current))
                    {
                        current = configured;
                        _isinTrailingStopCurrentThresholds[key] = current;
                        if (!_isinTrailingStopSetAtUtc.ContainsKey(key))
                            _isinTrailingStopSetAtUtc[key] = DateTime.UtcNow;
                        _trailingStateDirty = true;
                    }

                    if (current > configured)
                    {
                        current = configured;
                        _isinTrailingStopCurrentThresholds[key] = current;
                        _isinTrailingStopSetAtUtc[key] = DateTime.UtcNow;
                        _trailingStateDirty = true;
                    }

                    if (double.IsNaN(latestPlPercent) || double.IsInfinity(latestPlPercent))
                        return current;

                    if (_isinTrailingStopLastPlPercents.TryGetValue(key, out var last)
                        && !double.IsNaN(last) && !double.IsInfinity(last))
                    {
                        var delta = latestPlPercent - last;
                        if (Math.Abs(delta) > 1e-9)
                        {
                            if (delta < 0)
                                current = current + delta;
                            else
                                current = Math.Min(configured, current + delta);

                            _isinTrailingStopCurrentThresholds[key] = current;
                            _isinTrailingStopSetAtUtc[key] = DateTime.UtcNow;
                            _trailingStateDirty = true;
                        }
                    }

                    if (!_isinTrailingStopLastPlPercents.TryGetValue(key, out var previousLast)
                        || Math.Abs(previousLast - latestPlPercent) > 1e-9)
                    {
                        _isinTrailingStopLastPlPercents[key] = latestPlPercent;
                        _trailingStateDirty = true;
                    }
                    return current;
                }
            }
            catch { }

            return 0.0;
        }

        private void SaveTrailingStopState(bool force)
        {
            try
            {
                if (!force)
                {
                    if (!_trailingStateDirty)
                        return;
                    if ((DateTime.UtcNow - _lastTrailingStateSavedUtc) < TrailingStateSaveInterval)
                        return;
                }

                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings == null)
                    return;

                lock (_isinThresholdLock)
                {
                    settings.IsinTrailingStopCurrentPercentThresholds = new Dictionary<string, double>(_isinTrailingStopCurrentThresholds, StringComparer.OrdinalIgnoreCase);
                    settings.IsinTrailingStopLastPlPercents = new Dictionary<string, double>(_isinTrailingStopLastPlPercents, StringComparer.OrdinalIgnoreCase);
                    settings.IsinTrailingStopSetAtUtc = new Dictionary<string, DateTime>(_isinTrailingStopSetAtUtc, StringComparer.OrdinalIgnoreCase);
                }

                settings.Save();
                _trailingStateDirty = false;
                _lastTrailingStateSavedUtc = DateTime.UtcNow;
            }
            catch { }
        }

        // expose internal source holdings for other UI components (read-only)
        public List<Holding> GetSourceHoldings()
        {
            return _source ?? new List<Holding>();
        }

        private static string NormalizeIsin(string isin)
        {
            return (isin ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
        }

        private void LoadIsinAlertThresholds()
        {
            try
            {
                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings == null)
                    return;

                if (settings.AlertPricePercentThreshold > 0)
                    _defaultAlertThresholdPercent = settings.AlertPricePercentThreshold;

                lock (_isinThresholdLock)
                {
                    _isinAlertThresholds.Clear();
                    if (settings.IsinAlertPercentThresholds != null)
                    {
                        foreach (var kv in settings.IsinAlertPercentThresholds)
                        {
                            var key = NormalizeIsin(kv.Key);
                            if (string.IsNullOrWhiteSpace(key) || kv.Value < 0)
                                continue;
                            _isinAlertThresholds[key] = kv.Value;
                        }
                    }

                    _isinTrailingStopThresholds.Clear();
                    if (settings.IsinTrailingStopPercentThresholds != null)
                    {
                        foreach (var kv in settings.IsinTrailingStopPercentThresholds)
                        {
                            var key = NormalizeIsin(kv.Key);
                            if (string.IsNullOrWhiteSpace(key) || kv.Value < 0)
                                continue;
                            _isinTrailingStopThresholds[key] = kv.Value;
                        }
                    }

                    _isinTrailingStopCurrentThresholds.Clear();
                    if (settings.IsinTrailingStopCurrentPercentThresholds != null)
                    {
                        foreach (var kv in settings.IsinTrailingStopCurrentPercentThresholds)
                        {
                            var key = NormalizeIsin(kv.Key);
                            if (string.IsNullOrWhiteSpace(key) || double.IsNaN(kv.Value) || double.IsInfinity(kv.Value))
                                continue;
                            _isinTrailingStopCurrentThresholds[key] = kv.Value;
                        }
                    }

                    _isinTrailingStopLastPlPercents.Clear();
                    if (settings.IsinTrailingStopLastPlPercents != null)
                    {
                        foreach (var kv in settings.IsinTrailingStopLastPlPercents)
                        {
                            var key = NormalizeIsin(kv.Key);
                            if (string.IsNullOrWhiteSpace(key) || double.IsNaN(kv.Value) || double.IsInfinity(kv.Value))
                                continue;
                            _isinTrailingStopLastPlPercents[key] = kv.Value;
                        }
                    }

                    _isinTrailingStopSetAtUtc.Clear();
                    if (settings.IsinTrailingStopSetAtUtc != null)
                    {
                        foreach (var kv in settings.IsinTrailingStopSetAtUtc)
                        {
                            var key = NormalizeIsin(kv.Key);
                            if (string.IsNullOrWhiteSpace(key) || kv.Value == default(DateTime))
                                continue;
                            _isinTrailingStopSetAtUtc[key] = DateTime.SpecifyKind(kv.Value, DateTimeKind.Utc);
                        }
                    }
                }
            }
            catch { }
        }

        private double GetIsinAlertThreshold(string isin)
        {
            try
            {
                var key = NormalizeIsin(isin);
                if (string.IsNullOrWhiteSpace(key))
                    return _defaultAlertThresholdPercent;

                lock (_isinThresholdLock)
                {
                    if (_isinAlertThresholds.TryGetValue(key, out var value) && value >= 0)
                        return value;
                }
            }
            catch { }

            return _defaultAlertThresholdPercent;
        }

        private void PersistIsinAlertThreshold(string isin, double threshold)
        {
            try
            {
                var key = NormalizeIsin(isin);
                if (string.IsNullOrWhiteSpace(key) || threshold < 0)
                    return;

                lock (_isinThresholdLock)
                {
                    _isinAlertThresholds[key] = threshold;
                }

                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings != null)
                {
                    lock (_isinThresholdLock)
                    {
                        settings.IsinAlertPercentThresholds = new Dictionary<string, double>(_isinAlertThresholds, StringComparer.OrdinalIgnoreCase);
                    }
                    settings.Save();
                }
            }
            catch { }
        }

        private double GetIsinTrailingStopThreshold(string isin)
        {
            try
            {
                var key = NormalizeIsin(isin);
                if (string.IsNullOrWhiteSpace(key))
                    return 0.0;

                lock (_isinThresholdLock)
                {
                    if (_isinTrailingStopThresholds.TryGetValue(key, out var value) && value >= 0)
                        return value;
                }
            }
            catch { }

            return 0.0;
        }

        private void PersistIsinTrailingStopThreshold(string isin, double threshold)
        {
            try
            {
                var key = NormalizeIsin(isin);
                if (string.IsNullOrWhiteSpace(key) || threshold < 0)
                    return;

                lock (_isinThresholdLock)
                {
                    _isinTrailingStopThresholds[key] = threshold;
                }

                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings != null)
                {
                    lock (_isinThresholdLock)
                    {
                        settings.IsinTrailingStopPercentThresholds = new Dictionary<string, double>(_isinTrailingStopThresholds, StringComparer.OrdinalIgnoreCase);
                    }
                    settings.Save();
                }
            }
            catch { }
        }

        private void AttachThresholdHandler(HoldingsRow row)
        {
            if (row == null)
                return;

            row.PropertyChanged += (s, e) =>
            {
                try
                {
                    if (!string.Equals(e.PropertyName, nameof(HoldingsRow.AlertThresholdPercent), StringComparison.Ordinal))
                    {
                        if (!string.Equals(e.PropertyName, nameof(HoldingsRow.TrailingStopPercent), StringComparison.Ordinal))
                            return;
                    }

                    var r = s as HoldingsRow;
                    if (r == null)
                        return;

                    if (string.Equals(e.PropertyName, nameof(HoldingsRow.AlertThresholdPercent), StringComparison.Ordinal))
                    {
                        if (double.IsNaN(r.AlertThresholdPercent) || double.IsInfinity(r.AlertThresholdPercent) || r.AlertThresholdPercent < 0)
                            return;

                        PersistIsinAlertThreshold(r.ISIN, r.AlertThresholdPercent);
                    }

                    if (string.Equals(e.PropertyName, nameof(HoldingsRow.TrailingStopPercent), StringComparison.Ordinal))
                    {
                        if (double.IsNaN(r.TrailingStopPercent) || double.IsInfinity(r.TrailingStopPercent) || r.TrailingStopPercent < 0)
                            return;

                        PersistIsinTrailingStopThreshold(r.ISIN, r.TrailingStopPercent);
                        ResetTrailingStopState(r.ISIN, r.TrailingStopPercent, DateTime.UtcNow);
                        r.TrailingStopConfiguredPercent = r.TrailingStopPercent;
                        SaveTrailingStopState(force: true);
                    }
                }
                catch { }
            };
        }

        public void MarkPolled(IEnumerable<string> isins)
        {
            if (isins == null)
                return;

            var set = new HashSet<string>(
                isins
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Select(x => x.Replace("\u00A0", string.Empty).Trim().ToUpperInvariant())
                    .Where(x => !string.IsNullOrWhiteSpace(x)),
                StringComparer.OrdinalIgnoreCase);
            if (set.Count == 0)
                return;

            foreach (var row in Holdings)
            {
                var normalizedRowIsin = (row.ISIN ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                if (!string.IsNullOrWhiteSpace(normalizedRowIsin) && set.Contains(normalizedRowIsin))
                    row.IsRecentlyPolled = true;
            }
        }

        public void SetAllAlertThresholdPercent(double threshold)
        {
            try
            {
                if (double.IsNaN(threshold) || double.IsInfinity(threshold) || threshold < 0)
                    return;

                foreach (var row in Holdings)
                    row.AlertThresholdPercent = threshold;
            }
            catch { }
        }

        public void SetAllTrailingStopPercent(double threshold)
        {
            try
            {
                if (double.IsNaN(threshold) || double.IsInfinity(threshold) || threshold < 0)
                    return;

                foreach (var row in Holdings)
                    row.TrailingStopPercent = threshold;
            }
            catch { }
        }

        // Simple forecast computation using recent stored points: positive slope -> Up, negative -> Down, else Neutral
        private ForecastDirection ComputeForecast(string isin)
        {
            try
            {
                var repo = new PriceRepository();
                var points = repo.LoadByIsin(isin);
                if (points == null || points.Count < 3)
                    return ForecastDirection.Neutral;

                // use all stored points sorted ascending by time
                // take most recent up to 20 points and sort ascending by time
                var recent = points.OrderByDescending(p => p.Time).Take(20).OrderBy(p => p.Time).ToList();
                if (recent.Count < 3) return ForecastDirection.Neutral;

                // compute simple linear regression slope over indices
                double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0; int n = recent.Count;
                for (int i = 0; i < n; i++)
                {
                    double x = i;
                    double y = recent[i].Price;
                    sumX += x; sumY += y; sumXY += x * y; sumXX += x * x;
                }
                double denom = (n * sumXX - sumX * sumX);
                if (Math.Abs(denom) < 1e-9) return ForecastDirection.Neutral;
                double slope = (n * sumXY - sumX * sumY) / denom;

                double mean = recent.Average(p => p.Price);
                if (double.IsNaN(mean) || mean == 0) return ForecastDirection.Neutral;
                double rel = slope / mean;

                if (rel > 0.002) return ForecastDirection.Up;
                if (rel < -0.002) return ForecastDirection.Down;
                return ForecastDirection.Neutral;
            }
            catch { return ForecastDirection.Neutral; }
        }

        /// <summary>
        /// Patches a latest StockPoint with fallback values from earlier DB entries
        /// when Price is NaN/0 or ProviderTime is null.
        /// </summary>
        private static void PatchWithLastValidEntry(TradeMVVM.Domain.StockPoint latest, IEnumerable<TradeMVVM.Domain.StockPoint> allForIsin)
        {
            if (latest == null) return;

            bool needsPrice = double.IsNaN(latest.Price) || latest.Price == 0;
            bool needsTime = !latest.ProviderTime.HasValue || latest.ProviderTime.Value == default(DateTime);

            if (!needsPrice && !needsTime)
                return;

            // walk backwards through the history (most recent first, skipping the latest itself)
            foreach (var older in allForIsin.OrderByDescending(p => p.Time))
            {
                if (ReferenceEquals(older, latest))
                    continue;

                if (needsPrice && !double.IsNaN(older.Price) && older.Price != 0)
                {
                    latest.Price = older.Price;
                    // also take Percent from the same point to stay consistent
                    if (double.IsNaN(latest.Percent))
                        latest.Percent = older.Percent;
                    needsPrice = false;
                }

                if (needsTime && older.ProviderTime.HasValue && older.ProviderTime.Value != default(DateTime))
                {
                    latest.ProviderTime = older.ProviderTime;
                    needsTime = false;
                }

                if (!needsPrice && !needsTime)
                    break;
            }
        }

        // Update only the rows for the provided ISINs using latest DB values.
        // Runs DB work on a background thread and applies UI updates via Dispatcher to avoid blocking the UI.
        public void UpdatePrices(List<string> isins)
        {
            if (isins == null || isins.Count == 0)
            {
                // fallback: trigger full refresh (async)
                Refresh();
                return;
            }

            var isinsSet = new HashSet<string>(isins.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x => x.Replace("\u00A0", string.Empty).Trim().ToUpperInvariant()), StringComparer.OrdinalIgnoreCase);

            Task.Run(() =>
            {
                var repo = new TradeMVVM.Trading.Data.PriceRepository();
                var latestPoints = new Dictionary<string, TradeMVVM.Domain.StockPoint>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    foreach (var isin in isinsSet)
                    {
                        try
                        {
                            var points = repo.LoadByIsin(isin);
                            if (points == null || points.Count == 0)
                                continue;

                            var latest = points.OrderByDescending(p => p.Time).First();
                            PatchWithLastValidEntry(latest, points);
                            latestPoints[isin] = latest;
                        }
                        catch { }
                    }
                }
                catch { }

                var conv = new CurrencyConverter();
                var updates = new List<(string isin, HoldingsRow row)>();

                // compute updated rows off the UI thread
                foreach (var h in _source)
                {
                    try
                    {
                        if (h == null) continue;
                        if (!isinsSet.Contains((h.ISIN ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant()))
                            continue;

                        var avgBuy = h.RemainingBoughtShares > 0 ? h.RemainingBoughtAmount / h.RemainingBoughtShares : double.NaN;
                        double plPercent = double.NaN;
                        if (latestPoints.TryGetValue(h.ISIN, out var spProv))
                            plPercent = spProv.Percent;

                        var trailingVals = GetTrailingStopValues(h.ISIN);
                        var trailingConfigured = trailingVals.configured;
                        var trailingCurrent = UpdateTrailingStopCurrent(h.ISIN, plPercent, trailingConfigured);

                        double plAmount = double.NaN;
                        double currentPrice = double.NaN;
                        if (latestPoints.TryGetValue(h.ISIN, out var spCur))
                            currentPrice = spCur.Price;

                        if (!double.IsNaN(currentPrice))
                        {
                            var currentMarketValueNative = h.Shares * currentPrice;
                            if (!double.IsNaN(h.RemainingBoughtAmount) && h.RemainingBoughtAmount != 0)
                                plAmount = currentMarketValueNative - h.RemainingBoughtAmount;
                            else if (!double.IsNaN(avgBuy))
                                plAmount = h.Shares * (currentPrice - avgBuy);
                        }

                        double plAmountEur = double.NaN;
                        if (!double.IsNaN(plAmount))
                        {
                            try { plAmountEur = conv.ConvertToEur(plAmount, h.Currency); } catch { plAmountEur = double.NaN; }
                        }

                        DateTime? providerTime = null;
                        if (latestPoints.TryGetValue(h.ISIN, out var spProvEntry))
                            providerTime = spProvEntry.ProviderTime;

                        double realized = double.IsNaN(h.RealizedPL) || h.RealizedPL == 0.0 ? double.NaN : h.RealizedPL;
                        double unrealized = double.NaN;
                        if (!double.IsNaN(avgBuy) && !double.IsNaN(currentPrice))
                            unrealized = h.Shares * (currentPrice - avgBuy);

                        double total = (double.IsNaN(realized) ? 0.0 : realized) + (double.IsNaN(unrealized) ? 0.0 : unrealized);

                        var newRow = new HoldingsRow
                        {
                            ISIN = h.ISIN,
                            Forecast = ComputeForecast(h.ISIN),
                            Name = h.Name,
                            LastTradeTime = h.LastPriceTime,
                            Currency = h.Currency,
                            Shares = h.Shares,
                            LastPrice = h.LastPrice,
                            MarketValueEur = Math.Round(conv.ConvertToEur(!double.IsNaN(avgBuy) ? h.Shares * avgBuy : h.MarketValue, h.Currency), 2),
                            CurrentPrice = !double.IsNaN(currentPrice) ? currentPrice : double.NaN,
                            MarketValue = !double.IsNaN(currentPrice) ? Math.Round(h.Shares * currentPrice, 2) : double.NaN,
                            TotalBoughtShares = h.TotalBoughtShares,
                            AvgBuyPrice = avgBuy,
                            TotalSoldShares = h.TotalSoldShares,
                            AvgSellPrice = double.IsNaN(h.TotalSoldShares) ? double.NaN : (h.TotalSoldAmount / h.TotalSoldShares),
                            TotalTaxes = h.TotalTaxes,
                            RealizedPL = realized,
                            UnrealizedPL = unrealized,
                            TotalPL = Math.Round(total, 2),
                            PLPercent = double.IsNaN(plPercent) ? double.NaN : Math.Round(plPercent, 2),
                            PLAmount = double.IsNaN(plAmountEur) ? double.NaN : Math.Round(plAmountEur, 2),
                            AlertThresholdPercent = GetIsinAlertThreshold(h.ISIN),
                            TrailingStopPercent = Math.Round(trailingCurrent, 2),
                            TrailingStopConfiguredPercent = Math.Round(trailingConfigured, 2),
                            IsTotal = false,
                            ProviderTime = providerTime,
                            // IsRecentlyUpdated will be decided when applying updates on the UI thread
                            IsRecentlyUpdated = false
                        };

                        updates.Add((h.ISIN, newRow));
                    }
                    catch { }
                }

                // apply updates on UI thread
                try
                {
                    Application.Current?.Dispatcher?.BeginInvoke((Action)(() =>
                    {
                        try
                        {
                            foreach (var tup in updates)
                            {
                                try
                                {
                                    AttachThresholdHandler(tup.row);
                                    var idx = -1;
                                    for (int i = 0; i < Holdings.Count; i++)
                                    {
                                        if (string.Equals(Holdings[i].ISIN, tup.isin, StringComparison.OrdinalIgnoreCase))
                                        {
                                            idx = i; break;
                                        }
                                    }
                                    bool markUpdated = false;
                                    // mark every applied update as recently updated so user sees all changes
                                    try
                                    {
                                        tup.row.IsRecentlyUpdated = true;
                                    }
                                    catch { }

                                    if (idx >= 0)
                                    {
                                        Holdings[idx] = tup.row;
                                    }
                                    else
                                    {
                                        Holdings.Add(tup.row);
                                    }
                                }
                                catch { }
                            }

                            SaveTrailingStopState(force: false);

                            // lightweight totals update
                            try
                            {
                                TotalTaxes = Math.Round(Holdings.Sum(x => new CurrencyConverter().ConvertToEur(x.TotalTaxes, x.Currency)), 2);
                            }
                            catch { }

                            // recompute realized/unrealized totals in EUR
                            try
                            {
                                double totalRealizedEur = 0, totalUnrealizedEur = 0;
                                var conv2 = new CurrencyConverter();
                                foreach (var r in Holdings)
                                {
                                    try
                                    {
                                        totalRealizedEur += conv2.ConvertToEur(double.IsNaN(r.RealizedPL) ? 0.0 : r.RealizedPL, r.Currency);
                                        totalUnrealizedEur += conv2.ConvertToEur(double.IsNaN(r.UnrealizedPL) ? 0.0 : r.UnrealizedPL, r.Currency);
                                    }
                                    catch { }
                                }
                                TotalRealizedPL = Math.Round(totalRealizedEur, 2);
                                TotalUnrealizedPL = Math.Round(totalUnrealizedEur, 2);
                                TotalPL = Math.Round(TotalRealizedPL + TotalUnrealizedPL, 2);
                            }
                            catch { }

                            HoldingsUpdated?.Invoke(_source);
                        }
                        catch { }
                    }));
                }
                catch { }
            });
        }

        private void Refresh()
        {
            // rebuild source list and then apply filter
            Holdings.Clear();
            // load latest prices from DB to populate CurrentPrice (Akt. Kurs)
            var repo = new TradeMVVM.Trading.Data.PriceRepository();
            var latestPoints = new Dictionary<string, TradeMVVM.Domain.StockPoint>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var allPoints = repo.LoadAll();
                if (allPoints != null)
                {
                    foreach (var grp in allPoints.GroupBy(p => p.ISIN))
                    {
                        var pointsList = grp.ToList();
                        var latest = pointsList.OrderByDescending(p => p.Time).FirstOrDefault();
                        if (latest != null && !latestPoints.ContainsKey(grp.Key))
                        {
                            PatchWithLastValidEntry(latest, pointsList);
                            latestPoints[grp.Key] = latest;
                        }
                    }

            // re-apply any search filter after rebuilding
            ApplyFilter();
                }

            }
            catch { }
            double sumRealized = 0;
            double sumUnrealized = 0;
            double sumTaxes = 0;
            double sumMarketEur = 0;
            foreach (var h in _source.OrderBy(h => h.ISIN))
            {
                // only show holdings with remaining held shares (use net Shares value to avoid
                // inconsistencies from transaction-type parsing or rounding)
                if (h.Shares <= 0)
                    continue;
                // use FIFO remaining cost basis (cost of currently held shares) for avg buy
                var avgBuy = h.RemainingBoughtShares > 0 ? h.RemainingBoughtAmount / h.RemainingBoughtShares : double.NaN;
                var avgSell = h.TotalSoldShares > 0 ? h.TotalSoldAmount / h.TotalSoldShares : double.NaN;

                // realized P/L comes from FIFO matching computed in the calculator
                var realized = double.IsNaN(h.RealizedPL) || h.RealizedPL == 0.0 ? double.NaN : h.RealizedPL;

                var unrealized = double.NaN;
                if (!double.IsNaN(avgBuy) && !double.IsNaN(h.LastPrice))
                    unrealized = h.Shares * (h.LastPrice - avgBuy);

                var total = (double.IsNaN(realized) ? 0.0 : realized) + (double.IsNaN(unrealized) ? 0.0 : unrealized);

                var converter = new CurrencyConverter();
                // Marktwert Kauf = Anteile * durchschnittlicher Kaufpreis (wenn vorhanden),
                // sonst fallback auf h.MarketValue
                var mvKaufNative = !double.IsNaN(avgBuy) ? h.Shares * avgBuy : h.MarketValue;
                var mvEur = converter.ConvertToEur(mvKaufNative, h.Currency);

                // determine PL percent: use percent provided by the data provider (latest DB point)
                double plPercent = double.NaN;
                if (latestPoints.TryGetValue(h.ISIN, out var spProvider))
                {
                    plPercent = spProvider.Percent;
                }

                var trailingVals = GetTrailingStopValues(h.ISIN);
                var trailingConfigured = trailingVals.configured;
                var trailingCurrent = UpdateTrailingStopCurrent(h.ISIN, plPercent, trailingConfigured);

                // compute PL amount: difference between current total market value and total purchase cost
                double plAmount = double.NaN;
                double currentPrice = double.NaN;
                if (latestPoints.TryGetValue(h.ISIN, out var spCurrent))
                    currentPrice = spCurrent.Price;

                if (!double.IsNaN(currentPrice))
                {
                    var currentMarketValueNative = h.Shares * currentPrice;
                    // prefer the remaining bought amount (cost basis for held shares)
                    if (!double.IsNaN(h.RemainingBoughtAmount) && h.RemainingBoughtAmount != 0)
                    {
                        plAmount = currentMarketValueNative - h.RemainingBoughtAmount;
                    }
                    else if (!double.IsNaN(avgBuy))
                    {
                        plAmount = h.Shares * (currentPrice - avgBuy);
                    }
                }

                // convert PL amount to EUR for display/export
                double plAmountEur = double.NaN;
                if (!double.IsNaN(plAmount))
                {
                    try
                    {
                        plAmountEur = converter.ConvertToEur(plAmount, h.Currency);
                    }
                    catch
                    {
                        plAmountEur = double.NaN;
                    }
                }

                // provider time if available
                DateTime? providerTime = null;
                if (latestPoints.TryGetValue(h.ISIN, out var spProviderEntry))
                    providerTime = spProviderEntry.ProviderTime;

                Holdings.Add(new HoldingsRow
                {
                    ISIN = h.ISIN,
                    Forecast = ComputeForecast(h.ISIN),
                    Name = h.Name,
                    Currency = h.Currency,
                    LastTradeTime = h.LastPriceTime,
                    Shares = h.Shares,
                    LastPrice = h.LastPrice,
                    MarketValueEur = Math.Round(mvEur, 2),
                    // market value and current price will be filled later; leave empty (NaN) for now
                    // fill current price from latest DB point when available
                    CurrentPrice = latestPoints.TryGetValue(h.ISIN, out var sp) ? sp.Price : double.NaN,
                    // Akt. Marktwert = Anteile * Akt. Kurs (wenn aktueller Kurs vorhanden)
                    MarketValue = !double.IsNaN(currentPrice) ? Math.Round(h.Shares * currentPrice, 2) : double.NaN,
                    TotalBoughtShares = h.TotalBoughtShares,
                    AvgBuyPrice = avgBuy,
                    TotalSoldShares = h.TotalSoldShares,
                    AvgSellPrice = avgSell,
                    TotalTaxes = h.TotalTaxes
                    ,RealizedPL = realized
                    ,UnrealizedPL = unrealized
                    ,TotalPL = Math.Round(total, 2)
                    // PLPercent and PLAmount
                    ,PLPercent = double.IsNaN(plPercent) ? double.NaN : Math.Round(plPercent, 2)
                    // store PLAmount in EUR
                    ,PLAmount = double.IsNaN(plAmountEur) ? double.NaN : Math.Round(plAmountEur, 2)
                    ,AlertThresholdPercent = GetIsinAlertThreshold(h.ISIN)
                    ,TrailingStopPercent = Math.Round(trailingCurrent, 2)
                    ,TrailingStopConfiguredPercent = Math.Round(trailingConfigured, 2)
                    ,ProviderTime = providerTime
                });

                try { AttachThresholdHandler(Holdings.LastOrDefault()); } catch { }

                sumRealized += double.IsNaN(realized) ? 0.0 : realized;
                sumUnrealized += double.IsNaN(unrealized) ? 0.0 : unrealized;
                sumTaxes += h.TotalTaxes;
                sumMarketEur += mvEur;
            }

            TotalRealizedPL = sumRealized;
            TotalUnrealizedPL = sumUnrealized;
            TotalPL = sumRealized + sumUnrealized;
            TotalTaxes = sumTaxes;
            // convert per-position P/L to EUR
            double totalRealizedEur = 0;
            double totalUnrealizedEur = 0;
            try
            {
                var conv = new CurrencyConverter();
                foreach (var r in Holdings)
                {
                    try
                    {
                        totalRealizedEur += conv.ConvertToEur(double.IsNaN(r.RealizedPL) ? 0.0 : r.RealizedPL, r.Currency);
                        totalUnrealizedEur += conv.ConvertToEur(double.IsNaN(r.UnrealizedPL) ? 0.0 : r.UnrealizedPL, r.Currency);
                    }
                    catch { }
                }
            }
            catch { }

            // override totals to EUR values (rounded)
            TotalRealizedPL = Math.Round(totalRealizedEur, 2);
            TotalUnrealizedPL = Math.Round(totalUnrealizedEur, 2);
            TotalPL = Math.Round(TotalRealizedPL + TotalUnrealizedPL, 2);
            TotalTaxes = Math.Round(Holdings.Sum(x => new CurrencyConverter().ConvertToEur(x.TotalTaxes, x.Currency)), 2);

            // compute total gains and losses (based on TotalPL per holding) in EUR
            double gains = 0.0;
            double losses = 0.0; // will be negative or zero
            foreach (var r in Holdings)
            {
                var conv = new CurrencyConverter();
                var plEur = conv.ConvertToEur(double.IsNaN(r.TotalPL) ? 0.0 : r.TotalPL, r.Currency);
                if (plEur > 0)
                    gains += plEur;
                else if (plEur < 0)
                    losses += plEur; // negative value
            }

            TotalGains = Math.Round(gains, 2);
            TotalLosses = Math.Round(losses, 2);

            SaveTrailingStopState(force: false);

            // format numbers / rounding could be applied here if needed

            // notify listeners that holdings have been refreshed
            HoldingsUpdated?.Invoke(_source);
        }

        private void ApplyFilter()
        {
            if (_holdingsView == null)
                return;

            if (string.IsNullOrWhiteSpace(_searchText))
            {
                // clear filter to show all rows
                _holdingsView.Filter = null;
            }
            else
            {
                var q = _searchText.Trim();
                _holdingsView.Filter = o =>
                {
                    if (o is HoldingsRow r)
                    {
                        if (!string.IsNullOrWhiteSpace(r.ISIN) && r.ISIN.IndexOf(q, StringComparison.OrdinalIgnoreCase) >= 0)
                            return true;
                        if (!string.IsNullOrWhiteSpace(r.Name) && r.Name.IndexOf(q, StringComparison.OrdinalIgnoreCase) >= 0)
                            return true;
                    }
                    return false;
                };
            }
        }

        private void ExportCsv()
        {
            try
            {
                var outPath = Path.Combine(Path.GetDirectoryName(_csvPath) ?? "", "HoldingsReport_Export.csv");
                var lines = new List<string>();
                lines.Add("ISIN;Name;Currency;Shares;LastPrice;MarketValueEur;TotalBoughtShares;AvgBuyPrice;TotalSoldShares;AvgSellPrice;Taxes;RealizedPL;UnrealizedPL;TotalPL;PLAmountEUR;PLPercent");
                foreach (var r in Holdings)
                {
                    lines.Add(string.Join(";", new string[] {
                        r.ISIN,
                        r.Name?.Replace(";", ","),
                        r.Shares.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        double.IsNaN(r.LastPrice) ? "" : r.LastPrice.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        r.MarketValueEur.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        r.TotalBoughtShares.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        double.IsNaN(r.AvgBuyPrice) ? "" : r.AvgBuyPrice.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        r.TotalSoldShares.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        double.IsNaN(r.AvgSellPrice) ? "" : r.AvgSellPrice.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        r.TotalTaxes.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        double.IsNaN(r.RealizedPL) ? "" : r.RealizedPL.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        double.IsNaN(r.UnrealizedPL) ? "" : r.UnrealizedPL.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        double.IsNaN(r.TotalPL) ? "" : r.TotalPL.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        double.IsNaN(r.PLAmount) ? "" : r.PLAmount.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE")),
                        double.IsNaN(r.PLPercent) ? "" : r.PLPercent.ToString(System.Globalization.CultureInfo.GetCultureInfo("de-DE"))
                    }));
                }
                File.WriteAllLines(outPath, lines);

                // Öffne die Datei im Standard-Programm (Excel oder LibreOffice)
                Process.Start(new ProcessStartInfo
                {
                    FileName = outPath,
                    UseShellExecute = true
                });
            }
            catch
            {
                // ignore for now
            }
        }

        // allow view to ask VM to load CSV from a given path
        public void LoadCsvFromPath(string path)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(path))
                    return;
                _csvPath = path;
                var dict = TradeMVVM.Trading.DataAnalysis.HoldingsCalculator.ComputeHoldingsFromCsv(_csvPath);
                // explicit sanitization: only keep holdings with positive shares
                _source = (dict ?? new Dictionary<string, TradeMVVM.Trading.DataAnalysis.Holding>())
                    .Values
                    .Where(h => h != null && h.Shares > 0 && !string.IsNullOrWhiteSpace((h.ISIN ?? string.Empty).Replace("\u00A0", string.Empty).Trim()))
                    .ToList();
                Refresh();
            }
            catch
            {
                // ignore
            }
        }

        // Remove all transactions for the provided ISIN from the backing CSV and refresh view.
        public void RemoveHoldingByIsin(string isin)
        {
            RemoveHoldingsByIsins(new[] { isin });
        }

        // Remove multiple ISINs from the backing CSV. Creates a timestamped backup before overwriting
        // and appends removed lines to a .trash file. Stores last backup path for undo.
        public void RemoveHoldingsByIsins(IEnumerable<string> isins)
        {
            try
            {
                var set = new HashSet<string>(isins.Where(x => !string.IsNullOrWhiteSpace(x)), StringComparer.OrdinalIgnoreCase);
                if (set.Count == 0 || string.IsNullOrWhiteSpace(_csvPath) || !File.Exists(_csvPath))
                    return;

                // create backup
                try
                {
                    var dir = Path.GetDirectoryName(_csvPath) ?? ".";
                    var backupName = Path.Combine(dir, $"{Path.GetFileNameWithoutExtension(_csvPath)}_backup_{DateTime.UtcNow:yyyyMMdd_HHmmss}.bak");
                    File.Copy(_csvPath, backupName, overwrite: false);
                    _lastBackupPath = backupName;
                }
                catch { /* ignore backup failures */ }

                string[] lines;
                using (var fs = new FileStream(_csvPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var sr = new StreamReader(fs))
                {
                    var list = new List<string>();
                    string l;
                    while ((l = sr.ReadLine()) != null)
                        list.Add(l);
                    lines = list.ToArray();
                }

                if (lines == null || lines.Length == 0)
                    return;

                var header = lines[0];
                var kept = new List<string> { header };
                var removed = new List<string>();

                for (int i = 1; i < lines.Length; i++)
                {
                    var parts = lines[i].Split(';');
                    if (parts.Length > 7)
                    {
                        var lineIsin = parts[7].Trim();
                        if (set.Contains(lineIsin))
                        {
                            removed.Add(lines[i]);
                            continue; // skip
                        }
                    }
                    kept.Add(lines[i]);
                }

                // append removed lines to trash file for audit/undo reference
                try
                {
                    var trashPath = _csvPath + ".trash";
                    using (var sw = new StreamWriter(trashPath, true))
                    {
                        sw.WriteLine($"### Deleted at {DateTime.UtcNow:O} | ISINs={string.Join(",", set)}");
                        foreach (var r in removed)
                            sw.WriteLine(r);
                        sw.WriteLine();
                    }
                }
                catch { }

                // overwrite CSV
                File.WriteAllLines(_csvPath, kept);

                // reload source and refresh
                var dict = TradeMVVM.Trading.DataAnalysis.HoldingsCalculator.ComputeHoldingsFromCsv(_csvPath);
                _source = dict.Values.ToList();
                Refresh();
            }
            catch
            {
                // ignore errors for now
            }
        }

        // Undo last delete by restoring the last backup created (if present)
        public void UndoLastDelete()
        {
            try
            {
                if (string.IsNullOrWhiteSpace(_lastBackupPath) || !File.Exists(_lastBackupPath) || string.IsNullOrWhiteSpace(_csvPath))
                    return;

                // restore backup
                File.Copy(_lastBackupPath, _csvPath, overwrite: true);
                // reload
                var dict = TradeMVVM.Trading.DataAnalysis.HoldingsCalculator.ComputeHoldingsFromCsv(_csvPath);
                _source = dict.Values.ToList();
                Refresh();
                // clear last backup path
                try { File.Delete(_lastBackupPath); } catch { }
                _lastBackupPath = null;
            }
            catch { }
        }

        private static bool IsHoldingsSnapshotLoggingEnabled()
        {
            try
            {
                var settings = TradeMVVM.Trading.App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                return settings?.EnableHoldingsSnapshotLogging == true;
            }
            catch { }

            return false;
        }
    }
}
