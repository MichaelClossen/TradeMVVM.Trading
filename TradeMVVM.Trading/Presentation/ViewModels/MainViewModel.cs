using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TradeMVVM.Trading.Chart;
using TradeMVVM.Trading.Data;
using TradeMVVM.Trading.Services;
using System.Windows.Input;
using System.Collections.ObjectModel;
using TradeMVVM.Trading.Models;
using System.IO;
using System.Windows;
using TradeMVVM.Trading.DataAnalysis;
using System.Windows.Threading;
using TradeMVVM.Trading.ViewModels;
using Microsoft.Extensions.DependencyInjection;

namespace TradeMVVM.Trading.Presentation.ViewModels
{
    public class MainViewModel : BaseViewModel, IDisposable
    {
        private readonly TradeMVVM.Trading.Services.SettingsService _settingsService;
        private readonly TradeMVVM.Trading.Services.ServerControlService _serverControl;
        private PricePollingService _pollingService;
        private readonly ChartDataProvider _chartDataProvider;
        private readonly PriceRepository _repository = new PriceRepository();

        private CancellationTokenSource _cts;
        private volatile bool _isPollingRunning;
        private Task _pollingTask;
        private readonly object _pollingLifecycleLock = new object();
        // timestamp of last incremental DB load (for timer-based display refresh)
        private DateTime _lastDbLoadTime = DateTime.MinValue;
        // keys of points loaded in the previous batch to de-duplicate the >= overlap window
        private HashSet<string> _lastBatchKeys = new HashSet<string>(StringComparer.Ordinal);
        // watchdog to detect stalled polling (uses thread-pool timer to avoid UI thread blocking)
        private System.Threading.Timer _pollWatchdogTimer;
        private readonly TimeSpan _pollWatchdogInterval = TimeSpan.FromSeconds(5);
        private readonly TimeSpan _pollWatchdogTimeout = TimeSpan.FromSeconds(20);
        private DateTime _lastPriceUpdate = DateTime.MinValue;
        private string _lastPolledIsin = string.Empty;
        private int _missedCycleCount = 0;
        private const int _missedCycleThreshold = 3; // require several missed cycles before restarting

        public bool IsPollingRunning => _isPollingRunning;

        public Dictionary<string, List<Tuple<DateTime, double>>> PriceHistory { get; }
        public ICommand StartCommand { get; }
        public ICommand StopCommand { get; }
        public ICommand ClearDbCommand { get; }
        public ICommand BackfillPercentCommand { get; }
        public ICommand GenerateHoldingsCommand { get; }
        public ZoomCommands Zoom { get; }

        private readonly List<(string isin_wkn, string name, TradeMVVM.Domain.StockType type)> _stocks =
            new List<(string, string, TradeMVVM.Domain.StockType)>
        {
        // initial defaults; will be replaced by holdings-derived stocks
        ("US5949181045", string.Empty, TradeMVVM.Domain.StockType.Aktie),
        ("PJ538W", string.Empty, TradeMVVM.Domain.StockType.Knockout),
        };

        public List<(string isin_wkn, string name, TradeMVVM.Domain.StockType type)> Stocks => _stocks;

        // currently owned positions (isin, name, shares) for holdings with shares > 0
        private readonly List<(string isin, string name, double shares)> _ownedPositions =
            new List<(string, string, double)>();

        public IReadOnlyList<(string isin, string name, double shares)> OwnedPositions => _ownedPositions.AsReadOnly();

        public ObservableCollection<LegendItem> LegendItems { get; } = new ObservableCollection<LegendItem>();

        // (deleted) UI helper for DeleteIsinCommand removed

        // Embedded holdings report VM
        private HoldingsReportViewModel _holdingsReport;
        public HoldingsReportViewModel HoldingsReport
        {
            get => _holdingsReport;
            private set
            {
                // unsubscribe old
                if (_holdingsReport != null)
                    _holdingsReport.HoldingsUpdated -= OnHoldingsUpdated;

                _holdingsReport = value;

                // subscribe new
                if (_holdingsReport != null)
                    _holdingsReport.HoldingsUpdated += OnHoldingsUpdated;

                OnPropertyChanged(nameof(HoldingsReport));
            }
        }

        private void OnPollingCycleCompleted()
        {
            try
            {
                _lastPriceUpdate = DateTime.UtcNow;
            }
            catch { }

            // After a full polling cycle completed, compute total unrealized PL once and persist it.
            try
            {
                ComputeAndPersistTotalPl();
            }
            catch { }
        }

        // Compute total unrealized PL across holdings (in EUR) and persist into TotalPLHistory
        private void ComputeAndPersistTotalPl()
        {
            try
            {
                if (_holdingsReport == null) return;

                var source = _holdingsReport.GetSourceHoldings();
                if (source == null || source.Count == 0) return;

                var converter = new TradeMVVM.Trading.DataAnalysis.CurrencyConverter();
                double sumTotalPlEur = 0.0;

                // follow same logic as ChartsView: depending on display mode (use unrealized only here)
                foreach (var h in source)
                {
                    try
                    {
                        double unrealized = double.NaN;
                        // compute average buy if available
                        double avgBuy = h.RemainingBoughtShares > 0 ? h.RemainingBoughtAmount / h.RemainingBoughtShares : double.NaN;
                        if (!double.IsNaN(avgBuy) && !double.IsNaN(h.LastPrice))
                            unrealized = h.Shares * (h.LastPrice - avgBuy);

                        if (!double.IsNaN(unrealized) && !double.IsInfinity(unrealized))
                            sumTotalPlEur += converter.ConvertToEur(unrealized, h.Currency);
                    }
                    catch { }
                }

                if (double.IsNaN(sumTotalPlEur) || double.IsInfinity(sumTotalPlEur))
                    return;

                try
                {
                    var db = new TradeMVVM.Trading.Services.DatabaseService();
                    db.InsertTotalPLHistory(DateTime.Now, sumTotalPlEur);
                    try { db.UpsertAggregate("TotalPL", sumTotalPlEur); } catch { }
                }
                catch { }
            }
            catch { }
        }

        private void OnPollAttempted(string isin)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(isin) || _holdingsReport == null)
                    return;

                var normalizedIsin = isin.Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                if (string.IsNullOrWhiteSpace(normalizedIsin))
                    return;

                try { _lastPolledIsin = normalizedIsin; } catch { }

                Application.Current?.Dispatcher?.BeginInvoke((Action)(() =>
                {
                    try { _holdingsReport?.MarkPolled(new[] { normalizedIsin }); } catch { }
                }));
            }
            catch { }
        }

        private void OnHoldingsUpdated(List<TradeMVVM.Trading.DataAnalysis.Holding> holdings)
        {
            // update stocks whenever holdings viewmodel refreshes
            UpdateStocksFromHoldings(holdings ?? Enumerable.Empty<TradeMVVM.Trading.DataAnalysis.Holding>());
        }

        public MainViewModel(DualZoomController zoomController)
        {
            // use DI-provided settings and provider
            _settingsService = App.Services.GetRequiredService<TradeMVVM.Trading.Services.SettingsService>();
            _serverControl = App.Services.GetService<TradeMVVM.Trading.Services.ServerControlService>() ?? new TradeMVVM.Trading.Services.ServerControlService();
            _chartDataProvider = App.Services.GetRequiredService<ChartDataProvider>();
            // subscribe to settings changes and reload provider keywords (update ChartDataProvider via polling service)
            _settingsService.SettingsChanged += () =>
            {
                try
                {
                    // recreate polling service with new provider instance that reads new settings
                    var newProvider = App.Services.GetRequiredService<ChartDataProvider>();
                    var old = _pollingService;
                    var repo = _repository;
                    // replace polling service
                    // ensure any running polling is stopped and disposed before replacing
                    try { old?.Dispose(); } catch { }
                    _pollingService = new PricePollingService(newProvider, repo);
                    // subscribe to cycle completed to update watchdog timestamp
                    _pollingService.CycleCompleted += () => { try { _lastPriceUpdate = DateTime.UtcNow; } catch { } };
                    _pollingService.PollAttempted += OnPollAttempted;
                    try { old?.Dispose(); } catch { }
                }
                catch { }
            };
            // Use the shared repository instance (_repository) instead of creating a second one
            _pollingService = new PricePollingService(_chartDataProvider, _repository);
            // subscribe to cycle heartbeat so watchdog sees activity even when no individual price updates
            try { _pollingService.CycleCompleted += OnPollingCycleCompleted; } catch { }
            try { _pollingService.PollAttempted += OnPollAttempted; } catch { }

            // polling watchdog (disabled until polling starts) - use thread-pool timer so UI work (zoom) doesn't block it
            _pollWatchdogTimer = new System.Threading.Timer(_ => CheckPollingWatchdog(), null, System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);

            // DB cleanup is now manual via toolbar button

            PriceHistory = new Dictionary<string, List<Tuple<DateTime, double>>>();

            Zoom = new ZoomCommands(zoomController);   // 🔥 DAS FEHLT BEI DIR

            StartCommand = new AsyncRelayCommand(StartAsync);
            StopCommand = new RelayCommand(Stop);
            // server control commands (override Start/Stop to use DB flag when server mode is used)
            ServerStartCommand = new RelayCommand(() => { try { _serverControl.SetPollingEnabled(true); } catch { } });
            ServerStopCommand = new RelayCommand(() => { try { _serverControl.SetPollingEnabled(false); } catch { } });

            // poll server heartbeat timer
            _ = StartServerHeartbeatWatcher();

            ClearDbCommand = new RelayCommand(() =>
            {
                if (MessageBox.Show("Delete ALL price records?", "Confirm", MessageBoxButton.YesNo, MessageBoxImage.Warning) == MessageBoxResult.Yes)
                {
                    _repository.ClearAll();
                    // Always clear in-memory plot data immediately (even if polling is running)
                    try
                    {
                        lock (PriceHistory)
                        {
                            foreach (var k in PriceHistory.Keys.ToList())
                                PriceHistory[k].Clear();
                        }

                        Application.Current?.Dispatcher?.Invoke(() => { try { LegendItems.Clear(); } catch { } });
                    }
                    catch { }
                }
            });

            BackfillPercentCommand = new RelayCommand(() =>
            {
                // run backfill on background thread to avoid UI freeze
                Task.Run(() =>
                {
                    try
                    {
                        var db = new TradeMVVM.Trading.Services.DatabaseService();
                        var updated = db.BackfillPercentWhereZero();
                        try { System.Diagnostics.Debug.WriteLine($"BackfillPercentWhereZero updated {updated} rows"); } catch { }
                        // notify user on UI thread and refresh in-memory view
                        Application.Current?.Dispatcher?.BeginInvoke((Action)(() =>
                        {
                            try { MessageBox.Show($"Backfilled {updated} rows (Price!=0 && Percent==0).", "Backfill Percent", MessageBoxButton.OK, MessageBoxImage.Information); } catch { }
                            try { RefreshFromDb(); } catch { }
                        }));
                    }
                    catch (Exception ex)
                    {
                        try { System.Diagnostics.Debug.WriteLine($"BackfillPercentCommand failed: {ex.Message}"); } catch { }
                        Application.Current?.Dispatcher?.BeginInvoke((Action)(() =>
                        {
                            try { MessageBox.Show($"Backfill failed: {ex.Message}", "Backfill Percent", MessageBoxButton.OK, MessageBoxImage.Error); } catch { }
                        }));
                    }
                });
            });

            // CleanDbCommand, RemoveZeroPricesCommand and DeleteIsinCommand removed from ViewModel

            InitializeStocks();
            // initialize empty holdings view
            HoldingsReport = new TradeMVVM.Trading.ViewModels.HoldingsReportViewModel(new List<TradeMVVM.Trading.DataAnalysis.Holding>(), string.Empty);

            // load holdings immediately (silent, no message boxes)
            _ = GenerateHoldingsAsync(false);

            // expose Settings command
            OpenSettingsCommand = new RelayCommand(OpenSettings);
        }

        public ICommand OpenSettingsCommand { get; }
        public ICommand ServerStartCommand { get; }
        public ICommand ServerStopCommand { get; }

        private DispatcherTimer _serverHeartbeatTimer;
        private DateTime? _lastServerHeartbeat;
        public DateTime? LastServerHeartbeat => _lastServerHeartbeat;
        // Timer to poll DB for new rows when an external server is running
        private DispatcherTimer _dbRefreshTimer;

        private void OpenSettings()
        {
            // show simple modal window with settings
            Application.Current?.Dispatcher?.Invoke(() =>
            {
                var win = new Window
                {
                    Title = "Preferences",
                    SizeToContent = System.Windows.SizeToContent.WidthAndHeight,
                    MinWidth = 420,
                    MinHeight = 240,
                    // limit window height to 2/3 of the available work area
                    MaxHeight = SystemParameters.WorkArea.Height * 0.66
                };

                // create view and viewmodel
                var view = new TradeMVVM.Trading.Views.Settings.SettingsView();
                var vm = new TradeMVVM.Trading.ViewModels.SettingsViewModel(_settingsService, () => win.Close());
                view.DataContext = vm;

                // populate providers list viewmodel so the embedded ProvidersListView shows entries
                try
                {
                    var providersVm = new TradeMVVM.Trading.ViewModels.SettingsProvidersViewModel(_chartDataProvider);
                    var holdings = _holdingsReport != null ? _holdingsReport.GetSourceHoldings() : new System.Collections.Generic.List<TradeMVVM.Trading.DataAnalysis.Holding>();
                    if (holdings == null || holdings.Count == 0)
                    {
                        // fallback: load ISINs from DB holding totals so preferences show only ISINs with data
                        try { providersVm.LoadFromDb(); }
                        catch { providersVm.Load(holdings); }
                    }
                    else
                    {
                        providersVm.Load(holdings);
                    }
                    vm.ProvidersVM = providersVm;
                }
                catch { }

                win.Content = view;

                win.Owner = Application.Current?.MainWindow;
                win.WindowStartupLocation = WindowStartupLocation.CenterOwner;
                win.ShowDialog();
            });
        }

        private async Task StartServerHeartbeatWatcher()
        {
            await Task.Yield();
            try
            {
                // heartbeat timer: checks server heartbeat every 5s
                _serverHeartbeatTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(5) };

                // DB refresh timer: always run every 5s so GUI picks up new DB rows
                _dbRefreshTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(5) };
                _dbRefreshTimer.Tick += (ss, ee) =>
                {
                    try { RefreshFromDb(); } catch (Exception ex) { try { System.Diagnostics.Debug.WriteLine($"RefreshFromDb tick failed: {ex.Message}"); } catch { } }
                };

                _serverHeartbeatTimer.Tick += (s, e) =>
                {
                    try
                    {
                        var hb = _serverControl.GetLastHeartbeat();
                        _lastServerHeartbeat = hb;
                        OnPropertyChanged(nameof(LastServerHeartbeat));

                        // if server heartbeat is recent, ensure we refresh DB every 5s
                        bool serverAlive = false;
                        try
                        {
                            if (hb.HasValue)
                            {
                                var age = DateTime.UtcNow - hb.Value.ToUniversalTime();
                                serverAlive = age <= TimeSpan.FromSeconds(10);
                            }
                        }
                        catch { }

                        // no-op: DB refresh continues independently of heartbeat
                    }
                    catch { }
                };
                _serverHeartbeatTimer.Start();
                // start DB refresh timer immediately
                try { _dbRefreshTimer.Start(); } catch { }
                // perform an immediate heartbeat check so we start refreshing DB without waiting for the first tick
                try
                {
                    var hb = _serverControl.GetLastHeartbeat();
                    _lastServerHeartbeat = hb;
                    OnPropertyChanged(nameof(LastServerHeartbeat));

                    bool serverAlive = false;
                    try
                    {
                        if (hb.HasValue)
                        {
                            var age = DateTime.UtcNow - hb.Value.ToUniversalTime();
                            serverAlive = age <= TimeSpan.FromSeconds(10);
                        }
                    }
                    catch { }

                    try { RefreshFromDb(); } catch { }
                }
                catch { }
            }
            catch { }
        }

        // update the polling stock list based on currently held positions
        private void UpdateStocksFromHoldings(IEnumerable<TradeMVVM.Trading.DataAnalysis.Holding> holdings)
        {
            _ownedPositions.Clear();

            foreach (var h in holdings.OrderBy(h => h.ISIN))
            {
                if (h.Shares <= 0)
                    continue;

                var normalizedIsin = (h.ISIN ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                if (string.IsNullOrWhiteSpace(normalizedIsin))
                    continue;

                // store for UI/inspection
                _ownedPositions.Add((normalizedIsin, h.Name ?? "", h.Shares));
            }

            // build new list first, then swap atomically so the polling thread
            // never sees a partially-built or empty _stocks list
            var newStocks = new List<(string isin_wkn, string name, TradeMVVM.Domain.StockType type)>();
            foreach (var (isin, name, _) in _ownedPositions)
            {
                var detected = _chartDataProvider.DetectStockType(name);
                newStocks.Add((isin, name, detected));
                if (!PriceHistory.ContainsKey(isin))
                    PriceHistory[isin] = new List<Tuple<DateTime, double>>();
            }

            // atomic swap: replace all items under lock so polling snapshot is always consistent
            lock (_stocks)
            {
                _stocks.Clear();
                _stocks.AddRange(newStocks);
            }

            System.Diagnostics.Debug.WriteLine($"UpdateStocksFromHoldings: {_stocks.Count} ISINs: {string.Join(", ", _stocks.Select(s => s.isin_wkn))}");
        }

        private async Task GenerateHoldingsAsync()
        {
            await GenerateHoldingsAsync(true);
        }

        private async Task GenerateHoldingsAsync(bool showMessages)
        {
            try
            {
                var baseDir = AppDomain.CurrentDomain.BaseDirectory;
                var dataDir = Path.Combine(baseDir, "DataAnalysis");
                // if user configured a default holdings CSV, load it and skip computing from Transactions.csv
                try
                {
                    var cfgPath = _settingsService?.HoldingsCsvPath;
                    if (!string.IsNullOrWhiteSpace(cfgPath) && File.Exists(cfgPath))
                    {
                        // load configured CSV into embedded viewmodel and update stocks
                        Application.Current?.Dispatcher?.Invoke(() =>
                        {
                            try
                            {
                                if (HoldingsReport != null)
                                {
                                    HoldingsReport.LoadCsvFromPath(cfgPath);
                                    UpdateStocksFromHoldings(HoldingsReport.GetSourceHoldings());
                                }
                            }
                            catch { }
                        });

                        return;
                    }
                }
                catch { }

                var csvPath = Path.Combine(dataDir, "Transactions.csv");

                if (!File.Exists(csvPath))
                {
                    if (showMessages)
                        MessageBox.Show($"Transactions CSV not found: {csvPath}", "Holdings Report", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }

                // compute holdings on background thread
                var holdings = await Task.Run(() => HoldingsCalculator.ComputeHoldingsFromCsv(csvPath));

                // update embedded viewmodel on UI thread
                Application.Current?.Dispatcher?.Invoke(() =>
                {
                    HoldingsReport = new HoldingsReportViewModel(holdings.Values.ToList(), csvPath);
                    // update polling stocks based on current holdings
                    UpdateStocksFromHoldings(holdings.Values);
                });
            }
            catch (Exception ex)
            {
                if (showMessages)
                {
                    Application.Current?.Dispatcher?.Invoke(() =>
                        MessageBox.Show($"Error generating holdings report: {ex.Message}", "Holdings Report", MessageBoxButton.OK, MessageBoxImage.Error));
                }
            }
        }



        private void InitializeStocks()
        {
            foreach (var (isin_wkn, _, _) in _stocks)
            {
                PriceHistory[isin_wkn] =
                    new List<Tuple<DateTime, double>>();
            }

            LoadFromDb();

            // If holdings/stocks are empty after loading DB, populate Stocks from available DB ISINs
            try
            {
                if ((_stocks == null || _stocks.Count == 0) && PriceHistory != null && PriceHistory.Count > 0)
                {
                    // If a holdings report is available, prefer ISINs from the holdings with positive shares.
                    // This avoids polling ISINs that exist only in historical DB rows but are not currently held
                    // (e.g. zero shares / NaN average buy entries like DE000PK5QAE5).
                    HashSet<string>? validHoldingsIsins = null;
                    try
                    {
                        var src = HoldingsReport?.GetSourceHoldings();
                        if (src != null && src.Count > 0)
                        {
                            validHoldingsIsins = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            foreach (var h in src)
                            {
                                try
                                {
                                    if (h.Shares <= 0) continue;
                                    var normalized = (h.ISIN ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                                    if (!string.IsNullOrWhiteSpace(normalized)) validHoldingsIsins.Add(normalized);
                                }
                                catch { }
                            }
                            if (validHoldingsIsins.Count == 0)
                                validHoldingsIsins = null; // treat as no holdings with positive shares
                        }
                    }
                    catch { validHoldingsIsins = null; }

                    // If holdings viewmodel is not yet populated, try a quick synchronous read
                    // of a configured holdings CSV so we avoid populating Stocks from historical DB rows.
                    try
                    {
                        if (validHoldingsIsins == null)
                        {
                            var cfgPath = _settingsService?.HoldingsCsvPath;
                            if (!string.IsNullOrWhiteSpace(cfgPath) && File.Exists(cfgPath))
                            {
                                try
                                {
                                    var dict = TradeMVVM.Trading.DataAnalysis.HoldingsCalculator.ComputeHoldingsFromCsv(cfgPath);
                                    if (dict != null && dict.Count > 0)
                                    {
                                        validHoldingsIsins = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                        foreach (var kv in dict)
                                        {
                                            try
                                            {
                                                var h = kv.Value;
                                                if (h == null) continue;
                                                if (h.Shares <= 0) continue;
                                                var norm = (h.ISIN ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                                                if (!string.IsNullOrWhiteSpace(norm)) validHoldingsIsins.Add(norm);
                                            }
                                            catch { }
                                        }
                                        if (validHoldingsIsins.Count == 0)
                                            validHoldingsIsins = null;
                                    }
                                }
                                catch { }
                            }
                        }
                    }
                    catch { }

                    lock (_stocks)
                    {
                        foreach (var k in PriceHistory.Keys)
                        {
                            try
                            {
                                if (_stocks.Any(s => string.Equals(s.isin_wkn, k, StringComparison.OrdinalIgnoreCase)))
                                    continue;

                                if (validHoldingsIsins != null)
                                {
                                    var norm = (k ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                                    if (!validHoldingsIsins.Contains(norm))
                                        continue; // skip historical-only ISINs when holdings exist
                                }

                                _stocks.Add((k, string.Empty, TradeMVVM.Domain.StockType.Aktie));
                            }
                            catch { }
                        }
                    }
                }
            }
            catch { }
        }

        private async Task StartAsync()
        {
            // Do not change zoom on start - keep the user-selected/initial zoom (e.g. 2h)

            var existingCts = _cts;
            if (existingCts != null)
            {
                // if an existing cancellation source is present and not cancelled, don't start another
                if (!existingCts.IsCancellationRequested)
                    return;

                // previous polling was cancelled (watchdog or Stop). Wait briefly for background cleanup to finish
                try
                {
                    if (_pollingTask != null)
                    {
                        var finished = await Task.WhenAny(_pollingTask, Task.Delay(3000));
                        if (finished == _pollingTask)
                        {
                            try { await _pollingTask; } catch { }
                        }
                    }
                }
                catch { }

                // ensure previous state cleared
                try { existingCts.Dispose(); } catch { }
                if (ReferenceEquals(_cts, existingCts))
                    _cts = null;
                _isPollingRunning = false;
            }

            var runCts = new CancellationTokenSource();
            _cts = runCts;

            // signal providers that shutdown is no longer in effect (in case Stop() set it)
            try { TradeMVVM.Trading.Services.Providers.GettexProvider.IsShuttingDown = false; } catch { }

            int restartCount = 0;
            const int maxRestarts = 5;

            _pollingTask = null;
            try
            {
                // mark polling as running when we start the background task
                _isPollingRunning = true;
                // reset watchdog timestamp when starting
                _lastPriceUpdate = DateTime.UtcNow;
                try { _pollWatchdogTimer.Change(0, (int)_pollWatchdogInterval.TotalMilliseconds); } catch { }
                while (!runCts.Token.IsCancellationRequested && restartCount <= maxRestarts)
                {
                    try
                    {
                        // start dual polling in background (2 workers, staggered)
                        _pollingTask = Task.Run(() => _polling_service(runCts.Token));

                        // attach cycle heartbeat listener so watchdog knows polling is alive
                        try { _pollingService.CycleCompleted += () => { try { _lastPriceUpdate = DateTime.UtcNow; } catch { } }; } catch { }

                        // give the polling task a short moment to surface startup errors
                        try { await Task.Delay(500, runCts.Token); } catch { }

                        if (_pollingTask.IsCompleted)
                        {
                            // task finished immediately -> propagate exception if any
                            if (_pollingTask.IsFaulted)
                                throw _pollingTask.Exception ?? new Exception("Polling task faulted");
                            // otherwise it ended normally (cancellation) -> exit
                            break;
                        }

                        // polling started successfully in background -> attach continuation to cleanup when it ends
                        // attach continuation to cleanup when polling ends
                        _ = _pollingTask.ContinueWith(t =>
                        {
                            _isPollingRunning = false;
                            try { _pollWatchdogTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite); } catch { }
                            try { _cts = null; } catch { }
                            if (t.IsFaulted)
                            {
                                try { System.Diagnostics.Debug.WriteLine($"MainViewModel: polling task faulted: {t.Exception?.GetBaseException()?.Message}"); } catch { }
                            }
                        }, TaskScheduler.Default);

                        // return from StartAsync immediately (polling runs in background)
                        break;

                        // local helper to avoid repetitive long lambda signature
                        async Task _polling_service(CancellationToken token)
                        {
                            const int workerCount = 2;
                            var t1 = _pollingService.StartAsync(_stocks, token, workerIndex: 0, workerCount: workerCount);
                            var t2 = Task.Run(async () =>
                            {
                                try { await Task.Delay(1500, token).ConfigureAwait(false); } catch { }
                                if (token.IsCancellationRequested) return;
                                await _pollingService.StartAsync(_stocks, token, workerIndex: 1, workerCount: workerCount).ConfigureAwait(false);
                            }, token);

                            await Task.WhenAll(t1, t2).ConfigureAwait(false);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // cancellation requested -> exit
                        try { _pollWatchdogTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite); } catch { }
                        break;
                    }
                    catch (Exception ex)
                    {
                        // unexpected crash in polling service: log, recreate service and retry
                        System.Diagnostics.Debug.WriteLine($"MainViewModel: polling crashed ({ex.Message}), restarting (attempt {restartCount + 1})");
                        try
                        {
                            if (_pollingService != null)
                            {
                                try { _pollingService.Dispose(); } catch { }
                            }
                        }
                        catch { }

                        try
                        {
                            var provider = App.Services.GetRequiredService<ChartDataProvider>();
                            _pollingService = new PricePollingService(provider, _repository);
                            try { _pollingService.CycleCompleted += OnPollingCycleCompleted; } catch { }
                            try { _pollingService.PollAttempted += OnPollAttempted; } catch { }
                        }
                        catch (Exception ex2)
                        {
                            System.Diagnostics.Debug.WriteLine($"MainViewModel: failed to recreate polling service: {ex2.Message}");
                        }

                        restartCount++;
                        try { await Task.Delay(5000, runCts.Token); } catch { }
                        continue;
                    }
                }
            }
            finally
            {
                // if we never started the pollingTask, clear running state and cts here
                if (_pollingTask == null)
                {
                    _isPollingRunning = false;
                    if (ReferenceEquals(_cts, runCts))
                        _cts = null;
                    try { _pollWatchdogTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite); } catch { }
                    if (ReferenceEquals(_cts, runCts))
                        _cts = null;
                    try { _pollWatchdogTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite); } catch { }
                }
                // otherwise the continuation attached to the background task will handle cleanup
            }
        }

        private void Stop()
        {
            // request cancellation for the polling loop (if running)
            try
            {
                if (_cts != null)
                {
                    try { _cts.Cancel(); } catch { }
                    // do not null _cts here; cleanup task will set it when cleanup completes
                }
            }
            catch { }

            // request cancellation; actual disposal/cleanup is performed asynchronously below
            // signal providers that shutdown is requested so they abort gracefully
            try { TradeMVVM.Trading.Services.Providers.GettexProvider.IsShuttingDown = true; } catch { }

            // Asynchronous cleanup: request cancellation, wait for polling loop to stop, then dispose and kill tracked processes if needed
            Task.Run(async () =>
            {
                try { _pollWatchdogTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite); } catch { }

                // wait for the background polling task to complete (if started) with a timeout
                var timeout = TimeSpan.FromSeconds(15);
                try
                {
                    if (_pollingTask != null)
                    {
                        var finished = await Task.WhenAny(_pollingTask, Task.Delay(timeout));
                        if (finished == _pollingTask)
                        {
                            // polling finished gracefully
                            try { await _pollingTask; } catch { }
                        }
                        else
                        {
                            // timeout -> will perform forced cleanup below
                        }
                    }
                }
                catch { }

                // if polling is no longer running -> graceful cleanup
                if (!_isPollingRunning && (_pollingTask == null || _pollingTask.IsCompleted))
                {
                    try { _pollingService?.Dispose(); } catch { }
                    try { TradeMVVM.Trading.Services.Providers.GettexProvider.CleanupRegisteredDrivers(force: false); } catch { }
                }
                else
                {
                    // forced fallback: try to dispose and then aggressively kill leftover processes
                    try { _pollingService?.Dispose(); } catch { }
                    try { TradeMVVM.Trading.Services.Providers.GettexProvider.CleanupRegisteredDrivers(force: true); } catch { }
                    try
                    {
                        foreach (var p in System.Diagnostics.Process.GetProcessesByName("chrome"))
                        {
                            try { p.Kill(); } catch { }
                        }
                    }
                    catch { }
                }

                // clear cancellation source
                try { _cts?.Cancel(); } catch { }
                _cts = null;

                // ensure StartCommand CanExecute state is refreshed
                try
                {
                    var arc = StartCommand as AsyncRelayCommand;
                    if (arc != null)
                        arc.RefreshCanExecute();
                }
                catch { }
            });
        }


        private void LoadFromDb()
        {
            var all = _repository.LoadAll();

            foreach (var p in all)
            {
                if (string.IsNullOrWhiteSpace(p.ISIN))
                    continue;

                if (!PriceHistory.ContainsKey(p.ISIN))
                    PriceHistory[p.ISIN] =
                        new List<Tuple<DateTime, double>>();

                // load percent values (not raw prices) into the in-memory history
                PriceHistory[p.ISIN]
                    .Add(new Tuple<DateTime, double>(p.Time, p.Percent));
            }

            // set watermark so RefreshFromDb only loads new points from now on
            if (all.Count > 0)
                _lastDbLoadTime = all[all.Count - 1].Time;
        }

        public void Dispose()
        {
            _cts?.Cancel();
            _pollingService?.Dispose();
            try { _serverHeartbeatTimer?.Stop(); } catch { }
            try { _dbRefreshTimer?.Stop(); } catch { }
        }

        /// <summary>
        /// Incremental DB refresh: loads only new rows since the last call and appends
        /// them to PriceHistory. Called by the MainWindow display timer every second.
        /// Also triggers an incremental update of the holdings list for any ISINs that
        /// received new data so that prices and provider times stay current.
        /// </summary>
        public void RefreshFromDb()
        {
            try
            {
                try { System.Diagnostics.Debug.WriteLine($"RefreshFromDb: lastDbLoadTime={_lastDbLoadTime:O}"); } catch { }
                var newPoints = _repository.LoadSince(_lastDbLoadTime);
                try { System.Diagnostics.Debug.WriteLine($"RefreshFromDb: loaded {newPoints?.Count ?? 0} new points"); } catch { }
                if (newPoints == null || newPoints.Count == 0)
                {
                    // fallback: if in-memory history is empty, perform a full load to populate UI (handles startup mismatch)
                    try
                    {
                        if ((PriceHistory == null) || PriceHistory.Keys.Count == 0)
                        {
                            try { System.Diagnostics.Debug.WriteLine("RefreshFromDb: performing full LoadAll fallback"); } catch { }
                            var all = _repository.LoadAll();
                            if (all != null && all.Count > 0)
                            {
                                lock (PriceHistory)
                                {
                                    foreach (var p in all)
                                    {
                                        if (string.IsNullOrWhiteSpace(p.ISIN)) continue;
                                        if (!PriceHistory.ContainsKey(p.ISIN)) PriceHistory[p.ISIN] = new List<Tuple<DateTime, double>>();
                                        PriceHistory[p.ISIN].Add(new Tuple<DateTime, double>(p.Time, p.Percent));
                                    }
                                }
                                _lastDbLoadTime = all[all.Count - 1].Time;
                                try { _lastBatchKeys = new HashSet<string>(all.Select(p => string.Concat(p.ISIN, "|", p.Time.Ticks.ToString(), "|", p.Price.ToString("R", System.Globalization.CultureInfo.InvariantCulture), "|", p.Percent.ToString("R", System.Globalization.CultureInfo.InvariantCulture)))); } catch { }
                            }
                        }
                    }
                    catch { }
                    return;
                }

                var changedIsins = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var currentBatchKeys = new HashSet<string>(StringComparer.Ordinal);

                lock (PriceHistory)
                {
                    foreach (var p in newPoints)
                    {
                        if (string.IsNullOrWhiteSpace(p.ISIN))
                            continue;

                        // deterministic key for de-duplication in the overlap window
                        var providerTimeTicks = p.ProviderTime.HasValue ? p.ProviderTime.Value.Ticks.ToString() : "";
                        var key = string.Concat(
                            p.ISIN, "|",
                            p.Time.Ticks.ToString(), "|",
                            p.Price.ToString("R", System.Globalization.CultureInfo.InvariantCulture), "|",
                            p.Percent.ToString("R", System.Globalization.CultureInfo.InvariantCulture), "|",
                            providerTimeTicks);
                        currentBatchKeys.Add(key);

                        // skip points already processed in the previous batch (overlap from >=)
                        if (_lastBatchKeys.Contains(key))
                            continue;

                        if (!PriceHistory.ContainsKey(p.ISIN))
                            PriceHistory[p.ISIN] = new List<Tuple<DateTime, double>>();

                        PriceHistory[p.ISIN].Add(new Tuple<DateTime, double>(p.Time, p.Percent));
                        changedIsins.Add(p.ISIN);
                    }
                }

                // advance watermark to the latest loaded time
                _lastDbLoadTime = newPoints[newPoints.Count - 1].Time;
                _lastBatchKeys = currentBatchKeys;

                // update holdings rows for the ISINs that received new prices
                if (changedIsins.Count > 0 && _holdingsReport != null)
                {
                    try { _holdingsReport.UpdatePrices(changedIsins.ToList()); } catch { }
                }
            }
            catch { }
        }

        private void CheckPollingWatchdog()
        {
            try
            {
                if (_cts == null) return;
                var elapsed = DateTime.UtcNow - _lastPriceUpdate;
                if (elapsed > _pollWatchdogTimeout)
                {
                    _missedCycleCount++;
                    var lastIsin = string.IsNullOrWhiteSpace(_lastPolledIsin) ? "n/a" : _lastPolledIsin;
                    try { System.Diagnostics.Trace.TraceWarning($"Polling watchdog: no price updates for {elapsed.TotalSeconds}s (missed {_missedCycleCount}) - last ISIN: {lastIsin} - no restart, continue polling."); } catch { }
                }
                else
                {
                    _missedCycleCount = 0;
                }
            }
            catch { }
        }

    }



}
