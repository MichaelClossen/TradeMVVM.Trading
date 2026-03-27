using Microsoft.Data.Sqlite;
using Microsoft.Win32;
using ScottPlot;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Threading;

namespace TradeMVVM.ReadHoldings
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        // holds the loaded CSV lines
        private List<string> _csvLines = new List<string>();
        // simple held ISINs (kept for compatibility)
        private List<string> _heldIsins = new List<string>();
        // Holdings computed after FIFO processing
        private List<HoldingRow> _holdings = new List<HoldingRow>();
        private readonly object _holdingsLock = new object();
        private Timer? _dbTimer;
        // UI-timer to refresh holdings from DB periodically
        private DispatcherTimer? _refreshTimer;
        // timer to refresh header totals from DB every 60 seconds
        private DispatcherTimer? _totalValuesTimer;
        // periodic reapply timer to enforce stored X limits every N seconds (workaround for Hot Reload / ScottPlot resets)
        private DispatcherTimer? _reapplyPeriodicTimer;
        // ScottPlot control for total value history (bound from XAML)
        // prevent DB writes on startup when loading existing data
        private bool _allowDbWrites = false;
        private readonly string _dbPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "Trade", "trading.db");
        // zoom state for Ctrl+MouseWheel zooming
        private double _zoom = 1.0;
        private readonly double _minZoom = 0.5;
        private readonly double _maxZoom = 3.0;
        private readonly double _zoomStep = 0.05;
        private double _baseFontSize = 12.0;
        // cached total-value history times loaded for the plot (used to find nearest point for deletion)
        private List<DateTime> _totalValueTimes = new List<DateTime>();
        // cached rowids aligned with _totalValueTimes when available - prefer deletion by rowid for determinism
        private List<long?> _totalValueRowIds = new List<long?>();
        private double _totalValueMedianDeltaDays = 1.0;
        // whether the user has interacted with the plot to change X-axis view (pan/zoom)
        private bool _plotUserZoomed = false;
        // temporary suppression flag to avoid automatic reapply/refresh overwriting user-forced changes
        private bool _suppressAutoApply = false;
        private DispatcherTimer? _autoApplySuppressTimer;
        // explicit store of last user-set X limits when they zoom/pan so we can reliably restore/shift them
        private double? _userXMin = null;
        private double? _userXMax = null;
        // whether initial plot render has completed (used to detect "no previous limits" at first load)
        private bool _plotInitialized = false;
        // UDP listener for Firefox extension notifications
        private System.Net.Sockets.UdpClient? _udpListener = null;
        private DispatcherTimer? _udpStatusTimer = null;
        private readonly int _udpPort = 54123;
        // reference to checkbox in XAML to enable/disable automatic X-axis shifting
        private bool _autoShiftEnabled = false;
        // current search filter text (null = no filter)
        private string? _currentSearch = null;

        public MainWindow()
        {
            InitializeComponent();
            // ensure CSV active table exists as early as possible
            try { EnsureCsvActiveTableExists(_dbPath); } catch { }

            // attempt to read active CSV from shared DB and show filename in UI
            try
            {
                var active = GetActiveCsvFromDb(_dbPath);
                if (!string.IsNullOrWhiteSpace(active))
                    try { if (TxtPath != null) TxtPath.Text = active; } catch { }
            }
            catch { }

            try { _baseFontSize = DgHoldings.FontSize; DgHoldings.PreviewMouseWheel += DgHoldings_PreviewMouseWheel; } catch { }
            // apply any restored zoom (RestoreLayout may have set _zoom before Initialize completed)
            try { if (DgScale != null) { DgScale.ScaleX = _zoom; DgScale.ScaleY = _zoom; } } catch { }
            try { LoadLastHoldings(); } catch { }
            // start periodic refresh from DB every 15 seconds to keep purchase/today values up-to-date
            try { StartRefreshTimer(); } catch { }
            // perform one immediate refresh from DB at startup
            try { RefreshTimer_Tick(null, EventArgs.Empty); } catch { }
            // start periodic header totals refresh from NEW_TotalValues every 60 seconds
            try { StartTotalValuesTimer(); } catch { }
            // render initial total value history once controls are loaded
            try { this.Loaded += (s, e) => { LoadAndRenderTotalValueHistory(); }; } catch { }
            try { PlotTotalValueHistory.PreviewMouseWheel += PlotTotalValueHistory_PreviewMouseWheel; } catch { }
            try { PlotTotalValueHistory.MouseDoubleClick += PlotTotalValueHistory_MouseDoubleClick; } catch { }
            try { PlotTotalValueHistory.PreviewMouseLeftButtonDown += PlotTotalValueHistory_PreviewMouseLeftButtonDown; } catch { }
            // MouseWheel handler will be hooked after constructor ends (method defined below)
            try { PlotTotalValueHistory.MouseLeftButtonUp += PlotTotalValueHistory_MouseLeftButtonUp; } catch { }
            try { PlotTotalValueHistory.PreviewMouseDown += PlotTotalValueHistory_PreviewMouseDown; } catch { }
            try { PlotTotalValueHistory.PreviewMouseUp += PlotTotalValueHistory_PreviewMouseUp; } catch { }
            // wire up interval checkbox events after InitializeComponent to ensure FindName works
            try
            {
                var names = new[] { "ChkInterval5Min", "ChkInterval15Min", "ChkInterval30Min", "ChkInterval1H", "ChkInterval6H", "ChkInterval1D", "ChkInterval1W", "ChkInterval1M", "ChkInterval1Y" };
                foreach (var n in names)
                {
                    try
                    {
                        var cb = this.FindName(n) as CheckBox;
                        if (cb != null)
                        {
                            cb.Checked += IntervalCheckbox_Checked;
                            cb.Unchecked += IntervalCheckbox_Unchecked;
                        }
                    }
                    catch { }
                }
                // wire up auto-shift checkbox to allow immediate re-enabling of automatic shifting after user zoom
                try
                {
                    var auto = this.FindName("ChkAutoShiftX") as CheckBox;
                    if (auto != null)
                    {
                        auto.Checked += (s, ev) => { try { AutoShiftCheckbox_Checked(s, ev); } catch { } };
                        auto.Unchecked += (s, ev) => { try { AutoShiftCheckbox_Unchecked(s, ev); } catch { } };
                        // initialize internal flag from control state
                        try { _autoShiftEnabled = auto.IsChecked == true; } catch { _autoShiftEnabled = false; }
                    }
                }
                catch { }
                // Default to 15min interval via checkbox on startup (handlers wired above)
                try
                {
                    var cb15 = this.FindName("ChkInterval15Min") as CheckBox;
                    if (cb15 != null)
                        cb15.IsChecked = true;
                }
                catch { }
            }
            catch { }
            try { StartClock(); } catch { }
            try { StartPeriodicReapply(); } catch { }
            try { StartUdpListener(); } catch { }
            try { StartUdpStatusTimer(); } catch { }
            try { StartHttpListener(); } catch { }
            // Ensure the NEW_CSV_ACTIVE table exists on startup so other tools can read active CSV state
            try { EnsureCsvActiveTableExists(_dbPath); } catch { }

            // ensure columns are sized to content on first load
            try { this.Loaded += MainWindow_Loaded; } catch { }
            // wire up search textbox if present
            try
            {
                var tb = this.FindName("TxtSearch") as TextBox;
                if (tb != null)
                {
                    tb.TextChanged += TxtSearch_TextChanged;
                    tb.KeyDown += TxtSearch_KeyDown;
                }
                var btn = this.FindName("BtnClearSearch") as Button;
                if (btn != null) btn.Click += BtnClearSearch_Click;
            }
            catch { }
        }
        private void AutoShiftCheckbox_Checked(object sender, RoutedEventArgs e)
        {
            try
            {
                _plotUserZoomed = false;
                _autoShiftEnabled = true;

                // optional: sofort neu zeichnen
                LoadAndRenderTotalValueHistory();
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }
        }

        private void AutoShiftCheckbox_Unchecked(object sender, RoutedEventArgs e)
        {
            try
            {
                // Benutzer übernimmt Kontrolle
                _plotUserZoomed = true;
                _autoShiftEnabled = false;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }
        }

        // Search/filter the holdings shown in the DataGrid
        private void TxtSearch_TextChanged(object? sender, TextChangedEventArgs e)
        {
            try
            {
                _currentSearch = (this.FindName("TxtSearch") as TextBox)?.Text;
                UpdateItemsSourcePreserveSortAndSelection();
            }
            catch { }
        }

        private void TxtSearch_KeyDown(object? sender, KeyEventArgs e)
        {
            try
            {
                if (e.Key == Key.Escape)
                {
                    ClearSearch();
                    e.Handled = true;
                }
            }
            catch { }
        }

        private void BtnClearSearch_Click(object? sender, RoutedEventArgs e)
        {
            try { ClearSearch(); } catch { }
        }

        private void ClearSearch()
        {
            try
            {
                var tb = this.FindName("TxtSearch") as TextBox;
                if (tb != null) tb.Text = string.Empty;
                _currentSearch = null;
                UpdateItemsSourcePreserveSortAndSelection();
            }
            catch { }
        }

        // Update DataGrid ItemsSource according to current search filter and optionally reapply sorts and restore selection
        private void UpdateItemsSourcePreserveSortAndSelection(List<SortDescription>? sorts = null, string? selectedIsin = null)
        {
            try
            {
                if (DgHoldings == null) return;

                // Always bind to the master _holdings list so we can use CollectionView.Filter for filtering
                if (!object.ReferenceEquals(DgHoldings.ItemsSource, _holdings))
                {
                    DgHoldings.ItemsSource = null;
                    DgHoldings.ItemsSource = _holdings;
                }

                var view = CollectionViewSource.GetDefaultView(DgHoldings.ItemsSource);
                if (view == null) return;

                // apply sort descriptions if provided
                try
                {
                    if (sorts != null && sorts.Count > 0)
                    {
                        view.SortDescriptions.Clear();
                        foreach (var sd in sorts) view.SortDescriptions.Add(sd);
                    }
                }
                catch { }

                // apply filter based on current search
                try
                {
                    if (string.IsNullOrWhiteSpace(_currentSearch))
                    {
                        view.Filter = null;
                    }
                    else
                    {
                        var q = _currentSearch.Trim();
                        // support simple "not" operator: e.g. "not Long" excludes items that contain "Long"
                        var includeTokens = new List<string>();
                        var excludeTokens = new List<string>();
                        var parts = q.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        for (int i = 0; i < parts.Length; i++)
                        {
                            var p = parts[i];
                            if (string.Equals(p, "not", StringComparison.OrdinalIgnoreCase) && i + 1 < parts.Length)
                            {
                                var nex = parts[i + 1];
                                if (!string.IsNullOrWhiteSpace(nex)) excludeTokens.Add(nex);
                                i++; // skip next
                            }
                            else
                            {
                                includeTokens.Add(p);
                            }
                        }

                        view.Filter = obj =>
                        {
                            try
                            {
                                if (obj is HoldingRow h)
                                {
                                    string combined = (h.Isin ?? string.Empty) + " " + (h.Name ?? string.Empty) + " " + (h.Provider ?? string.Empty);
                                    // check excludes first
                                    foreach (var ex in excludeTokens)
                                    {
                                        if (!string.IsNullOrEmpty(ex) && combined.IndexOf(ex, StringComparison.OrdinalIgnoreCase) >= 0) return false;
                                    }
                                    // require all include tokens to be present (AND semantics)
                                    foreach (var inc in includeTokens)
                                    {
                                        if (string.IsNullOrEmpty(inc)) continue;
                                        if (combined.IndexOf(inc, StringComparison.OrdinalIgnoreCase) < 0) return false;
                                    }
                                    return true;
                                }
                            }
                            catch { }
                            return false;
                        };
                    }
                    view.Refresh();
                }
                catch { }

                // restore selection by ISIN if requested
                try
                {
                    if (!string.IsNullOrWhiteSpace(selectedIsin))
                    {
                        var item = _holdings.FirstOrDefault(h => string.Equals(h.Isin, selectedIsin, StringComparison.OrdinalIgnoreCase));
                        if (item != null) DgHoldings.SelectedItem = item;
                    }
                }
                catch { }
            }
            catch { }
        }



        // Interval checkbox handlers (right area 1)
        private bool _suspendIntervalHandlers = false;
        // when user selects an interval, remember it so render logic can respect it across reloads
        private TimeSpan? _forcedInterval = null;
        private bool _forcedIntervalUserSet = false;

        private void IntervalCheckbox_Checked(object sender, RoutedEventArgs e)
        {
            try
            {
                if (!(sender is CheckBox cb)) return;
                var names = new[] { "ChkInterval5Min", "ChkInterval15Min", "ChkInterval30Min", "ChkInterval1H", "ChkInterval6H", "ChkInterval1D", "ChkInterval1W", "ChkInterval1M", "ChkInterval1Y" };
                try
                {
                    _suspendIntervalHandlers = true;
                    foreach (var n in names)
                    {
                        try
                        {
                            if (n == cb.Name) continue;
                            var other = this.FindName(n) as CheckBox;
                            if (other != null && other.IsChecked == true) other.IsChecked = false;
                        }
                        catch { }
                    }
                }
                finally { _suspendIntervalHandlers = false; }

                var ts = GetTimeSpanForCheckbox(cb.Name);
                _forcedInterval = ts;
                _forcedIntervalUserSet = true;
                try { System.Diagnostics.Debug.WriteLine($"TVH: Interval checkbox checked {cb.Name}, timespan={ts}"); } catch { }
                try
                {
                    this.Dispatcher.BeginInvoke(new Action(() =>
                    {
                        try { var info = this.FindName("TxtInfo") as TextBlock; if (info != null) info.Text = $"Intervall gesetzt: {cb.Content} @ {DateTime.Now:HH:mm:ss}"; } catch { }
                    }));
                }
                catch { }
                // Apply interval immediately so tick generator and axis update take effect at once
                try { SuppressAutoApplyFor(1200); } catch { }
                try { ApplyXAxisInterval(ts); } catch { }
                // Refresh data/plot to pick up any new data
                try { LoadAndRenderTotalValueHistory(); } catch { }
                // Schedule re-apply in case data is not yet available; this will re-run ApplyXAxisInterval when data appears
                try { ScheduleApplyInterval(ts); } catch { }
            }
            catch { }
        }

        private void IntervalCheckbox_Unchecked(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_suspendIntervalHandlers) return;

                // Delay processing slightly to allow the Checked handler from the newly checked box
                // to run first. This avoids a brief state where no checkbox is checked and the
                // Unchecked handler clears the forced interval before the new Checked handler sets it.
                try
                {
                    this.Dispatcher.BeginInvoke(new Action(() =>
                    {
                        try
                        {
                            var names = new[] { "ChkInterval5Min", "ChkInterval15Min", "ChkInterval30Min", "ChkInterval1H", "ChkInterval6H", "ChkInterval1D", "ChkInterval1W", "ChkInterval1M", "ChkInterval1Y" };
                            bool any = false;
                            foreach (var n in names)
                            {
                                try { var other = this.FindName(n) as CheckBox; if (other != null && other.IsChecked == true) { any = true; break; } } catch { }
                            }
                            if (!any)
                            {
                                _forcedInterval = null;
                                _forcedIntervalUserSet = false;
                                try { SuppressAutoApplyFor(800); } catch { }
                                try { System.Diagnostics.Debug.WriteLine("TVH: Interval checkbox unchecked, clearing forced interval"); } catch { }
                                try
                                {
                                    try { var info = this.FindName("TxtInfo") as TextBlock; if (info != null) info.Text = $"Intervall gelöscht @ {DateTime.Now:HH:mm:ss}"; } catch { }
                                }
                                catch { }
                                try { ApplyXAxisInterval(null); } catch { }
                                try { LoadAndRenderTotalValueHistory(); } catch { }
                                try { ScheduleApplyInterval(null); } catch { }
                            }
                        }
                        catch { }
                    }), System.Windows.Threading.DispatcherPriority.Background);
                }
                catch
                {
                    // fallback to immediate processing on error
                    var names = new[] { "ChkInterval5Min", "ChkInterval15Min", "ChkInterval30Min", "ChkInterval1H", "ChkInterval6H", "ChkInterval1D", "ChkInterval1W", "ChkInterval1M", "ChkInterval1Y" };
                    bool any = false;
                    foreach (var n in names)
                    {
                        try { var other = this.FindName(n) as CheckBox; if (other != null && other.IsChecked == true) { any = true; break; } } catch { }
                    }
                    if (!any)
                    {
                        _forcedInterval = null;
                        _forcedIntervalUserSet = false;
                        try { System.Diagnostics.Debug.WriteLine("TVH: Interval checkbox unchecked, clearing forced interval (fallback)"); } catch { }
                        try { ApplyXAxisInterval(null); } catch { }
                        try { LoadAndRenderTotalValueHistory(); } catch { }
                        try { ScheduleApplyInterval(null); } catch { }
                    }
                }
            }
            catch { }
        }

        // Try to apply the requested interval as soon as data is available.
        // Will poll a few times (200ms intervals) and then apply even if no data.
        private void ScheduleApplyInterval(TimeSpan? ts)
        {
            try
            {
                if (ts == null)
                {
                    // remove forced interval immediately
                    ApplyXAxisInterval(null);
                    return;
                }

                var attempts = 0;
                var timer = new DispatcherTimer();
                timer.Interval = TimeSpan.FromMilliseconds(200);
                timer.Tick += (s, e) =>
                {
                    try
                    {
                        attempts++;
                        if ((_totalValueTimes != null && _totalValueTimes.Count > 0) || attempts >= 10)
                        {
                            try { ApplyXAxisInterval(ts); } catch { }
                            try { timer.Stop(); } catch { }
                        }
                    }
                    catch { try { timer.Stop(); } catch { } }
                };
                timer.Start();
            }
            catch
            {
                ApplyXAxisInterval(ts);
            }
        }

        private TimeSpan? GetTimeSpanForCheckbox(string name)
        {
            return name switch
            {
                "ChkInterval5Min" => TimeSpan.FromMinutes(5),
                "ChkInterval15Min" => TimeSpan.FromMinutes(15),
                "ChkInterval30Min" => TimeSpan.FromMinutes(30),
                "ChkInterval1H" => TimeSpan.FromHours(1),
                "ChkInterval6H" => TimeSpan.FromHours(6),
                "ChkInterval1D" => TimeSpan.FromDays(1),
                "ChkInterval1W" => TimeSpan.FromDays(7),
                "ChkInterval1M" => TimeSpan.FromDays(30),
                "ChkInterval1Y" => TimeSpan.FromDays(365),
                _ => null,
            };
        }

        // Apply a fixed X-axis interval (TimeSpan) or restore default if null
        private void ApplyXAxisInterval(TimeSpan? interval)
        {
            try
            {
                // Ensure UI thread
                this.Dispatcher.Invoke(() =>
                {
                    try
                    {
                        System.Diagnostics.Debug.WriteLine($"TVH: ApplyXAxisInterval start interval={(interval.HasValue ? interval.Value.ToString() : "null")}");
                        if (PlotTotalValueHistory == null) { System.Diagnostics.Debug.WriteLine("TVH: PlotTotalValueHistory is null"); return; }
                        var plt = PlotTotalValueHistory.Plot;
                        if (plt == null) { System.Diagnostics.Debug.WriteLine("TVH: plt is null"); return; }

                        // Use the automatic DateTime tick generator to avoid compatibility issues with ScottPlot versions.
                        try
                        {
                            plt.Axes.Bottom.TickGenerator = new ScottPlot.TickGenerators.DateTimeAutomatic()
                            {
                                LabelFormatter = (DateTime date) => { try { return date.ToString("dd.MM HH:mm"); } catch { return date.ToString(); } }
                            };
                        }
                        catch (Exception ex)
                        {
                            try { System.Diagnostics.Debug.WriteLine("TVH: Failed to set DateTimeAutomatic tick generator: " + ex.Message); } catch { }
                        }

                        // Also adjust X axis limits to show the selected interval if we have data and the view allows shifting.
                        try
                        {
                            // If the user explicitly selected a forced interval via checkbox, always apply it immediately
                            if (_forcedIntervalUserSet && interval.HasValue)
                            {
                                try
                                {
                                    // determine dataMax fallback to now when no data present yet
                                    double dataMax;
                                    double dataMin;
                                    if (_totalValueTimes != null && _totalValueTimes.Count > 0)
                                    {
                                        dataMax = _totalValueTimes.Last().ToOADate();
                                        dataMin = _totalValueTimes.First().ToOADate();
                                    }
                                    else
                                    {
                                        dataMax = DateTime.Now.ToOADate();
                                        dataMin = DateTime.FromOADate(dataMax).AddDays(-1).ToOADate();
                                    }
                                    var spanDays = interval.Value.TotalDays;
                                    var chosenMax = dataMax + 0.05 * spanDays;
                                    var chosenMin = chosenMax - spanDays;
                                    if (chosenMin < dataMin) { chosenMin = dataMin; chosenMax = chosenMin + spanDays; }
                                    plt.Axes.SetLimitsX(chosenMin, chosenMax);
                                    try { _userXMin = chosenMin; _userXMax = chosenMax; _plotUserZoomed = true; } catch { }
                                    try { System.Diagnostics.Debug.WriteLine($"TVH: Forced SetLimitsX applied (user): chosenMin={DateTime.FromOADate(chosenMin):o}..chosenMax={DateTime.FromOADate(chosenMax):o}"); } catch { }
                                }
                                catch (Exception ex) { System.Diagnostics.Debug.WriteLine("TVH: Forced SetLimitsX failed: " + ex.Message); }
                            }
                            else if (interval.HasValue && _totalValueTimes != null && _totalValueTimes.Count > 0)
                            {
                                var dataMax = _totalValueTimes.Last().ToOADate();
                                var dataMin = _totalValueTimes.First().ToOADate();
                                try { System.Diagnostics.Debug.WriteLine($"TVH: totalValueTimes.Count={_totalValueTimes.Count}, dataMin={DateTime.FromOADate(dataMin):o}, dataMax={DateTime.FromOADate(dataMax):o}"); } catch { }
                                var spanDays = interval.Value.TotalDays;
                                var chosenMax = dataMax + 0.05 * spanDays; // place last point ~5% from right edge
                                var chosenMin = chosenMax - spanDays;
                                if (chosenMin < dataMin)
                                {
                                    chosenMin = dataMin;
                                    chosenMax = chosenMin + spanDays;
                                }

                                // respect the "rightmost 5%" rule: only shift if previous view had newest data within rightmost 5% or no valid prev limits
                                try
                                {
                                    var prevMin = plt.Axes.Bottom.Min;
                                    var prevMax = plt.Axes.Bottom.Max;
                                    try { System.Diagnostics.Debug.WriteLine($"TVH: prevAxisMin={prevMin}, prevAxisMax={prevMax}"); } catch { }
                                    bool apply = false;
                                    if (double.IsNaN(prevMin) || double.IsNaN(prevMax) || prevMax <= prevMin) apply = true;
                                    else
                                    {
                                        var prevWidth = prevMax - prevMin;
                                        if (prevWidth > 0 && dataMax > prevMax - 0.05 * prevWidth) apply = true;
                                    }

                                    // if the interval was explicitly chosen by the user, force apply so checkbox selection takes effect
                                    try { if (_forcedIntervalUserSet) apply = true; } catch { }

                                    if (apply)
                                    {
                                        try
                                        {
                                            plt.Axes.SetLimitsX(chosenMin, chosenMax);
                                            // remember these as user limits so subsequent plot renders do not overwrite them
                                            try { _userXMin = chosenMin; _userXMax = chosenMax; _plotUserZoomed = true; } catch { }
                                            try { System.Diagnostics.Debug.WriteLine($"TVH: SetLimitsX applied: chosenMin={DateTime.FromOADate(chosenMin):o}..chosenMax={DateTime.FromOADate(chosenMax):o}"); } catch { }
                                            try { System.Diagnostics.Debug.WriteLine($"TVH: after set prevAxisMin={plt.Axes.Bottom.Min}, prevAxisMax={plt.Axes.Bottom.Max}"); } catch { }
                                        }
                                        catch (Exception ex) { System.Diagnostics.Debug.WriteLine("TVH: SetLimitsX failed: " + ex.Message); }
                                    }
                                }
                                catch (Exception ex) { System.Diagnostics.Debug.WriteLine("TVH: ApplyXAxisInterval limits logic failed: " + ex.Message); }
                            }
                        }
                        catch (Exception ex) { System.Diagnostics.Debug.WriteLine("TVH: ApplyXAxisInterval interval limits failed: " + ex.Message); }

                        try
                        {
                            PlotTotalValueHistory.Refresh();
                            System.Diagnostics.Debug.WriteLine("TVH: PlotTotalValueHistory.Refresh called");
                        }
                        catch (Exception ex) { System.Diagnostics.Debug.WriteLine("TVH: Refresh failed: " + ex.Message); }
                        System.Diagnostics.Debug.WriteLine("TVH: ApplyXAxisInterval end");
                    }
                    catch (Exception ex)
                    {
                        try { System.Diagnostics.Debug.WriteLine("TVH: ApplyXAxisInterval outer error: " + ex.Message); } catch { }
                    }
                });
            }
            catch { }
        }

        // Mouse wheel zoom handler for the ScottPlot WpfPlot control
        private void PlotTotalValueHistory_PreviewMouseWheel(object? sender, MouseWheelEventArgs e)
        {
            try
            {
                if (PlotTotalValueHistory == null) return;

                // get mouse position and convert to data coordinates
                var pos = e.GetPosition(PlotTotalValueHistory);
                if (!TryGetMouseCoordinates(PlotTotalValueHistory, pos, out var mouseX, out var mouseY))
                {
                    // fallback: use center of current axis range
                    try
                    {
                        mouseX = (PlotTotalValueHistory.Plot.Axes.Bottom.Min + PlotTotalValueHistory.Plot.Axes.Bottom.Max) / 2.0;
                        mouseY = (PlotTotalValueHistory.Plot.Axes.Left.Min + PlotTotalValueHistory.Plot.Axes.Left.Max) / 2.0;
                    }
                    catch { return; }
                }

                // zoom factor: <1 zooms in, >1 zooms out
                double factor = e.Delta > 0 ? 1.0 / 1.1 : 1.1;

                var mods = Keyboard.Modifiers;

                double minX = PlotTotalValueHistory.Plot.Axes.Bottom.Min; double maxX = PlotTotalValueHistory.Plot.Axes.Bottom.Max;
                double minY = PlotTotalValueHistory.Plot.Axes.Left.Min; double maxY = PlotTotalValueHistory.Plot.Axes.Left.Max;

                // helper to scale limits around center
                static (double, double) ScaleAround(double min, double max, double center, double scale)
                {
                    var left = center - (center - min) * scale;
                    var right = center + (max - center) * scale;
                    return (left, right);
                }

                try
                {
                    if (mods == ModifierKeys.None)
                    {
                        // zoom X only
                        var (nminX, nmaxX) = ScaleAround(minX, maxX, mouseX, factor);
                        PlotTotalValueHistory.Plot.Axes.SetLimitsX(nminX, nmaxX);
                        try { _userXMin = nminX; _userXMax = nmaxX; } catch { }
                    }
                    else if ((mods & ModifierKeys.Control) == ModifierKeys.Control && (mods & ModifierKeys.Shift) == ModifierKeys.Shift)
                    {
                        // Ctrl+Shift -> Y only
                        var (nminY, nmaxY) = ScaleAround(minY, maxY, mouseY, factor);
                        PlotTotalValueHistory.Plot.Axes.SetLimitsY(nminY, nmaxY);
                    }
                    else if ((mods & ModifierKeys.Control) == ModifierKeys.Control)
                    {
                        // Ctrl -> both axes
                        var (nminX, nmaxX) = ScaleAround(minX, maxX, mouseX, factor);
                        var (nminY, nmaxY) = ScaleAround(minY, maxY, mouseY, factor);
                        PlotTotalValueHistory.Plot.Axes.SetLimitsX(nminX, nmaxX);
                        PlotTotalValueHistory.Plot.Axes.SetLimitsY(nminY, nmaxY);
                        try { _userXMin = nminX; _userXMax = nmaxX; } catch { }
                    }
                    else
                    {
                        // other modifiers: default to X zoom
                        var (nminX, nmaxX) = ScaleAround(minX, maxX, mouseX, factor);
                        PlotTotalValueHistory.Plot.Axes.SetLimitsX(nminX, nmaxX);
                        try { _userXMin = nminX; _userXMax = nmaxX; } catch { }
                    }

                    PlotTotalValueHistory.Refresh();
                    // note: user performed a zoom interaction; preserve X limits on subsequent renders
                    try { _plotUserZoomed = true; } catch { }
                    e.Handled = true;
                }
                catch { }
            }
            catch { }
        }

        private bool TryGetMouseCoordinates(ScottPlot.WPF.WpfPlot plot, System.Windows.Point pos, out double x, out double y)
        {
            x = double.NaN; y = double.NaN;
            try
            {
                var plotObj = plot?.Plot;
                if (plotObj != null)
                {
                    var getCoordinates = plotObj.GetType().GetMethods()
                        .FirstOrDefault(m => string.Equals(m.Name, "GetCoordinates", StringComparison.Ordinal) && m.GetParameters().Length == 2);

                    if (getCoordinates != null)
                    {
                        object res = null;
                        try { res = getCoordinates.Invoke(plotObj, new object[] { pos.X, pos.Y }); } catch { }
                        if (res != null)
                        {
                            var t = res.GetType();
                            var p1 = t.GetProperty("Item1");
                            var p2 = t.GetProperty("Item2");
                            if (p1 != null && p2 != null)
                            {
                                try
                                {
                                    x = Convert.ToDouble(p1.GetValue(res));
                                    y = Convert.ToDouble(p2.GetValue(res));
                                    return true;
                                }
                                catch { }
                            }
                        }
                    }
                }

                // fallback: simple linear mapping using axis limits and plot size
                try
                {
                    double minX = plot.Plot.Axes.Bottom.Min;
                    double maxX = plot.Plot.Axes.Bottom.Max;
                    double minY = plot.Plot.Axes.Left.Min;
                    double maxY = plot.Plot.Axes.Left.Max;
                    double w = plot.ActualWidth; double h = plot.ActualHeight;
                    if (w <= 0 || h <= 0 || double.IsNaN(minX) || double.IsNaN(maxX) || double.IsNaN(minY) || double.IsNaN(maxY)) return false;
                    x = minX + (pos.X / w) * (maxX - minX);
                    // invert Y: screen Y increases downwards
                    y = maxY - (pos.Y / h) * (maxY - minY);
                    return true;
                }
                catch { }
            }
            catch { }
            return false;
        }

        // handle mouse up to detect user interaction end (panning via mouse drag)
        private void PlotTotalValueHistory_MouseLeftButtonUp(object? sender, MouseButtonEventArgs e)
        {
            try
            {
                // any mouse interaction should mark that the user has adjusted the view
                _plotUserZoomed = true;
                // capture current axis limits as user's preferred limits
                try
                {
                    if (PlotTotalValueHistory != null)
                    {
                        var plt = PlotTotalValueHistory.Plot;
                        _userXMin = plt.Axes.Bottom.Min;
                        _userXMax = plt.Axes.Bottom.Max;
                    }
                }
                catch { }
            }
            catch { }
        }

        private void PlotTotalValueHistory_PreviewMouseDown(object? sender, MouseButtonEventArgs e)
        {
            try
            {
                // user starts interacting; do not auto-shift while interactions are active
                _plotUserZoomed = true;
            }
            catch { }
        }

        // Fallback handler: sometimes WpfPlot does not raise MouseDoubleClick reliably.
        // Detect double-click ourselves on PreviewMouseLeftButtonDown combined with Shift key.
        private DateTime _lastPreviewLeftDown = DateTime.MinValue;
        private void PlotTotalValueHistory_PreviewMouseLeftButtonDown(object? sender, MouseButtonEventArgs e)
        {
            try
            {
                // only consider left button
                if (e == null || e.ChangedButton != MouseButton.Left) return;

                var now = DateTime.Now;
                var interval = now - _lastPreviewLeftDown;
                _lastPreviewLeftDown = now;

                // treat as double-click if within 500ms
                if (interval.TotalMilliseconds <= 500)
                {
                    // require Shift pressed
                    if ((Keyboard.Modifiers & ModifierKeys.Shift) == ModifierKeys.Shift)
                    {
                        // invoke same logic as MouseDoubleClick
                        try { PlotTotalValueHistory_MouseDoubleClick(sender, e); } catch { }
                    }
                }
            }
            catch { }
        }

        private void PlotTotalValueHistory_PreviewMouseUp(object? sender, MouseButtonEventArgs e)
        {
            try
            {
                // interaction ended: store current limits
                try
                {
                    if (PlotTotalValueHistory != null)
                    {
                        var plt = PlotTotalValueHistory.Plot;
                        _userXMin = plt.Axes.Bottom.Min;
                        _userXMax = plt.Axes.Bottom.Max;
                    }
                }
                catch { }
            }
            catch { }
        }

        // Handle shift-doubleclick on plot: delete corresponding row from NEW_TotalValues
        private void PlotTotalValueHistory_MouseDoubleClick(object? sender, MouseButtonEventArgs e)
        {
            try
            {
                System.Diagnostics.Debug.WriteLine("TVH: MouseDoubleClick fired");
                // require Shift to be pressed (allow other modifiers as well) - check mask
                if (e == null || (Keyboard.Modifiers & ModifierKeys.Shift) == 0) return;
                if (PlotTotalValueHistory == null) return;

                var pos = e.GetPosition(PlotTotalValueHistory);
                if (!TryGetMouseCoordinates(PlotTotalValueHistory, pos, out var x, out var y)) return;

                // x is OADate
                var clicked = DateTime.FromOADate(x);

                // find nearest cached time
                if (_totalValueTimes == null || _totalValueTimes.Count == 0) return;
                int bestIdx = -1; double bestDist = double.MaxValue;
                for (int i = 0; i < _totalValueTimes.Count; i++)
                {
                    var d = Math.Abs((_totalValueTimes[i] - clicked).TotalSeconds);
                    if (d < bestDist) { bestDist = d; bestIdx = i; }
                }

                if (bestIdx < 0) return;

                var targetTime = _totalValueTimes[bestIdx];
                var targetRowId = (_totalValueRowIds != null && bestIdx < _totalValueRowIds.Count) ? _totalValueRowIds[bestIdx] : null;

                // Confirm with user
                var res = MessageBox.Show(this, $"Zeile mit Zeit {targetTime:yyyy-MM-dd HH:mm:ss} aus NEW_TotalValues löschen?", "Löschen bestätigen", MessageBoxButton.YesNo, MessageBoxImage.Question);
                if (res != MessageBoxResult.Yes) return;

                // delete from DB by matching best-effort on timestamp column using tolerant matching
                System.Threading.Tasks.Task.Run(() =>
                {
                    try
                    {
                        var cs = new SqliteConnectionStringBuilder { DataSource = _dbPath }.ToString();
                        using var conn = new SqliteConnection(cs);
                        conn.Open();

                        // detect datetime-like column name
                        string timeCol = null;
                        using (var pragma = conn.CreateCommand())
                        {
                            pragma.CommandText = "PRAGMA table_info(NEW_TotalValues);";
                            using var r = pragma.ExecuteReader();
                            var cols = new List<string>();
                            while (r.Read()) { try { cols.Add(r.GetString(1)); } catch { } }
                            var candidates = new[] { "LastUpdated", "Created", "Time", "Timestamp", "CreatedAt" };
                            timeCol = cols.FirstOrDefault(c => candidates.Any(cc => string.Equals(c, cc, StringComparison.OrdinalIgnoreCase))) ?? cols.FirstOrDefault();
                        }

                        if (string.IsNullOrWhiteSpace(timeCol)) return;

                        int affected = 0;
                        // prefer deletion by rowid when available
                        if (targetRowId.HasValue)
                        {
                            try
                            {
                                using var cmdRow = conn.CreateCommand();
                                cmdRow.CommandText = "DELETE FROM NEW_TotalValues WHERE rowid = $id;";
                                cmdRow.Parameters.AddWithValue("$id", targetRowId.Value);
                                affected = cmdRow.ExecuteNonQuery();
                            }
                            catch { affected = 0; }
                        }

                        // fallback: perform tolerant delete: try exact text match first, then numeric epoch match +/- median delta
                        if (affected == 0)
                        {
                            using var cmd = conn.CreateCommand();
                            // try exact text
                            cmd.CommandText = $"DELETE FROM NEW_TotalValues WHERE \"{timeCol}\" = $t;";
                            cmd.Parameters.AddWithValue("$t", targetTime.ToString("yyyy-MM-dd HH:mm:ss"));
                            try { affected = cmd.ExecuteNonQuery(); } catch { affected = 0; }

                            if (affected == 0)
                            {
                                // try matching by epoch milliseconds tolerance
                                try
                                {
                                    var ms = new DateTimeOffset(targetTime).ToUnixTimeMilliseconds();
                                    var tol = (long)Math.Max(1, Math.Round(_totalValueMedianDeltaDays * 24 * 3600 * 1000));
                                    // delete rows where numeric value is within +/- tol
                                    cmd.Parameters.Clear();
                                    cmd.CommandText = $"DELETE FROM NEW_TotalValues WHERE CAST(\"{timeCol}\" AS INTEGER) BETWEEN $low AND $high;";
                                    cmd.Parameters.AddWithValue("$low", ms - tol);
                                    cmd.Parameters.AddWithValue("$high", ms + tol);
                                    affected = cmd.ExecuteNonQuery();
                                }
                                catch { }
                            }
                        }

                        if (affected > 0)
                        {
                            this.Dispatcher.Invoke(() => MessageBox.Show(this, $"Gelöscht: {affected} Zeile(n)", "Erfolg", MessageBoxButton.OK, MessageBoxImage.Information));
                            // reload plot
                            LoadAndRenderTotalValueHistory();
                        }
                        else
                        {
                            this.Dispatcher.Invoke(() => MessageBox.Show(this, "Keine passende Zeile gefunden oder Löschvorgang fehlgeschlagen.", "Info", MessageBoxButton.OK, MessageBoxImage.Information));
                        }
                    }
                    catch (Exception ex)
                    {
                        this.Dispatcher.Invoke(() => MessageBox.Show(this, "Fehler beim Löschen: " + ex.Message, "Fehler", MessageBoxButton.OK, MessageBoxImage.Error));
                    }
                });
            }
            catch { }
        }

        // Start a periodic timer that reapplies stored X limits every 5 seconds to keep user zoom stable
        private void StartPeriodicReapply()
        {
            try
            {
                // If already running, restart
                try { _reapplyPeriodicTimer?.Stop(); } catch { }
                _reapplyPeriodicTimer = new DispatcherTimer();
                _reapplyPeriodicTimer.Interval = TimeSpan.FromSeconds(5);
                _reapplyPeriodicTimer.Tick += (s, e) =>
                {
                    try
                    {
                        // do not auto reapply if suppression active
                        try { if (_suppressAutoApply) return; } catch { }
                        if (PlotTotalValueHistory == null) return;
                        // If the user explicitly selected a fixed interval, ensure it stays applied
                        // Only reapply periodically when automatic shifting is enabled (_autoShiftEnabled).
                        try
                        {
                            if (_autoShiftEnabled && _forcedIntervalUserSet && _forcedInterval.HasValue && _totalValueTimes != null && _totalValueTimes.Count > 0)
                            {
                                var pltF = PlotTotalValueHistory.Plot;
                                var dataMaxOa = _totalValueTimes.Last().ToOADate();
                                var spanDaysF = _forcedInterval.Value.TotalDays;
                                var chosenMaxF = dataMaxOa + 0.05 * spanDaysF;
                                var chosenMinF = chosenMaxF - spanDaysF;
                                var dataMinOa = _totalValueTimes.First().ToOADate();
                                if (chosenMinF < dataMinOa)
                                {
                                    chosenMinF = dataMinOa;
                                    chosenMaxF = chosenMinF + spanDaysF;
                                }
                                try { pltF.Axes.SetLimitsX(chosenMinF, chosenMaxF); } catch { }
                                try { ApplyXAxisInterval(_forcedInterval); } catch { }
                                try { PlotTotalValueHistory.Refresh(); } catch { }
                                return;
                            }
                        }
                        catch { }
                        // respect user toggle (use internal flag for reliability)
                        try { if (!_autoShiftEnabled) return; } catch { return; }

                        var plt = PlotTotalValueHistory.Plot;

                        // use cached loaded times to determine latest data point
                        if (_totalValueTimes == null || _totalValueTimes.Count == 0)
                        {
                            // nothing to shift to
                            return;
                        }

                        try
                        {
                            double prevMin = plt.Axes.Bottom.Min;
                            double prevMax = plt.Axes.Bottom.Max;
                            if (double.IsNaN(prevMin) || double.IsNaN(prevMax) || prevMax <= prevMin) return;

                            double prevWidth = prevMax - prevMin;
                            // newest data time
                            double dataMax = _totalValueTimes.Last().ToOADate();

                            // When automatic shift is enabled (checkbox), always shift the window to include newest data
                            // This overrides any prior user zoom so the automatic behavior is reliable.
                            try
                            {
                                var shift = 0.05 * prevWidth;
                                double chosenMax = dataMax + shift;
                                double chosenMin = chosenMax - prevWidth;

                                // ensure not before earliest data
                                double dataMin = _totalValueTimes.First().ToOADate();
                                if (chosenMin < dataMin)
                                {
                                    chosenMin = dataMin;
                                    chosenMax = chosenMin + prevWidth;
                                }

                                try
                                {
                                    plt.Axes.SetLimitsX(chosenMin, chosenMax);
                                    PlotTotalValueHistory.Refresh();
                                    System.Diagnostics.Debug.WriteLine($"TVH: periodic auto-shift applied (forced) chosenMin={chosenMin} chosenMax={chosenMax} actualMin={plt.Axes.Bottom.Min} actualMax={plt.Axes.Bottom.Max}");
                                    // clear user-zoom flag so future renders allow autoscale/shift
                                    try { _plotUserZoomed = false; _userXMin = null; _userXMax = null; } catch { }
                                }
                                catch (Exception ex) { System.Diagnostics.Debug.WriteLine("TVH: periodic auto-shift failed: " + ex.Message); }
                            }
                            catch (Exception ex) { System.Diagnostics.Debug.WriteLine("TVH: periodic reapply error: " + ex.Message); }
                        }
                        catch (Exception ex) { System.Diagnostics.Debug.WriteLine("TVH: periodic reapply outer error: " + ex.Message); }
                    }
                    catch { }
                };
                _reapplyPeriodicTimer.Start();
            }
            catch { }
        }

        // Start a simple UDP listener on localhost for incoming notifications from Firefox extension
        private void StartUdpListener()
        {
            try
            {
                StopUdpListener();
                _udpListener = new System.Net.Sockets.UdpClient(_udpPort);
                _udpListener.Client.SetSocketOption(System.Net.Sockets.SocketOptionLevel.Socket, System.Net.Sockets.SocketOptionName.ReuseAddress, true);

                // receive loop on thread pool
                System.Threading.ThreadPool.QueueUserWorkItem(async _ =>
                {
                    try
                    {
                        while (_udpListener != null)
                        {
                            var res = await _udpListener.ReceiveAsync();
                            try
                            {
                                var msg = System.Text.Encoding.UTF8.GetString(res.Buffer);
                                this.Dispatcher.BeginInvoke(new Action(() =>
                                {
                                    try
                                    {
                                        var lb = this.FindName("LstRightArea4") as System.Windows.Controls.ListBox;
                                        if (lb != null)
                                        {
                                            lb.Items.Insert(0, $"{DateTime.Now:HH:mm:ss} - {msg}");
                                            // keep list short
                                            while (lb.Items.Count > 200) lb.Items.RemoveAt(lb.Items.Count - 1);
                                        }
                                    }
                                    catch { }
                                }));
                            }
                            catch { }
                        }
                    }
                    catch { }
                });
            }
            catch { }
        }

        private void StopUdpListener()
        {
            try
            {
                try { _udpListener?.Close(); } catch { }
                try { _udpListener?.Dispose(); } catch { }
                _udpListener = null;
            }
            catch { }
        }

        private void StartUdpStatusTimer()
        {
            try
            {
                _udpStatusTimer?.Stop();
                _udpStatusTimer = new DispatcherTimer();
                _udpStatusTimer.Interval = TimeSpan.FromSeconds(5);
                _udpStatusTimer.Tick += (s, e) =>
                {
                    try
                    {
                        var tb = this.FindName("TxtUdpInfo") as System.Windows.Controls.TextBlock;
                        if (tb != null)
                        {
                            if (_udpListener != null)
                                tb.Text = $"UDP listener: port {_udpPort} (listening)";
                            else
                                tb.Text = $"UDP listener: port {_udpPort} (stopped)";
                        }
                    }
                    catch { }
                };
                _udpStatusTimer.Start();
            }
            catch { }
        }

        private void SuppressAutoApplyFor(int milliseconds)
        {
            try
            {
                _suppressAutoApply = true;
                try { _autoApplySuppressTimer?.Stop(); } catch { }
                _autoApplySuppressTimer = new DispatcherTimer();
                _autoApplySuppressTimer.Interval = TimeSpan.FromMilliseconds(milliseconds);
                _autoApplySuppressTimer.Tick += (s, e) =>
                {
                    try
                    {
                        _suppressAutoApply = false;
                        try { _autoApplySuppressTimer?.Stop(); } catch { }
                    }
                    catch { try { _autoApplySuppressTimer?.Stop(); } catch { } }
                };
                _autoApplySuppressTimer.Start();
            }
            catch { _suppressAutoApply = false; }
        }

        // Simple HTTP listener fallback so extension can POST messages to http://127.0.0.1:54123/notify
        private HttpListener? _httpListener = null;

        private void StartHttpListener()
        {
            try
            {
                StopHttpListener();
                _httpListener = new HttpListener();
                _httpListener.Prefixes.Add("http://127.0.0.1:54123/");
                _httpListener.Start();

                System.Threading.ThreadPool.QueueUserWorkItem(async _ =>
                {
                    try
                    {
                        while (_httpListener != null && _httpListener.IsListening)
                        {
                            var ctx = await _httpListener.GetContextAsync();
                            try
                            {
                                string body = string.Empty;
                                try
                                {
                                    using var sr = new StreamReader(ctx.Request.InputStream, ctx.Request.ContentEncoding);
                                    body = await sr.ReadToEndAsync();
                                }
                                catch { }

                                try
                                {
                                    ctx.Response.StatusCode = 200;
                                    var respBytes = System.Text.Encoding.UTF8.GetBytes("OK");
                                    ctx.Response.OutputStream.Write(respBytes, 0, respBytes.Length);
                                }
                                catch { }
                                finally { try { ctx.Response.Close(); } catch { } }

                                // dispatch message to UI
                                try
                                {
                                    this.Dispatcher.BeginInvoke(new Action(() =>
                                    {
                                        try
                                        {
                                            var lb = this.FindName("LstRightArea4") as System.Windows.Controls.ListBox;
                                            if (lb != null)
                                            {
                                                lb.Items.Insert(0, $"{DateTime.Now:HH:mm:ss} - {body}");
                                                while (lb.Items.Count > 200) lb.Items.RemoveAt(lb.Items.Count - 1);
                                            }
                                        }
                                        catch { }
                                    }));
                                }
                                catch { }
                            }
                            catch { }
                        }
                    }
                    catch { }
                });
            }
            catch { }
        }

        private void StopHttpListener()
        {
            try
            {
                try { if (_httpListener != null && _httpListener.IsListening) _httpListener.Stop(); } catch { }
                try { _httpListener?.Close(); } catch { }
                _httpListener = null;
            }
            catch { }
        }

        private void LoadAndRenderTotalValueHistory()
        {
            Task.Run(() =>
            {
                try
                {
                    var activeCsv = GetActiveCsvFromDb(_dbPath);
                    if (string.IsNullOrWhiteSpace(activeCsv))
                        return;

                    //var points = LoadTotalValuePoints(activeCsv);
                    var points = LoadTotalValuePoints();

                    if (points.Count == 0)
                        return;

                    Dispatcher.BeginInvoke(() =>
                    {
                        RenderTotalValuePlot(points);
                    });
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex);
                }
            });
        }

        private long? GetActiveCsvId()
        {
            try
            {
                using var conn =
                    new SqliteConnection(
                        $"Data Source={_dbPath}");

                conn.Open();

                using var cmd = conn.CreateCommand();

                cmd.CommandText = @"
            SELECT Id
            FROM NEW_CSV_ACTIVE
            WHERE Active = 1
            LIMIT 1;
        ";

                var result = cmd.ExecuteScalar();

                if (result != null &&
                    result != DBNull.Value)
                {
                    return Convert.ToInt64(result);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }

            return null;
        }
        private List<(DateTime time, double avg, double today, long rowid)>
        LoadTotalValuePoints()
        {
            var list =
                new List<(DateTime, double, double, long)>();

            var csvId = GetActiveCsvId();

            if (!csvId.HasValue)
                return list;

            using var conn =
                new SqliteConnection(
                    $"Data Source={_dbPath}");

            conn.Open();

            using var cmd = conn.CreateCommand();

            cmd.CommandText = @"
        SELECT
            LastUpdated,
            SumAvgTotal,
            SumTodayValue,
            rowid
        FROM NEW_TotalValues
        WHERE CSV = @csvId
        ORDER BY LastUpdated ASC;
    ";

            cmd.Parameters.AddWithValue(
                "@csvId",
                csvId.Value);

            using var reader =
                cmd.ExecuteReader();

            while (reader.Read())
            {
                try
                {
                    DateTime time =
                        DateTime.Parse(reader.GetString(0));

                    double avg =
                        reader.IsDBNull(1)
                        ? 0
                        : Convert.ToDouble(reader.GetValue(1));

                    double today =
                        reader.IsDBNull(2)
                        ? 0
                        : Convert.ToDouble(reader.GetValue(2));

                    long rowid =
                        reader.GetInt64(3);

                    list.Add((time, avg, today, rowid));
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex);
                }
            }

            Debug.WriteLine(
                $"Loaded points: {list.Count}");

            return list;
        }

        // Load NEW_Prices grouped by ISIN. Returns dictionary ISIN -> list of (time, change) sorted by time.
        // If an optional set of ISINs is provided, only rows for these ISINs are loaded (reduces DB work and memory).
        private Dictionary<string, List<(DateTime time, double change)>> LoadNewChangesByIsin(IEnumerable<string>? filterIsins = null)
        {
            var dict = new Dictionary<string, List<(DateTime, double)>>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var cs = new SqliteConnectionStringBuilder { DataSource = _dbPath }.ToString();
                using var conn = new SqliteConnection(cs);
                conn.Open();

                using var cmd = conn.CreateCommand();

                if (filterIsins != null)
                {
                    var list = filterIsins.Where(s => !string.IsNullOrWhiteSpace(s)).Select(s => s.Trim()).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                    if (list.Count == 0)
                        return dict;

                    // build parameterized IN clause
                    var placeholders = new List<string>();
                    for (int i = 0; i < list.Count; i++)
                    {
                        var pn = "$p" + i;
                        placeholders.Add(pn);
                        cmd.Parameters.AddWithValue(pn, list[i]);
                    }
                    var inClause = string.Join(",", placeholders);
                    cmd.CommandText = $"SELECT Isin, Datum, Change FROM NEW_Prices WHERE Isin IN ({inClause}) ORDER BY Isin, Datum;";
                }
                else
                {
                    cmd.CommandText = "SELECT Isin, Datum, Change FROM NEW_Prices ORDER BY Isin, Datum;";
                }

                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    try
                    {
                        var isinObj = reader[0];
                        var timeObj = reader[1];
                        var changeObj = reader[2];

                        if (isinObj == null || isinObj == DBNull.Value) continue;
                        var isin = isinObj.ToString();
                        if (string.IsNullOrWhiteSpace(isin)) continue;

                        DateTime time;
                        if (timeObj is DateTime dt) time = dt;
                        else
                        {
                            var s = timeObj?.ToString();
                            if (!DateTime.TryParse(s, out time))
                            {
                                // try numeric
                                if (long.TryParse(s, out var lv))
                                {
                                    if (Math.Abs(lv) > 1000000000000L) time = DateTimeOffset.FromUnixTimeMilliseconds(lv).DateTime;
                                    else if (Math.Abs(lv) > 1000000000L) time = DateTimeOffset.FromUnixTimeSeconds(lv).DateTime;
                                    else time = DateTime.FromOADate(lv);
                                }
                                else continue;
                            }
                        }

                        if (changeObj == null || changeObj == DBNull.Value) continue;
                        var changeText = changeObj.ToString();
                        if (string.IsNullOrWhiteSpace(changeText)) continue;
                        changeText = changeText.Replace("%", "").Trim();
                        if (!double.TryParse(changeText, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double change)) continue;

                        if (!dict.TryGetValue(isin, out var list))
                        {
                            list = new List<(DateTime, double)>();
                            dict[isin] = list;
                        }
                        list.Add((time, change));
                    }
                    catch { }
                }

                // sort each list by time
                foreach (var k in dict.Keys.ToList())
                {
                    try { dict[k] = dict[k].OrderBy(p => p.Item1).ToList(); } catch { }
                }
            }
            catch { }
            return dict;
        }

        // Load mapping of ISIN -> display name from available tables (NEW_Holdings, Holdings, NEW_Prices)
        private Dictionary<string, string> LoadIsinNames()
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var cs = new SqliteConnectionStringBuilder { DataSource = _dbPath }.ToString();
                using var conn = new SqliteConnection(cs);
                conn.Open();

                // candidate tables to look for Isin/Name
                var candidates = new[] { "NEW_Holdings", "Holdings", "NEW_Prices" };

                foreach (var table in candidates)
                {
                    try
                    {
                        // check columns
                        var cols = new List<string>();
                        using (var pragma = conn.CreateCommand())
                        {
                            pragma.CommandText = $"PRAGMA table_info({table});";
                            using var r = pragma.ExecuteReader();
                            while (r.Read())
                            {
                                try { cols.Add(r.GetString(1)); } catch { }
                            }
                        }

                        if (cols.Count == 0) continue;

                        bool hasIsin = cols.Any(c => string.Equals(c, "Isin", StringComparison.OrdinalIgnoreCase) || string.Equals(c, "ISIN", StringComparison.OrdinalIgnoreCase));
                        bool hasName = cols.Any(c => string.Equals(c, "Name", StringComparison.OrdinalIgnoreCase) || string.Equals(c, "Bezeichnung", StringComparison.OrdinalIgnoreCase) || string.Equals(c, "LongName", StringComparison.OrdinalIgnoreCase));

                        if (!hasIsin) continue;

                        // build query depending on availability of Name
                        string qry;
                        if (hasName) qry = $"SELECT DISTINCT Isin, Name FROM {table} WHERE Isin IS NOT NULL;";
                        else qry = $"SELECT DISTINCT Isin FROM {table} WHERE Isin IS NOT NULL;";

                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = qry;
                        using var rdr = cmd.ExecuteReader();
                        while (rdr.Read())
                        {
                            try
                            {
                                var isinObj = rdr.GetValue(0);
                                if (isinObj == null || isinObj == DBNull.Value) continue;
                                var isin = isinObj.ToString()?.Trim();
                                if (string.IsNullOrWhiteSpace(isin)) continue;

                                string name = string.Empty;
                                if (hasName && rdr.FieldCount > 1)
                                {
                                    try { var nobj = rdr.GetValue(1); name = nobj == null || nobj == DBNull.Value ? string.Empty : nobj.ToString()?.Trim() ?? string.Empty; } catch { name = string.Empty; }
                                }

                                if (!result.ContainsKey(isin)) result[isin] = name;
                            }
                            catch { }
                        }

                        // if we found any, stop searching further tables
                        if (result.Count > 0) break;
                    }
                    catch { }
                }
            }
            catch { }
            return result;
        }

      
        private static double ToMedian(IEnumerable<double> values)
        {
            try
            {
                if (values == null) return 0.0;
                var arr = values.Where(d => !double.IsNaN(d) && !double.IsInfinity(d)).OrderBy(d => d).ToArray();
                if (arr.Length == 0) return 0.0;
                int n = arr.Length;
                if ((n & 1) == 1) return arr[n / 2];
                return (arr[n / 2 - 1] + arr[n / 2]) / 2.0;
            }
            catch { return 0.0; }
        }

        private void RenderTotalValuePlot(
            List<(DateTime time, double avg, double today, long rowid)> points)
        {
            if (points.Count == 0)
                return;

            var plt =
                PlotTotalValueHistory.Plot;

            plt.Clear();

            var xs = points.Select(p => p.time.ToOADate()).ToArray();

            var ysToday = points.Select(p => p.today).ToArray();

            // cache times and row ids so interactive actions (double-click delete) can map back to DB rows
            try
            {
                _totalValueTimes = points.Select(p => p.time).ToList();
                _totalValueRowIds = points.Select(p => (long?)p.rowid).ToList();
                // compute median delta for tolerant matching used during delete
                try
                {
                    if (_totalValueTimes != null && _totalValueTimes.Count >= 2)
                    {
                        var deltas = new List<double>();
                        for (int i = 1; i < _totalValueTimes.Count; i++) deltas.Add((_totalValueTimes[i] - _totalValueTimes[i - 1]).TotalDays);
                        _totalValueMedianDeltaDays = deltas.Count > 0 ? ToMedian(deltas) : 1.0;
                    }
                    else _totalValueMedianDeltaDays = 1.0;
                }
                catch { _totalValueMedianDeltaDays = 1.0; }
            }
            catch { }

            // add series (only 'today' series is displayed to avoid showing overall series)
            var scToday = plt.Add.Scatter(xs, ysToday);

            try { plt.Axes.DateTimeTicksBottom(); } catch { }

            // Determine X range to consider for Y autoscale
            double currentXMin = double.NaN, currentXMax = double.NaN;
            try
            {
                // use internal flag instead of querying UI control every render/tick
                bool autoShiftEnabled = false;
                try { autoShiftEnabled = _autoShiftEnabled; } catch { autoShiftEnabled = false; }

                // If auto-shift is enabled, ignore any user zoom and compute range based on forced interval or data
                if (autoShiftEnabled)
                {
                    try { _plotUserZoomed = false; } catch { }
                    if (_forcedIntervalUserSet && _forcedInterval.HasValue)
                    {
                        double dataMax = xs.Length > 0 ? xs.Last() : DateTime.Now.ToOADate();
                        var spanDays = _forcedInterval.Value.TotalDays;
                        currentXMax = dataMax + 0.05 * spanDays;
                        currentXMin = currentXMax - spanDays;
                    }
                    else
                    {
                        if (xs.Length > 0)
                        {
                            // keep same window width as current plot if possible
                            try
                            {
                                var pltMin = PlotTotalValueHistory.Plot.Axes.Bottom.Min;
                                var pltMax = PlotTotalValueHistory.Plot.Axes.Bottom.Max;
                                if (!double.IsNaN(pltMin) && !double.IsNaN(pltMax) && pltMax > pltMin)
                                {
                                    var width = pltMax - pltMin;
                                    var dataMax = xs.Max();
                                    currentXMax = dataMax + 0.05 * width; // shift slightly right
                                    currentXMin = currentXMax - width;
                                }
                                else
                                {
                                    currentXMin = xs.Min();
                                    currentXMax = xs.Max();
                                }
                            }
                            catch { currentXMin = xs.Min(); currentXMax = xs.Max(); }
                        }
                    }
                }
                else
                {
                    if (_plotUserZoomed && _userXMin.HasValue && _userXMax.HasValue)
                    {
                        currentXMin = _userXMin.Value;
                        currentXMax = _userXMax.Value;
                    }
                    else if (_forcedIntervalUserSet && _forcedInterval.HasValue)
                    {
                        double dataMax = xs.Length > 0 ? xs.Last() : DateTime.Now.ToOADate();
                        var spanDays = _forcedInterval.Value.TotalDays;
                        currentXMax = dataMax + 0.05 * spanDays;
                        currentXMin = currentXMax - spanDays;
                    }
                    else
                    {
                        if (xs.Length > 0)
                        {
                            currentXMin = xs.Min();
                            currentXMax = xs.Max();
                        }
                    }
                }
            }
            catch { }

            // Ensure valid range
            if (double.IsNaN(currentXMin) || double.IsNaN(currentXMax) || currentXMax <= currentXMin)
            {
                if (xs.Length > 0)
                {
                    currentXMin = xs.Min(); currentXMax = xs.Max();
                }
                else
                {
                    currentXMin = DateTime.Now.AddDays(-1).ToOADate(); currentXMax = DateTime.Now.ToOADate();
                }
            }

            // Compute Y min/max only for points inside current X range
            double minY = double.PositiveInfinity, maxY = double.NegativeInfinity;
            try
            {
                for (int i = 0; i < xs.Length; i++)
                {
                    var x = xs[i];
                    if (x < currentXMin || x > currentXMax) continue;
                    if (ysToday != null && i < ysToday.Length)
                    {
                        var v2 = ysToday[i]; if (!double.IsNaN(v2) && !double.IsInfinity(v2)) { minY = Math.Min(minY, v2); maxY = Math.Max(maxY, v2); }
                    }
                }
                // if no points inside range, fallback to full data (use today series only)
                if (double.IsInfinity(minY) || double.IsInfinity(maxY))
                {
                    if (ysToday != null && ysToday.Length > 0) { minY = ysToday.Min(); maxY = ysToday.Max(); }
                }
            }
            catch { }

            // Apply X limits (respect user forced selection)
            try
            {
                if (_forcedIntervalUserSet && _forcedInterval.HasValue)
                {
                    // ensure stored user limits are in sync
                    try { var pltMin = plt.Axes.Bottom.Min; var pltMax = plt.Axes.Bottom.Max; } catch { }
                    try { plt.Axes.SetLimitsX(currentXMin, currentXMax); _userXMin = currentXMin; _userXMax = currentXMax; _plotUserZoomed = true; } catch { }
                }
                else if (_plotUserZoomed && _userXMin.HasValue && _userXMax.HasValue)
                {
                    try { plt.Axes.SetLimitsX(_userXMin.Value, _userXMax.Value); } catch { }
                }
            }
            catch { }

            // Apply Y limits computed for visible X range with at least 5% padding above and below.
            try
            {
                if (!(double.IsInfinity(minY) || double.IsInfinity(maxY)))
                {
                    double visibleRange = Math.Max(1e-9, maxY - minY);

                    // compute total data range so padding can be at least 5% of overall data (avoid too-small pad)
                    double totalMinY = double.PositiveInfinity, totalMaxY = double.NegativeInfinity;
                    try
                    {
                        if (ysToday != null && ysToday.Length > 0) { totalMinY = Math.Min(totalMinY, ysToday.Min()); totalMaxY = Math.Max(totalMaxY, ysToday.Max()); }
                        if (double.IsInfinity(totalMinY) || double.IsInfinity(totalMaxY)) { totalMinY = minY; totalMaxY = maxY; }
                    }
                    catch { totalMinY = minY; totalMaxY = maxY; }

                    double totalRange = Math.Max(1e-9, totalMaxY - totalMinY);

                    // choose pad as the larger of 5% of visible range and 5% of total data range
                    double pad = Math.Max(visibleRange * 0.05, totalRange * 0.05);

                    // if visible range is effectively zero, ensure a sensible absolute pad
                    if (visibleRange < 1e-9)
                    {
                        double baseVal = Math.Max(Math.Abs(maxY), 1.0);
                        pad = Math.Max(pad, baseVal * 0.05);
                    }

                    try { plt.Axes.SetLimitsY(minY - pad, maxY + pad); } catch { }
                }
            }
            catch { }

            PlotTotalValueHistory.Refresh();

            // Also render copies into the right-area plots (duplicate rendering)
            try
            {
                // prepare arrays for duplicate plots
                var xsDup = xs;
                var ysDup = ysToday;

                // compute final Y limits used for main plot (if set)
                double? finalYMin = null, finalYMax = null;
                try
                {
                    if (!(double.IsInfinity(minY) || double.IsInfinity(maxY)))
                    {
                        double visibleRange = Math.Max(1e-9, maxY - minY);
                        double totalMinY = double.PositiveInfinity, totalMaxY = double.NegativeInfinity;
                        try
                        {
                            if (ysToday != null && ysToday.Length > 0) { totalMinY = Math.Min(totalMinY, ysToday.Min()); totalMaxY = Math.Max(totalMaxY, ysToday.Max()); }
                            if (double.IsInfinity(totalMinY) || double.IsInfinity(totalMaxY)) { totalMinY = minY; totalMaxY = maxY; }
                        }
                        catch { totalMinY = minY; totalMaxY = maxY; }
                        double totalRange = Math.Max(1e-9, totalMaxY - totalMinY);
                        double pad = Math.Max(visibleRange * 0.05, totalRange * 0.05);
                        if (visibleRange < 1e-9)
                        {
                            double baseVal = Math.Max(Math.Abs(maxY), 1.0);
                            pad = Math.Max(pad, baseVal * 0.05);
                        }
                        finalYMin = minY - pad;
                        finalYMax = maxY + pad;
                    }
                }
                catch { }
                //TODO


















                // render into Tops, All and Bottoms plots with titles (use FindName to avoid relying on generated fields)
                try
                {
                    var pt = this.FindName("Tops") as ScottPlot.WPF.WpfPlot;
                    if (pt != null)
                    {

                        pt.Plot.Title("Tops");
                        pt.Refresh();
                        try
                        {
                            // compute top 4 increases over current X window
                            var isinNamesMap = LoadIsinNames();
                            var changeSeries = LoadNewChangesByIsin();

                            var perIsin = new List<(string Isin, string Name, double Delta, List<(DateTime t, double v)> Points)>();
                            foreach (var kv in changeSeries)
                            {
                                try
                                {
                                    var isin = kv.Key;
                                    var all = kv.Value.Select(p => (t: p.Item1, v: p.Item2)).OrderBy(p => p.t).ToList();
                                    if (all.Count < 2) continue;
                                    // restrict to window
                                    var window = all.Where(p => p.t.ToOADate() >= currentXMin && p.t.ToOADate() <= currentXMax).ToList();
                                    if (window.Count < 2) continue;
                                    var first = window.First().v;
                                    var last = window.Last().v;
                                    var delta = last - first;
                                    var name = isinNamesMap.TryGetValue(isin, out var n) && !string.IsNullOrWhiteSpace(n) ? n : isin;
                                    perIsin.Add((isin, name, delta, window));
                                }
                                catch { }
                            }

                            var topInc = perIsin.OrderByDescending(x => x.Delta).Take(4).ToList();
                            //var topDec = perIsin.OrderBy(x => x.Delta).Take(4).ToList();

                            var pltTop = pt.Plot;
                            pltTop.Clear();

                            // plot top increases (label prefixed with "+")
                            foreach (var item in topInc)
                            {
                                try
                                {
                                    var xsTop = item.Points.Select(p => p.t.ToOADate()).ToArray();
                                    var ysTop = item.Points.Select(p => p.v).ToArray();
                                    var s = pltTop.Add.Scatter(xsTop, ysTop);
                                   
                                }
                                catch { }
                            }

                       
                            try { pltTop.Axes.DateTimeTicksBottom(); } catch { }
                            try { pltTop.Legend.IsVisible = true; } catch { }
                            pt.Refresh();

                            // Also populate left-area 3 with a tidy list of top increases
                            try
                            {
                                var lb = this.FindName("LstLeftArea3") as System.Windows.Controls.ListBox
                                         ?? this.FindName("LstLeft3") as System.Windows.Controls.ListBox
                                         ?? this.FindName("LstLinksArea3") as System.Windows.Controls.ListBox
                                         ?? this.FindName("LstRightArea3") as System.Windows.Controls.ListBox;

                                if (lb != null)
                                {
                                    lb.Items.Clear();
                                    int rank = 1;
                                    foreach (var item in topInc)
                                    {
                                        try
                                        {
                                            var panel = new System.Windows.Controls.StackPanel { Orientation = System.Windows.Controls.Orientation.Horizontal };
                                            var tbName = new System.Windows.Controls.TextBlock
                                            {
                                                Text = $"{rank}. {item.Name}",
                                                FontWeight = System.Windows.FontWeights.SemiBold,
                                                Margin = new System.Windows.Thickness(2, 0, 8, 0)
                                            };

                                            var tbDelta = new System.Windows.Controls.TextBlock
                                            {
                                                Text = $"{item.Delta:+0.00;-0.00;0.00} %",
                                                Foreground = item.Delta >= 0 ? System.Windows.Media.Brushes.Green : System.Windows.Media.Brushes.Red,
                                                VerticalAlignment = System.Windows.VerticalAlignment.Center,
                                                Margin = new System.Windows.Thickness(0, 0, 4, 0)
                                            };

                                            panel.Children.Add(tbName);
                                            panel.Children.Add(tbDelta);

                                            // optional small subtitle with first/last values
                                            try
                                            {
                                                var first = item.Points.First();
                                                var last = item.Points.Last();
                                                var tbSub = new System.Windows.Controls.TextBlock
                                                {
                                                    Text = $"  ({first.t:HH:mm} → {last.t:HH:mm}) {first.v:+0.00;-0.00;0.00}% → {last.v:+0.00;-0.00;0.00}%",
                                                    Foreground = System.Windows.Media.Brushes.Gray,
                                                    Margin = new System.Windows.Thickness(6, 0, 0, 0)
                                                };
                                                panel.Children.Add(tbSub);
                                            }
                                            catch { }

                                            lb.Items.Add(new System.Windows.Controls.ListBoxItem { Content = panel });
                                            rank++;
                                        }
                                        catch { }
                                    }
                                }
                            }
                            catch { }
                        }
                        catch
                        {
                            // fallback to simple copy
                            try { RenderCopyPlot(pt, xsDup, ysDup, "Tops", currentXMin, currentXMax, finalYMin, finalYMax); } catch { }
                        }
                    }
                }
                catch { }

                try
                {
                    var pt = this.FindName("Flops") as ScottPlot.WPF.WpfPlot;
                    if (pt != null)
                    {
                        pt.Plot.Title("Flops");
                        pt.Refresh();
                        try
                        {
                            // compute top 4 increases over current X window
                            var isinNamesMap = LoadIsinNames();
                            var changeSeries = LoadNewChangesByIsin();

                            var perIsin = new List<(string Isin, string Name, double Delta, List<(DateTime t, double v)> Points)>();
                            foreach (var kv in changeSeries)
                            {
                                try
                                {
                                    var isin = kv.Key;
                                    var all = kv.Value.Select(p => (t: p.Item1, v: p.Item2)).OrderBy(p => p.t).ToList();
                                    if (all.Count < 2) continue;
                                    // restrict to window
                                    var window = all.Where(p => p.t.ToOADate() >= currentXMin && p.t.ToOADate() <= currentXMax).ToList();
                                    if (window.Count < 2) continue;
                                    var first = window.First().v;
                                    var last = window.Last().v;
                                    var delta = last - first;
                                    var name = isinNamesMap.TryGetValue(isin, out var n) && !string.IsNullOrWhiteSpace(n) ? n : isin;
                                    perIsin.Add((isin, name, delta, window));
                                }
                                catch { }
                            }

                            //var topInc = perIsin.OrderByDescending(x => x.Delta).Take(4).ToList();
                            var topDec = perIsin.OrderBy(x => x.Delta).Take(4).ToList();

                            var pltTop = pt.Plot;
                            pltTop.Clear();                          

                            // plot top decreases (label prefixed with "-")
                            foreach (var item in topDec)
                            {
                                try
                                {
                                    var xsTop = item.Points.Select(p => p.t.ToOADate()).ToArray();
                                    var ysTop = item.Points.Select(p => p.v).ToArray();
                                    var s = pltTop.Add.Scatter(xsTop, ysTop);
                                   
                                }
                                catch { }
                            }

                            try { pltTop.Axes.DateTimeTicksBottom(); } catch { }
                            try { pltTop.Legend.IsVisible = true; } catch { }
                            pt.Refresh();

                            // Also populate left-area 3 with a tidy list of top increases
                            try
                            {
                                var lb = this.FindName("LstLeftArea4") as System.Windows.Controls.ListBox
                                         ?? this.FindName("LstLeft3") as System.Windows.Controls.ListBox
                                         ?? this.FindName("LstLinksArea3") as System.Windows.Controls.ListBox
                                         ?? this.FindName("LstRightArea3") as System.Windows.Controls.ListBox;

                                if (lb != null)
                                {
                                    lb.Items.Clear();
                                    int rank = 1;
                                    foreach (var item in topDec)
                                    {
                                        try
                                        {
                                            var panel = new System.Windows.Controls.StackPanel { Orientation = System.Windows.Controls.Orientation.Horizontal };
                                            var tbName = new System.Windows.Controls.TextBlock
                                            {
                                                Text = $"{rank}. {item.Name}",
                                                FontWeight = System.Windows.FontWeights.SemiBold,
                                                Margin = new System.Windows.Thickness(2, 0, 8, 0)
                                            };

                                            var tbDelta = new System.Windows.Controls.TextBlock
                                            {
                                                Text = $"{item.Delta:+0.00;-0.00;0.00} %",
                                                Foreground = item.Delta >= 0 ? System.Windows.Media.Brushes.Green : System.Windows.Media.Brushes.Red,
                                                VerticalAlignment = System.Windows.VerticalAlignment.Center,
                                                Margin = new System.Windows.Thickness(0, 0, 4, 0)
                                            };

                                            panel.Children.Add(tbName);
                                            panel.Children.Add(tbDelta);

                                            // optional small subtitle with first/last values
                                            try
                                            {
                                                var first = item.Points.First();
                                                var last = item.Points.Last();
                                                var tbSub = new System.Windows.Controls.TextBlock
                                                {
                                                    Text = $"  ({first.t:HH:mm} → {last.t:HH:mm}) {first.v:+0.00;-0.00;0.00}% → {last.v:+0.00;-0.00;0.00}%",
                                                    Foreground = System.Windows.Media.Brushes.Gray,
                                                    Margin = new System.Windows.Thickness(6, 0, 0, 0)
                                                };
                                                panel.Children.Add(tbSub);
                                            }
                                            catch { }

                                            lb.Items.Add(new System.Windows.Controls.ListBoxItem { Content = panel });
                                            rank++;
                                        }
                                        catch { }
                                    }
                                }
                            }
                            catch { }




                        }
                        catch
                        {
                            // fallback to simple copy
                            try { RenderCopyPlot(pt, xsDup, ysDup, "Flops", currentXMin, currentXMax, finalYMin, finalYMax); } catch { }                       
                        }
                    }






                }
                catch { }


                try
                {
                    var pt = this.FindName("Plot") as ScottPlot.WPF.WpfPlot;
                    if (pt != null) RenderCopyPlot(pt, xsDup, ysDup, "", currentXMin, currentXMax, finalYMin, finalYMax);
                }
                catch { }

                //--------------------------------------------

                try
                {
                    var pa = this.FindName("Knockouts") as ScottPlot.WPF.WpfPlot;
                    if (pa != null)
                    {
                        pa.Plot.Title("Knockouts");
                        pa.Refresh();
                        // Instead of duplicating TotalValues here, load data from NEW_Prices and render
                        try
                        {
                            //var pricePoints = LoadNewPricesPoints();
                      
                            //TODO
                            // prepare list of ISINs and their display names (if available) before loading changes
                            var isinNames = LoadIsinNames();
                            var isinNameList = isinNames.Select(kv => (Isin: kv.Key, Name: kv.Value)).ToList();

                            // filter list to entries whose Name contains "Long" or "Short" (case-insensitive)
                            var filteredIsinNameList = isinNameList
                                .Where(x => !string.IsNullOrWhiteSpace(x.Name) && (
                                    x.Name.IndexOf("Long", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                    x.Name.IndexOf("Short", StringComparison.OrdinalIgnoreCase) >= 0))
                                .ToList();

                            // load per-ISIN series and render each as its own line (only for filtered ISINs)
                            var filterIsins = filteredIsinNameList.Select(x => x.Isin).ToList();
                            var changeSeries = LoadNewChangesByIsin(filterIsins);
                            if (changeSeries != null && changeSeries.Count > 0)
                            {
                                var pltTarget = pa.Plot;
                                pltTarget.Clear();

                                double globalMin = double.PositiveInfinity, globalMax = double.NegativeInfinity;

                                foreach (var kv in changeSeries.OrderBy(k => k.Key))
                                {
                                    try
                                    {
                                        var isin = kv.Key;
                                        var list = kv.Value;
                                        if (list == null || list.Count == 0) continue;

                                        var xsSeries = list.Select(p => p.Item1.ToOADate()).ToArray();
                                        var ysSeries = list.Select(p => p.Item2).ToArray();

                                        // add series
                                        var pl = pltTarget.Add.Scatter(xsSeries, ysSeries);
                                        //try
                                        //{
                                        //    var prop = pl.GetType().GetProperty("Label") ?? pl.GetType().GetProperty("LegendText") ?? pl.GetType().GetProperty("LegendLabel");
                                        //    if (prop != null && prop.CanWrite && prop.PropertyType == typeof(string)) prop.SetValue(pl, isin);
                                        //}
                                        //catch { }

                                        // update global min/max
                                        for (int i = 0; i < ysSeries.Length; i++)
                                        {
                                            var v = ysSeries[i];
                                            if (!double.IsNaN(v) && !double.IsInfinity(v))
                                            {
                                                globalMin = Math.Min(globalMin, v);
                                                globalMax = Math.Max(globalMax, v);
                                            }
                                        }
                                    }
                                    catch { }
                                }

                                try { pltTarget.Axes.DateTimeTicksBottom(); } catch { }

                                // compute Y limits with padding
                                double yMinC = double.NaN, yMaxC = double.NaN;
                                if (!double.IsInfinity(globalMin) && !double.IsInfinity(globalMax))
                                {
                                    var range = Math.Max(1e-9, globalMax - globalMin);
                                    var pad = Math.Max(0.5, range * 0.1);
                                    yMinC = globalMin - pad;
                                    yMaxC = globalMax + pad;
                                }
                                else
                                {
                                    yMinC = (double?)finalYMin ?? -1.0;
                                    yMaxC = (double?)finalYMax ?? 1.0;
                                }

                                // apply X/Y limits
                                try { if (!double.IsNaN(currentXMin) && !double.IsNaN(currentXMax)) pltTarget.Axes.SetLimitsX(currentXMin, currentXMax); } catch { }
                                try { pltTarget.Axes.SetLimitsY(yMinC, yMaxC); } catch { }

                                // do not show legend for Change plot (keep UI compact)
                                try { pltTarget.Legend.IsVisible = false; } catch { }

                                pa.Refresh();
                            }
                        }
                        catch
                        {
                            // on any error, fallback to original behavior
                            RenderCopyPlot(pa, xsDup, ysDup, "", currentXMin, currentXMax, finalYMin, finalYMax);
                        }
                    }
                }
                catch { }
              

                //---------------------------

                try
                {
                    var pa = this.FindName("ETF") as ScottPlot.WPF.WpfPlot;
                    if (pa != null)
                    {
                        pa.Plot.Title("ETFs");
                        pa.Refresh();
                        // Instead of duplicating TotalValues here, load data from NEW_Prices and render
                        try
                        {
                            var isinNames = LoadIsinNames();
                            var isinNameList = isinNames.Select(kv => (Isin: kv.Key, Name: kv.Value)).ToList();

                            // filter list to entries whose Name contains "Long" or "Short" (case-insensitive)
                            var filteredIsinNameList = isinNameList
                                .Where(x => !string.IsNullOrWhiteSpace(x.Name) && (
                                    x.Name.IndexOf("Long", StringComparison.OrdinalIgnoreCase) < 0 &&
                                    x.Name.IndexOf("Short", StringComparison.OrdinalIgnoreCase) < 0))
                                .ToList();

                            // load per-ISIN series and render each as its own line (only for filtered ISINs)
                            var filterIsins = filteredIsinNameList.Select(x => x.Isin).ToList();
                            var changeSeries = LoadNewChangesByIsin(filterIsins);

                            // load per-ISIN series and render each as its own line
                            //var changeSeries = LoadNewChangesByIsin();
                            if (changeSeries != null && changeSeries.Count > 0)
                            {
                                var pltTarget = pa.Plot;
                                pltTarget.Clear();

                                double globalMin = double.PositiveInfinity, globalMax = double.NegativeInfinity;

                                foreach (var kv in changeSeries.OrderBy(k => k.Key))
                                {
                                    try
                                    {
                                        var isin = kv.Key;
                                        var list = kv.Value;
                                        if (list == null || list.Count == 0) continue;

                                        var xsSeries = list.Select(p => p.Item1.ToOADate()).ToArray();
                                        var ysSeries = list.Select(p => p.Item2).ToArray();

                                        // add series
                                        var pl = pltTarget.Add.Scatter(xsSeries, ysSeries);
                                        //try
                                        //{
                                        //    var prop = pl.GetType().GetProperty("Label") ?? pl.GetType().GetProperty("LegendText") ?? pl.GetType().GetProperty("LegendLabel");
                                        //    if (prop != null && prop.CanWrite && prop.PropertyType == typeof(string)) prop.SetValue(pl, isin);
                                        //}
                                        //catch { }

                                        // update global min/max
                                        for (int i = 0; i < ysSeries.Length; i++)
                                        {
                                            var v = ysSeries[i];
                                            if (!double.IsNaN(v) && !double.IsInfinity(v))
                                            {
                                                globalMin = Math.Min(globalMin, v);
                                                globalMax = Math.Max(globalMax, v);
                                            }
                                        }
                                    }
                                    catch { }
                                }

                                try { pltTarget.Axes.DateTimeTicksBottom(); } catch { }

                                // compute Y limits with padding
                                double yMinC = double.NaN, yMaxC = double.NaN;
                                if (!double.IsInfinity(globalMin) && !double.IsInfinity(globalMax))
                                {
                                    var range = Math.Max(1e-9, globalMax - globalMin);
                                    var pad = Math.Max(0.5, range * 0.1);
                                    yMinC = globalMin - pad;
                                    yMaxC = globalMax + pad;
                                }
                                else
                                {
                                    yMinC = (double?)finalYMin ?? -1.0;
                                    yMaxC = (double?)finalYMax ?? 1.0;
                                }

                                // apply X/Y limits
                                try { if (!double.IsNaN(currentXMin) && !double.IsNaN(currentXMax)) pltTarget.Axes.SetLimitsX(currentXMin, currentXMax); } catch { }
                                try { pltTarget.Axes.SetLimitsY(yMinC, yMaxC); } catch { }

                                // do not show legend for Change plot (keep UI compact)
                                try { pltTarget.Legend.IsVisible = false; } catch { }

                                pa.Refresh();
                            }
                        }
                        catch
                        {
                            // on any error, fallback to original behavior
                            RenderCopyPlot(pa, xsDup, ysDup, "", currentXMin, currentXMax, finalYMin, finalYMax);
                        }
                    }
                }
                catch { }
            }
            catch { }
        }

        // Simple helper to render a copy of the timeseries into another WpfPlot
        private void RenderCopyPlot(ScottPlot.WPF.WpfPlot target, double[] xs, double[] ys, string title, double? xMin = null, double? xMax = null, double? yMin = null, double? yMax = null)
        {
            try
            {
                if (target == null) return;
                var plt = target.Plot;
                if (plt == null) return;
                plt.Clear();
                plt.Add.Scatter(xs, ys);
                try { plt.Axes.DateTimeTicksBottom(); } catch { }
                try { plt.Title(title); } catch { }

                // apply X/Y limits if provided so copies respect interval/auto-shift/user zoom
                try
                {
                    if (xMin.HasValue && xMax.HasValue)
                        plt.Axes.SetLimitsX(xMin.Value, xMax.Value);
                }
                catch { }
                try
                {
                    if (yMin.HasValue && yMax.HasValue)
                        plt.Axes.SetLimitsY(yMin.Value, yMax.Value);
                }
                catch { }

                target.Refresh();
            }
            catch { }
        }

      

        // Read the filename of the currently active CSV (Active = 1) from NEW_CSV_ACTIVE
        private string GetActiveCsvFromDb(string dbPath)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) return null;
                var cs = new SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
                using var conn = new SqliteConnection(cs);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT CSV FROM NEW_CSV_ACTIVE WHERE Active = 1 LIMIT 1;";
                var res = cmd.ExecuteScalar();
                if (res == null || res == DBNull.Value) return null;
                return res.ToString();
            }
            catch { return null; }
        }

        private void MainWindow_Loaded(object? sender, RoutedEventArgs e)
        {
            try
            {
                // ensure layout has finished and then autosize columns
                this.Dispatcher.BeginInvoke(new Action(() =>
                {
                    try { AutoSizeColumns(); } catch { }
                }), DispatcherPriority.ApplicationIdle);
            }
            catch { }
        }

        // Open the DatePicker popup immediately when it is loaded (referenced from XAML)
        private void GewinnVerlust2DateDatum_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                if (sender is System.Windows.Controls.Calendar cal)
                {
                    this.Dispatcher.BeginInvoke(new Action(() =>
                    {
                        try
                        {
                            if (!cal.SelectedDate.HasValue)
                            {
                                // default to previous business day (skip weekends)
                                var prev = DateTime.Today.AddDays(-1);
                                while (prev.DayOfWeek == DayOfWeek.Saturday || prev.DayOfWeek == DayOfWeek.Sunday)
                                    prev = prev.AddDays(-1);
                                cal.SelectedDate = prev;
                            }
                            // open calendar dropdown is not applicable for Calendar control, ignore IsDropDownOpen
                            // subscribe to date changed so we can show the last DB entry for the selected date
                            try { cal.SelectedDatesChanged += GewinnVerlust2DateDatum_SelectedDatesChanged; } catch { }
                            // update display for initial/default date
                            try { if (cal.SelectedDate.HasValue) UpdateTotalForSelectedDate(cal.SelectedDate.Value.Date); } catch { }
                        }
                        catch { }
                    }), DispatcherPriority.Background);
                }
            }
            catch { }
        }

        private void GewinnVerlust2DateDatum_SelectedDatesChanged(object? sender, SelectionChangedEventArgs e)
        {
            try
            {
                if (sender is System.Windows.Controls.Calendar cal && cal.SelectedDate.HasValue)
                {
                    UpdateTotalForSelectedDate(cal.SelectedDate.Value.Date);
                }
                else
                {
                    try { var tb = this.FindName("TxtTotalValueLeft4") as TextBlock; if (tb != null) tb.Text = "-"; } catch { }
                }
            }
            catch { }
        }

        private void GewinnVerlust2DateDatum_SelectedDateChanged(object? sender, SelectionChangedEventArgs e)
        {
            try
            {
                if (sender is DatePicker dp && dp.SelectedDate.HasValue)
                {
                    UpdateTotalForSelectedDate(dp.SelectedDate.Value.Date);
                }
                else
                {
                    try { var tb = this.FindName("TxtTotalValueLeft4") as TextBlock; if (tb != null) tb.Text = "-"; } catch { }
                }
            }
            catch { }
        }

        private void UpdateTotalForSelectedDate(DateTime dateLocal)
        {
            Task.Run(() =>
            {
                double? valueToShow = null;

                try
                {
                    using var conn = new SqliteConnection(
                        new SqliteConnectionStringBuilder
                        {
                            DataSource = _dbPath
                        }.ToString());

                    conn.Open();

                    DateTime start = dateLocal.Date;
                    DateTime end = start.AddDays(1);

                    using var cmd = conn.CreateCommand();

                    cmd.CommandText = @"
                SELECT SumTodayValue
                FROM NEW_TotalValues
                WHERE LastUpdated >= @start
                  AND LastUpdated < @end
                ORDER BY LastUpdated DESC
                LIMIT 1;
            ";

                    cmd.Parameters.AddWithValue(
                        "@start",
                        start.ToString("yyyy-MM-dd 00:00:00"));

                    cmd.Parameters.AddWithValue(
                        "@end",
                        end.ToString("yyyy-MM-dd 00:00:00"));

                    var result = cmd.ExecuteScalar();

                    if (result != null && result != DBNull.Value)
                    {
                        valueToShow = Convert.ToDouble(
                            result,
                            CultureInfo.InvariantCulture);
                    }
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex);
                }

                Dispatcher.BeginInvoke(() =>
                {
                    var tb = FindName("TxtTotalValueLeft4") as TextBlock;

                    if (tb != null)
                    {
                        tb.Text =
                            valueToShow?.ToString(
                                "N1",
                                CultureInfo.GetCultureInfo("de-DE"))
                            ?? "-";
                    }
                });
            });
        }

        private void DgHoldings_PreviewMouseWheel(object? sender, MouseWheelEventArgs e)
        {
            try
            {
                if ((Keyboard.Modifiers & ModifierKeys.Control) == ModifierKeys.Control)
                {

                    if (e.Delta > 0) _zoom = Math.Min(_maxZoom, _zoom + _zoomStep);
                    else _zoom = Math.Max(_minZoom, _zoom - _zoomStep);

                    try
                    {
                        // apply scale transform for smooth zoom (affects headers and cells)
                        if (DgScale != null)
                        {
                            DgScale.ScaleX = _zoom;
                            DgScale.ScaleY = _zoom;
                        }
                        else
                        {
                            // fallback: change font size
                            DgHoldings.FontSize = _baseFontSize * _zoom;
                        }
                    }
                    catch { }

                    // recalc column widths to fit new zoomed content and headers
                    try { AutoSizeColumns(); } catch { }

                    e.Handled = true;
                }
            }
            catch { }
        }

        private void StartTotalValuesTimer()
        {
            try
            {
                _totalValuesTimer = new DispatcherTimer();
                // update plotted total-value history every 30 seconds
                _totalValuesTimer.Interval = TimeSpan.FromSeconds(30);
                _totalValuesTimer.Tick += TotalValuesTimer_Tick;
                _totalValuesTimer.Start();

                // run once immediately to populate headers on startup
                _ = System.Threading.Tasks.Task.Run(() =>
                {
                    try
                    {
                        var vals = LoadLatestTotalsFromDb(_dbPath);
                        if (vals != null)
                        {
                            this.Dispatcher.Invoke(() =>
                            {
                                ApplyTotalsToHeaders(vals.Value.TotalRows, vals.Value.TotalShares, vals.Value.SumAvgTotal, vals.Value.SumToday, vals.Value.LastUpdated);
                                try
                                {
                                    var tb = this.FindName("TxtTotalValueLeft1") as TextBlock;
                                    var tb2 = this.FindName("TxtTotalValueLeft3") as TextBlock;
                                    if (tb != null)
                                    {
                                        var culture = CultureInfo.GetCultureInfo("de-DE");
                                        tb.Text = vals.Value.SumToday.ToString("N1", culture);
                                    }
                                    if (tb2 != null)
                                    {
                                        var culture = CultureInfo.GetCultureInfo("de-DE");
                                        tb2.Text = vals.Value.SumAvgTotal.ToString("N1", culture);
                                    }
                                }
                                catch { }
                            });
                            //// also update LastUpdated display
                            //if (!string.IsNullOrWhiteSpace(vals.Value.LastUpdated))
                            //    this.Dispatcher.Invoke(() => { try { var tb = this.FindName("TxtLastUpdated") as TextBlock; if (tb != null) tb.Text = vals.Value.LastUpdated; } catch { } });
                        }
                    }
                    catch { }
                });
            }
            catch { }
        }

        // Ensure the NEW_CSV_ACTIVE table exists without changing any rows.
        private void EnsureCsvActiveTableExists(string dbPath)
        {
            if (string.IsNullOrWhiteSpace(dbPath)) return;
            try
            {
                try { var dbDir = Path.GetDirectoryName(dbPath); if (!string.IsNullOrEmpty(dbDir) && !Directory.Exists(dbDir)) Directory.CreateDirectory(dbDir); } catch { }
                var cs = new SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
                using var conn = new SqliteConnection(cs);
                conn.Open();
                try { using var busy = conn.CreateCommand(); busy.CommandText = "PRAGMA busy_timeout = 5000;"; busy.ExecuteNonQuery(); } catch { }
                using var cmd = conn.CreateCommand();
                cmd.CommandText = @"CREATE TABLE IF NOT EXISTS NEW_CSV_ACTIVE (
    CSV TEXT PRIMARY KEY,
    Active INTEGER NOT NULL DEFAULT 0,
    Created TEXT
);";
                cmd.ExecuteNonQuery();
                try
                {
                    using var idx = conn.CreateCommand();
                    idx.CommandText = "CREATE UNIQUE INDEX IF NOT EXISTS idx_new_csv_active_csv ON NEW_CSV_ACTIVE (CSV);";
                    idx.ExecuteNonQuery();
                }
                catch { }
            }
            catch { }
        }



        private void TotalValuesTimer_Tick(object? sender, EventArgs e)
        {
            try
            {
                // load off-ui thread
                System.Threading.Tasks.Task.Run(() =>
                {
                    try
                    {
                        var vals = LoadLatestTotalsFromDb(_dbPath);
                        if (vals != null)
                        {
                            // update headers and left-top total on the UI thread
                            this.Dispatcher.Invoke(() =>
                            {
                                ApplyTotalsToHeaders(vals.Value.TotalRows, vals.Value.TotalShares, vals.Value.SumAvgTotal, vals.Value.SumToday, vals.Value.LastUpdated);
                                try
                                {
                                    var tb = this.FindName("TxtTotalValueLeft1") as TextBlock;
                                    if (tb != null)
                                    {
                                        var culture = CultureInfo.GetCultureInfo("de-DE");
                                        tb.Text = vals.Value.SumToday.ToString("N1", culture);
                                    }


                                    var tb2 = this.FindName("TxtTotalValueLeft2") as TextBlock;
                                    if (tb2 != null)
                                    {
                                        var culture = CultureInfo.GetCultureInfo("de-DE");
                                        var dif = vals.Value.SumToday - vals.Value.SumAvgTotal;
                                        tb2.Text = (dif).ToString("N1", culture);
                                        var tb3 = this.FindName("GewinnVerlust") as Border;

                                        if (tb3 != null)
                                        {
                                            tb3.Background = Brushes.OrangeRed;
                                            if (dif >= 0)
                                                tb3.Background = Brushes.MediumSeaGreen;
                                        }
                                    }
                                    var tb4 = this.FindName("TxtTotalValueLeft3") as TextBlock;
                                    if (tb4 != null)
                                    {
                                        var culture = CultureInfo.GetCultureInfo("de-DE");
                                        tb4.Text = vals.Value.SumAvgTotal.ToString("N1", culture);
                                    }


                                }
                                catch { }
                            });

                            // also update the displayed CSV path to the active CSV recorded in the shared DB
                            try
                            {
                                var active = GetActiveCsvFromDb(_dbPath);
                                if (!string.IsNullOrWhiteSpace(active))
                                {
                                    this.Dispatcher.Invoke(() => { try { if (TxtPath != null) TxtPath.Text = active; } catch { } });
                                }
                            }
                            catch { }
                            // refresh plotted total value history asynchronously on each timer tick (every 30s)
                            try { System.Threading.Tasks.Task.Run(() => LoadAndRenderTotalValueHistory()); } catch { }
                        }
                    }
                    catch { }
                });
            }
            catch { }
        }

        private void ApplyTotalsToHeaders(long totalRows, long totalShares, double sumAvgTotal, double sumToday, string lastUpdated)
        {
            try
            {
                var culture = CultureInfo.GetCultureInfo("de-DE");
                if (DgHoldings != null && DgHoldings.Columns != null && DgHoldings.Columns.Count > 8)
                {
                    // set headers based on DB values
                    DgHoldings.Columns[3].Header = $"#\n{totalShares.ToString("N0", culture)}";
                    DgHoldings.Columns[5].Header = $"Ø Gesamtwert\n{sumAvgTotal.ToString("N1", culture)}";
                    DgHoldings.Columns[8].Header = $"Gesamtwert\n{sumToday.ToString("N1", culture)}";
                    DgHoldings.Columns[10].Header = $"Updated\n{lastUpdated}";
                }
            }
            catch { }
        }

        // Resize DataGrid columns to fit current content. Measure using SizeToCells then lock to pixel width.
        private void AutoSizeColumns()
        {
            try
            {
                if (DgHoldings == null || DgHoldings.Columns == null) return;
                // Force layout so ActualWidth is updated after SizeToCells measurement
                DgHoldings.UpdateLayout();
                foreach (var col in DgHoldings.Columns)
                {
                    try
                    {
                        // measure to cells (content)
                        col.Width = new DataGridLength(1, DataGridLengthUnitType.SizeToCells);
                        DgHoldings.UpdateLayout();
                        double wCells = col.ActualWidth;

                        // measure to header
                        col.Width = new DataGridLength(1, DataGridLengthUnitType.SizeToHeader);
                        DgHoldings.UpdateLayout();
                        double wHeader = col.ActualWidth;

                        // choose the larger and freeze to pixel width
                        var final = Math.Max(wCells, wHeader);
                        if (double.IsNaN(final) || final <= 0) final = col.ActualWidth;
                        col.Width = new DataGridLength(final, DataGridLengthUnitType.Pixel);
                    }
                    catch { }
                }
            }
            catch { }
        }

        private DispatcherTimer? _clockTimer;
        private void StartClock()
        {
            try
            {
                _clockTimer = new DispatcherTimer();
                _clockTimer.Interval = TimeSpan.FromSeconds(1);
                _clockTimer.Tick += (s, e) =>
                {
                    try
                    {
                        var tb = this.FindName("TxtClock") as TextBlock;
                        if (tb != null) tb.Text = DateTime.Now.ToString("HH:mm:ss");
                    }
                    catch { }
                };
                _clockTimer.Start();
            }
            catch { }
        }

        private void UpdateSummaryAndHeaders()
        {
            try
            {
                var culture = CultureInfo.GetCultureInfo("de-DE");
                // Sum the displayed values in the '#' column (rounded to whole numbers) so header shows the same total
                var totalCount = _holdings.Sum(h => Math.Round(h.Shares, 0));
                // sum of values shown in the "Ø Gesamtwert" column (use 1 decimal like display)
                var sumAvgTotalDisplayed = _holdings.Sum(h => Math.Round(h.TotalValue, 1));
                // sum of values shown in the "Gesamtwert" column (TodayValue), match displayed rounding (1 decimal)
                var sumTodayDisplayed = _holdings.Sum(h => Math.Round(h.TodayValue, 1));

                // update only the '#' column header to include the sum of the column
                try
                {
                    if (DgHoldings != null && DgHoldings.Columns != null && DgHoldings.Columns.Count > 8)
                    {
                        // keep header label and append sum on new line; show integer total (no decimals)
                        DgHoldings.Columns[3].Header = $"#\n{totalCount.ToString("N0", culture)}";
                        // show sum of displayed Ø Gesamtwert values under that header (1 decimal)
                        DgHoldings.Columns[5].Header = $"Ø Gesamtwert\n{sumAvgTotalDisplayed.ToString("N1", culture)}";
                        // show sum of displayed Gesamtwert (TodayValue) under that header (1 decimal)
                        DgHoldings.Columns[8].Header = $"Gesamtwert\n{sumTodayDisplayed.ToString("N1", culture)}";
                    }
                }
                catch { }
            }
            catch { }
        }

        private void StartRefreshTimer()
        {
            try
            {
                _refreshTimer = new DispatcherTimer();
                _refreshTimer.Interval = TimeSpan.FromSeconds(15);
                _refreshTimer.Tick += RefreshTimer_Tick;
                _refreshTimer.Start();
            }
            catch { }
        }

        private async void RefreshTimer_Tick(object? sender, EventArgs e)
        {
            try
            {
                // load DB values off the UI thread
                var dbMap = await System.Threading.Tasks.Task.Run(() => LoadValuesFromDb(_dbPath));
                if (dbMap == null || dbMap.Count == 0) return;

                var updated = false;
                lock (_holdingsLock)
                {
                    if (_holdings != null && _holdings.Count > 0)
                    {
                        foreach (var h in _holdings)
                        {
                            try
                            {
                                if (h == null || string.IsNullOrWhiteSpace(h.Isin)) continue;
                                if (dbMap.TryGetValue(h.Isin, out var vals))
                                {
                                    // merge values from DB
                                    h.PurchaseValue = vals.PurchaseValue;
                                    h.Percent = vals.Percent;
                                    h.TodayValue = vals.TodayValue;
                                    h.Provider = vals.Provider ?? string.Empty;
                                    h.Updated = DateTime.Now;
                                    updated = true;
                                }
                            }
                            catch { }
                        }
                    }
                }

                if (updated)
                {
                    // refresh UI on the dispatcher
                    try
                    {
                        this.Dispatcher.Invoke(() =>
                        {
                            // preserve existing sort order and selection when refreshing
                            List<SortDescription> sorts = new List<SortDescription>();
                            string? selectedIsin = null;
                            try
                            {
                                var currentView = DgHoldings.ItemsSource != null ? CollectionViewSource.GetDefaultView(DgHoldings.ItemsSource) : null;
                                if (currentView != null)
                                {
                                    sorts = currentView.SortDescriptions.ToList();
                                }
                                if (DgHoldings.SelectedItem is HoldingRow sel) selectedIsin = sel.Isin;
                            }
                            catch { }

                            // update items source honoring current search filter and preserve sorts/selection
                            try { _currentSearch = (_currentSearch ?? (this.FindName("TxtSearch") as TextBox)?.Text); } catch { }
                            UpdateItemsSourcePreserveSortAndSelection(sorts, selectedIsin);

                            // restore selection if possible
                            try
                            {
                                if (!string.IsNullOrWhiteSpace(selectedIsin))
                                {
                                    var item = _holdings.FirstOrDefault(h => string.Equals(h.Isin, selectedIsin, StringComparison.OrdinalIgnoreCase));
                                    if (item != null) DgHoldings.SelectedItem = item;
                                }
                            }
                            catch { }

                            TxtInfo.Text = $"Geladene Zeilen: {_csvLines.Count} | Gehaltene ISINs: {_holdings.Count} | Letzte Aktualisierung: {DateTime.Now:HH:mm:ss}";
                            // Summary above list disabled per request
                            try { UpdateSummaryAndHeaders(); } catch { }
                            try { AutoSizeColumns(); } catch { }
                        });
                    }
                    catch { }
                }
            }
            catch { }
        }

        private Dictionary<string, (double PurchaseValue, double Percent, double TodayValue, string Provider)> LoadValuesFromDb(string dbPath)
        {
            var map = new Dictionary<string, (double PurchaseValue, double Percent, double TodayValue, string Provider)>(StringComparer.OrdinalIgnoreCase);
            try
            {
                if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) return map;
                var connStr = new SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
                using var conn = new SqliteConnection(connStr);
                conn.Open();
                using var cmd = conn.CreateCommand();
                // detect if the Provider column exists in the table to remain compatible with older DBs
                bool hasProvider = false;
                try
                {
                    using var pragma = conn.CreateCommand();
                    pragma.CommandText = "PRAGMA table_info(NEW_Holdings);";
                    using var pReader = pragma.ExecuteReader();
                    while (pReader.Read())
                    {
                        var col = pReader.IsDBNull(1) ? null : pReader.GetString(1);
                        if (string.Equals(col, "Provider", StringComparison.OrdinalIgnoreCase)) { hasProvider = true; break; }
                    }
                }
                catch { }

                if (hasProvider)
                {
                    cmd.CommandText = "SELECT Isin, PurchaseValue, Percent, TodayValue, Provider FROM NEW_Holdings WHERE Isin IS NOT NULL";
                }
                else
                {
                    cmd.CommandText = "SELECT Isin, PurchaseValue, Percent, TodayValue FROM NEW_Holdings WHERE Isin IS NOT NULL";
                }
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    try
                    {
                        var isin = reader.IsDBNull(0) ? null : reader.GetString(0)?.Trim();
                        if (string.IsNullOrWhiteSpace(isin)) continue;
                        double purchase = 0.0;
                        double percent = 0.0;
                        double today = 0.0;
                        string provider = string.Empty;
                        // Try parsing numeric values with German culture (comma decimal) first, then fall back to invariant (dot decimal)
                        if (!reader.IsDBNull(1))
                        {
                            var raw = reader.GetValue(1)?.ToString();
                            if (!double.TryParse(raw, NumberStyles.Any, CultureInfo.GetCultureInfo("de-DE"), out purchase))
                                double.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out purchase);
                        }
                        if (!reader.IsDBNull(2))
                        {
                            var raw = reader.GetValue(2)?.ToString();
                            if (!double.TryParse(raw, NumberStyles.Any, CultureInfo.GetCultureInfo("de-DE"), out percent))
                                double.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out percent);
                        }
                        if (!reader.IsDBNull(3))
                        {
                            var raw = reader.GetValue(3)?.ToString();
                            if (!double.TryParse(raw, NumberStyles.Any, CultureInfo.GetCultureInfo("de-DE"), out today))
                                double.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out today);
                        }
                        if (hasProvider)
                        {
                            if (!reader.IsDBNull(4)) provider = reader.GetString(4)?.Trim() ?? string.Empty;
                        }
                        map[isin] = (purchase, percent, today, provider);
                    }
                    catch { }
                }
                conn.Close();
            }
            catch { }
            return map;
        }

        private (long TotalRows, long TotalShares, double SumAvgTotal, double SumToday, string LastUpdated)? LoadLatestTotalsFromDb(string dbPath)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) return null;
                var connStr = new SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
                using var conn = new SqliteConnection(connStr);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT TotalRows, TotalShares, SumAvgTotal, SumTodayValue, LastUpdated FROM NEW_TotalValues ORDER BY Id DESC LIMIT 1;";
                using var reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    long rows = reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
                    long shares = 0;
                    if (!reader.IsDBNull(1))
                    {
                        try { shares = reader.GetInt64(1); }
                        catch { try { shares = Convert.ToInt64(Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture)); } catch { shares = 0; } }
                    }
                    double sumAvg = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture);
                    double sumToday = reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3), CultureInfo.InvariantCulture);
                    string lastUpdated = null;
                    try { lastUpdated = reader.IsDBNull(4) ? null : reader.GetString(4); } catch { lastUpdated = null; }
                    conn.Close();
                    return (rows, shares, sumAvg, sumToday, lastUpdated);
                }
                conn.Close();
            }
            catch { }
            return null;
        }

        protected override void OnClosed(EventArgs e)
        {
            try { _dbTimer?.Dispose(); } catch { }
            try { _refreshTimer?.Stop(); _refreshTimer = null; } catch { }
            try { _clockTimer?.Stop(); _clockTimer = null; } catch { }
            try { _reapplyPeriodicTimer?.Stop(); _reapplyPeriodicTimer = null; } catch { }
            try { _udpStatusTimer?.Stop(); _udpStatusTimer = null; } catch { }
            try { StopUdpListener(); } catch { }
            // Layout persistence disabled: do not save layout on close
            base.OnClosed(e);
        }

        // No periodic DB timer. DB updates are performed explicitly after loading a CSV.

        private void WriteHoldingsToDb()
        {
            // do not write to DB unless explicitly allowed (e.g. after loading a new CSV)
            if (!_allowDbWrites) return;
            try
            {
                // if we have no holdings in memory, try loading last saved holdings
                if ((_holdings == null || _holdings.Count == 0))
                {
                    try { LoadLastHoldings(); } catch { }
                }
                // ensure table exists and overwrite data
                var connStr = new SqliteConnectionStringBuilder { DataSource = _dbPath }.ToString();
                using var conn = new SqliteConnection(connStr);
                conn.Open();

                using var cmdCreate = conn.CreateCommand();
                cmdCreate.CommandText = @"
CREATE TABLE IF NOT EXISTS NEW_Holdings (
    Isin TEXT PRIMARY KEY,
    Name TEXT,
    Shares REAL,
    AvgBuyPrice REAL,
    PurchaseValue REAL,
    Percent REAL,
    TotalValue REAL,
    TodayValue REAL,
    Provider TEXT,
    Updated TEXT
);
";
                cmdCreate.ExecuteNonQuery();
                // Ensure compatibility with older DBs: add Provider/Updated columns if missing
                try
                {
                    using var pragma = conn.CreateCommand();
                    pragma.CommandText = "PRAGMA table_info(NEW_Holdings);";
                    using var pReader = pragma.ExecuteReader();
                    var existingCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    while (pReader.Read())
                    {
                        try { existingCols.Add(pReader.GetString(1)); } catch { }
                    }
                    if (!existingCols.Contains("Provider"))
                    {
                        try { using var alter = conn.CreateCommand(); alter.CommandText = "ALTER TABLE NEW_Holdings ADD COLUMN Provider TEXT;"; alter.ExecuteNonQuery(); } catch { }
                    }
                    if (!existingCols.Contains("Updated"))
                    {
                        try { using var alter2 = conn.CreateCommand(); alter2.CommandText = "ALTER TABLE NEW_Holdings ADD COLUMN Updated TEXT;"; alter2.ExecuteNonQuery(); } catch { }
                    }
                }
                catch { }

                using var tx = conn.BeginTransaction();
                using var cmdDel = conn.CreateCommand();
                cmdDel.CommandText = "DELETE FROM NEW_Holdings;";
                cmdDel.ExecuteNonQuery();

                // copy holdings under lock to avoid concurrent modification
                List<HoldingRow> snapshot;
                lock (_holdingsLock)
                {
                    snapshot = _holdings != null ? new List<HoldingRow>(_holdings) : new List<HoldingRow>();
                }

                foreach (var h in snapshot)
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"INSERT INTO NEW_Holdings (Isin, Name, Shares, AvgBuyPrice, PurchaseValue, Percent, TotalValue, TodayValue, Provider, Updated)
VALUES ($isin, $name, $shares, $avg, $purchase, $percent, $total, $today, $provider, $updated);";
                    cmd.Parameters.AddWithValue("$isin", h.Isin ?? string.Empty);
                    cmd.Parameters.AddWithValue("$name", h.Name ?? string.Empty);
                    // round numeric values to 2 decimal places before storing in DB
                    // limit stored numeric precision to 1 decimal place
                    cmd.Parameters.AddWithValue("$shares", Math.Round(h.Shares, 1));
                    cmd.Parameters.AddWithValue("$avg", Math.Round(h.AvgBuyPrice, 1));
                    cmd.Parameters.AddWithValue("$purchase", Math.Round(h.PurchaseValue, 1));
                    cmd.Parameters.AddWithValue("$percent", Math.Round(h.Percent, 1));
                    cmd.Parameters.AddWithValue("$total", Math.Round(h.TotalValue, 1));
                    cmd.Parameters.AddWithValue("$today", Math.Round(h.TodayValue, 1));
                    cmd.Parameters.AddWithValue("$provider", h.Provider ?? string.Empty);
                    cmd.Parameters.AddWithValue("$updated", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                    cmd.ExecuteNonQuery();
                }

                tx.Commit();
            }
            catch
            {
                // skip errors to keep polling
            }
        }

        private void ComputeHoldings(List<string> lines)
        {
            _holdings.Clear();
            if (lines == null || lines.Count == 0) return;

            var culture = CultureInfo.GetCultureInfo("de-DE");
            var transactions = new List<(int Index, DateTime Timestamp, string ISIN, string Name, double SignedShares, double Price, double Fee, double Tax)>();
            int lineIndex = 0;

            for (int i = 0; i < lines.Count; i++)
            {
                var raw = lines[i];
                if (string.IsNullOrWhiteSpace(raw)) continue;
                var parts = raw.Split(';');
                if (i == 0 && parts.Length > 5 && parts[0].ToLowerInvariant().Contains("date")) continue;

                lineIndex++;
                if (parts.Length < 9) continue;

                var dateText = parts.Length > 0 ? parts[0].Trim() : string.Empty;
                var timeText = parts.Length > 1 ? parts[1].Trim() : string.Empty;
                var name = parts.Length > 4 ? parts[4].Trim('"') : string.Empty;
                var type = parts.Length > 6 ? parts[6].Trim() : string.Empty;
                var isin = parts.Length > 7 ? parts[7].Trim() : string.Empty;
                var sharesText = parts.Length > 8 ? parts[8].Trim() : string.Empty;
                var priceText = parts.Length > 9 ? parts[9].Trim() : string.Empty;
                var amountText = parts.Length > 10 ? parts[10].Trim() : string.Empty;
                var feeText = parts.Length > 11 ? parts[11].Trim() : string.Empty;
                var taxText = parts.Length > 12 ? parts[12].Trim() : string.Empty;

                if (string.IsNullOrWhiteSpace(isin)) continue;

                DateTime timestamp = DateTime.MinValue;
                try { timestamp = DateTime.ParseExact(dateText + " " + timeText, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture); } catch { }

                if (!double.TryParse(sharesText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double shares)) shares = 0;
                if (!double.TryParse(priceText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double price)) price = double.NaN;
                if (!double.TryParse(feeText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double fee)) fee = 0.0;
                if (!double.TryParse(taxText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double tax)) tax = 0.0;

                double signedShares = 0;
                if (!string.IsNullOrWhiteSpace(amountText) && double.TryParse(amountText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double amount))
                {
                    signedShares = amount < 0 ? shares : (amount > 0 ? -shares : 0);
                }

                if (signedShares == 0 && !string.IsNullOrWhiteSpace(type))
                {
                    var t = type.ToLowerInvariant();
                    if (t.Contains("buy") || t.Contains("kauf") || t.Contains("acq") || t.Contains("purchase")) signedShares = shares;
                    else if (t.Contains("sell") || t.Contains("verkauf") || t.Contains("short") || t.Contains("leer")) signedShares = -shares;
                }

                transactions.Add((lineIndex, timestamp, isin, name, signedShares, price, fee, tax));
            }

            var ordered = transactions.OrderBy(t => t.Timestamp).ThenBy(t => t.Index).ToList();
            var lots = new Dictionary<string, Queue<(double shares, double price, string name)>>(StringComparer.OrdinalIgnoreCase);

            foreach (var tx in ordered)
            {
                if (!lots.TryGetValue(tx.ISIN, out var q)) { q = new Queue<(double, double, string)>(); lots[tx.ISIN] = q; }

                if (tx.SignedShares > 0)
                {
                    var lotPrice = double.IsNaN(tx.Price) ? 0.0 : tx.Price;
                    q.Enqueue((tx.SignedShares, lotPrice, tx.Name));
                }
                else if (tx.SignedShares < 0)
                {
                    var remaining = -tx.SignedShares;
                    while (remaining > 0 && q.Count > 0)
                    {
                        var head = q.Peek();
                        if (head.shares <= remaining)
                        {
                            remaining -= head.shares;
                            q.Dequeue();
                        }
                        else
                        {
                            var newHead = (head.shares - remaining, head.price, head.name);
                            q.Dequeue();
                            q.Enqueue(newHead);
                            remaining = 0;
                        }
                    }
                }
            }

            foreach (var kv in lots)
            {
                var isin = kv.Key;
                var q = kv.Value;
                var totalShares = q.Sum(x => x.shares);
                if (totalShares <= 0) continue;
                var totalCost = q.Sum(x => x.shares * x.price);
                var avg = totalShares > 0 ? totalCost / totalShares : 0.0;
                var displayName = q.Select(x => x.name).FirstOrDefault(n => !string.IsNullOrWhiteSpace(n)) ?? string.Empty;

                // initial purchase value is 0 (will be updated later with current buy value)
                double purchaseValue = 0.0;
                _holdings.Add(new HoldingRow
                {
                    Isin = isin,
                    Name = displayName,
                    Shares = totalShares,
                    AvgBuyPrice = avg,
                    TotalValue = totalShares * avg,
                    Percent = 0.0,
                    PurchaseValue = purchaseValue,
                    // TodayValue should be computed from the (possibly later updated) PurchaseValue
                    TodayValue = totalShares * purchaseValue,
                    Updated = DateTime.Now
                });
            }

            try { SaveLastHoldings(); } catch { }

            // update UI
            DgHoldings.ItemsSource = null;
            // apply current search filter when recomputing holdings
            UpdateItemsSourcePreserveSortAndSelection();
        }

        private void BtnExportCsv_Click(object sender, RoutedEventArgs e)
        {
            if (_holdings == null || _holdings.Count == 0)
            {
                MessageBox.Show(this, "Keine gehaltenen Positionen zum Exportieren.", "Info", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var dlg = new Microsoft.Win32.SaveFileDialog()
            {
                Title = "Exportiere Holdings als CSV",
                Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                FileName = "holdings_export.csv"
            };

            if (dlg.ShowDialog(this) == true)
            {
                try
                {
                    var path = dlg.FileName;
                    var culture = CultureInfo.GetCultureInfo("de-DE");
                    var lines = new List<string>();
                    lines.Add("ISIN;Name;Shares;AvgBuyPrice;PurchaseValue;Percent;TotalValue;TodayValue;Updated;Trail");
                    foreach (var h in _holdings)
                    {
                        var updated = h.Updated.HasValue ? h.Updated.Value.ToString("yyyy-MM-dd HH:mm:ss") : string.Empty;
                        var line = string.Join(";", new[] {
                            h.Isin,
                            Escape(h.Name),
                            h.Shares.ToString("N1", culture),
                            h.AvgBuyPrice.ToString("N1", culture),
                            h.PurchaseValue.ToString("N1", culture),
                            h.Percent.ToString("N1", culture),
                            h.TotalValue.ToString("N1", culture),
                            h.TodayValue.ToString("N1", culture),
                            updated,
                            h.Trail.ToString("N1", culture)
                        });
                        lines.Add(line);
                    }
                    File.WriteAllLines(path, lines, Encoding.UTF8);
                    MessageBox.Show(this, "Export erfolgreich: " + path, "Erfolg", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, "Fehler beim Export: " + ex.Message, "Fehler", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private string Escape(string s)
        {
            if (s == null) return string.Empty;
            if (s.Contains(";") || s.Contains('"') || s.Contains('\n'))
                return '"' + s.Replace("\"", "\"\"") + '"';
            return s;
        }

        private void SaveLastHoldings()
        {
            try
            {
                var appData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
                var dir = Path.Combine(appData, "TradeMVVM.ReadHoldings");
                if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
                var file = Path.Combine(dir, "last_holdings.json");
                var opts = new JsonSerializerOptions { WriteIndented = true };
                File.WriteAllText(file, JsonSerializer.Serialize(_holdings, opts), Encoding.UTF8);
            }
            catch { }
        }

        private void LoadLastHoldings()
        {
            try
            {
                var appData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
                var file = Path.Combine(appData, "TradeMVVM.ReadHoldings", "last_holdings.json");
                if (!File.Exists(file)) return;
                var txt = File.ReadAllText(file, Encoding.UTF8);
                var list = JsonSerializer.Deserialize<List<HoldingRow>>(txt);
                if (list != null)
                {
                    // try to merge live values from DB so Gewinn/Verlust can be shown immediately
                    try
                    {
                        var dbMap = LoadValuesFromDb(_dbPath);
                        if (dbMap != null && dbMap.Count > 0)
                        {
                            foreach (var h in list)
                            {
                                try
                                {
                                    if (h == null || string.IsNullOrWhiteSpace(h.Isin)) continue;
                                    if (dbMap.TryGetValue(h.Isin, out var vals))
                                    {
                                        h.PurchaseValue = vals.PurchaseValue;
                                        h.Percent = vals.Percent;
                                        h.TodayValue = vals.TodayValue;
                                        h.Provider = vals.Provider ?? string.Empty;
                                        h.Updated = DateTime.Now;
                                    }
                                    else
                                    {
                                        // fall back: preserve stored values; if none, keep 0
                                        h.PurchaseValue = h.PurchaseValue;
                                        h.TodayValue = h.Shares * h.PurchaseValue;
                                    }
                                }
                                catch { }
                            }
                        }
                        else
                        {
                            // no live DB values -> ensure TodayValue computed from PurchaseValue (may be 0)
                            foreach (var h in list) { try { h.TodayValue = h.Shares * h.PurchaseValue; } catch { } }
                        }
                    }
                    catch { }

                    _holdings = list;
                    // apply current search filter when loading
                    UpdateItemsSourcePreserveSortAndSelection();

                    // update Gewinn/Verlust display from loaded holdings
                    try
                    {
                        var culture = CultureInfo.GetCultureInfo("de-DE");
                        var sumAvgTotalDisplayed = _holdings.Sum(h => Math.Round(h.TotalValue, 1));
                        var sumTodayDisplayed = _holdings.Sum(h => Math.Round(h.TodayValue, 1));
                        var dif = sumTodayDisplayed - sumAvgTotalDisplayed;

                        try
                        {
                            var tb = this.FindName("TxtTotalValueLeft1") as TextBlock;
                            if (tb != null) tb.Text = sumTodayDisplayed.ToString("N1", culture);
                        }
                        catch { }



                        try
                        {
                            var tb2 = this.FindName("TxtTotalValueLeft2") as TextBlock;
                            if (tb2 != null) tb2.Text = dif.ToString("N1", culture);
                        }
                        catch { }

                        try
                        {
                            var tb3 = this.FindName("TxtTotalValueLeft3") as TextBlock;
                            if (tb3 != null) tb3.Text = sumAvgTotalDisplayed.ToString("N1", culture);
                        }
                        catch { }

                        try
                        {
                            var border = this.FindName("GewinnVerlust") as Border;
                            if (border != null)
                            {
                                border.Background = Brushes.OrangeRed;
                                if (dif >= 0) border.Background = Brushes.MediumSeaGreen;
                            }
                        }
                        catch { }
                    }
                    catch { }

                    TxtInfo.Text = $"Geladene Zeilen: {_csvLines.Count} | Gehaltene ISINs: {_holdings.Count} (aus letzter Sitzung)";
                }
            }
            catch { }
        }

        private void BtnOpenCsv_Click(object sender, RoutedEventArgs e)
        {
            var dlg = new OpenFileDialog()
            {
                Title = "CSV-Datei wählen",
                Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                CheckFileExists = true,
                Multiselect = false
            };

            if (dlg.ShowDialog(this) == true)
            {
                try
                {
                    var path = dlg.FileName;
                    TxtPath.Text = path;
                    _csvLines = new List<string>(File.ReadAllLines(path, Encoding.UTF8));
                    // Track active CSV in the shared trading DB: create table if missing and mark this CSV active
                    try { SetActiveCsvInDb(_dbPath, path); } catch { }
                    TxtInfo.Text = $"Geladene Zeilen: {_csvLines.Count}";

                    // compute holdings using FIFO logic
                    ComputeHoldings(_csvLines);
                    // merge purchase/today/percent values from DB for existing ISINs
                    try
                    {
                        var dbMap = LoadValuesFromDb(_dbPath);
                        lock (_holdingsLock)
                        {
                            foreach (var h in _holdings)
                            {
                                if (h == null || string.IsNullOrWhiteSpace(h.Isin)) continue;
                                if (dbMap.TryGetValue(h.Isin, out var vals))
                                {
                                    h.PurchaseValue = vals.PurchaseValue;
                                    h.Percent = vals.Percent;
                                    h.TodayValue = vals.TodayValue;
                                    h.Provider = vals.Provider ?? string.Empty;
                                    // keep Updated as now
                                    h.Updated = DateTime.Now;
                                }
                            }
                        }
                    }
                    catch { }

                    // compute and display Gewinn/Verlust (difference between SumToday and SumAvg) immediately after loading
                    try
                    {
                        var culture = CultureInfo.GetCultureInfo("de-DE");
                        var sumAvgTotalDisplayed = _holdings.Sum(h => Math.Round(h.TotalValue, 1));
                        var sumTodayDisplayed = _holdings.Sum(h => Math.Round(h.TodayValue, 1));
                        var dif = sumTodayDisplayed - sumAvgTotalDisplayed;

                        try
                        {
                            var tbLeft1 = this.FindName("TxtTotalValueLeft1") as TextBlock;
                            if (tbLeft1 != null) tbLeft1.Text = sumTodayDisplayed.ToString("N1", culture);
                        }
                        catch { }

                        try
                        {
                            var tbLeft2 = this.FindName("TxtTotalValueLeft2") as TextBlock;
                            if (tbLeft2 != null) tbLeft2.Text = dif.ToString("N1", culture);
                        }
                        catch { }

                        try
                        {
                            var border = this.FindName("GewinnVerlust") as Border;
                            if (border != null)
                            {
                                border.Background = Brushes.OrangeRed;
                                if (dif >= 0) border.Background = Brushes.MediumSeaGreen;
                            }
                        }
                        catch { }
                    }
                    catch { }

                    TxtInfo.Text += $" | Gehaltene ISINs: {_holdings.Count}";
                    if (_holdings.Count > 0)
                    {
                        TxtInfo.Text += " -> " + string.Join(", ", _holdings.Select(h => h.Isin).Take(10));
                        // apply current filter when setting ItemsSource after CSV load
                        UpdateItemsSourcePreserveSortAndSelection();
                        try { AutoSizeColumns(); } catch { }
                    }

                    // enable DB writes and write merged holdings to DB (single update after loading CSV)
                    try { _allowDbWrites = true; WriteHoldingsToDb(); } catch { }
                    // also ensure NEW_CSV_ACTIVE reflects this CSV selection
                    try { SetActiveCsvInDb(_dbPath, path); } catch { }
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, "Fehler beim Lesen der Datei: " + ex.Message, "Fehler", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        // Ensure NEW_CSV_ACTIVE exists and set only this CSV filename as active (store filename only)
        private void SetActiveCsvInDb(string dbPath, string csvPathOrName)
        {
            if (string.IsNullOrWhiteSpace(dbPath) || string.IsNullOrWhiteSpace(csvPathOrName)) return;
            var csvName = Path.GetFileName(csvPathOrName);

            // Try a few times in case DB is temporarily locked by another process
            for (int attempt = 1; attempt <= 5; attempt++)
            {
                try
                {
                    // ensure directory exists
                    try { var dbDir = Path.GetDirectoryName(dbPath); if (!string.IsNullOrEmpty(dbDir) && !Directory.Exists(dbDir)) Directory.CreateDirectory(dbDir); } catch { }

                    var cs = new SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
                    using var conn = new SqliteConnection(cs);
                    conn.Open();

                    using var busy = conn.CreateCommand();
                    busy.CommandText = "PRAGMA busy_timeout = 5000;";
                    busy.ExecuteNonQuery();

                    using var cmdCreate = conn.CreateCommand();
                    cmdCreate.CommandText = @"
CREATE TABLE IF NOT EXISTS NEW_CSV_ACTIVE (
    CSV TEXT PRIMARY KEY,
    Active INTEGER NOT NULL DEFAULT 0,
    Created TEXT
);";
                    cmdCreate.ExecuteNonQuery();

                    using var tran = conn.BeginTransaction();
                    try
                    {
                        using var updAll = conn.CreateCommand();
                        updAll.Transaction = tran;
                        updAll.CommandText = "UPDATE NEW_CSV_ACTIVE SET Active = 0;";
                        updAll.ExecuteNonQuery();

                        using var ins = conn.CreateCommand();
                        ins.Transaction = tran;
                        // Insert only if missing, then update existing row so rowid/Id is preserved.
                        ins.CommandText = @"INSERT OR IGNORE INTO NEW_CSV_ACTIVE (CSV, Active, Created) VALUES (@csv, 1, @created);";
                        ins.Parameters.AddWithValue("@csv", csvName ?? string.Empty);
                        ins.Parameters.AddWithValue("@created", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                        ins.ExecuteNonQuery();

                        using var upd = conn.CreateCommand();
                        upd.Transaction = tran;
                        upd.CommandText = @"UPDATE NEW_CSV_ACTIVE SET Active = 1, Created = @created WHERE CSV = @csv;";
                        upd.Parameters.AddWithValue("@csv", csvName ?? string.Empty);
                        upd.Parameters.AddWithValue("@created", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                        upd.ExecuteNonQuery();

                        tran.Commit();
                    }
                    catch { try { tran.Rollback(); } catch { } throw; }

                    // verify table exists
                    using var chk = conn.CreateCommand();
                    chk.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='NEW_CSV_ACTIVE';";
                    var exists = chk.ExecuteScalar();
                    if (exists != null) return;
                }
                catch (Exception ex)
                {
                    // last attempt -> rethrow silently, otherwise wait and retry
                    if (attempt == 5) return;
                    try { Thread.Sleep(50 * attempt); } catch { }
                }
            }
        }
    }
}
