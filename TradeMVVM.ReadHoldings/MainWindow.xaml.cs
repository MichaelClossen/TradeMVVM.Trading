using Microsoft.Data.Sqlite;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
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
using ScottPlot;

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
                {
                    try { if (TxtPath != null) TxtPath.Text = active; } catch { }
                }
            }
            catch { }
            try { _baseFontSize = DgHoldings.FontSize; DgHoldings.PreviewMouseWheel += DgHoldings_PreviewMouseWheel; } catch { }
            // apply any restored zoom (RestoreLayout may have set _zoom before Initialize completed)
            try { if (DgScale != null) { DgScale.ScaleX = _zoom; DgScale.ScaleY = _zoom; } } catch { }
            try { LoadLastHoldings(); } catch { }
            // restore window size and splitter from settings (disabled)
            // Layout persistence is disabled per user request.
            // On startup, prefer showing the active CSV recorded in the shared DB
            // (NEW_CSV_ACTIVE table) instead of the default placeholder text.
            // start periodic refresh from DB every 15 seconds to keep purchase/today values up-to-date
            try { StartRefreshTimer(); } catch { }
            // perform one immediate refresh from DB at startup
            try { RefreshTimer_Tick(null, EventArgs.Empty); } catch { }
            // start periodic header totals refresh from NEW_TotalValues every 60 seconds
            try { StartTotalValuesTimer(); } catch { }
            // render initial total value history once controls are loaded
            try { this.Loaded += (s, e) => { LoadAndRenderTotalValueHistory(); }; } catch { }
            try { StartClock(); } catch { }
            // Ensure the NEW_CSV_ACTIVE table exists on startup so other tools can read active CSV state
            try { EnsureCsvActiveTableExists(_dbPath); } catch { }

            // ensure columns are sized to content on first load
            try { this.Loaded += MainWindow_Loaded; } catch { }
        }

        // Load total value history for the active CSV portfolio from NEW_TotalValues and render into ScottPlot
        private void LoadAndRenderTotalValueHistory()
        {
            try
            {
                System.Threading.Tasks.Task.Run(() =>
                {
                    try
                    {
                        // read active CSV name (filename) and then load history rows for that portfolio
                        var activeCsv = GetActiveCsvFromDb(_dbPath);
                        if (string.IsNullOrWhiteSpace(activeCsv)) return;

                        var points = new List<(DateTime time, double avg, double today)>();
                        // read from sqlite - detect actual column names for Created and SumTodayValue, optionally filter by portfolio/CSV
                        try
                        {
                            var csb = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder { DataSource = _dbPath }.ToString();
                            using var conn = new Microsoft.Data.Sqlite.SqliteConnection(csb);
                            conn.Open();

                            // inspect table columns
                            var createdCol = "Created";
                            var valueAvgCol = "SumAvgTotal";
                            var valueTodayCol = "SumTodayValue";
                            var portfolioCol = (string?)null;
                            try
                            {
                                using var pragma = conn.CreateCommand();
                                pragma.CommandText = "PRAGMA table_info(NEW_TotalValues);";
                                using var r = pragma.ExecuteReader();
                                var cols = new List<string>();
                                while (r.Read())
                                {
                                    try { cols.Add(r.GetString(1)); } catch { }
                                }
                                // pick created/last-updated column. Prefer explicit "LastUpdated" when present.
                                var candidatesCreated = new[] { "LastUpdated", "Created", "Time", "Timestamp", "CreatedAt" };
                                createdCol = cols.FirstOrDefault(c => candidatesCreated.Any(cc => string.Equals(c, cc, StringComparison.OrdinalIgnoreCase))) ?? cols.FirstOrDefault() ?? "LastUpdated";
                                // pick value column
                                var candidatesValueAvg = new[] { "SumAvgTotal", "SumAvg", "AvgTotal", "SumAvg" };
                                var candidatesValueToday = new[] { "SumTodayValue", "SumToday", "SumTodayVal", "TodayValue" };
                                valueAvgCol = cols.FirstOrDefault(c => candidatesValueAvg.Any(cc => string.Equals(c, cc, StringComparison.OrdinalIgnoreCase))) ?? cols.FirstOrDefault(c => c.IndexOf("Avg", StringComparison.OrdinalIgnoreCase) >= 0) ?? (cols.Count > 2 ? cols[2] : cols.FirstOrDefault() ?? "SumAvgTotal");
                                valueTodayCol = cols.FirstOrDefault(c => candidatesValueToday.Any(cc => string.Equals(c, cc, StringComparison.OrdinalIgnoreCase))) ?? cols.FirstOrDefault(c => c.IndexOf("Today", StringComparison.OrdinalIgnoreCase) >= 0) ?? (cols.Count > 1 ? cols[1] : cols.FirstOrDefault() ?? "SumTodayValue");
                                // detect portfolio/csv column
                                var candidatesPortfolio = new[] { "Portfolio", "CSV", "Csv", "PortfolioId" };
                                portfolioCol = cols.FirstOrDefault(c => candidatesPortfolio.Any(cc => string.Equals(c, cc, StringComparison.OrdinalIgnoreCase)));
                            }
                            catch { }

                            using var cmd = conn.CreateCommand();

                            // Try to read active CSV info from NEW_CSV_ACTIVE in this DB (some schemas store numeric Id)
                            string activeCsvFromDb = activeCsv;
                            long? activeId = null;
                            try
                            {
                                using var cmdActive = conn.CreateCommand();
                                cmdActive.CommandText = "SELECT CSV, Id FROM NEW_CSV_ACTIVE WHERE Active = 1 LIMIT 1;";
                                using var r2 = cmdActive.ExecuteReader();
                                if (r2.Read())
                                {
                                    try { if (!r2.IsDBNull(0)) activeCsvFromDb = r2.GetString(0); } catch { }
                                    try { if (!r2.IsDBNull(1)) activeId = r2.GetInt64(1); } catch { }
                                }
                            }
                            catch { }

                            if (!string.IsNullOrWhiteSpace(portfolioCol))
                            {
                                // allow matching either CSV string or numeric id in portfolio column
                                cmd.CommandText = $"SELECT \"{createdCol}\", \"{valueAvgCol}\", \"{valueTodayCol}\" FROM NEW_TotalValues WHERE \"{portfolioCol}\" = $portfolio OR \"{portfolioCol}\" = $portfolioId ORDER BY \"{createdCol}\" ASC;";
                                cmd.Parameters.AddWithValue("$portfolio", activeCsvFromDb ?? string.Empty);
                                cmd.Parameters.AddWithValue("$portfolioId", activeId.HasValue ? (object)activeId.Value : DBNull.Value);
                            }
                            else
                            {
                                cmd.CommandText = $"SELECT \"{createdCol}\", \"{valueAvgCol}\", \"{valueTodayCol}\" FROM NEW_TotalValues ORDER BY \"{createdCol}\" ASC;";
                            }

                            using var reader = cmd.ExecuteReader();
                            while (reader.Read())
                            {
                                try
                                {
                                    DateTime? created = null;
                                    try
                                    {
                                        var obj = reader.GetValue(0);
                                        if (obj is DateTime dt) created = dt;
                                        if (obj is string s)
                                        {
                                            // try multiple common formats including German format used by this app
                                            if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var pdt)) created = pdt;
                                            else if (DateTime.TryParse(s, CultureInfo.CurrentCulture, DateTimeStyles.AssumeLocal, out pdt)) created = pdt;
                                            else if (DateTime.TryParseExact(s, new[] { "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "yyyy-MM-ddTHH:mm:ssZ", "o", "dd.MM.yyyy HH:mm:ss", "dd.MM.yyyy" }, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out pdt)) created = pdt;
                                            else
                                            {
                                                // sometimes LastUpdated might be a numeric string (ticks or unix ms)
                                                if (long.TryParse(s, out var lv))
                                                {
                                                    if (Math.Abs(lv) > 1000000000000L) created = DateTimeOffset.FromUnixTimeMilliseconds(lv).DateTime;
                                                    else if (Math.Abs(lv) > 1000000000L) created = DateTimeOffset.FromUnixTimeSeconds(lv).DateTime;
                                                    else created = DateTime.FromOADate(lv);
                                                }
                                            }
                                        }
                                        else if (obj is long l)
                                        {
                                            // treat large numbers as unix milliseconds, medium as unix seconds, small as OADate
                                            if (Math.Abs(l) > 1000000000000L) // > ~2001-09-09 in ms
                                                created = DateTimeOffset.FromUnixTimeMilliseconds(l).DateTime;
                                            else if (Math.Abs(l) > 1000000000L) // > ~2001 in seconds
                                                created = DateTimeOffset.FromUnixTimeSeconds(l).DateTime;
                                            else
                                                created = DateTime.FromOADate(l);
                                        }
                                        else if (obj is double d)
                                        {
                                            // similar heuristic for double
                                            if (Math.Abs(d) > 1e12) // treat as ms
                                                created = DateTimeOffset.FromUnixTimeMilliseconds((long)d).DateTime;
                                            else if (Math.Abs(d) > 1e9) // treat as seconds
                                                created = DateTimeOffset.FromUnixTimeSeconds((long)d).DateTime;
                                            else
                                                created = DateTime.FromOADate(d);
                                        }
                                    }
                                    catch { }

                                    double avg = 0.0;
                                    double today = 0.0;
                                    try { avg = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture); } catch { }
                                    try { today = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture); } catch { }

                                    if (created.HasValue) points.Add((created.Value, avg, today));
                                }
                                catch { }
                            }

                            conn.Close();
                        }
                        catch { }

                        if (points.Count == 0) return;

                        // prepare arrays for ScottPlot (use DateTime OADate for X axis)
                        var xs = points.Select(p => p.time.ToOADate()).ToArray();
                        var ysAvg = points.Select(p => p.avg).ToArray();
                        var ysToday = points.Select(p => p.today).ToArray();

                        // diagnostic logging: sample values (avoid referencing local DB column vars out of scope)
                        try
                        {
                            System.Diagnostics.Debug.WriteLine($"LoadAndRenderTotalValueHistory: points.Count={points.Count}, xs.Length={xs.Length}");
                            for (int i = 0; i < Math.Min(5, xs.Length); i++)
                            {
                                try
                                {
                                    var oa = xs[i];
                                    var dt = DateTime.FromOADate(oa);
                                    System.Diagnostics.Debug.WriteLine($"  sample[{i}] oa={oa} dt={dt:o} avg={ysAvg[i]} today={ysToday[i]}");
                                }
                                catch (Exception ex) { System.Diagnostics.Debug.WriteLine("  sample parse error: " + ex.Message); }
                            }
                        }
                        catch { }

                        this.Dispatcher.BeginInvoke(new Action(() =>
                        {
                            try
                            {
                                if (PlotTotalValueHistory == null) return;
                                var plt = PlotTotalValueHistory.Plot;
                                plt.Clear();

                                // ScottPlot expects double[] Xs in OADate for DateTime axis. Use Add.Scatter which accepts Xs and Ys.
                                var scatterAvg = plt.Add.Scatter(xs, ysAvg);
                                var scatterToday = plt.Add.Scatter(xs, ysToday);
                                try
                                {
                                    var lw = scatterAvg.GetType().GetProperty("LineWidth"); if (lw != null && lw.CanWrite) lw.SetValue(scatterAvg, Convert.ChangeType(1.5, lw.PropertyType));
                                    var ms = scatterAvg.GetType().GetProperty("MarkerSize"); if (ms != null && ms.CanWrite) ms.SetValue(scatterAvg, Convert.ChangeType(6.0, ms.PropertyType));
                                    var mshape = scatterAvg.GetType().GetProperty("MarkerShape") ?? scatterAvg.GetType().GetProperty("Marker");
                                    if (mshape != null && mshape.CanWrite)
                                    {
                                        try
                                        {
                                            var t = mshape.PropertyType;
                                            if (t.IsEnum)
                                            {
                                            var names = Enum.GetNames(t);
                                                var prefer = names.FirstOrDefault(nm => nm.IndexOf("Circle", StringComparison.OrdinalIgnoreCase) >= 0)
                                                            ?? names.FirstOrDefault(nm => nm.IndexOf("Dot", StringComparison.OrdinalIgnoreCase) >= 0)
                                                            ?? names.FirstOrDefault();
                                                if (!string.IsNullOrEmpty(prefer))
                                                {
                                                    var val = Enum.Parse(t, prefer);
                                                    mshape.SetValue(scatterAvg, val);
                                                }
                                            }
                                        }
                                        catch { }
                                    }

                                    // set color to blue if supported
                                    try
                                    {
                                    var propColor = scatterAvg.GetType().GetProperty("Color");
                                        if (propColor != null && propColor.CanWrite)
                                        {
                                            var pt = propColor.PropertyType;
                                            if (pt == typeof(System.Drawing.Color))
                                            propColor.SetValue(scatterAvg, System.Drawing.Color.DodgerBlue);
                                        }
                                    }
                                    catch { }
                                }
                                catch { }

                                // style second series (today) in a different color
                                try
                                {
                                    var propColorT = scatterToday.GetType().GetProperty("Color");
                                    if (propColorT != null && propColorT.CanWrite)
                                    {
                                        var pt = propColorT.PropertyType;
                                        if (pt == typeof(System.Drawing.Color)) propColorT.SetValue(scatterToday, System.Drawing.Color.MediumSeaGreen);
                                    }
                                }
                                catch { }

                                // format bottom axis as DateTime using DateTimeAutomatic tick generator
                                try
                                {
                                    plt.Axes.DateTimeTicksBottom();
                                    plt.Axes.Bottom.TickGenerator = new ScottPlot.TickGenerators.DateTimeAutomatic()
                                    {
                                        LabelFormatter = (DateTime date) =>
                                        {
                                            try { return date.ToString("dd.MM HH:mm"); } catch { return date.ToString(); }
                                        }
                                    };
                                }
                                catch { }
                                try { plt.YLabel("Gesamtwert"); } catch { }

                                // ensure X limits fit data so DateTime ticks render correctly
                                try { if (xs.Length > 0) plt.Axes.SetLimitsX(xs.Min(), xs.Max()); } catch { }

                                // ensure autoscaling applied (use Axes.AutoScale which exists on this project's ScottPlot version)
                                try { plt.Axes.AutoScale(); } catch { }

                                // set labels on series and show legend
                                try
                                {
                                    var setLabel = new Action<object, string>((pl, lbl) =>
                                    {
                                        try
                                        {
                                            var prop = pl.GetType().GetProperty("Label") ?? pl.GetType().GetProperty("LegendText") ?? pl.GetType().GetProperty("LegendLabel");
                                            if (prop != null && prop.CanWrite && prop.PropertyType == typeof(string)) prop.SetValue(pl, lbl);
                                            else
                                            {
                                                var mi = pl.GetType().GetMethod("SetLabel") ?? pl.GetType().GetMethod("SetLegend");
                                                if (mi != null) mi.Invoke(pl, new object[] { lbl });
                                            }
                                        }
                                        catch { }
                                    });
                                    try { setLabel(scatterAvg, "Ø Gesamtwert"); } catch { }
                                    try { setLabel(scatterToday, "Gesamtwert"); } catch { }
                                    try
                                    {
                                        // show legend and try to position it in the bottom-left corner
                                        plt.Legend.IsVisible = true;

                                        try
                                        {
                                            var legendObj = plt.Legend;
                                            if (legendObj != null)
                                            {
                                                var lt = legendObj.GetType();
                                                // try common property names first
                                                var prop = lt.GetProperty("Location") ?? lt.GetProperty("LegendLocation") ?? lt.GetProperty("Position") ?? lt.GetProperty("Anchor");
                                                if (prop != null && prop.CanWrite && prop.PropertyType.IsEnum)
                                                {
                                                    var names = Enum.GetNames(prop.PropertyType);
                                                    var prefer = names.FirstOrDefault(n => n.IndexOf("LowerLeft", StringComparison.OrdinalIgnoreCase) >= 0)
                                                                 ?? names.FirstOrDefault(n => n.IndexOf("BottomLeft", StringComparison.OrdinalIgnoreCase) >= 0)
                                                                 ?? names.FirstOrDefault(n => n.IndexOf("SouthWest", StringComparison.OrdinalIgnoreCase) >= 0)
                                                                 ?? names.FirstOrDefault();
                                                    if (!string.IsNullOrEmpty(prefer))
                                                    {
                                                        prop.SetValue(legendObj, Enum.Parse(prop.PropertyType, prefer));
                                                    }
                                                }
                                                else
                                                {
                                                    // try common setter method names that accept an enum
                                                    var methods = new[] { "SetLocation", "SetPosition", "SetLegendLocation", "SetAnchor" };
                                                    foreach (var mname in methods)
                                                    {
                                                        var mi = lt.GetMethod(mname);
                                                        if (mi != null)
                                                        {
                                                            var p = mi.GetParameters().FirstOrDefault();
                                                            if (p != null && p.ParameterType.IsEnum)
                                                            {
                                                                var names = Enum.GetNames(p.ParameterType);
                                                                var prefer = names.FirstOrDefault(n => n.IndexOf("LowerLeft", StringComparison.OrdinalIgnoreCase) >= 0)
                                                                             ?? names.FirstOrDefault(n => n.IndexOf("BottomLeft", StringComparison.OrdinalIgnoreCase) >= 0)
                                                                             ?? names.FirstOrDefault(n => n.IndexOf("SouthWest", StringComparison.OrdinalIgnoreCase) >= 0)
                                                                             ?? names.FirstOrDefault();
                                                                if (!string.IsNullOrEmpty(prefer))
                                                                {
                                                                    mi.Invoke(legendObj, new object[] { Enum.Parse(p.ParameterType, prefer) });
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        catch { }
                                    }
                                    catch { }
                                }
                                catch { }

                                // redraw
                                try { PlotTotalValueHistory.Refresh(); } catch { }
                                try { Mouse.OverrideCursor = null; } catch { }
                            }
                            catch { }
                        }), DispatcherPriority.Background);
                    }
                    catch { }
                });

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
                                    if (tb != null)
                                    {
                                        var culture = CultureInfo.GetCultureInfo("de-DE");
                                        tb.Text = vals.Value.SumToday.ToString("N1", culture);
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

                    // if ItemsSource already points to our holdings list, just refresh the view
                    if (object.ReferenceEquals(DgHoldings.ItemsSource, _holdings))
                    {
                        CollectionViewSource.GetDefaultView(DgHoldings.ItemsSource)?.Refresh();
                    }
                    else
                    {
                        // set ItemsSource (first time or if it changed) and reapply sorts
                        DgHoldings.ItemsSource = _holdings;
                        var view = CollectionViewSource.GetDefaultView(DgHoldings.ItemsSource);
                        if (view != null)
                        {
                            try
                            {
                                // only reapply stored sorts when we actually captured some; otherwise preserve current sorting
                                if (sorts != null && sorts.Count > 0)
                                {
                                    view.SortDescriptions.Clear();
                                    foreach (var sd in sorts) view.SortDescriptions.Add(sd);
                                }
                                view.Refresh();
                            }
                            catch { }
                        }
                    }

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

private void ComputeHeldIsins(List<string> lines)
{
    _heldIsins.Clear();
    if (lines == null || lines.Count == 0) return;

    foreach (var line in lines)
    {
        if (string.IsNullOrWhiteSpace(line)) continue;
        var first = line.Split(new[] { ';', ',', '\t' }, StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        if (string.IsNullOrWhiteSpace(first)) continue;
        if (!_heldIsins.Contains(first)) _heldIsins.Add(first);
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
    DgHoldings.ItemsSource = _holdings;
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

private string[] SplitCsvLine(string line)
{
    if (line == null) return Array.Empty<string>();
    var parts = new List<string>();
    var sb = new StringBuilder();
    bool inQuote = false;
    for (int i = 0; i < line.Length; i++)
    {
        var ch = line[i];
        if (ch == '"') { inQuote = !inQuote; continue; }
        if (ch == ',' && !inQuote) { parts.Add(sb.ToString()); sb.Clear(); continue; }
        sb.Append(ch);
    }
    parts.Add(sb.ToString());
    return parts.ToArray();
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
                DgHoldings.ItemsSource = null;
                DgHoldings.ItemsSource = _holdings;

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

private void RestoreLayout()
{
    // Layout persistence disabled per user request. Do not read layout.json.
    return;
}

private void SaveLayout()
{
    // Layout persistence disabled per user request. Do not write layout.json.
    return;
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
                DgHoldings.ItemsSource = null;
                DgHoldings.ItemsSource = _holdings;
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
