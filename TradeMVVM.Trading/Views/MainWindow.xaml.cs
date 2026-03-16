using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Threading;
using TradeMVVM.Trading.Services;
using TradeMVVM.Trading.Presentation.ViewModels;

namespace TradeMVVM.Trading.Views
{
    public partial class MainWindow : Window
    {
        private MainViewModel _vm;
        private DispatcherTimer _timer;
        private readonly SettingsService _settings;
        private bool _isApplyingSavedLayout;
        private bool _isRefreshInProgress;
        private DateTime _lastRenderUtc = DateTime.MinValue;
        private int _lastRenderedPointCount = -1;
        private DateTime _lastRenderedDataTimeUtc = DateTime.MinValue;
        private string _lastRenderedSignature = string.Empty;

        public MainWindow()
        {
            InitializeComponent();

            try
            {
                _settings = App.Services?.GetService(typeof(SettingsService)) as SettingsService ?? new SettingsService();
            }
            catch
            {
                _settings = new SettingsService();
            }

            ApplySavedLayout();

            this.Closing += MainWindow_Closing;
            this.SizeChanged += MainWindow_SizeChanged;
            this.LocationChanged += MainWindow_LocationChanged;

            if (MainColumnSplitter != null)
                MainColumnSplitter.DragCompleted += MainColumnSplitter_DragCompleted;

            // 🔥 Charts erzeugt eigenen ZoomController
            // VM creation deferred until Loaded to ensure all XAML parts are initialized
            this.Loaded += MainWindow_Loaded;
            this.Activated += MainWindow_Activated;
            // restore window if it's off-screen and ensure it can be activated
            this.StateChanged += MainWindow_StateChanged;
            // make sure saved position is visible on startup
            EnsureWindowIsOnScreen();
        }

        private void MainWindow_StateChanged(object? sender, EventArgs e)
        {
            try
            {
                if (WindowState == WindowState.Normal)
                {
                    // try to bring to foreground
                    try { Topmost = true; } catch { }
                    try { Topmost = false; } catch { }
                    try { Activate(); } catch { }
                }
            }
            catch { }
        }

        private void MainWindow_Activated(object? sender, EventArgs e)
        {
            try
            {
                // when activated ensure window is not hidden behind other windows
                try { Topmost = true; } catch { }
                try { Topmost = false; } catch { }
            }
            catch { }
        }

        // Ensure window is within the virtual screen bounds. If not, center it on the virtual screen.
        private void EnsureWindowIsOnScreen()
        {
            try
            {
                // tolerances and virtual screen bounds
                double vx = SystemParameters.VirtualScreenLeft;
                double vy = SystemParameters.VirtualScreenTop;
                double vw = SystemParameters.VirtualScreenWidth;
                double vh = SystemParameters.VirtualScreenHeight;

                // If width/height are not sensible, clamp to reasonable defaults
                double w = double.IsNaN(Width) || Width <= 0 ? 800 : Width;
                double h = double.IsNaN(Height) || Height <= 0 ? 600 : Height;

                // If the saved window is completely outside the virtual screen, center it
                bool outsideHoriz = Left + w < vx || Left > vx + vw;
                bool outsideVert = Top + h < vy || Top > vy + vh;

                if (outsideHoriz || outsideVert)
                {
                    Left = vx + Math.Max(0, (vw - w) / 2.0);
                    Top = vy + Math.Max(0, (vh - h) / 2.0);
                }
            }
            catch { }
        }

        private void MainWindow_Loaded(object? sender, RoutedEventArgs e)
        {
            try
            {
                // ensure shown in taskbar and can be activated
                try { ShowInTaskbar = true; } catch { }
                try { ShowActivated = true; } catch { }

                // create VM now that the visual tree is ready
                _vm = new MainViewModel(Charts?.ZoomController);
                DataContext = _vm;
                try { TradeMVVM.Trading.App.MainViewModelInstance = _vm; } catch { }

                _timer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1) };
                _timer.Tick += (s, ev) => RefreshViews();
                _timer.Start();

                // final attempt to ensure window is visible and active
                try { EnsureWindowIsOnScreen(); } catch { }
                try { Activate(); } catch { }
            }
            catch (Exception ex)
            {
                try { MessageBox.Show($"Fehler beim Erstellen des MainViewModel:\n{ex}", "Fehler beim Start", MessageBoxButton.OK, MessageBoxImage.Error); } catch { }
                throw;
            }
        }

        private void MainWindow_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            SaveLayout();
            _vm?.Dispose();
        }

        private void MainWindow_SizeChanged(object sender, SizeChangedEventArgs e)
        {
            SaveLayout();
        }

        private void MainColumnSplitter_DragCompleted(object sender, DragCompletedEventArgs e)
        {
            SaveLayout();
        }

        private void MainWindow_LocationChanged(object sender, EventArgs e)
        {
            SaveLayout();
        }

        private void ApplySavedLayout()
        {
            if (_settings == null)
                return;

            _isApplyingSavedLayout = true;
            try
            {
                if (_settings.MainWindowWidth > 0)
                    Width = _settings.MainWindowWidth;
                if (_settings.MainWindowHeight > 0)
                    Height = _settings.MainWindowHeight;
                Left = _settings.MainWindowLeft;
                Top = _settings.MainWindowTop;

                if (RightPanelColumn != null && _settings.MainWindowRightPanelWidth > 0)
                {
                    var clamped = Math.Max(320.0, Math.Min(2400.0, _settings.MainWindowRightPanelWidth));
                    RightPanelColumn.Width = new GridLength(clamped, GridUnitType.Pixel);
                }
            }
            catch { }
            finally
            {
                _isApplyingSavedLayout = false;
            }
        }

        private void SaveLayout()
        {
            if (_isApplyingSavedLayout || _settings == null)
                return;

            try
            {
                var bounds = WindowState == WindowState.Normal ? new Rect(Left, Top, Width, Height) : RestoreBounds;
                _settings.MainWindowWidth = bounds.Width;
                _settings.MainWindowHeight = bounds.Height;
                _settings.MainWindowLeft = bounds.Left;
                _settings.MainWindowTop = bounds.Top;
                if (RightPanelColumn != null)
                    _settings.MainWindowRightPanelWidth = RightPanelColumn.ActualWidth > 0 ? RightPanelColumn.ActualWidth : RightPanelColumn.Width.Value;
                _settings.Save();
            }
            catch { }
        }

        public void ResetLayoutDefaults()
        {
            _isApplyingSavedLayout = true;
            try
            {
                Width = 2200.0;
                Height = 1100.0;
                Left = 120.0;
                Top = 120.0;
                if (RightPanelColumn != null)
                    RightPanelColumn.Width = new GridLength(1200.0, GridUnitType.Pixel);
            }
            catch { }
            finally
            {
                _isApplyingSavedLayout = false;
            }

            try
            {
                Charts?.ResetLayoutDefaults();
            }
            catch { }

            SaveLayout();
        }

        private void RefreshViews()
        {
            if (_isRefreshInProgress)
                return;

            _isRefreshInProgress = true;
            try
            {
                try { System.Diagnostics.Debug.WriteLine($"RefreshViews start: Stocks={_vm?.Stocks?.Count ?? 0}, PriceHistoryKeys={_vm?.PriceHistory?.Count ?? 0}"); } catch { }
                if (WindowState == WindowState.Minimized)
                    return;

            // load new data points from DB since last refresh
            _vm.RefreshFromDb();

            // take a safe snapshot of the in-memory price history to avoid concurrent modification
            var stockData = new Dictionary<string, List<Tuple<DateTime, double>>>();
            var knockoutData = new Dictionary<string, List<Tuple<DateTime, double>>>();

            lock (_vm.PriceHistory)
            {
                foreach (var (isin_wkn, _, type) in _vm.Stocks)
                {
                    if (!_vm.PriceHistory.TryGetValue(isin_wkn, out var history) || history == null)
                        continue;

                    // copy the list to avoid enumerating a list that the polling service may modify
                    var copy = new List<Tuple<DateTime, double>>(history);

                    if (type == TradeMVVM.Domain.StockType.Knockout)
                        knockoutData[isin_wkn] = copy;
                    else
                        stockData[isin_wkn] = copy;
                }
            }

            var totalPoints = 0;
            try
            {
                foreach (var kv in stockData)
                    totalPoints += kv.Value?.Count ?? 0;
                foreach (var kv in knockoutData)
                    totalPoints += kv.Value?.Count ?? 0;
                try { System.Diagnostics.Debug.WriteLine($"RefreshViews: stockSeries={stockData.Count}, knockoutSeries={knockoutData.Count}, totalPoints={totalPoints}"); } catch { }
            }
            catch { }

            var nowUtc = DateTime.UtcNow;

            // determine latest data timestamp across all series to detect new incoming points
            DateTime latestDataTimeUtc = DateTime.MinValue;
            try
            {
                foreach (var kv in stockData)
                {
                    var list = kv.Value;
                    if (list != null && list.Count > 0)
                    {
                        var t = list[list.Count - 1].Item1;
                        if (t.Kind == DateTimeKind.Unspecified)
                            t = DateTime.SpecifyKind(t, DateTimeKind.Local);
                        var utc = t.ToUniversalTime();
                        if (utc > latestDataTimeUtc) latestDataTimeUtc = utc;
                    }
                }
                foreach (var kv in knockoutData)
                {
                    var list = kv.Value;
                    if (list != null && list.Count > 0)
                    {
                        var t = list[list.Count - 1].Item1;
                        if (t.Kind == DateTimeKind.Unspecified)
                            t = DateTime.SpecifyKind(t, DateTimeKind.Local);
                        var utc = t.ToUniversalTime();
                        if (utc > latestDataTimeUtc) latestDataTimeUtc = utc;
                    }
                }
            }
            catch { }

            // build a lightweight signature of the latest point per series (time+ticks+percent)
            string signature = string.Empty;
            try
            {
                var parts = new List<string>();
                foreach (var kv in stockData.OrderBy(k => k.Key))
                {
                    var list = kv.Value;
                    if (list != null && list.Count > 0)
                    {
                        var lp = list[list.Count - 1];
                        parts.Add($"S|{kv.Key}|{lp.Item1.Ticks}|{lp.Item2.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}");
                    }
                }
                foreach (var kv in knockoutData.OrderBy(k => k.Key))
                {
                    var list = kv.Value;
                    if (list != null && list.Count > 0)
                    {
                        var lp = list[list.Count - 1];
                        parts.Add($"K|{kv.Key}|{lp.Item1.Ticks}|{lp.Item2.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}");
                    }
                }
                signature = string.Join(";", parts);
            }
            catch { }

            var hasDataChanged = totalPoints != _lastRenderedPointCount || latestDataTimeUtc > _lastRenderedDataTimeUtc || !string.Equals(signature, _lastRenderedSignature, StringComparison.Ordinal);
            var periodicRefreshDue = (nowUtc - _lastRenderUtc) >= TimeSpan.FromSeconds(5);

            if (!hasDataChanged && !periodicRefreshDue)
                return;

            // 1️⃣ Render
            try
            {
                Charts.Render(stockData, knockoutData);
            }
            catch (Exception ex)
            {
                try { System.Diagnostics.Debug.WriteLine($"Charts.Render failed: {ex}"); } catch { }
            }
            _lastRenderedPointCount = totalPoints;
            _lastRenderedDataTimeUtc = latestDataTimeUtc > DateTime.MinValue ? latestDataTimeUtc : _lastRenderedDataTimeUtc;
            _lastRenderedSignature = signature;
            _lastRenderUtc = nowUtc;

            // legend removed — no-op
            }
            finally
            {
                _isRefreshInProgress = false;
            }
        }

        // Helper to allow external controls (e.g. toolbar) to request an immediate refresh
        public void TriggerRefresh()
        {
            try
            {
                // force a render even if data point count unchanged (zoom change should re-render)
                try { _lastRenderedPointCount = -1; } catch { }
                try { _lastRenderUtc = DateTime.MinValue; } catch { }
                RefreshViews();
            }
            catch { }
        }


    }

}
