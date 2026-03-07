using System;
using System.Collections.Generic;
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
        private readonly MainViewModel _vm;
        private readonly DispatcherTimer _timer;
        private readonly SettingsService _settings;
        private bool _isApplyingSavedLayout;
        private bool _isRefreshInProgress;
        private DateTime _lastRenderUtc = DateTime.MinValue;
        private int _lastRenderedPointCount = -1;

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
            _vm = new MainViewModel(Charts.ZoomController);
            DataContext = _vm;
            // register vm in App for global access on shutdown
            try { TradeMVVM.Trading.App.MainViewModelInstance = _vm; } catch { }

            _timer = new DispatcherTimer
            {
                Interval = TimeSpan.FromSeconds(1)
            };

            _timer.Tick += (s, e) => RefreshViews();
            _timer.Start();
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
            }
            catch { }

            var nowUtc = DateTime.UtcNow;
            var hasDataChanged = totalPoints != _lastRenderedPointCount;
            var periodicRefreshDue = (nowUtc - _lastRenderUtc) >= TimeSpan.FromSeconds(5);

            if (!hasDataChanged && !periodicRefreshDue)
                return;

            // 1️⃣ Render
            Charts.Render(stockData, knockoutData);
            _lastRenderedPointCount = totalPoints;
            _lastRenderUtc = nowUtc;

            // legend removed — no-op
            }
            finally
            {
                _isRefreshInProgress = false;
            }
        }


    }

}
