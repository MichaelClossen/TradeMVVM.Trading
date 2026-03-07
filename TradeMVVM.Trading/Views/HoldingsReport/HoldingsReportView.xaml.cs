using System.Windows.Controls;
using System.Linq;
using System.IO;
using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Media;
using System.Windows.Input;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Win32;
using TradeMVVM.Trading.ViewModels;

namespace TradeMVVM.Trading.Views.HoldingsReport
{
    public partial class HoldingsReportView : UserControl
    {
        private readonly string _settingsPath;
        private readonly TradeMVVM.Trading.Services.SettingsService _settingsService;
        private const string ZoomSettingsKey = "__ZoomPercent__";
        private const double BaseFontSize = 9.0;
        private const double MinZoomPercent = 70.0;
        private const double MaxZoomPercent = 170.0;
        private double _currentZoomPercent = 100.0;
        // removed external scrollbar integration, keep only settings path

        public HoldingsReportView()
        {
            InitializeComponent();
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var dataDir = System.IO.Path.Combine(baseDir, "DataAnalysis");
            if (!Directory.Exists(dataDir))
                Directory.CreateDirectory(dataDir);
            _settingsPath = System.IO.Path.Combine(dataDir, "column_widths.csv");
            try
            {
                _settingsService = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
            }
            catch { }

            this.Loaded += HoldingsReportView_Loaded;
            this.Unloaded += HoldingsReportView_Unloaded;
            this.Loaded += RegisterHandlersOnLoaded;
        }

        private void HoldingsDataGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            try
            {
                var dg = this.HoldingsDataGrid;
                if (dg == null) return;

                var item = dg.SelectedItem as TradeMVVM.Trading.ViewModels.HoldingsRow;
                if (item == null) return;

                try
                {
                    var provider = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.ChartDataProvider)) as TradeMVVM.Trading.Services.ChartDataProvider;
                    string prov = null;
                    if (provider != null)
                        prov = provider.GetPrimaryProviderForName(item.Name);

                    string url;
                    if (!string.IsNullOrWhiteSpace(prov) && prov.Equals("BNP", StringComparison.OrdinalIgnoreCase))
                        url = $"https://derivate.bnpparibas.com/product-details/{item.ISIN}/";
                    else
                        url = $"https://www.gettex.de/aktie/{item.ISIN}/";

                    Process.Start(new ProcessStartInfo { FileName = url, UseShellExecute = true });
                }
                catch { }
            }
            catch { }
        }

        private void SetAllAlertThreshold_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var vm = DataContext as HoldingsReportViewModel;
                if (vm == null) return;

                var txt = (TxtAllAlertPercent?.Text ?? string.Empty).Trim().Replace(',', '.');
                if (!double.TryParse(txt, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var value) || value < 0)
                {
                    MessageBox.Show("Ungültiger Alert%-Wert.", "Alert %", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }

                vm.SetAllAlertThresholdPercent(value);
            }
            catch { }
        }

        private void SetAllTrailingThreshold_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var vm = DataContext as HoldingsReportViewModel;
                if (vm == null) return;

                var txt = (TxtAllTrailingPercent?.Text ?? string.Empty).Trim().Replace(',', '.');
                if (!double.TryParse(txt, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var value) || value < 0)
                {
                    MessageBox.Show("Ungültiger Trail%-Wert.", "Trail %", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }

                vm.SetAllTrailingStopPercent(value);
            }
            catch { }
        }

        private void RegisterHandlersOnLoaded(object sender, RoutedEventArgs e)
        {
            try
            {
                if (HoldingsDataGrid != null)
                {
                    HoldingsDataGrid.MouseDoubleClick += HoldingsDataGrid_MouseDoubleClick;
                    // single handler on the DataGrid is sufficient
                    HoldingsDataGrid.PreviewMouseWheel += HoldingsDataGrid_PreviewMouseWheel;

            // If a default holdings CSV is configured, load it automatically on startup.
            try
            {
                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings != null && !string.IsNullOrWhiteSpace(settings.HoldingsCsvPath) && File.Exists(settings.HoldingsCsvPath))
                {
                    var vmLocal2 = this.DataContext as HoldingsReportViewModel;
                    if (vmLocal2 != null)
                    {
                        vmLocal2.LoadCsvFromPath(settings.HoldingsCsvPath);
                    }
                    else
                    {
                        try
                        {
                            var dc = this.DataContext;
                            if (dc != null)
                            {
                                var prop = dc.GetType().GetProperty("HoldingsReport");
                                if (prop != null)
                                {
                                    var hr = prop.GetValue(dc) as HoldingsReportViewModel;
                                    hr?.LoadCsvFromPath(settings.HoldingsCsvPath);
                                }
                            }
                        }
                        catch { }
                    }
                }
            }
            catch { }
                }

                    }
                    catch { }
                }

        private void HoldingsReportView_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                if (File.Exists(_settingsPath))
                {
                    var lines = File.ReadAllLines(_settingsPath);
                    if (lines != null && lines.Length > 0)
                    {
                        var dict = new Dictionary<string, double>();
                        double? savedZoom = null;
                        foreach (var l in lines)
                        {
                            var parts = l.Split(';');
                            if (parts.Length == 2 && TryParseSettingsDouble(parts[1], out double w))
                            {
                                if (string.Equals(parts[0], ZoomSettingsKey, StringComparison.Ordinal))
                                    savedZoom = w;
                                else
                                    dict[parts[0]] = w;
                            }
                        }

                        if (savedZoom.HasValue)
                        {
                            var clamped = Math.Max(MinZoomPercent, Math.Min(MaxZoomPercent, savedZoom.Value));
                            if (ZoomSlider != null)
                                ZoomSlider.Value = clamped;
                            else
                                ApplyGridZoom(clamped);
                        }
                        else
                        {
                            var settingsZoom = _settingsService?.HoldingsZoomPercent ?? 100.0;
                            var clamped = Math.Max(MinZoomPercent, Math.Min(MaxZoomPercent, settingsZoom));
                            if (ZoomSlider != null)
                                ZoomSlider.Value = clamped;
                            else
                                ApplyGridZoom(clamped);
                        }

                        ApplyWidthsByHeader(dict);
                    }
                }
                else
                {
                    try
                    {
                        var settingsZoom = _settingsService?.HoldingsZoomPercent ?? 100.0;
                        var clamped = Math.Max(MinZoomPercent, Math.Min(MaxZoomPercent, settingsZoom));
                        if (ZoomSlider != null)
                            ZoomSlider.Value = clamped;
                        else
                            ApplyGridZoom(clamped);
                    }
                    catch { }

                    // auto-size once then persist
                    var dg = HoldingsDataGrid;
                    if (dg != null)
                    {
                        dg.UpdateLayout();
                        dg.Dispatcher.BeginInvoke(new Action(() =>
                        {
                            foreach (var col in dg.Columns)
                                col.Width = DataGridLength.Auto;
                            // no external scrollbar wiring; keep default DataGrid scrolling
                        }));
                    }
                }
            }
            catch { }
        }


        private void ApplyWidthsByHeader(Dictionary<string, double> widths)
        {
            try
            {
                var dg = this.HoldingsDataGrid;
                if (dg == null) return;

                var zoomFactor = _currentZoomPercent / 100.0;

                foreach (var col in dg.Columns)
                {
                    if (widths.TryGetValue(col.Header?.ToString() ?? "", out double w))
                    {
                        var scaledWidth = Math.Max(col.MinWidth, w * zoomFactor);
                        col.Width = Math.Min(GetColumnMaxWidth(col), scaledWidth);
                    }
                }

                AutoFitColumnsToZoom();
            }
            catch { }
        }

        private void HoldingsReportView_Unloaded(object sender, RoutedEventArgs e)
        {
            try
            {
                var dg = this.HoldingsDataGrid;
                if (dg == null) return;

                var lines = new List<string>();
                lines.Add($"{ZoomSettingsKey};{_currentZoomPercent.ToString(CultureInfo.InvariantCulture)}");
                foreach (var col in dg.Columns)
                {
                    var h = col.Header?.ToString() ?? "";
                    var zoomFactor = _currentZoomPercent / 100.0;
                    var w = zoomFactor > 0 ? (col.ActualWidth / zoomFactor) : col.ActualWidth;
                    lines.Add($"{h};{w.ToString(CultureInfo.InvariantCulture)}");
                }
                File.WriteAllLines(_settingsPath, lines);
            }
            catch { }

            try
            {
                if (HoldingsDataGrid != null)
                    HoldingsDataGrid.PreviewMouseWheel -= HoldingsDataGrid_PreviewMouseWheel;
            }
            catch { }
        }

        private static bool TryParseSettingsDouble(string value, out double result)
        {
            return double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result)
                || double.TryParse(value, NumberStyles.Any, CultureInfo.CurrentCulture, out result);
        }

        private void ZoomSlider_ValueChanged(object sender, RoutedPropertyChangedEventArgs<double> e)
        {
            try
            {
                ApplyGridZoom(e.NewValue);
            }
            catch { }
        }

        private void ResetZoomButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (ZoomSlider != null)
                    ZoomSlider.Value = 100.0;
                else
                    ApplyGridZoom(100.0);
            }
            catch { }
        }

        private void ApplyGridZoom(double zoomPercent)
        {
            if (HoldingsDataGrid == null)
                return;

            var clampedZoom = Math.Max(MinZoomPercent, Math.Min(MaxZoomPercent, zoomPercent));
            UpdateZoomDisplay(clampedZoom);
            var scaledFont = BaseFontSize * (clampedZoom / 100.0);
            HoldingsDataGrid.FontSize = Math.Max(7.0, Math.Min(22.0, scaledFont));

            HoldingsDataGrid.MinRowHeight = Math.Max(18.0, HoldingsDataGrid.FontSize * 2.0);
            HoldingsDataGrid.ColumnHeaderHeight = Math.Max(20.0, HoldingsDataGrid.FontSize * 2.2);

            var previousZoom = _currentZoomPercent;
            _currentZoomPercent = clampedZoom;

            if (Math.Abs(previousZoom - clampedZoom) >= 0.001)
                PersistZoomToSettings(clampedZoom);

            if (previousZoom <= 0.0 || Math.Abs(previousZoom - clampedZoom) < 0.001)
                return;

            AutoFitColumnsToZoom();
        }

        private void UpdateZoomDisplay(double zoomPercent)
        {
            try
            {
                if (ZoomValueButton != null)
                    ZoomValueButton.Content = $"{Math.Round(zoomPercent, 0)}%";
            }
            catch { }
        }

        private void PersistZoomToSettings(double zoomPercent)
        {
            try
            {
                if (_settingsService == null)
                    return;

                _settingsService.HoldingsZoomPercent = zoomPercent;
                _settingsService.Save();
            }
            catch { }
        }

        private void AutoFitColumnsToZoom()
        {
            if (HoldingsDataGrid == null)
                return;

            foreach (var col in HoldingsDataGrid.Columns)
            {
                if (col == null)
                    continue;

                col.Width = DataGridLength.SizeToCells;
            }

            HoldingsDataGrid.UpdateLayout();

            foreach (var col in HoldingsDataGrid.Columns)
            {
                if (col == null)
                    continue;

                col.MinWidth = GetColumnMinWidth(col);

                var min = col.MinWidth;
                var max = GetColumnMaxWidth(col);
                var actual = double.IsNaN(col.ActualWidth) || col.ActualWidth <= 0 ? min : col.ActualWidth;
                col.Width = Math.Max(min, Math.Min(max, actual));
            }
        }

        private double GetColumnMaxWidth(DataGridColumn column)
        {
            var header = (column?.Header?.ToString() ?? string.Empty).Trim();
            var zoomFactor = _currentZoomPercent / 100.0;

            if (header.Equals("Name", StringComparison.OrdinalIgnoreCase))
                return Math.Max(GetColumnMinWidth(column), 320.0 * zoomFactor);
            if (header.IndexOf("Handel", StringComparison.OrdinalIgnoreCase) >= 0 ||
                header.IndexOf("Zeit", StringComparison.OrdinalIgnoreCase) >= 0)
                return Math.Max(GetColumnMinWidth(column), 150.0 * zoomFactor);
            if (header.IndexOf("Prognose", StringComparison.OrdinalIgnoreCase) >= 0)
                return Math.Max(GetColumnMinWidth(column), 85.0 * zoomFactor);

            if (header.Equals("ISIN", StringComparison.OrdinalIgnoreCase))
                return Math.Max(GetColumnMinWidth(column), 120.0 * zoomFactor);

            return Math.Max(GetColumnMinWidth(column), 120.0 * zoomFactor);
        }

        private double GetColumnMinWidth(DataGridColumn column)
        {
            var header = (column?.Header?.ToString() ?? string.Empty).Trim();
            var zoomFactor = _currentZoomPercent / 100.0;

            if (header.Equals("Name", StringComparison.OrdinalIgnoreCase))
                return Math.Max(70.0, 90.0 * zoomFactor);
            if (header.IndexOf("Handel", StringComparison.OrdinalIgnoreCase) >= 0 ||
                header.IndexOf("Zeit", StringComparison.OrdinalIgnoreCase) >= 0)
                return Math.Max(80.0, 100.0 * zoomFactor);
            if (header.IndexOf("Prognose", StringComparison.OrdinalIgnoreCase) >= 0)
                return Math.Max(60.0, 70.0 * zoomFactor);
            if (header.Equals("ISIN", StringComparison.OrdinalIgnoreCase))
                return Math.Max(70.0, 90.0 * zoomFactor);

            return Math.Max(60.0, 78.0 * zoomFactor);
        }

        private void HoldingsDataGrid_PreviewMouseWheel(object sender, System.Windows.Input.MouseWheelEventArgs e)
        {
            try
            {
                if ((Keyboard.Modifiers & ModifierKeys.Control) == ModifierKeys.Control)
                {
                    if (ZoomSlider != null)
                    {
                        var zoomStep = e.Delta > 0 ? 5.0 : -5.0;
                        ZoomSlider.Value = Math.Max(MinZoomPercent, Math.Min(MaxZoomPercent, ZoomSlider.Value + zoomStep));
                    }
                    e.Handled = true;
                }
            }
            catch { }
        }

        private T FindVisualParent<T>(DependencyObject child) where T : DependencyObject
        {
            if (child == null) return null;
            DependencyObject current = child;
            while (current != null)
            {
                if (current is T t) return t;
                current = VisualTreeHelper.GetParent(current);
            }
            return null;
        }

        private T FindVisualChild<T>(DependencyObject parent) where T : DependencyObject
        {
            if (parent == null) return null;
            for (int i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
            {
                var child = VisualTreeHelper.GetChild(parent, i);
                if (child is T t) return t;
                var result = FindVisualChild<T>(child);
                if (result != null) return result;
            }
            return null;
        }

        private void HideInternalVerticalScrollBars(DependencyObject root)
        {
            if (root == null) return;
            try
            {
                for (int i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
                {
                    var child = VisualTreeHelper.GetChild(root, i);
                    if (child is System.Windows.Controls.Primitives.ScrollBar sb && sb.Orientation == Orientation.Vertical)
                    {
                        // hide internal vertical scrollbar
                        sb.Visibility = Visibility.Collapsed;
                        sb.IsEnabled = false;
                    }
                    HideInternalVerticalScrollBars(child);
                }
            }
            catch { }
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {

        }

        private void DeleteHolding_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var btn = sender as System.Windows.Controls.Button;
                if (btn == null) return;

                // get the row DataContext
                var row = btn.DataContext as TradeMVVM.Trading.ViewModels.HoldingsRow;
                if (row == null)
                {
                    // try to find parent DataGridRow
                    var parent = FindVisualParent<System.Windows.Controls.DataGridRow>(btn);
                    if (parent != null)
                        row = parent.Item as TradeMVVM.Trading.ViewModels.HoldingsRow;
                }

                if (row == null) return;

                // ask user to confirm
                var res = MessageBox.Show($"Holding {row.ISIN} ({row.Name}) löschen?", "Löschen bestätigen", MessageBoxButton.YesNo, MessageBoxImage.Question);
                if (res != MessageBoxResult.Yes) return;

                // call viewmodel to remove from source and refresh
                try
                {
                    var dc = this.DataContext as TradeMVVM.Trading.ViewModels.HoldingsReportViewModel;
                    if (dc != null)
                    {
                        // remove matching holding from internal source and refresh view
                        var source = dc.GetSourceHoldings();
                        var toRemove = source.FirstOrDefault(h => string.Equals(h.ISIN, row.ISIN, StringComparison.OrdinalIgnoreCase));
                        if (toRemove != null)
                        {
                            // persistently remove from CSV and refresh
                            dc.RemoveHoldingsByIsins(new[] { toRemove.ISIN });
                        }
                        else
                        {
                            // remove by ISIN if no exact object match
                            var list = source.Where(h => string.Equals(h.ISIN, row.ISIN, StringComparison.OrdinalIgnoreCase)).ToList();
                            foreach (var r in list) source.Remove(r);
                            try { dc.RefreshCommand.Execute(null); } catch { }
                        }
                    }
                    else
                    {
                        // DataContext may be parent main view model exposing HoldingsReport property
                        var dcRoot = this.DataContext;
                        if (dcRoot != null)
                        {
                            var prop = dcRoot.GetType().GetProperty("HoldingsReport");
                            if (prop != null)
                            {
                                var hr = prop.GetValue(dcRoot) as TradeMVVM.Trading.ViewModels.HoldingsReportViewModel;
                                if (hr != null)
                                {
                                    hr.RemoveHoldingsByIsins(new[] { row.ISIN });
                                }
                            }
                        }
                    }
                }
                catch { }
            }
            catch { }
        }

        private void UndoLastDelete_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var vm = this.DataContext as TradeMVVM.Trading.ViewModels.HoldingsReportViewModel;
                if (vm != null)
                {
                    vm.UndoLastDelete();
                }
                else
                {
                    var dcRoot = this.DataContext;
                    if (dcRoot != null)
                    {
                        var prop = dcRoot.GetType().GetProperty("HoldingsReport");
                        if (prop != null)
                        {
                            var hr = prop.GetValue(dcRoot) as TradeMVVM.Trading.ViewModels.HoldingsReportViewModel;
                            hr?.UndoLastDelete();
                        }
                    }
                }
            }
            catch { }
        }

        private void OpenCsvButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dlg = new OpenFileDialog
                {
                    Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                    Title = "Open transactions CSV",
                    CheckFileExists = true
                };

                // if a default holdings CSV is configured, start dialog in its folder
                try
                {
                    var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                    if (settings != null && !string.IsNullOrWhiteSpace(settings.HoldingsCsvPath))
                    {
                        var dir = Path.GetDirectoryName(settings.HoldingsCsvPath);
                        if (!string.IsNullOrWhiteSpace(dir) && Directory.Exists(dir))
                            dlg.InitialDirectory = dir;
                    }
                }
                catch { }

                if (dlg.ShowDialog() == true)
                {
                // call VM to load CSV. DataContext may be either HoldingsReportViewModel or a parent MainViewModel
                    if (this.DataContext is HoldingsReportViewModel vm)
                    {
                        vm.LoadCsvFromPath(dlg.FileName);
                    }
                    else
                    {
                        // try to locate HoldingsReportViewModel on a parent view model (e.g. MainViewModel)
                        try
                        {
                            var dc = this.DataContext;
                            if (dc != null)
                            {
                                // attempt dynamic access to HoldingsReport property if present
                                var prop = dc.GetType().GetProperty("HoldingsReport");
                                if (prop != null)
                                {
                                    var hr = prop.GetValue(dc) as HoldingsReportViewModel;
                                    hr?.LoadCsvFromPath(dlg.FileName);
                                }
                            }
                        }
                        catch { }
                    }

                    // save chosen file as new default in settings
                    try
                    {
                        var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                        if (settings != null)
                        {
                            settings.HoldingsCsvPath = dlg.FileName;
                            settings.Save();
                        }
                    }
                    catch { }
                }
            }
            catch { }
        }
    }
}
