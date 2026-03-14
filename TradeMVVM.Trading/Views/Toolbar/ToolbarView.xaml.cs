using System;
using System.Windows.Controls;
using System.Windows;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using TradeMVVM.Trading.Presentation.ViewModels;

namespace TradeMVVM.Trading.Views.Toolbar
{
    public partial class ToolbarView : UserControl
    {
        public ToolbarView()
        {
            InitializeComponent();
        }

        private void ZoomRangeSlider_ValueChanged(object sender, RoutedPropertyChangedEventArgs<double> e)
        {
            try
            {
                // allow reacting even during initial load so the initial slider value (2h) applies
                if (!IsLoaded && e.NewValue == e.OldValue)
                    return;
                var idx = (int)System.Math.Round(e.NewValue);
                var text = GetZoomLabel(idx);
                if (TxtZoomRange != null)
                    TxtZoomRange.Text = text;

                var vm = DataContext as MainViewModel;
                if (vm == null)
                    vm = Application.Current?.MainWindow?.DataContext as MainViewModel;
                var zoom = vm?.Zoom;
                if (zoom == null)
                    return;

                try { System.Diagnostics.Debug.WriteLine($"Toolbar: executing zoom index {idx}"); } catch { }
                ExecuteCommand(GetZoomCommand(zoom, idx));

                // request an immediate refresh of the charts so the new zoom is applied now
                try
                {
                    Application.Current?.Dispatcher?.BeginInvoke(new Action(() =>
                    {
                        try
                        {
                            var mw = Application.Current?.MainWindow as TradeMVVM.Trading.Views.MainWindow;
                            mw?.TriggerRefresh();
                        }
                        catch { }
                    }));
                }
                catch { }
            }
            catch { }
        }

        private static string GetZoomLabel(int idx)
        {
            switch (idx)
            {
                case 0: return "1m";
                case 1: return "5m";
                case 2: return "15m";
                case 3: return "30m";
                case 4: return "1h";
                case 5: return "2h";
                case 6: return "4h";
                case 7: return "6h";
                case 8: return "8h";
                case 9: return "1D";
                case 10: return "1W";
                case 11: return "1M";
                case 12: return "1Y";
                default: return "Auto";
            }
        }

        private static ICommand GetZoomCommand(ZoomCommands zoom, int idx)
        {
            switch (idx)
            {
                case 0: return zoom.Zoom1MinCommand;
                case 1: return zoom.Zoom5MinCommand;
                case 2: return zoom.Zoom15MinCommand;
                case 3: return zoom.Zoom30MinCommand;
                case 4: return zoom.Zoom1HourCommand;
                case 5: return zoom.Zoom2HourCommand;
                case 6: return zoom.Zoom4HourCommand;
                case 7: return zoom.Zoom6HourCommand;
                case 8: return zoom.Zoom8HourCommand;
                case 9: return zoom.ZoomDayCommand;
                case 10: return zoom.ZoomWeekCommand;
                case 11: return zoom.ZoomMonthCommand;
                case 12: return zoom.ZoomYearCommand;
                default: return zoom.AutoCommand;
            }
        }

        private static void ExecuteCommand(ICommand command)
        {
            try
            {
                if (command != null && command.CanExecute(null))
                    command.Execute(null);
            }
            catch { }
        }

        private void ResetLayoutDefaultsButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (Application.Current?.MainWindow is TradeMVVM.Trading.Views.MainWindow mainWindow)
                    mainWindow.ResetLayoutDefaults();
            }
            catch { }
        }

        private void KillChromeButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                TradeMVVM.Trading.Services.Providers.GettexProvider.CleanupRegisteredDrivers(force: true);
                try
                {
                    foreach (var p in System.Diagnostics.Process.GetProcessesByName("chrome"))
                    {
                        try { p.Kill(); } catch { }
                    }
                }
                catch { }
            }
            catch { }
        }
    }
}

