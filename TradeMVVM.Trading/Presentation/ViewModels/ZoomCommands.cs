using System;
using System.Windows.Controls;
using System.Windows.Input;
using TradeMVVM.Trading.Chart;

namespace TradeMVVM.Trading.Presentation.ViewModels
{
    public class ZoomCommands
    {
        public ICommand Zoom1MinCommand { get; }
        public ICommand Zoom5MinCommand { get; }
        public ICommand Zoom15MinCommand { get; }
        public ICommand Zoom30MinCommand { get; }
        public ICommand Zoom1HourCommand { get; }
        public ICommand Zoom2HourCommand { get; }
        public ICommand Zoom4HourCommand { get; }
        public ICommand Zoom6HourCommand { get; }
        public ICommand Zoom8HourCommand { get; }
        public ICommand ZoomDayCommand { get; }
        public ICommand ZoomWeekCommand { get; }
        public ICommand ZoomMonthCommand { get; }
        public ICommand ZoomYearCommand { get; }
        public ICommand AutoCommand { get; }

        public ZoomCommands(DualZoomController zoom)
        {
            Zoom1MinCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromMinutes(1)));
            Zoom5MinCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromMinutes(5)));
            Zoom15MinCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromMinutes(15)));
            Zoom30MinCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromMinutes(30)));
            Zoom1HourCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromHours(1)));
            Zoom2HourCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromHours(2)));
            Zoom4HourCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromHours(4)));
            Zoom6HourCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromHours(6)));
            Zoom8HourCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromHours(8)));
            ZoomDayCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromDays(1)));
            ZoomWeekCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromDays(7)));
            ZoomMonthCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromDays(30)));
            ZoomYearCommand = new RelayCommand(() => zoom.SetZoom(TimeSpan.FromDays(365)));
            AutoCommand = new RelayCommand(() => zoom.Auto());
        }
    }

}
