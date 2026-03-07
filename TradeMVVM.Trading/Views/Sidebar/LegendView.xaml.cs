using System;
using System.Collections.Generic;
using System.Windows.Controls;
using System.Windows.Media;
using TradeMVVM.Trading.Chart;
using TradeMVVM.Trading.Models;
using TradeMVVM.Trading.Services;

namespace TradeMVVM.Trading.Views.Sidebar
{
    public partial class LegendView : UserControl
    {
        public System.Collections.ObjectModel.ObservableCollection<LegendItem> LegendItems { get; set; } = new System.Collections.ObjectModel.ObservableCollection<LegendItem>();

        public LegendView()
        {
            InitializeComponent();
            DataContext = this;
        }

        private void OnRefreshClicked(object sender, System.Windows.RoutedEventArgs e)
        {
            // call RefreshCommand on the HoldingsReport VM if available
            var mainVm = DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel;
            if (mainVm?.HoldingsReport != null)
            {
                var cmd = mainVm.HoldingsReport.RefreshCommand;
                if (cmd != null && cmd.CanExecute(null))
                    cmd.Execute(null);
            }
        }

        private void OnExportClicked(object sender, System.Windows.RoutedEventArgs e)
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var file = System.IO.Path.Combine(baseDir, "DataAnalysis", $"legend_export_{DateTime.Now:yyyyMMdd_HHmmss}.csv");
            System.IO.Directory.CreateDirectory(System.IO.Path.GetDirectoryName(file));

            using (var sw = new System.IO.StreamWriter(file))
            {
                sw.WriteLine("Name;Percent");
                foreach (var it in LegendItems)
                {
                    sw.WriteLine($"{it.Name};{it.PercentText}");
                }
            }

            System.Windows.MessageBox.Show($"Exported legend to {file}", "Export CSV", System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Information);
        }

        public void RenderLegend(
            Dictionary<string, List<Tuple<System.DateTime, double>>> history,
            List<(string isin_wkn, TradeMVVM.Domain.StockType type)> stocks,
            Dictionary<string, Brush> brushes)
        {
            var builder = new ChartLegendBuilder();

            var items = builder.Build(history, brushes, stocks);
            LegendItems.Clear();
            foreach (var it in items)
                LegendItems.Add(it);
        }
    }
}
