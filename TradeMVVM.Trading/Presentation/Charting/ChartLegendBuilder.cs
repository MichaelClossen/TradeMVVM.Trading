using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Media;
using TradeMVVM.Trading.Models;
using TradeMVVM.Domain;

namespace TradeMVVM.Trading.Chart
{
    public class ChartLegendBuilder
    {
        public List<LegendItem> Build(
     Dictionary<string, List<Tuple<DateTime, double>>> data,
     Dictionary<string, Brush> lineBrushes,
     List<(string isin_wkn, TradeMVVM.Domain.StockType type)> stocks)
        {
            var stockItems = new List<LegendItem>();
            var knockoutItems = new List<LegendItem>();

            foreach (var s in stocks)
            {
                if (!data.ContainsKey(s.isin_wkn))
                    continue;

                var values = data[s.isin_wkn];
                if (values.Count == 0)
                    continue;

                // values now hold percent values (stored by PricePollingService / DB)
                var percents = values.Select(v => v.Item2).ToArray();
                double lastPercent = 0.0;
                var last = percents.Last();
                if (!double.IsNaN(last) && !double.IsInfinity(last))
                    lastPercent = last;

                Brush lineBrush = lineBrushes.ContainsKey(s.isin_wkn)
                    ? lineBrushes[s.isin_wkn]
                    : Brushes.White;

                var item = new LegendItem
                {
                    Name = s.isin_wkn,
                    Color = lineBrush,
                    PercentText = lastPercent.ToString("0.00", System.Globalization.CultureInfo.CurrentCulture) + " %",
                    PercentBrush = lastPercent >= 0
                        ? Brushes.LimeGreen
                        : Brushes.Red,
                    IsHeader = false
                };

                if (s.type == StockType.Knockout)
                    knockoutItems.Add(item);
                else
                    stockItems.Add(item);
            }

            var result = new List<LegendItem>();

            if (stockItems.Any())
            {
                result.Add(new LegendItem
                {
                    Name = "Aktien / ETFs",
                    IsHeader = true
                });

                result.AddRange(stockItems.OrderByDescending(x => x.PercentText));
            }

            if (knockoutItems.Any())
            {
                result.Add(new LegendItem
                {
                    Name = "Knockouts",
                    IsHeader = true
                });

                result.AddRange(knockoutItems.OrderByDescending(x => x.PercentText));
            }

            return result;
        }

    }
}
