using ScottPlot;
using System;
using System.Linq;
using TradeMVVM.Trading.Data;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Globalization;
using TradeMVVM.Trading.Chart;
using TradeMVVM.Trading.DataAnalysis;
using TradeMVVM.Trading.Services;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.ObjectModel;

namespace TradeMVVM.Trading.Views.Charts
{
    public partial class ChartsView : UserControl
    {
        // simple view-model for Alert-Center bindings
        private class AlertCenterVm : System.ComponentModel.INotifyPropertyChanged
        {
            public event System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
            private void Raise(string n) => PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(n));

            private string _processes = string.Empty; public string AlertStatusProcesses { get => _processes; set { _processes = value; Raise(nameof(AlertStatusProcesses)); } }
            private string _stocks = string.Empty; public string AlertStatusStocks { get => _stocks; set { _stocks = value; Raise(nameof(AlertStatusStocks)); } }
            private string _history = string.Empty; public string AlertStatusHistory { get => _history; set { _history = value; Raise(nameof(AlertStatusHistory)); } }
            private string _time = string.Empty; public string AlertStatusTime { get => _time; set { _time = value; Raise(nameof(AlertStatusTime)); } }
            private string _msg = string.Empty; public string AlertMessage { get => _msg; set { _msg = value; Raise(nameof(AlertMessage)); } }
        }

        private readonly AlertCenterVm _alertVm = new AlertCenterVm();
        private class AlertItem
        {
            public DateTime Timestamp { get; set; }
            public string Message { get; set; }
            public string Isin { get; set; }
            public string Display => $"[{Timestamp:dd.MM.yyyy HH:mm:ss.fff}] {Message}";
        }

        // Ensure persisted/manual Y-limits for the top PL plot do not exclude all loaded data.
        private void EnsureTopPlotManualLimitsIncludeData(List<Tuple<DateTime, double>> compressed)
        {
            try
            {
                if (compressed == null || compressed.Count == 0) return;
                var validYs = compressed.Select(t => t.Item2).Where(v => !double.IsNaN(v) && !double.IsInfinity(v)).ToArray();
                if (validYs.Length == 0) return;

                double dataMin = validYs.Min();
                double dataMax = validYs.Max();

                var manual = PlotStocksTop.Tag as TradeMVVM.Trading.Chart.AxisManualLimits;
                var sett = _settingsService ?? (App.Services?.GetService(typeof(SettingsService)) as SettingsService);
                if (manual == null || !manual.HasManualY) return;

                double mMin = manual.MinY;
                double mMax = manual.MaxY;
                bool updated = false;

                // if manual limits exclude data entirely, first try interpreting them as euros (divide by 1000)
                if (mMin > dataMax || mMax < dataMin)
                {
                    double altMin = mMin / 1000.0;
                    double altMax = mMax / 1000.0;
                    if (!(altMin > dataMax || altMax < dataMin))
                    {
                        // conversion fixed the mismatch; apply converted limits
                        mMin = altMin; mMax = altMax; updated = true;
                    }
                }

                // if still excluding data, clamp to data bounds with a small pad
                if (mMin > dataMax || mMax < dataMin)
                {
                    double pad = Math.Max(0.02 * Math.Abs(dataMax - dataMin), 0.2);
                    mMin = dataMin - pad;
                    mMax = dataMax + pad;
                    updated = true;
                }

                if (updated)
                {
                    try { PlotStocksTop.Plot.Axes.SetLimitsY(mMin, mMax); } catch { }
                    try { PlotStocksTop.Tag = new TradeMVVM.Trading.Chart.AxisManualLimits { HasManualY = true, MinY = mMin, MaxY = mMax }; } catch { }
                    try
                    {
                        if (sett != null)
                        {
                            sett.ChartsStocksTopYMin = mMin; sett.ChartsStocksTopYMax = mMax; sett.Save();
                        }
                    }
                    catch { }
                    // update UI fields to reflect the corrected limits
                    try { Dispatcher.BeginInvoke(new Action(() => RefreshYAxisTextBoxesForPlot(PlotStocksTop))); } catch { }
                }
            }
            catch { }
        }

        private void RefreshYAxisTextBoxesForPlot(ScottPlot.WPF.WpfPlot plot)
        {
            try
            {
                if (plot == null) return;
                double min = double.NaN, max = double.NaN;
                try { min = plot.Plot.Axes.Left.Min; } catch { }
                try { max = plot.Plot.Axes.Left.Max; } catch { }

                // If axes are not yet initialized, fall back to persisted settings values so
                // the Ymin/Ymax textboxes show something useful at startup.
                try
                {
                    if (double.IsNaN(min) || double.IsNaN(max))
                    {
                        var sett = _settingsService ?? (App.Services?.GetService(typeof(SettingsService)) as SettingsService);
                        if (sett != null)
                        {
                            if (plot == PlotStocks)
                            {
                                if (double.IsNaN(min) && !double.IsNaN(sett.ChartsStocksYMin) && sett.ChartsStocksYMin != 0.0)
                                    min = sett.ChartsStocksYMin;
                                if (double.IsNaN(max) && !double.IsNaN(sett.ChartsStocksYMax) && sett.ChartsStocksYMax != 0.0)
                                    max = sett.ChartsStocksYMax;
                            }
                            else if (plot == PlotStocksTop)
                            {
                                if (double.IsNaN(min) && !double.IsNaN(sett.ChartsStocksTopYMin) && sett.ChartsStocksTopYMin != 0.0)
                                    min = sett.ChartsStocksTopYMin;
                                if (double.IsNaN(max) && !double.IsNaN(sett.ChartsStocksTopYMax) && sett.ChartsStocksTopYMax != 0.0)
                                    max = sett.ChartsStocksTopYMax;
                            }
                            else if (plot == PlotKnockouts)
                            {
                                if (double.IsNaN(min) && !double.IsNaN(sett.ChartsKnockoutsYMin) && sett.ChartsKnockoutsYMin != 0.0)
                                    min = sett.ChartsKnockoutsYMin;
                                if (double.IsNaN(max) && !double.IsNaN(sett.ChartsKnockoutsYMax) && sett.ChartsKnockoutsYMax != 0.0)
                                    max = sett.ChartsKnockoutsYMax;
                            }
                        }
                    }
                }
                catch { }

                var culture = CultureInfo.GetCultureInfo("de-DE");
                string sMin = double.IsNaN(min) ? string.Empty : string.Format(culture, "{0:0.##}", min);
                string sMax = double.IsNaN(max) ? string.Empty : string.Format(culture, "{0:0.##}", max);

                if (plot == PlotStocksTop)
                {
                    try
                    {
                        var tbMin = this.FindName("TxtStocksTopYMin") as TextBox;
                        var tbMax = this.FindName("TxtStocksTopYMax") as TextBox;
                        if (tbMin != null && string.IsNullOrWhiteSpace(tbMin.Text)) tbMin.Text = sMin;
                        if (tbMax != null && string.IsNullOrWhiteSpace(tbMax.Text)) tbMax.Text = sMax;
                    }
                    catch { }
                }
                else if (plot == PlotStocks)
                {
                    try
                    {
                        var tbMin = this.FindName("TxtStocksYMin") as TextBox;
                        var tbMax = this.FindName("TxtStocksYMax") as TextBox;
                        if (tbMin != null && string.IsNullOrWhiteSpace(tbMin.Text)) tbMin.Text = sMin;
                        if (tbMax != null && string.IsNullOrWhiteSpace(tbMax.Text)) tbMax.Text = sMax;
                    }
                    catch { }
                }
                else if (plot == PlotKnockouts)
                {
                    try
                    {
                        var tbMin = this.FindName("TxtKnockoutsYMin") as TextBox;
                        var tbMax = this.FindName("TxtKnockoutsYMax") as TextBox;
                        if (tbMin != null && string.IsNullOrWhiteSpace(tbMin.Text)) tbMin.Text = sMin;
                        if (tbMax != null && string.IsNullOrWhiteSpace(tbMax.Text)) tbMax.Text = sMax;
                    }
                    catch { }
                }
            }
            catch { }
        }

        private void UpdateAllYAxisTextBoxes()
        {
            try
            {
                try { RefreshYAxisTextBoxesForPlot(PlotStocksTop); } catch { }
                try { RefreshYAxisTextBoxesForPlot(PlotStocks); } catch { }
                try { RefreshYAxisTextBoxesForPlot(PlotKnockouts); } catch { }
            }
            catch { }
        }

        private void ApplyYAxisFromInputs(ScottPlot.WPF.WpfPlot plot, string minText, string maxText)
        {
            try
            {
                if (plot == null) return;
                double minVal = double.NaN, maxVal = double.NaN;
                bool hasMin = false, hasMax = false;
                // try German format first (comma), then invariant (dot)
                try
                {
                    if (!string.IsNullOrWhiteSpace(minText))
                    {
                        hasMin = double.TryParse(minText, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.GetCultureInfo("de-DE"), out minVal);
                        if (!hasMin)
                            hasMin = double.TryParse(minText, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out minVal);
                    }
                    if (!string.IsNullOrWhiteSpace(maxText))
                    {
                        hasMax = double.TryParse(maxText, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.GetCultureInfo("de-DE"), out maxVal);
                        if (!hasMax)
                            hasMax = double.TryParse(maxText, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out maxVal);
                    }
                }
                catch { hasMin = false; hasMax = false; }

                if (hasMin && hasMax)
                {
                    try
                    {
                        // ensure min <= max; swap if user entered inverted values
                        if (minVal > maxVal)
                        {
                            var tmp = minVal; minVal = maxVal; maxVal = tmp;
                        }

                        // protect against equal values by adding a tiny pad
                        if (Math.Abs(maxVal - minVal) < 1e-12)
                        {
                            double p = Math.Max(1e-6, Math.Abs(maxVal) * 1e-6);
                            minVal -= p; maxVal += p;
                        }

                        plot.Plot.Axes.SetLimitsY(minVal, maxVal);
                        // remember manual limits in plot.Tag so ChartManager respects them on re-render
                        plot.Tag = new TradeMVVM.Trading.Chart.AxisManualLimits { HasManualY = true, MinY = minVal, MaxY = maxVal };
                        plot.Refresh();
                        try { var tb = this.FindName("TxtAlertStatusLine") as System.Windows.Controls.TextBlock; if (tb != null) tb.Text = $"Y-Axis gesetzt: {minVal:0.##} .. {maxVal:0.##}"; } catch { }
                        try { Dispatcher.BeginInvoke(new Action(() => RefreshYAxisTextBoxesForPlot(plot))); } catch { }

                        // persist limits to settings service
                        try
                        {
                            var sett = _settingsService ?? (App.Services?.GetService(typeof(SettingsService)) as SettingsService);
                            if (sett != null)
                            {
                                if (plot == PlotStocks)
                                {
                                    sett.ChartsStocksYMin = minVal; sett.ChartsStocksYMax = maxVal; sett.Save();
                                }
                                else if (plot == PlotStocksTop)
                                {
                                    sett.ChartsStocksTopYMin = minVal; sett.ChartsStocksTopYMax = maxVal; sett.Save();
                                    // reload top-plot history for the current visible X-range so data is displayed immediately
                                    try { LoadTopPlotHistoryForVisibleRange(); } catch { }
                                    // ensure user's limits do not exclude all available data: clamp/expand to include data if necessary
                                    try
                                    {
                                        List<Tuple<DateTime, double>> listCopy = null;
                                        lock (_stockTopData)
                                        {
                                            if (_stockTopData.TryGetValue("zero", out var l) && l != null && l.Count > 0)
                                                listCopy = new List<Tuple<DateTime, double>>(l);
                                        }

                                        if (listCopy != null && listCopy.Count > 0)
                                        {
                                            var validYs = listCopy.Select(t => t.Item2).Where(v => !double.IsNaN(v) && !double.IsInfinity(v)).ToArray();
                                            if (validYs.Length > 0)
                                            {
                                                double dataMin = validYs.Min();
                                                double dataMax = validYs.Max();
                                                // if user's limits would hide all data, expand them to include data plus small pad
                                                if (minVal > dataMax || maxVal < dataMin)
                                                {
                                                    // expand to data bounds with small pad (2% or 0.2 k€)
                                                    double span = Math.Abs(dataMax - dataMin);
                                                    double pad = Math.Max(0.02 * span, 0.2);
                                                    minVal = dataMin - pad;
                                                    maxVal = dataMax + pad;
                                                    try { plot.Plot.Axes.SetLimitsY(minVal, maxVal); plot.Tag = new TradeMVVM.Trading.Chart.AxisManualLimits { HasManualY = true, MinY = minVal, MaxY = maxVal }; } catch { }
                                                    // update persisted values
                                                    try { sett.ChartsStocksTopYMin = minVal; sett.ChartsStocksTopYMax = maxVal; sett.Save(); } catch { }
                                                }
                                            }
                                        }
                                    }
                                    catch { }

                                    try { _stockTopChartManager.Render(_stockTopData); } catch { }
                                    try { PlotStocksTop.Refresh(); } catch { }
                                    try { RefreshYAxisTextBoxesForPlot(plot); } catch { }
                                }
                                else if (plot == PlotKnockouts)
                                {
                                    sett.ChartsKnockoutsYMin = minVal; sett.ChartsKnockoutsYMax = maxVal; sett.Save();
                                }
                            }
                        }
                        catch { }
                    }
                    catch { }
                }
                else if (hasMin && !hasMax)
                {
                    try
                    {
                        var curMax = plot.Plot.Axes.Left.Max;
                        if (!double.IsNaN(curMax))
                        {
                            plot.Plot.Axes.SetLimitsY(minVal, curMax);
                            plot.Tag = new TradeMVVM.Trading.Chart.AxisManualLimits { HasManualY = true, MinY = minVal, MaxY = curMax };
                        }
                        plot.Refresh();
                        try { var tb = this.FindName("TxtAlertStatusLine") as System.Windows.Controls.TextBlock; if (tb != null) tb.Text = $"Y-Axis gesetzt: min={minVal:0.##}"; } catch { }
                        try { Dispatcher.BeginInvoke(new Action(() => RefreshYAxisTextBoxesForPlot(plot))); } catch { }
                    }
                    catch { }
                }
                else if (!hasMin && hasMax)
                {
                    try
                    {
                        var curMin = plot.Plot.Axes.Left.Min;
                        if (!double.IsNaN(curMin))
                        {
                            plot.Plot.Axes.SetLimitsY(curMin, maxVal);
                            plot.Tag = new TradeMVVM.Trading.Chart.AxisManualLimits { HasManualY = true, MinY = curMin, MaxY = maxVal };
                        }
                        plot.Refresh();
                        try { var tb = this.FindName("TxtAlertStatusLine") as System.Windows.Controls.TextBlock; if (tb != null) tb.Text = $"Y-Axis gesetzt: max={maxVal:0.##}"; } catch { }
                        try { Dispatcher.BeginInvoke(new Action(() => RefreshYAxisTextBoxesForPlot(plot))); } catch { }
                    }
                    catch { }
                }
                else
                {
                    try { var tb = this.FindName("TxtAlertStatusLine") as System.Windows.Controls.TextBlock; if (tb != null) tb.Text = "Y-Axis: ungültige Eingabe"; } catch { }
                }
            }
            catch { }
        }

        private void AutoScaleYAxis(ScottPlot.WPF.WpfPlot plot)
        {
            try
            {
                if (plot == null) return;
                // clear manual limits marker and let ChartManager autoscale on next render
                try {
                    plot.Tag = null; plot.Plot.Axes.SetLimitsY(double.NaN, double.NaN);
                    // clear persisted setting
                    var sett = _settingsService ?? (App.Services?.GetService(typeof(SettingsService)) as SettingsService);
                    if (sett != null)
                    {
                        if (plot == PlotStocks) { sett.ChartsStocksYMin = 0.0; sett.ChartsStocksYMax = 0.0; }
                        else if (plot == PlotStocksTop) { sett.ChartsStocksTopYMin = 0.0; sett.ChartsStocksTopYMax = 0.0; }
                        else if (plot == PlotKnockouts) { sett.ChartsKnockoutsYMin = 0.0; sett.ChartsKnockoutsYMax = 0.0; }
                        sett.Save();
                    }
                    // when clearing manual limits on top plot, also reload data to ensure it is visible
                    try
                    {
                        if (plot == PlotStocksTop)
                        {
                            LoadTopPlotHistoryForVisibleRange();
                            try { _stockTopChartManager.Render(_stockTopData); } catch { }
                            try { PlotStocksTop.Refresh(); } catch { }
                        }
                    }
                    catch { }
                    plot.Refresh();
                } catch { }
            }
            catch { }
        }

        // XAML Click handlers
        private void BtnStocksTopApply_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                try { Trace.WriteLine("BtnStocksTopApply_Click invoked"); } catch { }
                try { var tb = this.FindName("TxtAlertStatusLine") as System.Windows.Controls.TextBlock; if (tb != null) tb.Text = "Apply Top Y-Axis..."; } catch { }
                var tmin = (this.FindName("TxtStocksTopYMin") as TextBox)?.Text ?? string.Empty;
                var tmax = (this.FindName("TxtStocksTopYMax") as TextBox)?.Text ?? string.Empty;
                ApplyYAxisFromInputs(PlotStocksTop, tmin, tmax);
            }
            catch { }
        }

        private void BtnStocksTopAuto_Click(object sender, RoutedEventArgs e)
        {
            try { Trace.WriteLine("BtnStocksTopAuto_Click invoked"); AutoScaleYAxis(PlotStocksTop); } catch { }
        }

        private void BtnStocksApply_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                try { Trace.WriteLine("BtnStocksApply_Click invoked"); } catch { }
                var tmin = (this.FindName("TxtStocksYMin") as TextBox)?.Text ?? string.Empty;
                var tmax = (this.FindName("TxtStocksYMax") as TextBox)?.Text ?? string.Empty;
                ApplyYAxisFromInputs(PlotStocks, tmin, tmax);
            }
            catch { }
        }

        private void BtnStocksAuto_Click(object sender, RoutedEventArgs e)
        {
            try { Trace.WriteLine("BtnStocksAuto_Click invoked"); AutoScaleYAxis(PlotStocks); } catch { }
        }

        private void BtnKnockoutsApply_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                try { Trace.WriteLine("BtnKnockoutsApply_Click invoked"); } catch { }
                var tmin = (this.FindName("TxtKnockoutsYMin") as TextBox)?.Text ?? string.Empty;
                var tmax = (this.FindName("TxtKnockoutsYMax") as TextBox)?.Text ?? string.Empty;
                ApplyYAxisFromInputs(PlotKnockouts, tmin, tmax);
            }
            catch { }
        }

        private void BtnKnockoutsAuto_Click(object sender, RoutedEventArgs e)
        {
            try { Trace.WriteLine("BtnKnockoutsAuto_Click invoked"); AutoScaleYAxis(PlotKnockouts); } catch { }
        }

        // Try to locate the previous business day's TotalPL value from a full history list.
        // Returns true when a previous value was found.
        private bool TryGetPreviousBusinessDayValue(List<Tuple<DateTime, double>> hist, DateTime lastTime, out DateTime prevTime, out double prevValue)
        {
            prevTime = DateTime.MinValue;
            prevValue = 0.0;
            try
            {
                if (hist == null || hist.Count == 0)
                    return false;

                // Work in UTC date space to avoid timezone mismatches
                var lastUtc = lastTime.Kind == DateTimeKind.Utc ? lastTime : lastTime.ToUniversalTime();
                var target = lastUtc.Date.AddDays(-1);
                while (target.DayOfWeek == DayOfWeek.Saturday || target.DayOfWeek == DayOfWeek.Sunday)
                    target = target.AddDays(-1);

                // prefer the last measurement on the previous business day (UTC)
                var exact = hist
                    .Where(t => t.Item1.ToUniversalTime().Date == target)
                    .OrderByDescending(t => t.Item1)
                    .ToList();
                if (exact.Count > 0)
                {
                    var pick = exact[0];
                    prevTime = pick.Item1;
                    prevValue = pick.Item2;
                    return true;
                }

                // fallback: take the latest entry up to the end of that previous business day
                var endOfTargetUtc = target.AddDays(1);
                var beforeEnd = hist
                    .Where(t => t.Item1.ToUniversalTime() < endOfTargetUtc)
                    .OrderByDescending(t => t.Item1)
                    .FirstOrDefault();
                if (beforeEnd != null)
                {
                    prevTime = beforeEnd.Item1;
                    prevValue = beforeEnd.Item2;
                    return true;
                }

                // final fallback: nearest earlier entry before the provided lastTime
                var earlier = hist.Where(t => t.Item1 < lastTime).OrderByDescending(t => t.Item1).FirstOrDefault();
                if (earlier != null)
                {
                    prevTime = earlier.Item1;
                    prevValue = earlier.Item2;
                    return true;
                }
            }
            catch { }

            return false;
        }

        private void UpdateHoldingsCsvPathDisplay()
        {
            try
            {
                var settings = _settingsService ?? (App.Services?.GetService(typeof(SettingsService)) as SettingsService);
                string path = settings?.HoldingsCsvPath;
                if (string.IsNullOrWhiteSpace(path))
                {
                    try
                    {
                        var tbNull = this.FindName("TxtHoldingsCsvPath") as System.Windows.Controls.TextBlock;
                        if (tbNull != null) tbNull.Text = "(nicht konfiguriert)";
                    }
                    catch { }
                    return;
                }

                string display;
                try
                {
                    if (File.Exists(path))
                        display = System.IO.Path.GetFullPath(path);
                    else
                        display = $"Nicht gefunden: {path}";
                }
                catch
                {
                    display = path;
                }

                try
                {
                    var tb = this.FindName("TxtHoldingsCsvPath") as System.Windows.Controls.TextBlock;
                    if (tb != null) tb.Text = display;
                }
                catch { }
            }
            catch { }
        }

        // Load persisted TotalPLHistory data for the currently visible X range and populate _stockTopData
        private void LoadTopPlotHistoryForVisibleRange()
        {
            try
            {
                DateTime? visMin = null, visMax = null;
                try
                {
                    var minX = PlotStocks.Plot.Axes.Bottom.Min;
                    var maxX = PlotStocks.Plot.Axes.Bottom.Max;
                    if (!double.IsNaN(minX) && !double.IsNaN(maxX) && maxX > minX)
                    {
                        visMin = DateTime.FromOADate(minX);
                        visMax = DateTime.FromOADate(maxX);
                    }
                }
                catch { }

                // if axes are not yet initialized (NaN), fall back to a reasonable default range (last 2 hours)
                if (!visMin.HasValue || !visMax.HasValue)
                {
                    var now = DateTime.Now;
                    visMax = now;
                    visMin = now - TimeSpan.FromHours(2);
                }

                try
                {
                    var db = new DatabaseService();
                    var hist = db.LoadTotalPLHistoryBetween(visMin.Value, visMax.Value);
                    try { Trace.WriteLine($"LoadTopPlotHistoryForVisibleRange: loaded {hist?.Count ?? 0} points between {visMin.Value:O} and {visMax.Value:O}"); } catch { }
                    if (hist != null && hist.Count > 0)
                    {
                        var inRange = hist.Where(t => t.Item1 >= visMin.Value && t.Item1 <= visMax.Value).OrderBy(t => t.Item1).ToList();
                        var compressed = new List<Tuple<DateTime, double>>();

                        // For short visible spans prefer to keep all DB samples (avoid aggressive compression)
                        var visSpan = visMax.Value - visMin.Value;
                        bool keepAll = visSpan <= TimeSpan.FromHours(1);

                        double? lastVal = null;
                        foreach (var pt in inRange)
                        {
                            var displayVal = pt.Item2 / 1000.0; // convert € -> k€ for display
                            if (!keepAll && lastVal.HasValue && Math.Abs(lastVal.Value - displayVal) < 1e-9)
                                continue;
                            compressed.Add(new Tuple<DateTime, double>(pt.Item1, displayVal));
                            lastVal = displayVal;
                        }

                        if (compressed.Count > 0)
                        {
                            lock (_stockTopData)
                            {
                                _stockTopData["zero"] = compressed;
                            }
                            try { _stockTopChartManager.Render(_stockTopData); } catch { }
                            try { EnsureTopPlotManualLimitsIncludeData(compressed); } catch { }
                                try
                                {
                                    // enforce X axis to the requested visible range so the data is visible immediately
                                    var minOa = visMin.Value.ToOADate();
                                    var maxOa = visMax.Value.ToOADate();
                                    if (!double.IsNaN(minOa) && !double.IsNaN(maxOa) && maxOa > minOa)
                                    {
                                        try { PlotStocksTop.Plot.Axes.SetLimitsX(minOa, maxOa); } catch { }
                                        try { _plLastXMin = minOa; _plLastXMax = maxOa; } catch { }
                                    }

                                    // If the lower stocks plot has not initialized X axis yet, fall back to using the loaded data bounds
                                    try
                                    {
                                        var lowerMin = PlotStocks.Plot.Axes.Bottom.Min;
                                        var lowerMax = PlotStocks.Plot.Axes.Bottom.Max;
                                        if (double.IsNaN(lowerMin) || double.IsNaN(lowerMax) || lowerMax <= lowerMin)
                                        {
                                            // derive bounds from compressed data
                                            var xs = compressed.Select(t => t.Item1.ToOADate()).ToArray();
                                            if (xs.Length > 1)
                                            {
                                                var dmin = xs.Min();
                                                var dmax = xs.Max();
                                                try { PlotStocksTop.Plot.Axes.SetLimitsX(dmin, dmax); } catch { }
                                            }
                                        }
                                    }
                                    catch { }
                                }
                                catch { }
                                try { PlotStocksTop.Refresh(); } catch { }
                            try { Trace.WriteLine($"LoadTopPlotHistoryForVisibleRange: populated _stockTopData[zero] with {compressed.Count} samples"); } catch { }
                            try
                            {
                                // enforce X axis to the requested visible range so the data is visible immediately
                                var minOa = visMin.Value.ToOADate();
                                var maxOa = visMax.Value.ToOADate();
                                if (!double.IsNaN(minOa) && !double.IsNaN(maxOa) && maxOa > minOa)
                                {
                                    try { PlotStocksTop.Plot.Axes.SetLimitsX(minOa, maxOa); } catch { }
                                    try { _plLastXMin = minOa; _plLastXMax = maxOa; } catch { }
                                }
                            }
                            catch { }
                            try { PlotStocksTop.Refresh(); } catch { }
                        }
                    }
                }
                catch { }
            }
            catch { }
        }

        // restore alert threshold values from settings
        private void LoadAlertThresholds()
        {
            try
            {
                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings != null)
                {
                    if (settings.AlertPricePercentThreshold > 0)
                        _pricePercentAlertThreshold = settings.AlertPricePercentThreshold;
                    if (settings.AlertPlDeltaThresholdEur > 0)
                        _plDeltaAlertThresholdEur = settings.AlertPlDeltaThresholdEur;
                }
            }
            catch { }
        }

        private void BtnAlertJump_Click(object sender, RoutedEventArgs e)
        {
            try { JumpToSelectedAlert(); } catch { }
        }

        private void BtnAlertTest_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string isin = null;
                try
                {
                    if (_stockChartManager?.SeriesPoints != null && _stockChartManager.SeriesPoints.Count > 0)
                        isin = _stockChartManager.SeriesPoints.Keys.FirstOrDefault();
                    if (string.IsNullOrWhiteSpace(isin) && _knockoutChartManager?.SeriesPoints != null && _knockoutChartManager.SeriesPoints.Count > 0)
                        isin = _knockoutChartManager.SeriesPoints.Keys.FirstOrDefault();
                }
                catch { }

                AddAlert("manual:test", $"Test-Alert ausgelöst{(string.IsNullOrWhiteSpace(isin) ? string.Empty : $" ({isin})")}", isin);
            }
            catch { }
        }

        private void BtnAlertClear_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                _alertItems.Clear();
                TxtAlertToast.Text = string.Empty;
            }
            catch { }
        }

        private void LstAlerts_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            try { JumpToSelectedAlert(); } catch { }
        }

        private void JumpToSelectedAlert()
        {
            try
            {
                var selected = LstAlerts?.SelectedItem as AlertItem;
                if (selected == null || string.IsNullOrWhiteSpace(selected.Isin))
                    return;

                if (TryJumpToSeries(selected.Isin))
                    TxtAlertToast.Text = $"[{DateTime.Now:HH:mm:ss}] Zum Chart gesprungen: {selected.Isin}";
            }
            catch { }
        }

        private bool TryJumpToSeries(string isin)
        {
            try
            {
                if (TryJumpToSeries(_stockChartManager, PlotStocks, isin))
                    return true;

                if (TryJumpToSeries(_knockoutChartManager, PlotKnockouts, isin))
                    return true;
            }
            catch { }

            return false;
        }

        private bool TryJumpToSeries(ChartManager manager, ScottPlot.WPF.WpfPlot plot, string isin)
        {
            try
            {
                if (manager?.SeriesPoints == null || !manager.SeriesPoints.TryGetValue(isin, out var arr))
                    return false;
                if (arr.Xs == null || arr.Ys == null || arr.Xs.Length == 0 || arr.Ys.Length == 0)
                    return false;

                _dualZoomController.DisableFollow();

                int last = Math.Min(arr.Xs.Length, arr.Ys.Length) - 1;
                double xMax = arr.Xs[last];
                int firstIndex = Math.Max(0, last - 30);
                double xMin = arr.Xs[firstIndex];
                double xPad = Math.Max(1e-6, (xMax - xMin) * 0.05);

                double y = arr.Ys[last];
                double yPad = Math.Max(0.5, Math.Abs(y) * 0.15);

                plot.Plot.Axes.SetLimitsX(xMin, xMax + xPad);
                plot.Plot.Axes.SetLimitsY(y - yPad, y + yPad);
                plot.Refresh();
                return true;
            }
            catch { }

            return false;
        }

        // PL visual functions removed. Keep value-only display.

        // remove all plot manipulation and plottable helpers - PL graph removed

        // hover tooltip state
        private System.Windows.Controls.ToolTip _hoverTooltip;
        private System.Threading.CancellationTokenSource _hoverCts;
        private string _hoverCurrentKey;
        // cache recent hover provider results to avoid repeated network/selenium calls
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, (DateTime fetchedAt, double price, double percent, DateTime? providerTime)> _hoverCache
            = new System.Collections.Concurrent.ConcurrentDictionary<string, (DateTime, double, double, DateTime?)>(StringComparer.OrdinalIgnoreCase);
        // track in-flight hover fetch tasks to deduplicate concurrent requests for the same key
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, Task> _hoverFetches
            = new System.Collections.Concurrent.ConcurrentDictionary<string, Task>(StringComparer.OrdinalIgnoreCase);
        private readonly ChartManager _stockChartManager;
        private readonly ChartManager _stockTopChartManager;
        private readonly ChartManager _knockoutChartManager;
        private bool _syncingStocksXAxis = false;
        // data storage for the top plot (keeps recent zero-values)
        private readonly Dictionary<string, List<Tuple<DateTime, double>>> _stockTopData = new Dictionary<string, List<Tuple<DateTime, double>>>(StringComparer.OrdinalIgnoreCase);
        private System.Windows.Threading.DispatcherTimer _topPlotTimer;
        // removed PL plot and plottable; keep only value and timers for refresh
        // PL plot removed.
        private bool _plRefreshPending = false;
        private double _plLastYMin = double.NaN, _plLastYMax = double.NaN;
        private double _plLastXMin = double.NaN, _plLastXMax = double.NaN;
        private enum PlDisplayMode
        {
            UnrealizedOnly,
            UnrealizedPlusRealized
        }

        private PlDisplayMode _plDisplayMode = PlDisplayMode.UnrealizedOnly;
        // keep only latest value; historical arrays removed but DB entries are preserved elsewhere
        private double? _lastPlValue = null;
        private bool _plHistoryDeferred = false; // keep flag for deferred logic but no history rendering
        private bool _plManualZoomActive = false;
        private TimeSpan? _plActiveZoomSpan = null;
        private DateTime _incomeCacheFileWriteUtc = DateTime.MinValue;
        private double _incomeCacheEur = 0.0;

        private readonly ChartZoomController _stockZoomController;
        private readonly ChartZoomController _knockoutZoomController;
        // PL chart removed; no ChartManager for PL
        private readonly DualZoomController _dualZoomController;
        private int _mouseOpCounter = 0;
        private DateTime _lastMouseProcessed = DateTime.MinValue;
        private const int MouseProcessIntervalMs = 150; // reduce mouse processing frequency
        private const int HoverDebounceMs = 250; // wait before starting expensive fetch
        private const int HoverCacheSeconds = 30; // cache TTL for hover provider results
        private const int DbFreshSeconds = 90; // treat DB row as fresh for this many seconds
        private double _pricePercentAlertThreshold = 1.0;
        private double _plDeltaAlertThresholdEur = 10.0;
        private static readonly TimeSpan AlertCooldown = TimeSpan.FromMinutes(3);
        private const int MaxAlertItems = 100;
        private readonly ObservableCollection<AlertItem> _alertItems = new ObservableCollection<AlertItem>();
        private readonly Dictionary<string, DateTime> _lastAlertAt = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _activePercentBreaches = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _activeTrailingBreaches = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, double> _trailingPeaks = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        private bool _trailingPeaksDirty = false;
        private DateTime _lastTrailingPeaksSavedUtc = DateTime.MinValue;
        private static readonly TimeSpan TrailingPeaksSaveInterval = TimeSpan.FromSeconds(10);
        private double? _lastPlAlertBase;
        private System.Windows.Threading.DispatcherTimer _alertToastTimer;
        private readonly SettingsService _settingsService;
        private static readonly Brush PositivePlBrush = new SolidColorBrush(System.Windows.Media.Colors.LightGreen);
        private static readonly Brush NegativePlBrush = new SolidColorBrush(System.Windows.Media.Colors.OrangeRed);
        private DateTime _lastProcessStatusUpdateUtc = DateTime.MinValue;
        private DateTime _lastHistoryStatusUpdateUtc = DateTime.MinValue;
        private DateTime _lastPlRefreshUtc = DateTime.MinValue;
        private DateTime _lastAlertEvalUtc = DateTime.MinValue;
        private string _lastStatusProcessesText = string.Empty;
        private string _lastStatusStocksText = string.Empty;
        private string _lastStatusHistoryText = string.Empty;
        // flag to ensure top-plot DB history is loaded once after axes/layout are initialized
        private bool _topPlotHistoryLoaded = false;

        public DualZoomController ZoomController => _dualZoomController;

        public ChartsView()
        {
            InitializeComponent();
            this.DataContext = _alertVm;
            try { UpdatePlTitle(); } catch { }

            try
            {
                _settingsService = App.Services?.GetService(typeof(SettingsService)) as SettingsService;
            }
            catch { }

            try { UpdateHoldingsCsvPathDisplay(); } catch { }

            try { ApplySavedLayout(); } catch { }
            try
            {
                if (AlertsColumnSplitter != null)
                    AlertsColumnSplitter.DragCompleted += AlertsColumnSplitter_DragCompleted;
            }
            catch { }

            try
            {
                LstAlerts.ItemsSource = _alertItems;
                _alertToastTimer = new System.Windows.Threading.DispatcherTimer { Interval = TimeSpan.FromSeconds(8) };
                _alertToastTimer.Tick += (s, e) =>
                {
                    try
                    {
                        TxtAlertToast.Text = string.Empty;
                        _alertToastTimer.Stop();
                    }
                    catch { }
                };

                // sync top-right PL label with the header PL text initially and on changes
                try
                {
                    TxtTotalPLTop.Text = TxtTotalPL.Text; // This line is unchanged
                }
                catch { }

            // Immediately load TotalPL history for the currently visible timespan so the top plot
            // shows persisted DB values right at startup (even before new PL samples arrive).
            try
            {
                this.Loaded += (s, e) =>
                {
                    try
                    {
                        // defer to background priority so layout/axes are initialized first
                        Dispatcher.BeginInvoke(new Action(() =>
                        {
                            try { LoadTopPlotHistoryForVisibleRange(); } catch { }
                        }), System.Windows.Threading.DispatcherPriority.Background);
                    }
                    catch { }
                };
            }
            catch { }

                // PL plot removed; no throttled refresh required.
                try { LoadAlertThresholds(); } catch { }
                LoadTrailingPeaks();
                AddAlert("system:init", "Alert-Center aktiv.", null);
            }
            catch { }

            this.Unloaded += (s, e) =>
            {
                try { SaveTrailingPeaks(force: true); } catch { }
            };

            // Y-axis controls are handled via XAML Click handlers defined below
            // wire up button events declared in XAML
            try
            {
                var b1 = this.FindName("BtnStocksTopApply") as Button;
                var b2 = this.FindName("BtnStocksTopAuto") as Button;
                var b3 = this.FindName("BtnStocksApply") as Button;
                var b4 = this.FindName("BtnStocksAuto") as Button;
                var b5 = this.FindName("BtnKnockoutsApply") as Button;
                var b6 = this.FindName("BtnKnockoutsAuto") as Button;

                if (b1 != null) b1.Click += BtnStocksTopApply_Click;
                if (b2 != null) b2.Click += BtnStocksTopAuto_Click;
                if (b3 != null) b3.Click += BtnStocksApply_Click;
                if (b4 != null) b4.Click += BtnStocksAuto_Click;
                if (b5 != null) b5.Click += BtnKnockoutsApply_Click;
                if (b6 != null) b6.Click += BtnKnockoutsAuto_Click;
            }
            catch { }

            _stockChartManager = new ChartManager(PlotStocks, "Aktien / ETFs");
            try { _stockChartManager.ForceScatter = true; } catch { }
            _stockTopChartManager = new ChartManager(PlotStocksTop, "Aktien / ETFs (Top)");
            try { _stockTopChartManager.ForceScatter = true; } catch { }
            _knockoutChartManager = new ChartManager(PlotKnockouts, "Knockouts", padYAxisForShortZoom: true);
            try { _knockoutChartManager.ForceScatter = true; } catch { }
            try
            {
                // For the top stocks plot use k€ as left axis label (values are plotted in thousands)
                PlotStocksTop.Plot.Axes.Left.Label.Text = "k€";
            }
            catch { }

            // apply persisted manual axis limits from settings
            try
            {
                var sett = _settingsService ?? (App.Services?.GetService(typeof(SettingsService)) as SettingsService);
                if (sett != null)
                {
                    if (!double.IsNaN(sett.ChartsStocksYMin) && !double.IsNaN(sett.ChartsStocksYMax) && sett.ChartsStocksYMin != 0.0 && sett.ChartsStocksYMax != 0.0)
                    {
                        try { PlotStocks.Plot.Axes.SetLimitsY(sett.ChartsStocksYMin, sett.ChartsStocksYMax); PlotStocks.Tag = new TradeMVVM.Trading.Chart.AxisManualLimits { HasManualY = true, MinY = sett.ChartsStocksYMin, MaxY = sett.ChartsStocksYMax }; } catch { }
                    }
                    if (!double.IsNaN(sett.ChartsStocksTopYMin) && !double.IsNaN(sett.ChartsStocksTopYMax) && sett.ChartsStocksTopYMin != 0.0 && sett.ChartsStocksTopYMax != 0.0)
                    {
                        try { PlotStocksTop.Plot.Axes.SetLimitsY(sett.ChartsStocksTopYMin, sett.ChartsStocksTopYMax); PlotStocksTop.Tag = new TradeMVVM.Trading.Chart.AxisManualLimits { HasManualY = true, MinY = sett.ChartsStocksTopYMin, MaxY = sett.ChartsStocksTopYMax }; } catch { }
                    }
                    if (!double.IsNaN(sett.ChartsKnockoutsYMin) && !double.IsNaN(sett.ChartsKnockoutsYMax) && sett.ChartsKnockoutsYMin != 0.0 && sett.ChartsKnockoutsYMax != 0.0)
                    {
                        try { PlotKnockouts.Plot.Axes.SetLimitsY(sett.ChartsKnockoutsYMin, sett.ChartsKnockoutsYMax); PlotKnockouts.Tag = new TradeMVVM.Trading.Chart.AxisManualLimits { HasManualY = true, MinY = sett.ChartsKnockoutsYMin, MaxY = sett.ChartsKnockoutsYMax }; } catch { }
                    }

                    // refresh UI text boxes to show current axis limits (including persisted ones)
                    try { UpdateAllYAxisTextBoxes(); } catch { }
                }
            }
            catch { }

            // if deferred PL history was waiting for stocks, try to render when stock manager gets data
            try
            {
                // attach a simple timer to attempt deferred PL history rendering shortly after startup
                var t = new System.Windows.Threading.DispatcherTimer { Interval = TimeSpan.FromMilliseconds(500) };
                int tries = 0;
                t.Tick += (s, e) =>
                {
                    try
                    {
                        tries++;
                        if (!_plHistoryDeferred) { t.Stop(); return; }

                        // check if stock series points are available
                        bool hasStocks = false;
                        try { hasStocks = _stockChartManager?.SeriesPoints != null && _stockChartManager.SeriesPoints.Count > 0; } catch { }
                        if (hasStocks)
                        {
                            try
                            {
                                // PL history rendering removed; keep flag and sync axis
                                try { SyncXAxis(); } catch { }
                                _plHistoryDeferred = false;
                                t.Stop();
                                return;
                            }
                            catch { }
                        }

                        if (tries > 20) t.Stop();
                    }
                    catch { t.Stop(); }
                };
                t.Start();
            }
            catch { }

            _stockZoomController = new ChartZoomController(PlotStocks);
            _knockoutZoomController = new ChartZoomController(PlotKnockouts);

            _dualZoomController = new DualZoomController(_stockZoomController, _knockoutZoomController);

            // start timer to plot zero value in upper plot every 30 seconds
            try
            {
                // run every 30s to persist and refresh top-plot data
                _topPlotTimer = new System.Windows.Threading.DispatcherTimer { Interval = TimeSpan.FromSeconds(30) };
                _topPlotTimer.Tick += (s, e) =>
                {
                    try
                    {
                        var key = "zero";
                        double value = 0.0;
                        try
                        {
                            if (_lastPlValue.HasValue)
                                value = _lastPlValue.Value;
                            else
                            {
                                // try parse TxtTotalPL formatted like "0.00 €"
                                var txt = (TxtTotalPL?.Text ?? string.Empty).Replace("€", string.Empty).Trim();
                                double.TryParse(txt, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.GetCultureInfo("de-DE"), out value);
                            }
                        }
                        catch { }

                        // The top-plot must be populated exclusively from DB TotalPLHistory entries
                        // maintained by the server. Do not insert or synthesize PL history points
                        // on the GUI side. Request a refresh of the DB-backed top-plot history.
                        try { LoadTopPlotHistoryForVisibleRange(); } catch { }

                        try
                        {
                            // Fill missing values for the currently visible timespan from DB TotalPL history
                            DateTime? visMin = null, visMax = null;
                            double visMinOa = double.NaN, visMaxOa = double.NaN;
                            try
                            {
                                var minX = PlotStocks.Plot.Axes.Bottom.Min;
                                var maxX = PlotStocks.Plot.Axes.Bottom.Max;
                                if (!double.IsNaN(minX) && !double.IsNaN(maxX) && maxX > minX)
                                {
                                    visMin = DateTime.FromOADate(minX);
                                    visMax = DateTime.FromOADate(maxX);
                                    visMinOa = minX;
                                    visMaxOa = maxX;
                                }
                            }
                            catch { }

                            // If the visible timespan is very short (1m or 5m), increase update frequency
                            // and prefer line rendering (no overlay scatter markers) so the top chart
                            // appears as a continuous line. Otherwise keep default behavior.
                            try
                            {
                                if (visMin.HasValue && visMax.HasValue)
                                {
                                    var visSpanTop = visMax.Value - visMin.Value;
                                if (visSpanTop <= TimeSpan.FromMinutes(1))
                                {
                                    // very small span: update often
                                    try { _topPlotTimer.Interval = TimeSpan.FromSeconds(2); } catch { }
                                    // force explicit Xs so short-range high-frequency samples are not decimated
                                    try { _stockTopChartManager.ForceScatter = true; } catch { }
                                }
                                else if (visSpanTop <= TimeSpan.FromMinutes(5))
                                {
                                    try { _topPlotTimer.Interval = TimeSpan.FromSeconds(5); } catch { }
                                    try { _stockTopChartManager.ForceScatter = true; } catch { }
                                }
                                else if (visSpanTop <= TimeSpan.FromHours(1))
                                {
                                    // for up to 1 hour visible span keep explicit points so the top plot shows available DB samples
                                    try { _topPlotTimer.Interval = TimeSpan.FromSeconds(30); } catch { }
                                    try { _stockTopChartManager.ForceScatter = true; } catch { }
                                }
                                else
                                {
                                    try { _topPlotTimer.Interval = TimeSpan.FromSeconds(30); } catch { }
                                    try { _stockTopChartManager.ForceScatter = false; } catch { }
                                }
                                }
                            }
                            catch { }

                            // if this is the first run or the visible range expanded to earlier times,
                            // load DB data for the entire visible range and use it to populate the top plot.
                            bool needFillFromDb = false;
                            try
                            {
                                if (visMin.HasValue && visMax.HasValue)
                                {
                                    if (double.IsNaN(_plLastXMin) || visMinOa < _plLastXMin - 1e-6)
                                        needFillFromDb = true;
                                }
                            }
                            catch { }

                            if (needFillFromDb && visMin.HasValue && visMax.HasValue)
                            {
                                try
                                {
                                    var db = new DatabaseService();
                                    var hist = db.LoadTotalPLHistoryBetween(visMin.Value, visMax.Value);
                                    if (hist != null && hist.Count > 0)
                                    {
                                        // only keep points within visible range and order
                                        var inRange = hist.Where(t => t.Item1 >= visMin.Value && t.Item1 <= visMax.Value)
                                                          .OrderBy(t => t.Item1)
                                                          .ToList();

                                        // compress consecutive identical values into single sample
                                        var compressed = new List<Tuple<DateTime, double>>();

                                        var spanLocal = visMax.Value - visMin.Value;
                                        bool keepAllLocal = spanLocal <= TimeSpan.FromHours(1);

                                        double? lastVal = null;
                                        foreach (var pt in inRange)
                                        {
                                            var displayVal = pt.Item2 / 1000.0; // convert € -> k€ for display
                                            if (!keepAllLocal && lastVal.HasValue && Math.Abs(lastVal.Value - displayVal) < 1e-9)
                                                continue;
                                            compressed.Add(new Tuple<DateTime, double>(pt.Item1, displayVal));
                                            lastVal = displayVal;
                                        }

                                        if (compressed.Count > 0)
                                        {
                                            lock (_stockTopData)
                                            {
                                                _stockTopData[key] = compressed;
                                            }
                                        }
                                    }
                                }
                                catch { }
                            }

                            // remember the last seen visible range
                            try { _plLastXMin = visMinOa; _plLastXMax = visMaxOa; } catch { }
                        }
                        catch { }

                        double lowerMin = double.NaN, lowerMax = double.NaN;
                        try
                        {
                            lowerMin = PlotStocks.Plot.Axes.Bottom.Min;
                            lowerMax = PlotStocks.Plot.Axes.Bottom.Max;
                        }
                        catch { }

                        try { _stockTopChartManager.Render(_stockTopData); } catch { }
                        try
                        {
                            // simple auto-zoom: compute data bounds and apply to axes
                            List<Tuple<DateTime, double>> listCopy = null;
                            lock (_stockTopData)
                            {
                                if (_stockTopData.TryGetValue(key, out var l) && l != null && l.Count > 0)
                                    listCopy = new List<Tuple<DateTime, double>>(l);
                            }

                            if (listCopy != null && listCopy.Count > 0)
                            {
                                try
                                {
                                    var xs = listCopy.Select(t => t.Item1.ToOADate()).ToArray();
                                    var ys = listCopy.Select(t => t.Item2).ToArray();
                                    double minX = xs.Min();
                                    double maxX = xs.Max();
                                    double minY = double.NaN, maxY = double.NaN;
                                    var validYs = ys.Where(v => !double.IsNaN(v) && !double.IsInfinity(v)).ToArray();
                                    if (validYs.Length > 0)
                                    {
                                        minY = validYs.Min();
                                        maxY = validYs.Max();
                                    }

                                    // Do not change X axis for the top plot here.
                                    // The X axis must strictly follow the lower stocks plot and only update
                                    // when the lower plots' X axis changes (SyncXAxis / SyncStocksXAxisFrom).

                                    if (!double.IsNaN(minY) && !double.IsNaN(maxY))
                                    {
                                        try
                                        {
                                            // respect user-configured PL axis half-span (in EUR) from settings
                                            double marginHalfK = double.NaN;
                                            try
                                            {
                                                var sett = _settingsService ?? (App.Services?.GetService(typeof(SettingsService)) as SettingsService);
                                                if (sett != null && !double.IsNaN(sett.PlAxisMarginEur) && !double.IsInfinity(sett.PlAxisMarginEur) && sett.PlAxisMarginEur > 0)
                                                    marginHalfK = sett.PlAxisMarginEur / 1000.0; // convert € -> k€
                                            }
                                            catch { }

                                            if (Math.Abs(maxY - minY) < double.Epsilon)
                                            {
                                                // single-value series: choose a sensible pad and also respect configured half-span
                                                var pad = Math.Max(0.5, Math.Abs(maxY) * 0.05);
                                                if (!double.IsNaN(marginHalfK)) pad = Math.Max(pad, marginHalfK);
                                                PlotStocksTop.Plot.Axes.SetLimitsY(minY - pad, maxY + pad);
                                            }
                                            else
                                            {
                                                // ensure the visible half-span is at least the configured value
                                                if (!double.IsNaN(marginHalfK))
                                                {
                                                    var currentHalf = (maxY - minY) / 2.0;
                                                    if (currentHalf < marginHalfK)
                                                    {
                                                        var center = (maxY + minY) / 2.0;
                                                        PlotStocksTop.Plot.Axes.SetLimitsY(center - marginHalfK, center + marginHalfK);
                                                    }
                                                    else
                                                    {
                                                        PlotStocksTop.Plot.Axes.SetLimitsY(minY, maxY);
                                                    }
                                                }
                                                else
                                                {
                                                    PlotStocksTop.Plot.Axes.SetLimitsY(minY, maxY);
                                                }
                                            }
                                        }
                                        catch { }
                                    }
                                }
                                catch { }
                            }
                        }
                        catch { }
                        try
                        {
                            // enforce X axis to match lower stocks plot so it never jumps on new top-data
                            if (!double.IsNaN(lowerMin) && !double.IsNaN(lowerMax) && lowerMax > lowerMin)
                            {
                                try { _syncingStocksXAxis = true; PlotStocksTop.Plot.Axes.SetLimitsX(lowerMin, lowerMax); } catch { }
                                finally { _syncingStocksXAxis = false; }
                            }
                        }
                        catch { }
                        try { PlotStocksTop.Refresh(); } catch { }
                    }
                    catch { }
                };
                _topPlotTimer.Start();
            }
            catch { }

            // start the charts in 2h fixed zoom as if the slider was manually set to 2h
            try
            {
                _dualZoomController.SetZoom(TimeSpan.FromHours(2));
            }
            catch { }

            // recognize manual zoom interactions
            PlotStocks.MouseWheel += (s, e) => _dualZoomController.DisableFollow();
            PlotStocks.MouseDown += (s, e) => _dualZoomController.DisableFollow();
            // Top stocks plot should also disable follow on manual interactions
            PlotStocksTop.MouseWheel += (s, e) => _dualZoomController.DisableFollow();
            PlotStocksTop.MouseDown += (s, e) => _dualZoomController.DisableFollow();

            // Sync X axis when user finishes interaction (wheel or mouse up)
            PlotStocks.MouseWheel += (s, e) => { try { SyncStocksXAxisFrom(PlotStocks); } catch { } };
            PlotStocksTop.MouseWheel += (s, e) => { try { SyncStocksXAxisFrom(PlotStocksTop); } catch { } };
            PlotStocks.AddHandler(System.Windows.UIElement.MouseLeftButtonUpEvent, new System.Windows.Input.MouseButtonEventHandler((s, e) => { try { SyncStocksXAxisFrom(PlotStocks); } catch { } }), true);
            PlotStocksTop.AddHandler(System.Windows.UIElement.MouseLeftButtonUpEvent, new System.Windows.Input.MouseButtonEventHandler((s, e) => { try { SyncStocksXAxisFrom(PlotStocksTop); } catch { } }), true);

            PlotKnockouts.MouseWheel += (s, e) => _dualZoomController.DisableFollow();
            PlotKnockouts.MouseDown += (s, e) => _dualZoomController.DisableFollow();

            // double-click handling: use AddHandler with handledEventsToo = true so we catch events even if WpfPlot marks them handled
            PlotStocks.AddHandler(System.Windows.UIElement.MouseLeftButtonDownEvent, new System.Windows.Input.MouseButtonEventHandler((s, e) =>
            {
                if (e.ClickCount == 2)
                    TryOpenGettexForNearestSeries(_stockChartManager, PlotStocks, e);
            }), true);

            PlotKnockouts.AddHandler(System.Windows.UIElement.MouseLeftButtonDownEvent, new System.Windows.Input.MouseButtonEventHandler((s, e) =>
            {
                if (e.ClickCount == 2)
                    TryOpenGettexForNearestSeries(_knockoutChartManager, PlotKnockouts, e);
            }), true);

            PlotStocksTop.AddHandler(System.Windows.UIElement.MouseLeftButtonDownEvent, new System.Windows.Input.MouseButtonEventHandler((s, e) =>
            {
                if (e.ClickCount == 2)
                    TryOpenGettexForNearestSeries(_stockTopChartManager, PlotStocksTop, e);
            }), true);

            // hover tooltip for series data (ISIN, time, percent, price)
            _hoverTooltip = new System.Windows.Controls.ToolTip { StaysOpen = true, Placement = System.Windows.Controls.Primitives.PlacementMode.Relative };

            PlotStocks.MouseMove += Plot_MouseMove;
            PlotStocksTop.MouseMove += Plot_MouseMove;
            PlotKnockouts.MouseMove += Plot_MouseMove;

            try
            {
                // PL plot removed; no handlers
            }
            catch { }

            PlotStocks.MouseLeave += Plot_MouseLeave;
            PlotStocksTop.MouseLeave += Plot_MouseLeave;
            PlotKnockouts.MouseLeave += Plot_MouseLeave;

            this.Loaded += (s, e) =>
            {
                try { AttachInnerHandlers(); } catch { }
                // subscribe to holdings updates for immediate PL refresh
                try
                {
                    var mainVm = TradeMVVM.Trading.App.MainViewModelInstance ?? Application.Current?.MainWindow?.DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel;
                    if (mainVm?.HoldingsReport != null)
                    {
                        mainVm.HoldingsReport.HoldingsUpdated += (holdingsList) =>
                        {
                            try { ComputeTotalPlFromDb(); } catch { }
                        };
                        // subscribe to settings changes
                        try
                        {
                            var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                            if (settings != null)
                                settings.SettingsChanged += () => Dispatcher.Invoke(() => { try { ComputeTotalPlFromDb(); } catch { } try { UpdateHoldingsCsvPathDisplay(); } catch { } });
                        }
                        catch { }

                        // subscribe to zoom changes so PL plot also respects quick-zoom commands
                        try
                        {
                            var dz = _dualZoomController;
                            if (dz != null)
                            {
                                dz.ZoomChanged += (ts) =>
                                {
                                    try
                                    {
                                        _plManualZoomActive = false;
                                        _plActiveZoomSpan = ts;

                                        // if ts is null => Auto/DisableFollow: do not change PL behavior
                                        if (ts == null) return;

                                        // compute referenceMaxTime from stock chart data via MainViewModel snapshot
                                        try
                                        {
                                            var mainVm = TradeMVVM.Trading.App.MainViewModelInstance ?? Application.Current?.MainWindow?.DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel;
                                            if (mainVm != null)
                                            {
                                                // take latest time across all visible series
                                                DateTime? refMax = null;
                                                lock (mainVm.PriceHistory)
                                                {
                                                    foreach (var kv in mainVm.PriceHistory)
                                                    {
                                                        var list = kv.Value;
                                                        if (list != null && list.Count > 0)
                                                        {
                                                            var t = list.Max(p => p.Item1);
                                                            if (refMax == null || t > refMax) refMax = t;
                                                        }
                                                    }
                                                }

                                                // apply the same timespan to PL calculation (DB-only) and update last PL value
                                                if (refMax.HasValue)
                                                {
                                                    try { _stockChartManager.SetActiveZoom(ts); } catch { }
                                                    try { _knockoutChartManager.SetActiveZoom(ts); } catch { }
                                                    try { ReloadUnrealizedPlSeriesFromDb(ts.Value, refMax.Value); } catch { }
                                                    return;
                                                }

                                            }
                                        }
                                        catch { }
                                    }
                                    catch { }
                                };
                            }
                        }
                        catch { }
                    }
                }
                catch { }
            };

                // Whenever TxtTotalPL is updated, mirror it into the top area label (display in €)
            try
            {
                // Use dispatcher timer to update the top-area PL label from the DB's TotalPLHistory.
                // The top display must reflect the server-written DB value (single source of truth).
                var mirrorTimer = new System.Windows.Threading.DispatcherTimer { Interval = TimeSpan.FromMilliseconds(300) };
                string lastText = null;
                mirrorTimer.Tick += (s, e) =>
                {
                    try
                    {
                        try
                        {
                            var db = new DatabaseService();
                            var hist = db.LoadTotalPLHistory();
                            try { Trace.WriteLine($"MirrorTimer: LoadTotalPLHistory returned {hist?.Count ?? 0} rows"); } catch { }
                            if (hist != null && hist.Count > 0)
                            {
                                var lastItem = hist[hist.Count - 1];
                                var last = lastItem.Item2; // value in €
                                var lastTime = lastItem.Item1;
                                try { Trace.WriteLine($"MirrorTimer: lastTime={lastTime:O}, last={last}"); } catch { }
                                var culture = CultureInfo.GetCultureInfo("de-DE");
                                TxtTotalPLTop.Text = string.Format(culture, "{0:0.00} €", last);
                                TxtTotalPLTop.Foreground = last >= 0 ? PositivePlBrush : NegativePlBrush;

                                // compute delta vs previous business day and vs start of current day (if available)
                                try
                                {
                                    // show two separate TextBlocks: start-of-day delta and previous-day delta
                                    try
                                    {
                                        var tbStart = this.FindName("TxtTotalPLTopDeltaStart") as System.Windows.Controls.TextBlock;
                                        var tbPrev = this.FindName("TxtTotalPLTopDeltaPrev") as System.Windows.Controls.TextBlock;

                                        // start of day delta
                                        try
                                        {
                                            var startEntry = hist.Where(t => t.Item1.Date == lastTime.Date).OrderBy(t => t.Item1).FirstOrDefault();
                                            if (startEntry != null)
                                            {
                                                var startVal = startEntry.Item2;
                                                var deltaStart = last - startVal;
                                                double pctStart = double.NaN;
                                                if (Math.Abs(startVal) > 1e-12)
                                                    pctStart = (deltaStart / Math.Abs(startVal)) * 100.0;
                                                var txtStart = string.Format(culture, "Δ seit Tagesbeginn: {0:+0.00;-0.00;0.00} €", deltaStart);
                                                var pctTxtStart = double.IsNaN(pctStart) ? string.Empty : string.Format(culture, " ({0:+0.##;-0.##;0.##}%)", pctStart);
                                                if (tbStart != null) { tbStart.Text = txtStart + pctTxtStart; tbStart.Foreground = deltaStart >= 0 ? PositivePlBrush : NegativePlBrush; }
                                            }
                                            else
                                            {
                                                if (tbStart != null) tbStart.Text = string.Empty;
                                            }
                                        }
                                        catch { if (tbStart != null) tbStart.Text = string.Empty; }

                                        // previous business day delta
                                        try
                                        {
                                            if (TryGetPreviousBusinessDayValue(hist, lastTime, out var prevTime, out var prevValue))
                                            {
                                                var deltaPrev = last - prevValue;
                                                double pctPrev = double.NaN;
                                                if (Math.Abs(prevValue) > 1e-12)
                                                    pctPrev = (deltaPrev / Math.Abs(prevValue)) * 100.0;
                                                var txtPrev = string.Format(culture, "Δ zum {0:dd.MM.yyyy}: {1:+0.00;-0.00;0.00} €", prevTime, deltaPrev);
                                                var pctTxtPrev = double.IsNaN(pctPrev) ? string.Empty : string.Format(culture, " ({0:+0.##;-0.##;0.##}%)", pctPrev);
                                                if (tbPrev != null) { tbPrev.Text = txtPrev + pctTxtPrev; tbPrev.Foreground = deltaPrev >= 0 ? PositivePlBrush : NegativePlBrush; }
                                            }
                                            else
                                            {
                                                if (tbPrev != null) tbPrev.Text = string.Empty;
                                            }
                                        }
                                        catch { if (tbPrev != null) tbPrev.Text = string.Empty; }
                                    }
                                    catch { }
                                }
                                catch { }

                                // ensure the top-plot series contains the latest DB point so the graph shows
                                // the most recent value immediately (do not wait for the slower topPlotTimer)
                                try
                                {
                                    // convert to k€ as used by top plot
                                    var displayVal = last / 1000.0;
                                    lock (_stockTopData)
                                    {
                                        if (!_stockTopData.TryGetValue("zero", out var list) || list == null)
                                            list = new List<Tuple<DateTime, double>>();
                                        // append if newer than last stored point to avoid duplicates
                                        if (list.Count == 0 || list[list.Count - 1].Item1 < lastTime)
                                        {
                                            list.Add(new Tuple<DateTime, double>(lastTime, displayVal));
                                            _stockTopData["zero"] = list;
                                            try { Trace.WriteLine($"MirrorTimer: appended point {lastTime:O} -> {displayVal} k€ (list now {list.Count})"); } catch { }
                                        }
                                    }
                                    try { _stockTopChartManager.Render(_stockTopData); } catch { }
                                    try { PlotStocksTop.Refresh(); } catch { }
                                }
                                catch { }

                                // remember to avoid unnecessary UI churn
                                lastText = TxtTotalPL?.Text ?? string.Empty;
                                return;
                            }
                        }
                        catch { }

                        // fallback: if no DB value available, mirror the left label as before
                        var cur = TxtTotalPL?.Text ?? string.Empty;
                        if (cur != lastText)
                        {
                            lastText = cur;
                            try { TxtTotalPLTop.Text = cur; } catch { }
                        }
                    }
                    catch { }
                };
                mirrorTimer.Start();
            }
            catch { }

            // start runtime status timer
            StartStatusTimer();

            // populate status once immediately so Alert-Center shows values right away
            try { UpdateStatus(); } catch { }

            // ensure Y axis textboxes reflect current axes once the visual tree/layout is ready
            try
            {
                this.Loaded += (s, e) =>
                {
                    try
                    {
                        // defer so ScottPlot axes/layout initialize first
                        Dispatcher.BeginInvoke(new Action(() => { try { UpdateAllYAxisTextBoxes(); } catch { } }), System.Windows.Threading.DispatcherPriority.Background);
                    }
                    catch { }
                };
            }
            catch { }
        }

        private void TrySetAxisMinimumSize(object axis, float size)
        {
            try
            {
                if (axis == null)
                    return;

                var p = axis.GetType().GetProperty("MinimumSize");
                if (p == null || !p.CanWrite)
                    return;

                if (p.PropertyType == typeof(float))
                    p.SetValue(axis, size);
                else if (p.PropertyType == typeof(double))
                    p.SetValue(axis, (double)size);
                else if (p.PropertyType == typeof(int))
                    p.SetValue(axis, (int)Math.Round(size));
            }
            catch { }
        }

        private void NormalizeAxisLabelAreas()
        {
            try
            {
                const float leftAxisSize = 100f;
                try { if (PlotStocks?.Plot?.Axes != null) TrySetAxisMinimumSize(PlotStocks.Plot.Axes.Left, leftAxisSize); } catch { }
                try { if (PlotStocksTop?.Plot?.Axes != null) TrySetAxisMinimumSize(PlotStocksTop.Plot.Axes.Left, leftAxisSize); } catch { }
                try { if (PlotKnockouts?.Plot?.Axes != null) TrySetAxisMinimumSize(PlotKnockouts.Plot.Axes.Left, leftAxisSize); } catch { }
            }
            catch { }
        }

        private void ApplyRightPadding(ScottPlot.WPF.WpfPlot plot, double percent)
        {
            try
            {
                if (plot == null || percent <= 0)
                    return;

                var min = plot.Plot.Axes.Bottom.Min;
                var max = plot.Plot.Axes.Bottom.Max;
                if (double.IsNaN(min) || double.IsNaN(max) || max <= min)
                    return;

                var span = max - min;
                plot.Plot.Axes.SetLimitsX(min, max + span * percent);
            }
            catch { }
        }

        // PL time axis configuration removed (no PL plot)

        private void ApplySavedLayout()
        {
            try
            {
                var width = _settingsService?.ChartsAlertPanelWidth ?? 0.0;
                if (width <= 0 || AlertsColumn == null)
                    return;

                var clamped = Math.Max(220.0, Math.Min(900.0, width));
                AlertsColumn.Width = new GridLength(clamped, GridUnitType.Pixel);
            }
            catch { }
        }

        private void AlertsColumnSplitter_DragCompleted(object sender, System.Windows.Controls.Primitives.DragCompletedEventArgs e)
        {
            try
            {
                if (_settingsService == null || AlertsColumn == null)
                    return;

                var width = AlertsColumn.ActualWidth > 0 ? AlertsColumn.ActualWidth : AlertsColumn.Width.Value;
                _settingsService.ChartsAlertPanelWidth = Math.Max(220.0, Math.Min(900.0, width));
                _settingsService.Save();
            }
            catch { }
        }

        public void ResetLayoutDefaults()
        {
            try
            {
                if (AlertsColumn != null)
                    AlertsColumn.Width = new GridLength(320.0, GridUnitType.Pixel);

                if (_settingsService != null)
                {
                    _settingsService.ChartsAlertPanelWidth = 320.0;
                    _settingsService.Save();
                }
            }
            catch { }
        }

        private string GetPlHistoryPath()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var dataDir = System.IO.Path.Combine(baseDir, "DataAnalysis");
            var fileName = _plDisplayMode == PlDisplayMode.UnrealizedPlusRealized
                ? "pl_history_total.csv"
                : "pl_history_unrealized.csv";
            return System.IO.Path.Combine(dataDir, fileName);
        }

        private double GetIncomeFromCsvEur()
        {
            try
            {
                var settings = _settingsService ?? (App.Services?.GetService(typeof(SettingsService)) as SettingsService);
                var csvPath = settings?.HoldingsCsvPath;
                if (string.IsNullOrWhiteSpace(csvPath) || !File.Exists(csvPath))
                    return 0.0;

                var lastWrite = File.GetLastWriteTimeUtc(csvPath);
                if (lastWrite == _incomeCacheFileWriteUtc)
                    return _incomeCacheEur;

                var converter = new CurrencyConverter();
                var culture = CultureInfo.GetCultureInfo("de-DE");
                double sum = 0.0;

                foreach (var line in File.ReadLines(csvPath).Skip(1))
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var parts = line.Split(';');
                    if (parts.Length < 14)
                        continue;

                    var status = (parts[2] ?? string.Empty).Trim();
                    if (!string.Equals(status, "Executed", StringComparison.OrdinalIgnoreCase))
                        continue;

                    var type = (parts[6] ?? string.Empty).Trim().ToLowerInvariant();
                    bool isIncome =
                        type.Contains("zins") ||
                        type.Contains("interest") ||
                        type.Contains("dividend") ||
                        type.Contains("coupon") ||
                        type.Contains("aussch") ||
                        type.Contains("kupon");

                    if (!isIncome)
                        continue;

                    var amountText = (parts[10] ?? string.Empty).Replace("\u00A0", string.Empty).Trim();
                    if (!double.TryParse(amountText, NumberStyles.Any, culture, out var amount))
                        continue;

                    var currency = (parts[13] ?? string.Empty).Trim();
                    try { sum += converter.ConvertToEur(amount, currency); } catch { }
                }

                _incomeCacheFileWriteUtc = lastWrite;
                _incomeCacheEur = sum;
                return sum;
            }
            catch
            {
                return 0.0;
            }
        }

        private void UpdatePlTitle()
        {
            try
            {
                // PL title control removed from XAML; nothing to update
                return;
            }
            catch { }
        }

        private void CmbPlMode_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                // PL mode selection control removed; do nothing
                return;
            }
            catch { }
        }

        /// <summary>
        /// Total PL im Diagramm kann als unrealisiert oder unrealisiert+realisiert angezeigt werden.
        /// </summary>
        // Read total P/L exclusively from DB. GUI must not compute it locally.
        private void ComputeTotalPlFromDb()
        {
            try
            {
                var db = new DatabaseService();
                var hist = db.LoadTotalPLHistory();
                if (hist == null || hist.Count == 0)
                    return;

                var lastItem = hist[hist.Count - 1];
                var last = lastItem.Item2; // value in EUR

                Action updateUi = () =>
                {
                    try
                    {
                        var culture = CultureInfo.GetCultureInfo("de-DE");
                        TxtTotalPL.Text = string.Format(culture, "{0:0.00} €", last);
                        TxtTotalPL.Foreground = last >= 0 ? PositivePlBrush : NegativePlBrush;

                        // also update the top PL label (display in €)
                        try
                        {
                            TxtTotalPLTop.Text = string.Format(culture, "{0:0.00} €", last);
                            TxtTotalPLTop.Foreground = last >= 0 ? PositivePlBrush : NegativePlBrush;
                        }
                        catch { }

                        _lastPlValue = last;
                    }
                    catch { }
                };

                if (Dispatcher.CheckAccess())
                    updateUi();
                else
                    Dispatcher.BeginInvoke(updateUi);
            }
            catch { }
        }

        private System.Windows.Threading.DispatcherTimer _statusTimer;

        private void StartStatusTimer()
        {
            try
            {
                _statusTimer = new System.Windows.Threading.DispatcherTimer()
                {
                    Interval = TimeSpan.FromSeconds(1)
                };
                _statusTimer.Tick += (s, e) => UpdateStatus();
                _statusTimer.Start();
            }
            catch { }
        }

        private void UpdateStatus()
        {
            try
            {
                var nowUtc = DateTime.UtcNow;

                // expensive process enumeration: throttle
                if ((nowUtc - _lastProcessStatusUpdateUtc) >= TimeSpan.FromSeconds(5))
                {
                    int chromedriverCount = 0;
                    int chromeCount = 0;
                    try { chromedriverCount = System.Diagnostics.Process.GetProcessesByName("chromedriver").Length; } catch { }
                    try { chromeCount = System.Diagnostics.Process.GetProcessesByName("chrome").Length; } catch { }

                    var processText = $"chromedriver: {chromedriverCount}, chrome: {chromeCount}";
                    // mirror into Alert-Center VM
                    try { if (_alertVm != null) _alertVm.AlertStatusProcesses = processText; } catch { }
                    if (!string.Equals(processText, _lastStatusProcessesText, StringComparison.Ordinal))
                    {
                        _lastStatusProcessesText = processText;
                        TxtStatusProcesses.Text = processText;
                    }

                    _lastProcessStatusUpdateUtc = nowUtc;
                }

                // stocks being polled
                int stockCount = 0;
                try
                {
                    var globalVm = TradeMVVM.Trading.App.MainViewModelInstance ?? Application.Current?.MainWindow?.DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel;
                    if (globalVm != null && globalVm.Stocks != null)
                        stockCount = globalVm.Stocks.Count;
                }
                catch { }

                var stocksText = $"stocks: {stockCount}";
                // mirror into Alert-Center VM
                try { if (_alertVm != null) _alertVm.AlertStatusStocks = stocksText; } catch { }
                if (!string.Equals(stocksText, _lastStatusStocksText, StringComparison.Ordinal))
                {
                    _lastStatusStocksText = stocksText;
                    TxtStatusStocks.Text = stocksText;
                }

                // DB rows in memory for visible charts
                if ((nowUtc - _lastHistoryStatusUpdateUtc) >= TimeSpan.FromSeconds(2))
                {
                    int points = 0;
                    try
                    {
                        // approximate: count entries for the current stocks in the in-memory PriceHistory via MainViewModel
                        var globalVm = TradeMVVM.Trading.App.MainViewModelInstance ?? Application.Current?.MainWindow?.DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel;
                        if (globalVm != null)
                        {
                            var hist = globalVm.PriceHistory;
                            if (hist != null)
                            {
                                foreach (var kv in hist)
                                    points += kv.Value?.Count ?? 0;
                            }
                        }
                    }
                    catch { }

                    var historyText = $"points: {points}";
                    // mirror into Alert-Center VM
                    try { if (_alertVm != null) _alertVm.AlertStatusHistory = historyText; } catch { }
                    if (!string.Equals(historyText, _lastStatusHistoryText, StringComparison.Ordinal))
                    {
                        _lastStatusHistoryText = historyText;
                        TxtStatusHistory.Text = historyText;
                    }

                    _lastHistoryStatusUpdateUtc = nowUtc;
                }

                var timeText = DateTime.Now.ToString("HH:mm:ss");
                TxtStatusTime.Text = timeText;
                try { if (_alertVm != null) _alertVm.AlertStatusTime = timeText; } catch { }

                // ensure Alert-Center bindings always reflect the latest known status values
                try
                {
                    if (_alertVm != null)
                    {
                        _alertVm.AlertStatusProcesses = _lastStatusProcessesText ?? string.Empty;
                        _alertVm.AlertStatusStocks = _lastStatusStocksText ?? string.Empty;
                        _alertVm.AlertStatusHistory = _lastStatusHistoryText ?? string.Empty;
                        _alertVm.AlertStatusTime = timeText;
                        try { _alertVm.AlertMessage = TxtAlertToast?.Text ?? string.Empty; } catch { }
                    }
                }
                catch { }
                // update total P/L from DB (only changes when a newer valid price exists)
                try
                {
                    var globalVm2 = TradeMVVM.Trading.App.MainViewModelInstance ?? Application.Current?.MainWindow?.DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel;
                    if (globalVm2?.IsPollingRunning == true
                        && (nowUtc - _lastPlRefreshUtc) >= TimeSpan.FromSeconds(2))
                    {
                        ComputeTotalPlFromDb();
                        _lastPlRefreshUtc = nowUtc;
                    }
                }
                catch { }
                if ((nowUtc - _lastAlertEvalUtc) >= TimeSpan.FromSeconds(2))
                {
                    try { EvaluateAlerts(); } catch { }
                    _lastAlertEvalUtc = nowUtc;
                }
            }
            catch { }
        }

        private void EvaluateAlerts()
        {
            try
            {
                var mainVm = TradeMVVM.Trading.App.MainViewModelInstance
                    ?? Application.Current?.MainWindow?.DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel;
                if (mainVm == null)
                    return;

                try
                {
                    var seenPercentKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var seenTrailingKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    lock (mainVm.PriceHistory)
                    {
                        foreach (var kv in mainVm.PriceHistory)
                        {
                            var isin = kv.Key;
                            var list = kv.Value;
                            if (list == null || list.Count == 0)
                                continue;

                            var latest = list[list.Count - 1];
                            var latestPct = latest.Item2;

                            var isinThreshold = GetPercentThresholdForIsin(isin);
                            var alertKey = $"pct:{isin}";
                            seenPercentKeys.Add(alertKey);

                            if (isinThreshold <= 0 || double.IsNaN(latestPct) || double.IsInfinity(latestPct))
                            {
                                _activePercentBreaches.Remove(alertKey);
                            }
                            else
                            {
                                var breached = Math.Abs(latestPct) >= isinThreshold;
                                if (breached)
                                {
                                    if (!_activePercentBreaches.Contains(alertKey))
                                    {
                                        AddAlert(
                                            alertKey,
                                            $"{isin}: Schwelle ±{isinThreshold:0.##}% erreicht ({latestPct:0.##}%)",
                                            isin);
                                        _activePercentBreaches.Add(alertKey);
                                    }
                                }
                                else
                                {
                                    _activePercentBreaches.Remove(alertKey);
                                }
                            }

                            var trailingStop = GetTrailingStopThresholdForIsin(isin);
                            var trailingCurrent = GetTrailingStopCurrentForIsin(isin, trailingStop);
                            try
                            {
                                var row = mainVm.HoldingsReport?.Holdings?
                                    .FirstOrDefault(r => string.Equals(r.ISIN, isin, StringComparison.OrdinalIgnoreCase));
                                if (row != null)
                                {
                                    trailingStop = row.TrailingStopConfiguredPercent;
                                    trailingCurrent = row.TrailingStopPercent;
                                }
                            }
                            catch { }

                            var trailingKey = $"trail:{isin}";
                            seenTrailingKeys.Add(trailingKey);

                            if (trailingStop <= 0)
                            {
                                _activeTrailingBreaches.Remove(trailingKey);
                                continue;
                            }

                            if (trailingCurrent <= 0)
                            {
                                if (!_activeTrailingBreaches.Contains(trailingKey))
                                {
                                    AddAlert(
                                        trailingKey,
                                        $"{isin}: Trailing-Stop ausgelöst (Rest {trailingCurrent:0.##}% von {trailingStop:0.##}%)",
                                        isin);
                                    _activeTrailingBreaches.Add(trailingKey);
                                }
                            }
                            else
                            {
                                _activeTrailingBreaches.Remove(trailingKey);
                            }
                        }
                    }

                    _activePercentBreaches.RemoveWhere(k => !seenPercentKeys.Contains(k));
                    _activeTrailingBreaches.RemoveWhere(k => !seenTrailingKeys.Contains(k));
                }
                catch { }

                // Global €-Delta alert intentionally disabled.
                // Alert-Center should only show ISIN-based percent alerts.
            }
            catch { }
        }

        private void LoadTrailingPeaks()
        {
            try
            {
                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings?.IsinTrailingPeakPercents == null)
                    return;

                _trailingPeaks.Clear();
                foreach (var kv in settings.IsinTrailingPeakPercents)
                {
                    var key = (kv.Key ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                    if (string.IsNullOrWhiteSpace(key) || double.IsNaN(kv.Value) || double.IsInfinity(kv.Value))
                        continue;
                    _trailingPeaks[key] = kv.Value;
                }

                _trailingPeaksDirty = false;
                _lastTrailingPeaksSavedUtc = DateTime.UtcNow;
            }
            catch { }
        }

        private void SaveTrailingPeaks(bool force)
        {
            try
            {
                if (!force)
                {
                    if (!_trailingPeaksDirty)
                        return;
                    if ((DateTime.UtcNow - _lastTrailingPeaksSavedUtc) < TrailingPeaksSaveInterval)
                        return;
                }

                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings == null)
                    return;

                settings.IsinTrailingPeakPercents = new Dictionary<string, double>(_trailingPeaks, StringComparer.OrdinalIgnoreCase);
                settings.Save();
                _trailingPeaksDirty = false;
                _lastTrailingPeaksSavedUtc = DateTime.UtcNow;
            }
            catch { }
        }

        private double GetPercentThresholdForIsin(string isin)
        {
            try
            {
                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings == null)
                    return _pricePercentAlertThreshold;

                var key = (isin ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                if (!string.IsNullOrWhiteSpace(key) && settings.IsinAlertPercentThresholds != null
                    && settings.IsinAlertPercentThresholds.TryGetValue(key, out var perIsin)
                    && perIsin >= 0)
                {
                    return perIsin;
                }

                if (settings.AlertPricePercentThreshold > 0)
                    return settings.AlertPricePercentThreshold;
            }
            catch { }

            return _pricePercentAlertThreshold;
        }

        private double GetTrailingStopThresholdForIsin(string isin)
        {
            try
            {
                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings == null)
                    return 0.0;

                var key = (isin ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                if (!string.IsNullOrWhiteSpace(key)
                    && settings.IsinTrailingStopPercentThresholds != null
                    && settings.IsinTrailingStopPercentThresholds.TryGetValue(key, out var perIsin)
                    && perIsin >= 0)
                {
                    return perIsin;
                }
            }
            catch { }

            return 0.0;
        }

        private double GetTrailingStopCurrentForIsin(string isin, double fallback)
        {
            try
            {
                var settings = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.SettingsService)) as TradeMVVM.Trading.Services.SettingsService;
                if (settings == null)
                    return fallback;

                var key = (isin ?? string.Empty).Replace("\u00A0", string.Empty).Trim().ToUpperInvariant();
                if (!string.IsNullOrWhiteSpace(key)
                    && settings.IsinTrailingStopCurrentPercentThresholds != null
                    && settings.IsinTrailingStopCurrentPercentThresholds.TryGetValue(key, out var current)
                    && !double.IsNaN(current)
                    && !double.IsInfinity(current))
                {
                    return current;
                }
            }
            catch { }

            return fallback;
        }

        private void AddAlert(string key, string message, string isin)
        {
            try
            {
                var now = DateTime.Now;
                if (_lastAlertAt.TryGetValue(key, out var lastAt) && (now - lastAt) < AlertCooldown)
                    return;

                _lastAlertAt[key] = now;

                Dispatcher.Invoke(() =>
                {
                    try
                    {
                        var item = new AlertItem { Timestamp = now, Message = message, Isin = isin };
                        _alertItems.Insert(0, item);
                        while (_alertItems.Count > MaxAlertItems)
                            _alertItems.RemoveAt(_alertItems.Count - 1);

                        TxtAlertToast.Text = item.Display;
                        try { _alertToastTimer.Stop(); _alertToastTimer.Start(); } catch { }
                    }
                    catch { }
                });
            }
            catch { }
        }

        // PL graph removed: keep only DB computation and UI value update
        private void InitPlPlot()
        {
            // No PL plot to initialize. Historical PL is kept in CSV/database for analysis.
        }

        // When previously a PL history series was rendered, this method rebuilt it from the DB.
        // PL graph removed — keep a lightweight stub that updates the current PL value from DB.
        private void ReloadUnrealizedPlSeriesFromDb(TimeSpan spanParam, DateTime referenceMax)
        {
            try
            {
                // Recompute latest PL value based on DB rows/windows
                ComputeTotalPlFromDb();
            }
            catch { }
        }

        private bool TryGetMouseCoordinates(ScottPlot.WPF.WpfPlot plot, System.Windows.Point pos, out double x, out double y)
        {
            x = double.NaN;
            y = double.NaN;

            try
            {
                // prefer ScottPlot API conversion when available (accounts for data area offsets/margins)
                var plotObj = plot?.Plot;
                if (plotObj != null)
                {
                    var getCoordinates = plotObj.GetType().GetMethods()
                        .FirstOrDefault(m => string.Equals(m.Name, "GetCoordinates", StringComparison.Ordinal)
                                             && m.GetParameters().Length == 2);

                    if (getCoordinates != null)
                    {
                        var result = getCoordinates.Invoke(plotObj, new object[] { (float)pos.X, (float)pos.Y });
                        if (result != null)
                        {
                            var resultType = result.GetType();
                            var propX = resultType.GetProperty("X");
                            var propY = resultType.GetProperty("Y");
                            if (propX != null && propY != null)
                            {
                                x = Convert.ToDouble(propX.GetValue(result));
                                y = Convert.ToDouble(propY.GetValue(result));
                                if (!double.IsNaN(x) && !double.IsNaN(y))
                                    return true;
                            }
                        }
                    }
                }
            }
            catch { }

            try
            {
                // fallback: linear axis mapping
                double plotWidth = plot.ActualWidth;
                double plotHeight = plot.ActualHeight;

                double axisMinX = plot.Plot.Axes.Bottom.Min;
                double axisMaxX = plot.Plot.Axes.Bottom.Max;
                if (!double.IsNaN(axisMinX) && !double.IsNaN(axisMaxX) && plotWidth > 0)
                    x = axisMinX + (pos.X / plotWidth) * (axisMaxX - axisMinX);

                double axisMinY = plot.Plot.Axes.Left.Min;
                double axisMaxY = plot.Plot.Axes.Left.Max;
                if (!double.IsNaN(axisMinY) && !double.IsNaN(axisMaxY) && plotHeight > 0)
                    y = axisMaxY - (pos.Y / plotHeight) * (axisMaxY - axisMinY);
            }
            catch { }

            return !double.IsNaN(x) && !double.IsNaN(y);
        }

        private async void Plot_MouseMove(object sender, System.Windows.Input.MouseEventArgs e)
        {
            try
            {
                if (!(sender is ScottPlot.WPF.WpfPlot plot)) return;

                bool isTopPlot = plot == PlotStocksTop;
                var mgr = isTopPlot ? _stockTopChartManager : (plot == PlotStocks ? _stockChartManager : _knockoutChartManager);

                var pos = e.GetPosition(plot);
                if (!TryGetMouseCoordinates(plot, pos, out var x, out var y))
                    return;

                // throttle mouse processing to avoid blocking UI and polling thread
                var now = DateTime.UtcNow;
                if ((now - _lastMouseProcessed).TotalMilliseconds < MouseProcessIntervalMs)
                    return;
                _lastMouseProcessed = now;

                // capture plot/axis state for background computation
                double minX = double.NaN, maxX = double.NaN, minY = double.NaN, maxY = double.NaN;
                double plotW = plot.ActualWidth, plotH = plot.ActualHeight;
                try { minX = plot.Plot.Axes.Bottom.Min; maxX = plot.Plot.Axes.Bottom.Max; } catch { }
                try { minY = plot.Plot.Axes.Left.Min; maxY = plot.Plot.Axes.Left.Max; } catch { }

                var seriesSnapshot = mgr.SeriesPoints.ToList();

                var opId = System.Threading.Interlocked.Increment(ref _mouseOpCounter);

                double axisMinXForPx = double.NaN, axisMaxXForPx = double.NaN;
                try
                {
                    axisMinXForPx = plot.Plot.Axes.Bottom.Min;
                    axisMaxXForPx = plot.Plot.Axes.Bottom.Max;
                }
                catch { }
                bool canUseXPixelThreshold = !double.IsNaN(axisMinXForPx) && !double.IsNaN(axisMaxXForPx)
                    && plotW > 0 && Math.Abs(axisMaxXForPx - axisMinXForPx) > double.Epsilon;
                double mouseXpx = 0;
                if (canUseXPixelThreshold)
                    mouseXpx = (x - axisMinXForPx) / (axisMaxXForPx - axisMinXForPx) * plotW;

                // compute nearest point by X proximity with coarse Y component
                var nearest = await Task.Run(() =>
                {
                    string bestKey = null;
                    int bestIdx = -1;
                    double bestDist = double.MaxValue;

                    try
                    {
                        foreach (var kv in seriesSnapshot)
                        {
                            var key = kv.Key;
                            try
                            {
                                var arr = kv.Value;
                                if (arr.Xs == null || arr.Ys == null)
                                    continue;

                                int len = Math.Min(arr.Xs.Length, arr.Ys.Length);
                                if (len <= 0)
                                    continue;

                                int idx = 0;
                                double minDx = double.MaxValue;
                                for (int i = 0; i < len; i++)
                                {
                                    var dx = Math.Abs(arr.Xs[i] - x);
                                    if (dx < minDx)
                                    {
                                        minDx = dx;
                                        idx = i;
                                    }
                                }

                                double dist;
                                if (canUseXPixelThreshold)
                                {
                                    var pointXpx = (arr.Xs[idx] - axisMinXForPx) / (axisMaxXForPx - axisMinXForPx) * plotW;
                                    var dxPx = Math.Abs(pointXpx - mouseXpx);

                                    double dyPx = 0;
                                    try
                                    {
                                        if (!double.IsNaN(minY) && !double.IsNaN(maxY)
                                            && plotH > 0 && Math.Abs(maxY - minY) > double.Epsilon)
                                        {
                                            var pointYpx = (maxY - arr.Ys[idx]) / (maxY - minY) * plotH;
                                            var mouseYpx = (maxY - y) / (maxY - minY) * plotH;
                                            dyPx = Math.Abs(pointYpx - mouseYpx);
                                        }
                                    }
                                    catch { }

                                    // X dominates, Y is only a coarse tie-breaker
                                    dist = dxPx + (dyPx * 0.30);
                                }
                                else
                                {
                                    // fallback in data-space: X dominates, Y coarse
                                    var dy = Math.Abs(arr.Ys[idx] - y);
                                    dist = minDx + (dy * 0.30);
                                }

                                if (dist < bestDist)
                                {
                                    bestDist = dist;
                                    bestIdx = idx;
                                    bestKey = key;
                                }
                            }
                            catch { }
                        }
                    }
                    catch { }

                    if (bestKey == null) return (null as string, -1, double.MaxValue);
                    return (bestKey, bestIdx, bestDist);
                });

                // if another mouse op started meanwhile, discard this result
                if (opId != _mouseOpCounter) return;

                string bestKey = nearest.Item1;
                int bestIdx = nearest.Item2;
                double bestDist = nearest.Item3;

                const double pixelThreshold = 45.0;
                if (bestKey == null || bestDist >= pixelThreshold)
                {
                    // too far - hide
                    if (_hoverTooltip != null)
                    {
                        _hoverTooltip.IsOpen = false;
                        _hoverCurrentKey = null;
                        _hoverCts?.Cancel();
                    }
                    return;
                }

                // compose initial tooltip content from stored series points (include name if available)
                string content = bestKey;
                string displayName = string.Empty;
                try
                {
                    // prefer global instance
                    var globalVm = TradeMVVM.Trading.App.MainViewModelInstance;
                    if (globalVm == null)
                    {
                        // fallback: try main window data context
                        try { globalVm = Application.Current?.MainWindow?.DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel; } catch { }
                    }

                    if (globalVm != null)
                    {
                        // prefer Stocks list
                        try
                        {
                            if (globalVm.Stocks != null)
                            {
                                var found = globalVm.Stocks.Find(s => string.Equals(s.isin_wkn, bestKey, StringComparison.OrdinalIgnoreCase));
                                if (!string.IsNullOrWhiteSpace(found.name))
                                    displayName = found.name;
                            }
                        }
                        catch { }

                        // fallback: check OwnedPositions which may contain names for holdings
                        if (string.IsNullOrWhiteSpace(displayName))
                        {
                            try
                            {
                                if (globalVm.OwnedPositions != null)
                                {
                                    var op = globalVm.OwnedPositions.FirstOrDefault(p => string.Equals(p.isin, bestKey, StringComparison.OrdinalIgnoreCase));
                                    if (op != default && !string.IsNullOrWhiteSpace(op.name))
                                        displayName = op.name;
                                }
                            }
                            catch { }
                        }
                    }
                }
                catch { }
                try
                {
                    var arr = mgr.SeriesPoints[bestKey];
                    if (arr.Xs != null && bestIdx >= 0 && bestIdx < arr.Xs.Length)
                    {
                        var dt = DateTime.FromOADate(arr.Xs[bestIdx]);
                        var yVal = arr.Ys[bestIdx];
                        if (isTopPlot)
                        {
                            // top plot stores values in k€
                            var valueK = yVal;
                            var valueEur = valueK * 1000.0;
                            if (!string.IsNullOrWhiteSpace(displayName))
                                content = $"{bestKey} - {displayName}\n{dt:G}\n{valueK:0.00} k€ ({valueEur:0.00} €)";
                            else
                                content = $"{bestKey}\n{dt:G}\n{valueK:0.00} k€ ({valueEur:0.00} €)";
                        }
                        else
                        {
                            var percent = yVal;
                            if (!string.IsNullOrWhiteSpace(displayName))
                                content = $"{bestKey} - {displayName}\n{dt:G}\n{percent:0.##}%";
                            else
                                content = $"{bestKey}\n{dt:G}\n{percent:0.##}%";
                        }
                    }
                }
                catch { }

                // show tooltip
                _hoverTooltip.PlacementTarget = plot;
                _hoverTooltip.HorizontalOffset = pos.X + 12;
                _hoverTooltip.VerticalOffset = pos.Y + 12;
                _hoverTooltip.Content = content;
                _hoverTooltip.IsOpen = true;

                // if same key already fetching, skip
                if (_hoverCurrentKey == bestKey) return;

                // For the top PL plot we show values from the series (k€) and do not perform
                // additional DB/provider lookups. Return early to avoid unnecessary work.
                if (isTopPlot)
                {
                    _hoverCurrentKey = bestKey;
                    return;
                }

                _hoverCts?.Cancel();
                _hoverCts = new System.Threading.CancellationTokenSource();
                _hoverCurrentKey = bestKey;
                var ctoken = _hoverCts.Token;

                // debounce: wait a short time before doing expensive work; cancelled if mouse moves
                try
                {
                    await Task.Delay(HoverDebounceMs, ctoken);
                }
                catch (TaskCanceledException)
                {
                    return;
                }

                if (_hoverCurrentKey != bestKey || ctoken.IsCancellationRequested) return;

                // Try to show latest DB value (after debounce) to avoid network calls
                try
                {
                    var repo = new PriceRepository();
                    var rows = repo.LoadByIsin(bestKey);
                    if (rows != null && rows.Count > 0)
                    {
                        var last = rows[rows.Count - 1];
                        var title = !string.IsNullOrWhiteSpace(displayName) ? $"{bestKey} - {displayName}" : bestKey;
                        Dispatcher.Invoke(() =>
                        {
                            if (_hoverCurrentKey == bestKey)
                                _hoverTooltip.Content = $"{title}\nPreis: {last.Price:0.##} €\nÄnderung: {last.Percent:0.##}%\n({last.Time:G})";
                        });

                        // if DB entry is recent, skip live provider fetch
                        if (DateTime.Now - last.Time < TimeSpan.FromSeconds(DbFreshSeconds))
                        {
                            return;
                        }
                    }
                }
                catch { }

                // use cached provider result when recent
                if (_hoverCache.TryGetValue(bestKey, out var cached2) && (DateTime.UtcNow - cached2.fetchedAt) < TimeSpan.FromSeconds(HoverCacheSeconds))
                {
                    Dispatcher.Invoke(() =>
                    {
                        if (_hoverCurrentKey != bestKey) return;
                        var title = !string.IsNullOrWhiteSpace(displayName) ? $"{bestKey} - {displayName}" : bestKey;
                        var contentLive = $"{title}\nPreis: {cached2.price:0.##} €\nÄnderung: {cached2.percent:0.##}%";
                        if (cached2.providerTime.HasValue)
                            contentLive += $"\n({cached2.providerTime.Value:G})";
                        _hoverTooltip.Content = contentLive;
                    });
                    return;
                }

                // Do not perform live provider fetches on mouse hover to avoid excessive network/Selenium calls.
                // Users can double-click a series to open the provider page. Keep UI responsive by showing a hint.
                Dispatcher.Invoke(() =>
                {
                    if (_hoverCurrentKey != bestKey) return;
                    var title = !string.IsNullOrWhiteSpace(displayName) ? $"{bestKey} - {displayName}" : bestKey;
                    var contentLive = $"{title}\nLive-Daten werden auf Hover nicht automatisch abgerufen. Doppelklick öffnet die Anbieter-Seite.";
                    _hoverTooltip.Content = contentLive;
                });
            }
            catch { }
        }

        private void Plot_MouseLeave(object sender, System.Windows.Input.MouseEventArgs e)
        {
            try
            {
                _hoverTooltip.IsOpen = false;
                _hoverCts?.Cancel();
                _hoverCurrentKey = null;
            }
            catch { }
        }

        // Fear&Greed button handler
        private void BtnFearGreed_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var url = "https://edition.cnn.com/markets/fear-and-greed";
                try
                {
                    Process.Start(new ProcessStartInfo { FileName = url, UseShellExecute = true });
                }
                catch (Exception ex)
                {
                    try { Trace.TraceWarning($"Failed to open Fear&Greed URL: {ex.Message}"); } catch { }
                }
            }
            catch { }
        }

        private static System.Windows.Controls.Panel FindVisualChildPanel(System.Windows.DependencyObject parent)
        {
            if (parent == null) return null;
            for (int i = 0; i < System.Windows.Media.VisualTreeHelper.GetChildrenCount(parent); i++)
            {
                var child = System.Windows.Media.VisualTreeHelper.GetChild(parent, i);
                if (child is System.Windows.Controls.Panel p) return p;
                var res = FindVisualChildPanel(child);
                if (res != null) return res;
            }
            return null;
        }

        private void AttachInnerHandlers()
        {
            try
            {
                Action<Control, ChartManager> attach = (ctrl, mgr) =>
                {
                    try
                    {
                        // reuse helper from HoldingsReportView
                        var panel = FindVisualChildPanel(ctrl);
                        if (panel != null)
                        {
                            panel.PreviewMouseLeftButtonDown += (s, e) => { if (e.ClickCount == 2) TryOpenGettexForNearestSeries(mgr, ctrl as ScottPlot.WPF.WpfPlot, e); };
                            panel.MouseLeftButtonDown += (s, e) => { if (e.ClickCount == 2) TryOpenGettexForNearestSeries(mgr, ctrl as ScottPlot.WPF.WpfPlot, e); };
                        }
                    }
                    catch { }
                };

                try { attach(PlotStocks, _stockChartManager); } catch { }
                try { attach(PlotKnockouts, _knockoutChartManager); } catch { }
            }
            catch { }
        }

        private T FindVisualChild<T>(DependencyObject parent) where T : DependencyObject
        {
            if (parent == null) return null;
            for (int i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
            {
                var child = VisualTreeHelper.GetChild(parent, i);
                if (child is T t) return t;
                var res = FindVisualChild<T>(child);
                if (res != null) return res;
            }
            return null;
        }

        // expose Render and ApplyZoom used by MainWindow
        public void Render(
             Dictionary<string, List<Tuple<DateTime, double>>> stockData,
             Dictionary<string, List<Tuple<DateTime, double>>> knockoutData)
        {
            try { NormalizeAxisLabelAreas(); } catch { }
            _stockChartManager.Render(stockData);
            _knockoutChartManager.Render(knockoutData);

            // clamp PL history/points to the latest stock timestamp when available so PL doesn't show
            // points to the right of the stocks chart (fixes discrepancy like PL showing 18:10 while stocks max is 18:00)
            try
            {
                DateTime? stocksMax = null;
                if (stockData != null)
                {
                    foreach (var kv in stockData)
                    {
                        try
                        {
                            var list = kv.Value;
                            if (list == null || list.Count == 0) continue;
                            var localMax = list.Max(p => p.Item1);
                            if (stocksMax == null || localMax > stocksMax) stocksMax = localMax;
                        }
                        catch { }
                    }
                }

                // PL graph removed: no trimming required
            }
            catch { }

            if (_dualZoomController.IsFollowMode)
            {
                if (_dualZoomController.HasActiveZoom)
                    _dualZoomController.Apply(stockData, knockoutData);
                else
                {
                    _dualZoomController.Auto();
                    SyncXAxis();
                }
            }

            try { SyncXAxis(); } catch { }

            // Ensure persisted TotalPL history for the current visible range is loaded so top plot shows data immediately
            try { LoadTopPlotHistoryForVisibleRange(); } catch { }

            PlotStocks.Refresh();
            PlotKnockouts.Refresh();
            try { Dispatcher.BeginInvoke(new Action(() => UpdateAllYAxisTextBoxes()), System.Windows.Threading.DispatcherPriority.Background); } catch { }
        }

        public void ApplyZoom(
            Dictionary<string, List<Tuple<DateTime, double>>> stockData,
            Dictionary<string, List<Tuple<DateTime, double>>> knockoutData)
        {
            try { NormalizeAxisLabelAreas(); } catch { }
            _plManualZoomActive = false;
            if (!_dualZoomController.HasActiveZoom) return;
            _dualZoomController.Apply(stockData, knockoutData);
            try { SyncXAxis(); } catch { }
            // keep existing top plot data unchanged
            PlotStocks.Refresh();
            PlotKnockouts.Refresh();
            try { Dispatcher.BeginInvoke(new Action(() => UpdateAllYAxisTextBoxes()), System.Windows.Threading.DispatcherPriority.Background); } catch { }
        }

        private void SyncXAxis()
        {
            double min = PlotStocks.Plot.Axes.Bottom.Min;
            double max = PlotStocks.Plot.Axes.Bottom.Max;
            if (double.IsNaN(min) || double.IsNaN(max) || max <= min)
                return;

            // use stocks axis as source of truth and copy it 1:1 to other charts
            // (do not add padding repeatedly here, otherwise X range drifts over time)
            try
            {
                _syncingStocksXAxis = true;
                PlotKnockouts.Plot.Axes.SetLimitsX(min, max);
                try { PlotStocksTop.Plot.Axes.SetLimitsX(min, max); } catch { }
            }
            finally
            {
                _syncingStocksXAxis = false;
            }

            PlotKnockouts.Refresh();
            try { PlotStocksTop.Refresh(); } catch { }

            try { Dispatcher.BeginInvoke(new Action(() => UpdateAllYAxisTextBoxes()), System.Windows.Threading.DispatcherPriority.Background); } catch { }

            // Ensure we load persisted top-plot history once after the X axis is finalized
            try
            {
                if (!_topPlotHistoryLoaded)
                {
                    _topPlotHistoryLoaded = true;
                    try { LoadTopPlotHistoryForVisibleRange(); } catch { }
                }
            }
            catch { }
        }

        private void SyncStocksXAxisFrom(ScottPlot.WPF.WpfPlot source)
        {
            try
            {
                if (_syncingStocksXAxis) return;
                var min = source.Plot.Axes.Bottom.Min;
                var max = source.Plot.Axes.Bottom.Max;
                if (double.IsNaN(min) || double.IsNaN(max) || max <= min) return;
                _syncingStocksXAxis = true;
                try { PlotStocks.Plot.Axes.SetLimitsX(min, max); } catch { }
                try { PlotStocksTop.Plot.Axes.SetLimitsX(min, max); } catch { }
                try { PlotKnockouts.Plot.Axes.SetLimitsX(min, max); } catch { }
                PlotStocks.Refresh();
                PlotStocksTop.Refresh();
                PlotKnockouts.Refresh();
            }
            catch { }
            finally { _syncingStocksXAxis = false; }
        }

        private void TryOpenGettexForNearestSeries(ChartManager manager, ScottPlot.WPF.WpfPlot plot, System.Windows.Input.MouseButtonEventArgs e)
        {
            try
            {
                var pos = e.GetPosition(plot);
                if (!TryGetMouseCoordinates(plot, pos, out var x, out var y))
                {
                    return;
                }

                // choose best candidate (pixel distance if available)
                // pixelThreshold removed: previously unused

                string bestKey = null;
                double bestDist = double.MaxValue;
                int bestIdx = -1;

                try
                {
                    foreach (var kv in manager.SeriesPoints)
                    {
                        var key = kv.Key;
                        var arr = kv.Value;
                        if (arr.Xs == null || arr.Ys == null || arr.Xs.Length != arr.Ys.Length) continue;

                        double minX = plot.Plot.Axes.Bottom.Min;
                        double maxX = plot.Plot.Axes.Bottom.Max;
                        double minY = plot.Plot.Axes.Left.Min;
                        double maxY = plot.Plot.Axes.Left.Max;
                        double plotW = plot.ActualWidth;
                        double plotH = plot.ActualHeight;
                        bool canPixel = !(double.IsNaN(minX) || double.IsNaN(maxX) || double.IsNaN(minY) || double.IsNaN(maxY) || plotW <= 0 || plotH <= 0);

                        for (int i = 0; i < arr.Xs.Length; i++)
                        {
                            double d;
                            if (canPixel)
                            {
                                double px = (arr.Xs[i] - minX) / (maxX - minX) * plotW;
                                double py = (maxY - arr.Ys[i]) / (maxY - minY) * plotH;
                                double mx = (x - minX) / (maxX - minX) * plotW;
                                double my = (maxY - y) / (maxY - minY) * plotH;
                                var dx = px - mx;
                                var dy = py - my;
                                d = Math.Sqrt(dx * dx + dy * dy);
                            }
                            else
                            {
                                var dx = arr.Xs[i] - x;
                                var dy = arr.Ys[i] - y;
                                d = Math.Sqrt(dx * dx + dy * dy);
                            }

                            if (d < bestDist)
                            {
                                bestDist = d;
                                bestIdx = i;
                                bestKey = key;
                            }
                        }
                    }
                }
                catch { }

                // If we found any candidate, open provider-specific URL (BNP for knockouts when preferred)
                if (bestKey != null)
                {
                    try
                    {
                        // determine displayName if available (prefer holdings / stocks list)
                        string displayName = string.Empty;
                        try
                        {
                            var globalVm = TradeMVVM.Trading.App.MainViewModelInstance ?? Application.Current?.MainWindow?.DataContext as TradeMVVM.Trading.Presentation.ViewModels.MainViewModel;
                            if (globalVm != null)
                            {
                                var found = globalVm.Stocks.Find(s => string.Equals(s.isin_wkn, bestKey, StringComparison.OrdinalIgnoreCase));
                                if (!string.IsNullOrWhiteSpace(found.name))
                                    displayName = found.name;

                                if (string.IsNullOrWhiteSpace(displayName))
                                {
                                    var op = globalVm.OwnedPositions.FirstOrDefault(p => string.Equals(p.isin, bestKey, StringComparison.OrdinalIgnoreCase));
                                    if (op != default && !string.IsNullOrWhiteSpace(op.name))
                                        displayName = op.name;
                                }
                            }
                        }
                        catch { }

                        string prov = null;
                        try
                        {
                            var provider = App.Services?.GetService(typeof(TradeMVVM.Trading.Services.ChartDataProvider)) as TradeMVVM.Trading.Services.ChartDataProvider;
                            if (provider != null)
                                prov = provider.GetPrimaryProviderForName(displayName);
                        }
                        catch { }

                        string url;
                        // prefer BNP for knockouts if provider indicates BNP
                        if (!string.IsNullOrWhiteSpace(prov) && prov.Equals("BNP", StringComparison.OrdinalIgnoreCase))
                            url = $"https://derivate.bnpparibas.com/product-details/{bestKey}/";
                        else
                            url = $"https://www.gettex.de/aktie/{bestKey}/";

                        Process.Start(new ProcessStartInfo { FileName = url, UseShellExecute = true });
                    }
                    catch (Exception ex)
                    {
                        try { Trace.TraceWarning($"Failed to open provider URL for {bestKey}: {ex.Message}"); } catch { }
                    }

                    return;
                }
            }
            catch { }
        }
    }
}
