using ScottPlot;
using ScottPlot.WPF;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Media;
using System.Globalization;


namespace TradeMVVM.Trading.Chart
{
    public class ChartManager
    {
        // When true, always use Scatter (explicit Xs) even for uniformly sampled data.
        public bool ForceScatter { get; set; } = false;
        private readonly WpfPlot _plot;

        // keep recent series raw point arrays for reliable nearest-point lookup
        private readonly Dictionary<string, (double[] Xs, double[] Ys)> _seriesPoints;

        public Dictionary<string, Brush> LineBrushes
        { get; private set; }

        // keep a reference to the plottable (scatter) for each series so callers can query nearest points
        public Dictionary<string, object> SeriesByKey { get; private set; }
        public Dictionary<string, (double[] Xs, double[] Ys)> SeriesPoints => _seriesPoints;

        public ChartManager(WpfPlot plot, string title)
        {
            _plot = plot;
            LineBrushes = new Dictionary<string, Brush>();
            SeriesByKey = new Dictionary<string, object>();
            _seriesPoints = new Dictionary<string, (double[] Xs, double[] Ys)>();
            _lastData = null;
            _activeZoomSpan = null;

            ConfigureAxes();
        }

        // remember last-rendered data so we can re-render with different visual settings when zoom changes
        private Dictionary<string, List<Tuple<DateTime, double>>> _lastData;
        private TimeSpan? _activeZoomSpan;

        public void SetActiveZoom(TimeSpan? span)
        {
            try
            {
                _activeZoomSpan = span;
                if (_lastData != null)
                {
                    // re-render with current data and adjusted visuals
                    Render(_lastData);
                }
            }
            catch { }
        }

        // Downsample to a representative point per bucket. Keeps first and last points.
        // Simple bucket-representative algorithm: split interior points into buckets and
        // pick the point with maximum absolute deviation from the bucket mean (preserves peaks).
        private (double[] Xs, double[] Ys) DownsampleToBucketRepresentative(double[] xs, double[] ys, int target)
        {
            try
            {
                if (xs == null || ys == null) return (xs, ys);
                int len = Math.Min(xs.Length, ys.Length);
                if (len <= 0) return (xs, ys);
                if (len <= target || target < 3)
                    return (xs, ys);

                var outXs = new List<double>();
                var outYs = new List<double>();

                // always keep first point
                outXs.Add(xs[0]);
                outYs.Add(ys[0]);

                int interior = len - 2; // excluding first and last
                int buckets = target - 2;
                if (buckets <= 0)
                {
                    // keep first and last only
                    outXs.Add(xs[len - 1]);
                    outYs.Add(ys[len - 1]);
                    return (outXs.ToArray(), outYs.ToArray());
                }

                for (int b = 0; b < buckets; b++)
                {
                    int start = 1 + (int)Math.Floor((double)b * interior / buckets);
                    int end = 1 + (int)Math.Floor((double)(b + 1) * interior / buckets) - 1;
                    if (start < 1) start = 1;
                    if (end < start) end = start;
                    if (end > len - 2) end = len - 2;

                    // compute bucket mean
                    double sum = 0; int count = 0;
                    for (int i = start; i <= end; i++)
                    {
                        double v = ys[i];
                        if (double.IsNaN(v) || double.IsInfinity(v)) continue;
                        sum += v; count++;
                    }
                    double mean = count > 0 ? sum / count : 0.0;

                    // choose representative: max abs deviation from mean
                    int bestIdx = start;
                    double bestScore = -1.0;
                    for (int i = start; i <= end; i++)
                    {
                        double v = ys[i];
                        if (double.IsNaN(v) || double.IsInfinity(v)) continue;
                        double score = Math.Abs(v - mean);
                        if (score > bestScore)
                        {
                            bestScore = score;
                            bestIdx = i;
                        }
                    }

                    outXs.Add(xs[bestIdx]);
                    outYs.Add(ys[bestIdx]);
                }

                // always keep last point
                outXs.Add(xs[len - 1]);
                outYs.Add(ys[len - 1]);

                return (outXs.ToArray(), outYs.ToArray());
            }
            catch
            {
                return (xs, ys);
            }
        }


        private void ConfigureAxes()
        {
            _plot.Plot.Axes.DateTimeTicksBottom();

            _plot.Plot.Axes.Bottom.TickGenerator =
                new ScottPlot.TickGenerators.DateTimeAutomatic()
                {
                    LabelFormatter = (DateTime date) =>
                    {
                        double min = _plot.Plot.Axes.Bottom.Min;
                        double max = _plot.Plot.Axes.Bottom.Max;

                        TimeSpan visible =
                            TimeSpan.FromDays(max - min);

                        if (visible.TotalMinutes <= 10)
                            return date.ToString("HH:mm:ss");

                        if (visible.TotalDays <= 1)
                            return date.ToString("HH:mm");

                        return date.ToString("dd.MM HH:mm");
                    }
                };

            _plot.Plot.Axes.Left.Label.Text = "%";
            // Note: Custom numeric tick formatting removed due to ScottPlot API changes
            // The default tick generator will be used instead
        }


        public void Render(
      Dictionary<string, List<Tuple<DateTime, double>>> data)
        {
            // keep last data snapshot for possible re-render when zoom/visual settings change
            try { _lastData = data; } catch { }
            // clear existing plottables to avoid old scales influencing axis autoscaling
            try
            {
                var all = _plot.Plot.GetPlottables().ToArray();
                foreach (var p in all)
                    _plot.Plot.Remove(p);
            }
            catch { }

            try { SeriesByKey.Clear(); } catch { }
            try { LineBrushes.Clear(); } catch { }
            try { _seriesPoints.Clear(); } catch { }

            foreach (var kvp in data)
            {
                var values = kvp.Value;
                if (values == null || values.Count == 0)
                    continue;

                // Ensure points are ordered by increasing time so the X axis is not inverted
                var ordered = values.OrderBy(v => v.Item1).ToList();

                double[] xs = ordered.Select(v => v.Item1.ToOADate()).ToArray();

                // values currently hold percent values (provided by ChartDataProvider / DB)
                var percents = ordered.Select(v => v.Item2).ToArray();

                double[] ys = percents.Select(p => (!double.IsNaN(p) && !double.IsInfinity(p)) ? p : double.NaN).ToArray();

                // store raw point arrays so nearest-point lookup can fall back to these
                try
                {
                    _seriesPoints[kvp.Key] = (xs, ys);
                }
                catch { }

                // downsample points for plotting based on plot pixel width to reduce rendering cost
                var plotXs = xs;
                var plotYs = ys;
                object scatter = null;
                try
                {
                    int target = (int)Math.Max(2, Math.Floor(_plot.ActualWidth));
                    if (target < 50) target = Math.Min(50, xs.Length);

                    // detect roughly uniform spacing in X (equal time steps)
                    bool isUniform = false;
                    double medianDelta = 0.0;
                    if (xs.Length >= 3)
                    {
                        var deltas = new double[xs.Length - 1];
                        for (int i = 0; i < deltas.Length; i++) deltas[i] = xs[i + 1] - xs[i];
                        var sorted = deltas.OrderBy(d => d).ToArray();
                        medianDelta = sorted[sorted.Length / 2];
                        if (!double.IsNaN(medianDelta) && Math.Abs(medianDelta) > double.Epsilon)
                        {
                            isUniform = true;
                            foreach (var d in deltas)
                            {
                                if (double.IsNaN(d) || double.IsInfinity(d) || Math.Abs((d - medianDelta) / medianDelta) > 1e-6)
                                {
                                    isUniform = false; break;
                                }
                            }
                        }
                    }

                    if (isUniform && !ForceScatter)
                    {
                        // use Signal plot which is much faster for uniformly sampled data
                        double sampleRate = 1.0 / medianDelta; // samples per OADate unit (days)
                        double xOffset = xs.Length > 0 ? xs[0] : 0.0;

                        // decimate if too many points
                        if (xs.Length > target)
                        {
                            int step = (int)Math.Ceiling((double)xs.Length / target);
                            var decYs = new List<double>();
                            for (int i = 0; i < ys.Length; i += step)
                                decYs.Add(ys[i]);
                            plotYs = decYs.ToArray();
                            // adjust sample rate because we skipped samples
                            sampleRate = sampleRate / step;
                        }

                        // use Signal plot and configure sample rate / offset via reflection (API varies by ScottPlot version)
                        var pl = _plot.Plot.Add.Signal(plotYs);
                        try
                        {
                            var t = pl.GetType();
                            var propSample = t.GetProperty("SampleRate") ?? t.GetProperty("sampleRate");
                            if (propSample != null && propSample.CanWrite)
                                propSample.SetValue(pl, sampleRate);

                            var offsetProps = new[] { "XOffset", "OffsetX", "Offset" };
                            foreach (var name in offsetProps)
                            {
                                var p = t.GetProperty(name);
                                if (p != null && p.CanWrite)
                                {
                                    p.SetValue(pl, xOffset);
                                    break;
                                }
                            }
                        }
                        catch { }
                        scatter = pl;
                    }
                    else
                    {
                        // non-uniform: fall back to bucket downsampling + scatter
                        if (xs.Length > target)
                        {
                            var ds = DownsampleToBucketRepresentative(xs, ys, target);
                            plotXs = ds.Xs;
                            plotYs = ds.Ys;
                        }
                        scatter = _plot.Plot.Add.Scatter(plotXs, plotYs);
                    }
                }
                catch
                {
                    // fallback: plain scatter
                    scatter = _plot.Plot.Add.Scatter(plotXs, plotYs);
                }
                // set legend/label text via reflection to support different plottable types (Signal/Scatter)
                try
                {
                    if (scatter != null)
                    {
                        var st = scatter.GetType();
                        var prop = st.GetProperty("LegendText") ?? st.GetProperty("Label") ?? st.GetProperty("LegendLabel");
                        if (prop != null && prop.CanWrite && prop.PropertyType == typeof(string))
                            prop.SetValue(scatter, kvp.Key);
                        else
                        {
                            // try method SetLabel(string)
                            var mi = st.GetMethod("SetLabel") ?? st.GetMethod("SetLegend") ?? st.GetMethod("Label");
                            if (mi != null)
                            {
                                try { mi.Invoke(scatter, new object[] { kvp.Key }); } catch { }
                            }
                        }
                    }
                }
                catch { }

                // store the plottable so callers (UI) can query nearest points on mouse events
                try
                {
                    SeriesByKey[kvp.Key] = scatter;
                }
                catch { }

                // store color for legend use (use reflection because plottable type may vary)
                try
                {
                    var brush = new SolidColorBrush(System.Windows.Media.Colors.LightGray);
                    if (scatter != null)
                    {
                        var st2 = scatter.GetType();
                        var propColor = st2.GetProperty("Color");
                        if (propColor != null)
                        {
                            var val = propColor.GetValue(scatter);
                            if (val is System.Drawing.Color sd)
                                brush = new SolidColorBrush(System.Windows.Media.Color.FromRgb(sd.R, sd.G, sd.B));
                            else if (val is System.Windows.Media.Color wm)
                                brush = new SolidColorBrush(wm);
                        }
                    }
                    LineBrushes[kvp.Key] = brush;
                }
                catch
                {
                    try { LineBrushes[kvp.Key] = new SolidColorBrush(System.Windows.Media.Colors.LightGray); } catch { }
                }

                _plot.Plot.Legend.IsVisible = false;
                _plot.Plot.Legend.Alignment = Alignment.LowerLeft;

                _plot.Plot.Legend.BackgroundColor = ScottPlot.Colors.Black.WithAlpha(.6);
                _plot.Plot.Legend.FontColor = ScottPlot.Colors.White;
            }

            // refresh WPF plot (this will auto-scale axes based on current plottables)
            try
            {
                // Use Refresh instead of full reinitialization to reduce flicker
                _plot.Refresh();
            }
            catch { }
        }

        // Attempts to find the nearest point on the series identified by `key` to the provided
        // data coordinates (x,y). Returns true if a candidate was found. `index` will contain
        // the point index (if available) and `distance` will contain the reported distance
        // (if available) or the computed euclidean distance in data coordinates as fallback.
        public bool TryGetNearestPoint(string key, double x, double y, out int index, out double distance)
        {
            index = -1;
            distance = double.MaxValue;

            if (key == null) return false;
            if (!SeriesByKey.TryGetValue(key, out var plottable) || plottable == null)
                return false;

            try
            {
                // attempt direct call to common API name
                var mi = plottable.GetType().GetMethod("GetPointNearest", new Type[] { typeof(double), typeof(double) });
                object res = null;
                if (mi != null)
                {
                    res = mi.Invoke(plottable, new object[] { x, y });
                }
                else
                {
                    // attempt to find any method with "Nearest" in the name accepting two parameters
                    var candidates = plottable.GetType().GetMethods().Where(m => (m.Name.IndexOf("Nearest", StringComparison.OrdinalIgnoreCase) >= 0 || m.Name.IndexOf("NearestPoint", StringComparison.OrdinalIgnoreCase) >= 0) && m.GetParameters().Length == 2).ToList();
                    foreach (var c in candidates)
                    {
                        try
                        {
                            // try invoking with double,double
                            res = c.Invoke(plottable, new object[] { x, y });
                            if (res != null)
                                break;
                        }
                        catch { }
                    }
                }

                if (res != null)
                {
                    var t = res.GetType();
                    // try tuple-style accessors Item1..Item4
                    var pItem1 = t.GetProperty("Item1");
                    var pItem2 = t.GetProperty("Item2");
                    var pItem3 = t.GetProperty("Item3");
                    var pItem4 = t.GetProperty("Item4");

                    try
                    {
                        if (pItem4 != null)
                        {
                            // common signature: (double x, double y, double distance, int index)
                            if (pItem3 != null)
                                distance = Convert.ToDouble(pItem3.GetValue(res));
                            index = Convert.ToInt32(pItem4.GetValue(res));
                            return true;
                        }

                        if (pItem3 != null)
                        {
                            // possible signatures: (double x, double y, int index) or (double x, double y, double distance)
                            var val3 = pItem3.GetValue(res);
                            if (val3 is int)
                            {
                                index = (int)val3;
                                distance = 0;
                                return true;
                            }
                            distance = Convert.ToDouble(val3);
                            // no index available but distance available
                            return true;
                        }
                    }
                    catch { }
                }
            }
            catch { }

            // fallback: try to use stored series point arrays for reliable nearest-point computation
            try
            {
                if (_seriesPoints.TryGetValue(key, out var arr) && arr.Xs != null && arr.Ys != null && arr.Xs.Length == arr.Ys.Length)
                {
                    var xs = arr.Xs;
                    var ys = arr.Ys;

                    double best = double.MaxValue;
                    int bestIdx = -1;

                    double minX = double.NaN, maxX = double.NaN, minY = double.NaN, maxY = double.NaN;
                    double plotW = 0, plotH = 0;
                    try
                    {
                        minX = _plot.Plot.Axes.Bottom.Min;
                        maxX = _plot.Plot.Axes.Bottom.Max;
                        minY = _plot.Plot.Axes.Left.Min;
                        maxY = _plot.Plot.Axes.Left.Max;
                        plotW = _plot.ActualWidth;
                        plotH = _plot.ActualHeight;
                    }
                    catch { }

                    bool canPixel = !(double.IsNaN(minX) || double.IsNaN(maxX) || double.IsNaN(minY) || double.IsNaN(maxY) || plotW <= 0 || plotH <= 0 || Math.Abs(maxX - minX) < double.Epsilon || Math.Abs(maxY - minY) < double.Epsilon);

                    for (int i = 0; i < xs.Length; i++)
                    {
                        double d;
                        if (canPixel)
                        {
                            double px = (xs[i] - minX) / (maxX - minX) * plotW;
                            double py = (maxY - ys[i]) / (maxY - minY) * plotH; // invert Y

                            var dx = px - ((x - minX) / (maxX - minX) * plotW);
                            var dy = py - ((maxY - y) / (maxY - minY) * plotH);
                            d = Math.Sqrt(dx * dx + dy * dy);
                        }
                        else
                        {
                            var dx = xs[i] - x;
                            var dy = ys[i] - y;
                            d = Math.Sqrt(dx * dx + dy * dy);
                        }

                        if (d < best)
                        {
                            best = d;
                            bestIdx = i;
                        }
                    }

                    if (bestIdx >= 0)
                    {
                        index = bestIdx;
                        distance = best;
                        return true;
                    }
                }
            }
            catch { }

            return false;
        }


    }
}
