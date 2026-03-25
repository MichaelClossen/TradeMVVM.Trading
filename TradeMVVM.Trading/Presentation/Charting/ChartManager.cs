using ScottPlot;
using ScottPlot.WPF;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Media;
using System.Globalization;


namespace TradeMVVM.Trading.Chart
{
    // Marker class that can be attached to a WpfPlot.Tag to indicate user-set manual axis limits
    internal class AxisManualLimits
    {
        public bool HasManualY { get; set; }
        public double MinY { get; set; }
        public double MaxY { get; set; }
    }

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

        public ChartManager(WpfPlot plot, string title, bool padYAxisForShortZoom = false)
        {
            _plot = plot;
            LineBrushes = new Dictionary<string, Brush>();
            SeriesByKey = new Dictionary<string, object>();
            _seriesPoints = new Dictionary<string, (double[] Xs, double[] Ys)>();
            _lastData = null;
            _activeZoomSpan = null;
            _padYAxisForShortZoom = padYAxisForShortZoom;

            ConfigureAxes();
        }

        // Create a deterministic brush for a series key so colors remain consistent across renders
        private static SolidColorBrush CreateBrushForKey(string key, int index)
        {
            try
            {
                if (string.IsNullOrEmpty(key))
                    return new SolidColorBrush(System.Windows.Media.Colors.LightGray);

                uint hash = (uint)key.GetHashCode();
                double hue = hash % 360; // 0..359
                double sat = 0.65 + ((hash >> 8) % 35) / 100.0; // 0.65..0.99
                double light = 0.45 + ((hash >> 16) % 20) / 100.0; // 0.45..0.64

                var c = HslToRgb(hue, sat, light);
                return new SolidColorBrush(c);
            }
            catch
            {
                return new SolidColorBrush(System.Windows.Media.Colors.LightGray);
            }
        }

        private static System.Windows.Media.Color HslToRgb(double h, double s, double l)
        {
            // h in [0,360], s and l in [0,1]
            h = (h % 360) / 360.0;
            double r = l, g = l, b = l;
            if (s > 0)
            {
                double q = l < 0.5 ? l * (1 + s) : l + s - l * s;
                double p = 2 * l - q;
                double hk = h;
                double[] t = new double[3];
                t[0] = hk + 1.0 / 3.0;
                t[1] = hk;
                t[2] = hk - 1.0 / 3.0;
                for (int i = 0; i < 3; i++)
                {
                    if (t[i] < 0) t[i] += 1;
                    if (t[i] > 1) t[i] -= 1;
                    if (t[i] < 1.0 / 6.0)
                        t[i] = p + (q - p) * 6 * t[i];
                    else if (t[i] < 1.0 / 2.0)
                        t[i] = q;
                    else if (t[i] < 2.0 / 3.0)
                        t[i] = p + (q - p) * (2.0 / 3.0 - t[i]) * 6;
                    else
                        t[i] = p;
                }
                r = t[0]; g = t[1]; b = t[2];
            }
            byte R = (byte)Math.Round(Math.Min(1.0, Math.Max(0.0, r)) * 255.0);
            byte G = (byte)Math.Round(Math.Min(1.0, Math.Max(0.0, g)) * 255.0);
            byte B = (byte)Math.Round(Math.Min(1.0, Math.Max(0.0, b)) * 255.0);
            return System.Windows.Media.Color.FromRgb(R, G, B);
        }

        // remember last-rendered data so we can re-render with different visual settings when zoom changes
        private Dictionary<string, List<Tuple<DateTime, double>>> _lastData;
        private TimeSpan? _activeZoomSpan;
        // if true, apply extra Y-axis padding when active zoom span is short (used for Knockouts)
        private readonly bool _padYAxisForShortZoom;

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

            // Prefer a DateTimeManual tick generator with 15-minute spacing when available (ScottPlot versions vary).
            try
            {
                Type manualType = null;
                foreach (var a in AppDomain.CurrentDomain.GetAssemblies())
                {
                    try { manualType = a.GetType("ScottPlot.TickGenerators.DateTimeManual"); } catch { }
                    if (manualType != null) break;
                }

                if (manualType != null)
                {
                    object generator = null;
                    try
                    {
                        var ct = manualType.GetConstructors().FirstOrDefault();
                        if (ct != null)
                        {
                            var pars = ct.GetParameters();
                            if (pars.Length == 1 && pars[0].ParameterType == typeof(TimeSpan))
                                generator = Activator.CreateInstance(manualType, TimeSpan.FromMinutes(15));
                            else if (pars.Length == 1 && pars[0].ParameterType == typeof(double))
                                generator = Activator.CreateInstance(manualType, TimeSpan.FromMinutes(15).TotalDays);
                            else
                                generator = Activator.CreateInstance(manualType);
                        }
                    }
                    catch { generator = null; }

                    if (generator != null)
                    {
                        try
                        {
                            var lfProp = manualType.GetProperty("LabelFormatter");
                            if (lfProp != null && lfProp.CanWrite && lfProp.PropertyType == typeof(Func<DateTime, string>))
                                lfProp.SetValue(generator, new Func<DateTime, string>(d => d.ToString("dd.MM HH:mm")));
                        }
                        catch { }

                        try
                        {
                            var bottom = _plot.Plot.Axes.Bottom;
                            var prop = bottom.GetType().GetProperty("TickGenerator");
                            if (prop != null && prop.CanWrite) prop.SetValue(bottom, generator);
                        }
                        catch { }
                    }
                }

                if (_plot.Plot.Axes.Bottom.TickGenerator == null)
                {
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
                }
            }
            catch
            {
                // fallback to automatic
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
            }

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

            // preserve existing brushes so series retain their selected colors across re-renders
            Dictionary<string, Brush> oldBrushes = null;
            try { oldBrushes = new Dictionary<string, Brush>(LineBrushes); } catch { oldBrushes = new Dictionary<string, Brush>(); }
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
                object signalPl = null;
                try
                {
                    int target = (int)Math.Max(2, Math.Floor(_plot.ActualWidth));
                    if (target < 50) target = Math.Min(50, xs.Length);
                    // when ForceScatter is requested, avoid automatic downsampling so all DB samples
                    // within the visible range are rendered (prevents aggressive decimation)
                    if (ForceScatter)
                        target = int.MaxValue / 4;

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
                        double[] signalXsForMarkers = xs;
                        if (xs.Length > target)
                        {
                            int step = (int)Math.Ceiling((double)xs.Length / target);
                            var decYs = new List<double>();
                            var decXs = new List<double>();
                            for (int i = 0; i < ys.Length; i += step)
                            {
                                decYs.Add(ys[i]);
                                decXs.Add(xs[i]);
                            }
                            plotYs = decYs.ToArray();
                            signalXsForMarkers = decXs.ToArray();
                            // adjust sample rate because we skipped samples
                            sampleRate = sampleRate / step;
                        }

                        // use Signal plot and configure sample rate / offset via reflection (API varies by ScottPlot version)
                        var pl = _plot.Plot.Add.Signal(plotYs);
                        signalPl = pl;
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

                        // Create an overlay scatter to render markers (Signal doesn't support markers)
                        try
                        {
                            var overlayXs = signalXsForMarkers;
                            var overlayYs = plotYs;
                            var overlay = _plot.Plot.Add.Scatter(overlayXs, overlayYs);
                            // ensure overlay shows markers only (no connecting line) by setting line width to 0
                            try
                            {
                                var stOverlay = overlay.GetType();
                                var lineNamesOverlay = new[] { "LineWidth", "lineWidth", "Width" };
                                foreach (var n2 in lineNamesOverlay)
                                {
                                    try
                                    {
                                        var p2 = stOverlay.GetProperty(n2);
                                        if (p2 != null && p2.CanWrite)
                                        {
                                            if (p2.PropertyType == typeof(double) || p2.PropertyType == typeof(float))
                                                p2.SetValue(overlay, Convert.ChangeType(0.0, p2.PropertyType));
                                            else if (p2.PropertyType == typeof(int))
                                                p2.SetValue(overlay, Convert.ChangeType(0, p2.PropertyType));
                                            break;
                                        }
                                    }
                                    catch { }
                                }

                                // set marker size and shape on overlay
                                int overlayCount = overlayXs != null ? overlayXs.Length : 0;
                                var markerNamesOv = new[] { "MarkerSize", "markerSize", "MarkerSizeF", "MarkerSizePx" };
                                foreach (var mn in markerNamesOv)
                                {
                                    try
                                    {
                                        var mp = stOverlay.GetProperty(mn);
                                        if (mp != null && mp.CanWrite)
                                        {
                                            if (overlayCount <= 1)
                                            {
                                                if (mp.PropertyType == typeof(double) || mp.PropertyType == typeof(float))
                                                    mp.SetValue(overlay, Convert.ChangeType(8.0, mp.PropertyType));
                                                else if (mp.PropertyType == typeof(int))
                                                    mp.SetValue(overlay, Convert.ChangeType(8, mp.PropertyType));
                                            }
                                            else
                                            {
                                                if (mp.PropertyType == typeof(double) || mp.PropertyType == typeof(float))
                                                    mp.SetValue(overlay, Convert.ChangeType(6.0, mp.PropertyType));
                                                else if (mp.PropertyType == typeof(int))
                                                    mp.SetValue(overlay, Convert.ChangeType(6, mp.PropertyType));
                                            }
                                            break;
                                        }
                                    }
                                    catch { }
                                }

                                var mpropOv = stOverlay.GetProperty("MarkerShape") ?? stOverlay.GetProperty("Marker");
                                if (mpropOv != null && mpropOv.CanWrite)
                                {
                                    try
                                    {
                                        var t = mpropOv.PropertyType;
                                        if (t.IsEnum)
                                        {
                                            var names = Enum.GetNames(t);
                                            var prefer = names.FirstOrDefault(nm => nm.IndexOf("Circle", StringComparison.OrdinalIgnoreCase) >= 0)
                                                        ?? names.FirstOrDefault(nm => nm.IndexOf("Dot", StringComparison.OrdinalIgnoreCase) >= 0)
                                                        ?? names.FirstOrDefault();
                                            if (!string.IsNullOrEmpty(prefer))
                                            {
                                                var val = Enum.Parse(t, prefer);
                                                mpropOv.SetValue(overlay, val);
                                            }
                                        }
                                    }
                                    catch { }
                                }
                            }
                            catch { }
                            scatter = overlay;

                            // ensure the signal plottable draws a visible line by setting its LineWidth
                            try
                            {
                                var stSig = pl.GetType();
                                var lineNamesSig = new[] { "LineWidth", "lineWidth", "Width" };
                                foreach (var n3 in lineNamesSig)
                                {
                                    try
                                    {
                                        var p3 = stSig.GetProperty(n3);
                                        if (p3 != null && p3.CanWrite)
                                        {
                                            if (p3.PropertyType == typeof(double) || p3.PropertyType == typeof(float))
                                                p3.SetValue(pl, Convert.ChangeType(1.5, p3.PropertyType));
                                            else if (p3.PropertyType == typeof(int))
                                                p3.SetValue(pl, Convert.ChangeType(1, p3.PropertyType));
                                            break;
                                        }
                                    }
                                    catch { }
                                }
                            }
                            catch { }

                            // also attempt to apply color to the Signal plottable so both line and markers match
                            try
                            {
                                var stSignal = pl.GetType();
                                var propColorSig = stSignal.GetProperty("Color");
                                if (propColorSig != null && propColorSig.CanWrite)
                                {
                                    // actual color application happens later when brush is known; mark both plottables by storing temporary mapping
                                }
                            }
                            catch { }
                        }
                        catch
                        {
                            // if overlay creation failed, fall back to using the Signal plottable for interaction
                            scatter = pl;
                        }
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

                // try to enforce line drawing (connect points) and hide markers so curves appear as continuous lines
                try
                {
                    if (scatter != null)
                    {
                        int plottedCount = 0;
                        try { plottedCount = (plotXs != null ? plotXs.Length : (plotYs != null ? plotYs.Length : 0)); } catch { plottedCount = 0; }
                        var st = scatter.GetType();

                        // set LineWidth if available
                        var lineNames = new[] { "LineWidth", "lineWidth", "Width" };
                        foreach (var n in lineNames)
                        {
                            try
                            {
                                var p = st.GetProperty(n);
                                if (p != null && p.CanWrite)
                                {
                                    if (p.PropertyType == typeof(double) || p.PropertyType == typeof(float))
                                        p.SetValue(scatter, Convert.ChangeType(1.5, p.PropertyType));
                                    else if (p.PropertyType == typeof(int))
                                        p.SetValue(scatter, Convert.ChangeType(1, p.PropertyType));
                                    break;
                                }
                            }
                            catch { }
                        }

                        // set MarkerSize to visible values so both points and lines are shown
                        var markerNames = new[] { "MarkerSize", "markerSize", "MarkerSizeF", "MarkerSizePx" };
                        foreach (var n in markerNames)
                        {
                            try
                            {
                                var p = st.GetProperty(n);
                                if (p != null && p.CanWrite)
                                {
                                    // show a larger marker when only a single point is plotted, otherwise small markers
                                    if (plottedCount <= 1)
                                    {
                                        if (p.PropertyType == typeof(double) || p.PropertyType == typeof(float))
                                            p.SetValue(scatter, Convert.ChangeType(8.0, p.PropertyType));
                                        else if (p.PropertyType == typeof(int))
                                            p.SetValue(scatter, Convert.ChangeType(8, p.PropertyType));
                                    }
                                    else
                                    {
                                        // make markers more visible for multi-point series
                                        if (p.PropertyType == typeof(double) || p.PropertyType == typeof(float))
                                            p.SetValue(scatter, Convert.ChangeType(6.0, p.PropertyType));
                                        else if (p.PropertyType == typeof(int))
                                            p.SetValue(scatter, Convert.ChangeType(6, p.PropertyType));
                                    }
                                    break;
                                }
                            }
                            catch { }
                        }

                        // prefer a visible marker shape (e.g. Circle) when the plottable supports a marker enum
                        try
                        {
                            var mprop = st.GetProperty("MarkerShape") ?? st.GetProperty("Marker");
                            if (mprop != null && mprop.CanWrite)
                            {
                                var t = mprop.PropertyType;
                                if (t.IsEnum)
                                {
                                    var names = Enum.GetNames(t);
                                    var prefer = names.FirstOrDefault(nm => nm.IndexOf("Circle", StringComparison.OrdinalIgnoreCase) >= 0)
                                                ?? names.FirstOrDefault(nm => nm.IndexOf("Dot", StringComparison.OrdinalIgnoreCase) >= 0)
                                                ?? names.FirstOrDefault();
                                    if (!string.IsNullOrEmpty(prefer))
                                    {
                                        var val = Enum.Parse(t, prefer);
                                        mprop.SetValue(scatter, val);
                                    }
                                }
                            }
                        }
                        catch { }
                    }
                }
                catch { }

                // store the plottable so callers (UI) can query nearest points on mouse events
                try
                {
                    SeriesByKey[kvp.Key] = scatter;
                }
                catch { }

                // determine brush to use: reuse old brush when available, otherwise create deterministic color per-key
                Brush brush = null;
                try
                {
                    if (oldBrushes != null && oldBrushes.TryGetValue(kvp.Key, out var old) && old != null)
                        brush = old;
                    else
                        brush = CreateBrushForKey(kvp.Key, LineBrushes.Count);



                    LineBrushes[kvp.Key] = brush ?? new SolidColorBrush(System.Windows.Media.Colors.LightGray);

                    // Apply brush color to the scatter/signal plottable if possible
                    try
                    {
                        if (brush is SolidColorBrush scb)
                        {
                            var wm = scb.Color;
                            // apply to overlay/primary scatter if present
                            if (scatter != null)
                            {
                                try
                                {
                                    var st = scatter.GetType();
                                    var propColor = st.GetProperty("Color");
                                    if (propColor != null && propColor.CanWrite)
                                    {
                                        var pt = propColor.PropertyType;
                                        if (pt == typeof(System.Drawing.Color))
                                        {
                                            var dc = System.Drawing.Color.FromArgb(wm.A, wm.R, wm.G, wm.B);
                                            propColor.SetValue(scatter, dc);
                                        }
                                        else if (pt == typeof(System.Windows.Media.Color))
                                        {
                                            propColor.SetValue(scatter, wm);
                                        }
                                    }
                                }
                                catch { }
                            }

                            // also apply to underlying signal plottable if created
                            if (signalPl != null)
                            {
                                try
                                {
                                    var stSig = signalPl.GetType();
                                    var propColorSig = stSig.GetProperty("Color");
                                    if (propColorSig != null && propColorSig.CanWrite)
                                    {
                                        var pt2 = propColorSig.PropertyType;
                                        if (pt2 == typeof(System.Drawing.Color))
                                        {
                                            var dc2 = System.Drawing.Color.FromArgb(wm.A, wm.R, wm.G, wm.B);
                                            propColorSig.SetValue(signalPl, dc2);
                                        }
                                        else if (pt2 == typeof(System.Windows.Media.Color))
                                        {
                                            propColorSig.SetValue(signalPl, wm);
                                        }
                                    }
                                }
                                catch { }
                            }
                        }
                    }
                    catch { }
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

            // If the plot has manual Y limits set by the user, respect them and skip auto adjustments
            try
            {
                var manual = _plot.Tag as AxisManualLimits;
                if (manual != null && manual.HasManualY)
                {
                    try { _plot.Plot.Axes.SetLimitsY(manual.MinY, manual.MaxY); } catch { }
                    try { _plot.Refresh(); } catch { }
                    return;
                }
            }
            catch { }

            // Attempt to nudge Y axis limits when a short active zoom span is set
            // This helps e.g. the Knockouts chart in 1h view to have a more readable vertical spread.
            try
            {
                if (_padYAxisForShortZoom && _activeZoomSpan.HasValue && _activeZoomSpan.Value <= TimeSpan.FromHours(1))
                {
                    try
                    {
                        // gather valid Y values across all series
                        var allYs = new List<double>();
                        foreach (var kv in _seriesPoints)
                        {
                            var arr = kv.Value;
                            if (arr.Ys == null) continue;
                            for (int i = 0; i < arr.Ys.Length; i++)
                            {
                                var v = arr.Ys[i];
                                if (!double.IsNaN(v) && !double.IsInfinity(v))
                                    allYs.Add(v);
                            }
                        }

                        if (allYs.Count > 0)
                        {
                            // set Y axis exactly to the data min/max within the current interval
                            double minY = allYs.Min();
                            double maxY = allYs.Max();

                            if (!double.IsNaN(minY) && !double.IsNaN(maxY))
                            {
                                if (Math.Abs(maxY - minY) < double.Epsilon)
                                {
                                    // avoid zero-span by adding a very small pad for single-value series
                                    double tinyPad = Math.Max(1e-6, Math.Abs(maxY) * 1e-6);
                                    _plot.Plot.Axes.SetLimitsY(minY - tinyPad, maxY + tinyPad);
                                }
                                else
                                {
                                    // apply a fixed 2 percentage-point padding (user request):
                                    // yMax = maxData + 2, yMin = minData - 2
                                    double padAbs = 2.0;
                                    _plot.Plot.Axes.SetLimitsY(minY - padAbs, maxY + padAbs);
                                }
                            }
                        }
                    }
                    catch { }
                }

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
