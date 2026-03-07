using ScottPlot.WPF;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TradeMVVM.Trading.Chart
{
    public class ChartZoomController
    {
        private readonly WpfPlot _plot;
        private TimeSpan? _activeZoom;
        private bool _followMode = true;

        public bool HasActiveZoom => _activeZoom != null;
        public bool IsFollowMode => _followMode;

        public ChartZoomController(WpfPlot plot)
        {
            _plot = plot;
        }

        public void SetZoom(TimeSpan span)
        {
            _activeZoom = span;
            _followMode = true;
        }

        public void DisableFollow()
        {
            _followMode = false;
        }

        public void Auto()
        {
            _activeZoom = null;
            _followMode = false;

            _plot.Plot.Axes.AutoScale();

            // add a small right padding (5% of visible X-range) so the latest point is not at the border
            try
            {
                double min = _plot.Plot.Axes.Bottom.Min;
                double max = _plot.Plot.Axes.Bottom.Max;
                if (!double.IsNaN(min) && !double.IsNaN(max) && max > min)
                {
                    double span = max - min;
                    double pad = span * 0.05; // 5% right padding
                    _plot.Plot.Axes.SetLimitsX(min, max + pad);
                }
            }
            catch { }

            _plot.Refresh();
        }


        private TimeSpan GetRightPadding(TimeSpan zoom)
        {
            if (zoom.TotalMinutes <= 5)
                return TimeSpan.FromSeconds(5);

            if (zoom.TotalHours <= 1)
                return TimeSpan.FromMinutes(2);

            if (zoom.TotalDays <= 1)
                return TimeSpan.FromMinutes(10);

            if (zoom.TotalDays <= 7)
                return TimeSpan.FromHours(2);

            if (zoom.TotalDays <= 30)
                return TimeSpan.FromDays(1);

            return TimeSpan.FromDays(3);
        }

        public void Apply(
     Dictionary<string, List<Tuple<DateTime, double>>> data,
     DateTime? referenceMaxTime = null)
        {
            if (!_followMode || _activeZoom == null)
                return;

            if (data.Count == 0)
                return;

            var allPoints = data.SelectMany(x => x.Value).ToList();
            if (allPoints.Count == 0)
                return;

            DateTime maxTime = referenceMaxTime ?? allPoints.Max(p => p.Item1);

                if (_activeZoom != null)
                {
                    DateTime minTime = maxTime - _activeZoom.Value;

                    // compute a small right padding (5% of the time window) so latest point isn't at the plot border
                    var span = maxTime - minTime;
                    var pad = TimeSpan.FromTicks((long)(span.Ticks * 0.05));

                    _plot.Plot.Axes.SetLimitsX(
                        minTime.ToOADate(),
                        (maxTime + pad).ToOADate());
                // compute Y limits from visible percent values (values are already percent)
                var visiblePercents = new List<double>();

                foreach (var series in data)
                {
                    var seriesValues = series.Value;
                    if (seriesValues == null || seriesValues.Count == 0)
                        continue;

                    // collect visible points for this series (use percent values directly)
                    var pointsInWindow = seriesValues.Where(p => p.Item1 >= minTime && p.Item1 <= maxTime).ToList();
                    if (pointsInWindow.Count == 0)
                        pointsInWindow = seriesValues;

                    foreach (var p in pointsInWindow)
                    {
                        var val = p.Item2;
                        if (double.IsNaN(val) || double.IsInfinity(val))
                            continue;

                        visiblePercents.Add(val);
                    }
                }

                if (visiblePercents.Count == 0)
                {
                    _plot.Plot.Axes.AutoScale();
                }
                else
                {
                    double yMin = visiblePercents.Min();
                    double yMax = visiblePercents.Max();
                    double range = yMax - yMin;
                    if (range <= double.Epsilon)
                    {
                        yMin -= 1.0;
                        yMax += 1.0;
                    }
                    else
                    {
                        double yPad = range * 0.1;
                        yMin -= yPad;
                        yMax += yPad;
                    }

                    _plot.Plot.Axes.SetLimitsY(yMin, yMax);
                }
            }
            else
            {
                _plot.Plot.Axes.AutoScale();
            }
            _plot.Refresh();
        }



    }
}
