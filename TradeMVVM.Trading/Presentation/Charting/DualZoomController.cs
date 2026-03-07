using System;
using System.Collections.Generic;
using System.Linq;
using TradeMVVM.Trading.Chart;

namespace TradeMVVM.Trading.Chart
{

    public class DualZoomController
    {
        private readonly ChartZoomController _stocksZoom;
        private readonly ChartZoomController _knockoutsZoom;
        private TimeSpan? _activeZoom = null;

        public bool HasActiveZoom => _activeZoom != null;
        public bool IsFollowMode => _stocksZoom.IsFollowMode || _knockoutsZoom.IsFollowMode;

        public DualZoomController(
            ChartZoomController stocks,
            ChartZoomController knockouts)
        {
            _stocksZoom = stocks;
            _knockoutsZoom = knockouts;
        }

        // Event fired when zoom changes. If TimeSpan? is null it indicates Auto (no fixed zoom).
        public event Action<TimeSpan?> ZoomChanged;

        public void SetZoom(TimeSpan span)
        {
            _activeZoom = span;
            try { _stocksZoom.SetZoom(span); } catch (Exception ex) { try { System.Diagnostics.Trace.TraceWarning($"DualZoomController: SetZoom stocks failed: {ex.Message}"); } catch { } }
            try { _knockoutsZoom.SetZoom(span); } catch (Exception ex) { try { System.Diagnostics.Trace.TraceWarning($"DualZoomController: SetZoom knockouts failed: {ex.Message}"); } catch { } }
            try { ZoomChanged?.Invoke(span); } catch { }
        }


        public void Apply(
    Dictionary<string, List<Tuple<DateTime, double>>> stocksData,
    Dictionary<string, List<Tuple<DateTime, double>>> knockoutsData)
        {
            // Determine a common reference max time (right edge) using the union of both datasets
            DateTime? stocksMax = null;
            DateTime? knockoutsMax = null;

            if (stocksData != null && stocksData.Count > 0)
            {
                var all = stocksData.SelectMany(x => x.Value).ToList();
                if (all.Count > 0)
                    stocksMax = all.Max(p => p.Item1);
            }

            if (knockoutsData != null && knockoutsData.Count > 0)
            {
                var all = knockoutsData.SelectMany(x => x.Value).ToList();
                if (all.Count > 0)
                    knockoutsMax = all.Max(p => p.Item1);
            }

            // choose the latest maxTime as reference so both plots show the same X-range end
            DateTime? referenceMax = null;
            if (stocksMax != null && knockoutsMax != null)
                referenceMax = stocksMax > knockoutsMax ? stocksMax : knockoutsMax;
            else if (stocksMax != null)
                referenceMax = stocksMax;
            else if (knockoutsMax != null)
                referenceMax = knockoutsMax;

            try { _stocksZoom.Apply(stocksData, referenceMax); } catch (Exception ex) { try { System.Diagnostics.Trace.TraceWarning($"DualZoomController: Apply stocks failed: {ex.Message}"); } catch { } }
            try { _knockoutsZoom.Apply(knockoutsData, referenceMax); } catch (Exception ex) { try { System.Diagnostics.Trace.TraceWarning($"DualZoomController: Apply knockouts failed: {ex.Message}"); } catch { } }
            try { ZoomChanged?.Invoke(_activeZoom); } catch { }
        }

        public void DisableFollow()
        {
            _activeZoom = null;
            try { _stocksZoom.DisableFollow(); } catch (Exception ex) { try { System.Diagnostics.Trace.TraceWarning($"DualZoomController: DisableFollow stocks failed: {ex.Message}"); } catch { } }
            try { _knockoutsZoom.DisableFollow(); } catch (Exception ex) { try { System.Diagnostics.Trace.TraceWarning($"DualZoomController: DisableFollow knockouts failed: {ex.Message}"); } catch { } }
            try { ZoomChanged?.Invoke(null); } catch { }
        }

        public void Auto()
        {
            _activeZoom = null;
            try { _stocksZoom.Auto(); } catch (Exception ex) { try { System.Diagnostics.Trace.TraceWarning($"DualZoomController: Auto stocks failed: {ex.Message}"); } catch { } }
            try { _knockoutsZoom.Auto(); } catch (Exception ex) { try { System.Diagnostics.Trace.TraceWarning($"DualZoomController: Auto knockouts failed: {ex.Message}"); } catch { } }
            try { ZoomChanged?.Invoke(null); } catch { }
        }

    }
}

