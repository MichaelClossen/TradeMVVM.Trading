using System.Windows.Media;

namespace TradeMVVM.Trading.Models
{
    public class LegendItem
    {
        public string Name { get; set; }
        public Brush Color { get; set; }
        public string PercentText { get; set; }
        public Brush PercentBrush { get; set; }

        public bool IsHeader { get; set; }
    }


}
