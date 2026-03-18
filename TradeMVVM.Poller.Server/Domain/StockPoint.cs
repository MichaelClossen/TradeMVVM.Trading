using System;

namespace TradeMVVM.Trading.Domain
{
    // Minimal duplicate of domain model to make server independent from TradeMVVM.Domain project
    public class StockPoint
    {
        public string ISIN { get; set; }
        public DateTime Time { get; set; }
        public double Price { get; set; }
        public double Percent { get; set; }
        public string Provider { get; set; }
        public DateTime? ProviderTime { get; set; }
        public string Forecast { get; set; }
        public double PredictedPrice { get; set; }
    }
}
