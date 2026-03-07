using System;

namespace TradeMVVM.Domain
{
    public class StockPoint
    {
        public string ISIN { get; set; }
        public DateTime Time { get; set; }
        public double Price { get; set; }
        public double Percent { get; set; }
        public string Provider { get; set; }
        public DateTime? ProviderTime { get; set; }
        // Prediction fields: stored as text/real in the DB
        // Forecast: string values like "Up", "Down", "Neutral"
        public string Forecast { get; set; }
        // PredictedPrice stores the computed moving-average (or ML) prediction value when available
        public double PredictedPrice { get; set; }
    }
}
