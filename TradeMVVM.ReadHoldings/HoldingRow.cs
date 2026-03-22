using System;

namespace TradeMVVM.ReadHoldings
{
    // Simple model for serialization and UI binding
    public class HoldingRow
    {
        public string Isin { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public double Trail { get; set; }
        public double Shares { get; set; }
        public double AvgBuyPrice { get; set; }
        public double PurchaseValue { get; set; }
        public double Percent { get; set; }
        public double TotalValue { get; set; }
        public double TodayValue { get; set; }
        public string Provider { get; set; } = string.Empty;
        public DateTime? Updated { get; set; }
    }
}
