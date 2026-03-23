using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace TradeMVVM.ReadHoldings
{
    // Simple model for serialization and UI binding
    // Now implements INotifyPropertyChanged so the UI is updated when properties change
    public class HoldingRow : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler? PropertyChanged;

        bool Set<T>(ref T field, T value, [CallerMemberName] string? propName = null)
        {
            if (EqualityComparer<T>.Default.Equals(field, value)) return false;
            field = value;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propName));
            return true;
        }

        private string _isin = string.Empty;
        public string Isin { get => _isin; set => Set(ref _isin, value); }

        private string _name = string.Empty;
        public string Name { get => _name; set => Set(ref _name, value); }

        private double _trail;
        public double Trail { get => _trail; set => Set(ref _trail, value); }

        private double _shares;
        public double Shares { get => _shares; set => Set(ref _shares, value); }

        private double _avgBuyPrice;
        public double AvgBuyPrice { get => _avgBuyPrice; set => Set(ref _avgBuyPrice, value); }

        private double _purchaseValue;
        public double PurchaseValue { get => _purchaseValue; set => Set(ref _purchaseValue, value); }

        private double _percent;
        public double Percent { get => _percent; set => Set(ref _percent, value); }

        private double _totalValue;
        public double TotalValue { get => _totalValue; set => Set(ref _totalValue, value); }

        private double _todayValue;
        public double TodayValue { get => _todayValue; set => Set(ref _todayValue, value); }

        private string _provider = string.Empty;
        public string Provider { get => _provider; set => Set(ref _provider, value); }

        private DateTime? _updated;
        public DateTime? Updated { get => _updated; set => Set(ref _updated, value); }
    }
}
