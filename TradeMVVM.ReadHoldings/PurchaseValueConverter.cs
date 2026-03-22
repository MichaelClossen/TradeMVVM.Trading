using System;
using System.Globalization;
using System.Windows.Data;

namespace TradeMVVM.ReadHoldings
{
    // Converter that displays empty string for zero purchase value, otherwise formats with 2 decimals
    public class PurchaseValueConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null) return string.Empty;
            if (value is double d)
            {
                if (Math.Abs(d) < 0.0000001) return string.Empty;
                return d.ToString("N2", CultureInfo.GetCultureInfo("de-DE"));
            }
            // try parse
            if (double.TryParse(value.ToString(), out double parsed))
            {
                if (Math.Abs(parsed) < 0.0000001) return string.Empty;
                return parsed.ToString("N2", CultureInfo.GetCultureInfo("de-DE"));
            }
            return value.ToString();
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            // not needed for one-way binding
            if (value == null) return 0.0;
            if (value is string s && string.IsNullOrWhiteSpace(s)) return 0.0;
            if (double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.GetCultureInfo("de-DE"), out double d)) return d;
            return 0.0;
        }
    }
}
