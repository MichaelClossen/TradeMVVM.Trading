using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;

namespace TradeMVVM.Trading.Presentation.Converters
{
    public class NumericToBrushConverter : IValueConverter
    {
        public Brush PositiveBrush { get; set; } = Brushes.Green;
        public Brush NegativeBrush { get; set; } = Brushes.Red;
        public Brush ZeroBrush { get; set; } = Brushes.Black;

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        { 
            if (value == null)
                return ZeroBrush;

            if (double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double d) ||
                double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.CurrentCulture, out d)) 
            {
                if (d > 0) return PositiveBrush;
                if (d < 0) return NegativeBrush;
                return ZeroBrush;
            }
            return ZeroBrush;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
