using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows;

namespace TradeMVVM.Trading.Presentation.Converters
{
    public class NumericToFontWeightConverter : IValueConverter
    {
        public FontWeight PositiveWeight { get; set; } = FontWeights.Bold;
        public FontWeight NegativeWeight { get; set; } = FontWeights.Bold;
        public FontWeight ZeroWeight { get; set; } = FontWeights.Normal;

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null)
                return ZeroWeight;

            if (double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double d) ||
                double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.CurrentCulture, out d))
            {
                if (d > 0) return PositiveWeight;
                if (d < 0) return NegativeWeight;
                return ZeroWeight;
            }
            return ZeroWeight;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
