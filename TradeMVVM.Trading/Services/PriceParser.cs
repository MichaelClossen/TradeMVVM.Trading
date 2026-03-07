using System.Globalization;

namespace TradeMVVM.Trading.Services
{
    public static class PriceParser
    {
        public static double Parse(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return double.NaN;

            value = value
                .Replace("€", "")
                .Replace(",", ".")
                .Trim();

            if (double.TryParse(
                value,
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out double result))
            {
                return result;
            }

            return double.NaN;
        }
    }
}
