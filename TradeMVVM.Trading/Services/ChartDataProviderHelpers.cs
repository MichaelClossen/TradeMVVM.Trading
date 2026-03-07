using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace TradeMVVM.Trading.Services
{
    internal static class ChartDataProviderHelpers
    {
        public static (double, double)? Validate(string price, string percent)
        {
            if (string.IsNullOrWhiteSpace(price) ||
                string.IsNullOrWhiteSpace(percent))
                return null;

            percent = percent.Replace("%", "").Trim();
            price = Regex.Replace(price,
                @"\s*(EUR|USD|€|\$)\b?", "",
                RegexOptions.IgnoreCase).Trim();

            price = Normalize(price);
            percent = Normalize(percent);

            if (double.TryParse(price,
                    NumberStyles.Any,
                    CultureInfo.InvariantCulture,
                    out double priceVal) &&
                double.TryParse(percent,
                    NumberStyles.Any,
                    CultureInfo.InvariantCulture,
                    out double percentVal))
            {
                return (priceVal, percentVal);
            }

            return null;
        }

        private static string Normalize(string input)
        {
            input = input.Replace(" ", "");

            if (input.Contains(",") && !input.Contains("."))
                input = input.Replace(",", ".");
            else if (input.Contains(".") && input.Contains(","))
            {
                input = input.Replace(".", "");
                input = input.Replace(",", ".");
            }

            return input;
        }
    }
}
