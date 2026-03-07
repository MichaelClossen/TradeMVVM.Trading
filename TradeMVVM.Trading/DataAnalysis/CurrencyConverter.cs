using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace TradeMVVM.Trading.DataAnalysis
{
    public class CurrencyConverter
    {
        private readonly Dictionary<string, double> _rates = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);

        public CurrencyConverter()
        {
            // default EUR
            _rates["EUR"] = 1.0;

            try
            {
                var baseDir = AppDomain.CurrentDomain.BaseDirectory;
                var dataDir = Path.Combine(baseDir, "DataAnalysis");
                var ratesPath = Path.Combine(dataDir, "fx_rates.csv");

                if (!File.Exists(ratesPath))
                    return;

                var lines = File.ReadAllLines(ratesPath);
                foreach (var l in lines)
                {
                    if (string.IsNullOrWhiteSpace(l))
                        continue;
                    var parts = l.Split(';');
                    if (parts.Length < 2)
                        continue;
                    var cur = parts[0].Trim();
                    var rateText = parts[1].Trim();
                    if (double.TryParse(rateText.Replace("\u00A0", ""), NumberStyles.Any, CultureInfo.GetCultureInfo("de-DE"), out double rate) ||
                        double.TryParse(rateText.Replace("\u00A0", ""), NumberStyles.Any, CultureInfo.InvariantCulture, out rate))
                    {
                        if (!string.IsNullOrEmpty(cur))
                            _rates[cur] = rate;
                    }
                }
            }
            catch
            {
                // ignore, use defaults
            }
        }

        public double GetRateToEur(string currency)
        {
            if (string.IsNullOrWhiteSpace(currency))
                return 1.0;
            if (_rates.TryGetValue(currency.Trim(), out var r))
                return r;
            // try common variants
            if (currency.Length >= 3)
            {
                var code = currency.Trim().ToUpperInvariant();
                if (_rates.TryGetValue(code, out r))
                    return r;
            }
            return 1.0; // fallback assume EUR
        }

        public double ConvertToEur(double amount, string currency)
        {
            var rate = GetRateToEur(currency);
            return amount * rate;
        }
    }
}
