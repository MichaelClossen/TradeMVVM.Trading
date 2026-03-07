using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TradeMVVM.Trading.Services;
using Microsoft.Extensions.DependencyInjection;

namespace TradeMVVM.Trading.Tests
{
    // Lightweight manual test helper. Call ChartDataProviderLogTest.RunAsync()
    // from a debug session or a simple test runner to verify that unresolved
    // ISINs are logged to the unresolved_isins.log file.
    public static class ChartDataProviderLogTest
    {
        public static async Task<bool> RunAsync()
        {
            var provider = App.Services.GetRequiredService<ChartDataProvider>();
            var isin = "INVALID-ISIN-TEST-000";
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

            try
            {
                // This is expected to throw because the ISIN is invalid
                await provider.DataProvider(isin, TradeMVVM.Domain.StockType.Aktie, token: cts.Token);
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Expected exception while resolving ISIN: {ex.Message}");
            }

            // give the provider a short moment to flush the log
            await Task.Delay(500);

            var folder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "TradeMVVM", "Logs");
            var file = Path.Combine(folder, "unresolved_isins.log");

            if (!File.Exists(file))
            {
                Trace.WriteLine($"Log file not found: {file}");
                return false;
            }

            var content = File.ReadAllText(file, Encoding.UTF8);

            var containsIsin = content.IndexOf(isin, StringComparison.OrdinalIgnoreCase) >= 0;
            var containsUrls = content.IndexOf("Attempted URLs", StringComparison.OrdinalIgnoreCase) >= 0;

            Trace.WriteLine($"Log file: {file}");
            Trace.WriteLine($"Contains ISIN: {containsIsin}");
            Trace.WriteLine($"Contains Attempted URLs section: {containsUrls}");

            if (!containsIsin || !containsUrls)
            {
                throw new InvalidOperationException("Log file does not contain expected entries (ISIN or Attempted URLs).");
            }

            return true;
        }
    }
}
