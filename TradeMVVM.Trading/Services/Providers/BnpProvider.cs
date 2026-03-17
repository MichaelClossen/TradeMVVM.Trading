using System;
using System.Net.Http;
using System.Collections.Generic;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace TradeMVVM.Trading.Services.Providers
{
    public class BnpProvider : IPriceProvider
    {
        private readonly System.Net.Http.HttpClient _client;
        private readonly SemaphoreSlim _throttle;

        public BnpProvider(System.Net.Http.HttpClient client, SemaphoreSlim throttle)
        {
            _client = client;
            _throttle = throttle;
        }

        public async Task<(double, double, DateTime?)?> GetPriceAsync(string isin, List<string> attemptedUrls, CancellationToken token)
        {
            try
            {
                await _throttle.WaitAsync(token);
                try
                {
  

                    var url = $"https://derivate.bnpparibas.com/product-details/{isin}/";
                    attemptedUrls?.Add(url);

               


                    var response = await _client.GetAsync(url, token);
                    if (!response.IsSuccessStatusCode)
                    {
                        // do not log here; let caller decide whether to warn if all providers fail
                        return null;
                    }

                    var html = await response.Content.ReadAsStringAsync();

                    var priceMatch = Regex.Match(html,
                        $@"data-field=""bid""[^>]*data-item=""[^""]*{isin}""[^>]*>\s*(?<price>\d+[.,]\d+)",
                        RegexOptions.IgnoreCase);

                    if (!priceMatch.Success)
                        return null;

                    var price = priceMatch.Groups["price"].Value.Trim();

                    var percentMatch = Regex.Match(html,
                        $@"data-field=""changePercent""[^>]*data-item=""[^""]*{isin}""[^>]*>\s*(?<percent>[+-]?\d+[.,]\d+)",
                        RegexOptions.IgnoreCase);

                    if (!percentMatch.Success)
                        return null;

                    var percent = percentMatch.Groups["percent"].Value.Trim();

                    price = WebUtility.HtmlDecode(price)
                        .Replace("\u00A0", "")
                        .Trim();

                    percent = WebUtility.HtmlDecode(percent)
                        .Replace("\u00A0", "")
                        .Replace("%", "")
                        .Trim();

                    var result = ChartDataProviderHelpers.Validate(price, percent);

                    if (result.HasValue)
                        return (result.Value.Item1, result.Value.Item2, DateTime.UtcNow);

                    return null;
                }
                finally
                {
                    _throttle.Release();
                }
            }
            catch (OperationCanceledException)
            {
                // Cancellation requested by caller - treat as non-error and return no result.
                // Log as informational to avoid noisy warnings when the polling watchdog cancels requests.
                try { Trace.TraceInformation($"BNP Paribas: Request canceled for ISIN {isin}"); } catch { }
                return null;
            }
            catch (HttpRequestException ex)
            {
                Trace.TraceWarning($"BNP Paribas: HttpRequestException for ISIN {isin}: {ex}");
                return null;
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"BNP Paribas: General exception for ISIN {isin}: {ex}");
                return null;
            }
        }
    }
}
