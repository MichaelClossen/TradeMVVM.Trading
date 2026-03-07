using System.Collections.Generic;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TradeMVVM.Trading.Services.Providers
{
    public interface IPriceProvider
    {
        // Returns price, percent and optional provider timestamp when available
        Task<(double, double, DateTime?)?> GetPriceAsync(string isin, List<string> attemptedUrls, CancellationToken token);
    }
}
