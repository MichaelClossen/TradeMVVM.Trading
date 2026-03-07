using System.Collections.Generic;

namespace TradeMVVM.Trading.Services.Infrastructure
{
    public interface IUnresolvedIsinLogger
    {
        void LogUnresolvedIsin(string isin, TradeMVVM.Domain.StockType type, List<string> attemptedUrls);
    }
}
