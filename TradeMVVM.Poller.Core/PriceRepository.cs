using System.Collections.Generic;
using TradeMVVM.Domain;

namespace TradeMVVM.Poller.Core.Data
{
    // Internal repository wrapper used by the poller core. Renamed to avoid duplicate type conflicts
    // with the application-level PriceRepository in TradeMVVM.Trading.Data.
    public class PollerPriceRepository
    {
        private readonly TradeMVVM.Poller.Core.DatabaseService _db = new TradeMVVM.Poller.Core.DatabaseService();

        public void Insert(StockPoint point)
        {
            _db.Insert(point);
        }

        // New overload to accept provider name when persisting
        public void Insert(StockPoint point, string provider)
        {
            point.Provider = provider;
            _db.Insert(point);
        }

        public List<StockPoint> LoadAll()
        {
            // passthrough to DB implementation (kept for compatibility)
            return _db.LoadAll();
        }

        public List<StockPoint> LoadByIsin(string isin) => _db.LoadByIsin(isin);

        public void ClearAll() => _db.ClearAll();
        public void DeleteByIsin(string isin) => _db.DeleteByIsin(isin);
        public void RemoveZeroOrNullPrices() => _db.RemoveZeroOrNullPrices();
        public void RemoveDuplicateEntries() => _db.RemoveDuplicateEntries();
        public void Vacuum() => _db.Vacuum();
        public void CleanDatabase() => _db.CleanDatabase();
    }
}
