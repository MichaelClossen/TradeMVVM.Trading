using System;
using System.Collections.Generic;
using TradeMVVM.Domain;
using TradeMVVM.Trading.Services;

namespace TradeMVVM.Trading.Data
{
    public class PriceRepository
    {
        private readonly DatabaseService _db = new DatabaseService();

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
            return _db.LoadAll();
        }

        // load by ISIN convenience passthrough
        public List<StockPoint> LoadByIsin(string isin) => _db.LoadByIsin(isin);

        // load only points inserted after a given timestamp (incremental refresh)
        public List<StockPoint> LoadSince(DateTime since) => _db.LoadSince(since);

        // convenience passthroughs for maintenance
        public void ClearAll() => _db.ClearAll();
        public void DeleteByIsin(string isin) => _db.DeleteByIsin(isin);
        public void RemoveZeroOrNullPrices() => _db.RemoveZeroOrNullPrices();
        public void RemoveDuplicateEntries() => _db.RemoveDuplicateEntries();
        public void Vacuum() => _db.Vacuum();
        public void CleanDatabase() => _db.CleanDatabase();
    }
}
