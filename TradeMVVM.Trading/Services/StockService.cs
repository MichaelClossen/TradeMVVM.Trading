using System;
using System.Threading.Tasks;

namespace TradeMVVM.Trading.Services
{
    public class StockService
    {
        private readonly Random _rand = new Random();
        public async Task<double> GetPriceAsync()
        {
            return await Task.Run(() => _rand.Next(100, 200));
        }
    }
}