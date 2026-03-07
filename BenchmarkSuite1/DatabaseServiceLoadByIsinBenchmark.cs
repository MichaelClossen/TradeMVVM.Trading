using System;
using System.Linq;
using BenchmarkDotNet.Attributes;
using TradeMVVM.Trading.Services;
using Microsoft.VSDiagnostics;

namespace TradeMVVM.Trading.Benchmarks
{
    [CPUUsageDiagnoser]
    public class DatabaseServiceLoadByIsinBenchmark
    {
        private DatabaseService _db;
        private string _isin;
        [GlobalSetup]
        public void Setup()
        {
            _db = new DatabaseService();
            _isin = _db.LoadAll().Select(x => x?.ISIN).FirstOrDefault(x => !string.IsNullOrWhiteSpace(x)) ?? "DUMMY";
        }

        [Benchmark]
        public int LoadByIsin_Count()
        {
            return _db.LoadByIsin(_isin).Count;
        }
    }
}