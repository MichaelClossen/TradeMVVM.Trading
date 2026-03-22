using System;
using System.Threading.Tasks;

namespace TradeMVVM.Poller.Host
{
    public static class Program
    {
        // Minimal entry point to satisfy build; this host is a stub for running the poller in isolation.
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Poller host started.");
            await Task.Delay(100);
        }
    }
}
