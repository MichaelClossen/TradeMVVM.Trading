using System;
using System.IO;

namespace TradeMVVM.Trading.Services
{
    internal static class FallbackLogger
    {
        private static readonly object _lock = new object();
        private static string LogPath
        {
            get
            {
                var baseDir = AppDomain.CurrentDomain.BaseDirectory;
                var dir = Path.Combine(baseDir, "DataAnalysis", "Logs");
                Directory.CreateDirectory(dir);
                return Path.Combine(dir, "fallback.log");
            }
        }

        public static void Log(string message)
        {
            try
            {
                lock (_lock)
                {
                    File.AppendAllText(LogPath, $"[{DateTime.Now:O}] {message}{Environment.NewLine}");
                }
            }
            catch { }
        }
    }
}
