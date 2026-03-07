using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;

namespace TradeMVVM.Trading.Services.Infrastructure
{
    public class DevFolderUnresolvedIsinLogger : IUnresolvedIsinLogger
    {
        private readonly string _devFolder;
        private readonly string _devFolderPath;
        private readonly object _logLock = new object();

        public DevFolderUnresolvedIsinLogger(string devFolder)
        {
            _devFolder = string.IsNullOrWhiteSpace(devFolder) ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "Trade", "TradeMVVM.Trading") : devFolder;
            _devFolderPath = devFolder;
        }

        public void LogUnresolvedIsin(string isin, TradeMVVM.Domain.StockType type, List<string> attemptedUrls)
        {
            try
            {
                Directory.CreateDirectory(_devFolder);

                var file = Path.Combine(_devFolder, "unresolved_isins.log");

                var sb = new StringBuilder();
                sb.AppendLine("------------------------------------------------------------");
                sb.AppendLine($"Time: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}");
                sb.AppendLine($"ISIN: {isin}");
                sb.AppendLine($"StockType: {type}");
                try
                {
                    sb.AppendLine($"ProcessId: {Process.GetCurrentProcess().Id}");
                }
                catch { }
                sb.AppendLine($"ThreadId: {Thread.CurrentThread.ManagedThreadId}");
                sb.AppendLine($"Machine: {Environment.MachineName}");
                sb.AppendLine($"OSVersion: {Environment.OSVersion}");
                sb.AppendLine($"Culture: {CultureInfo.CurrentCulture.Name}");
                sb.AppendLine($"Runtime: {Environment.Version}");
                sb.AppendLine();
                sb.AppendLine("Attempted URLs:");

                var urlsSnapshot = attemptedUrls == null ? null : new List<string>(attemptedUrls);
                if (urlsSnapshot == null || urlsSnapshot.Count == 0)
                {
                    sb.AppendLine("(none)");
                    Trace.TraceInformation("LogUnresolvedIsin: no attempted URLs recorded.");
                }
                else
                {
                    foreach (var u in urlsSnapshot)
                    {
                        sb.AppendLine(u);
                        Trace.TraceInformation($"LogUnresolvedIsin attempted URL: {u}");
                    }
                }
                sb.AppendLine();

                lock (_logLock)
                {
                    try
                    {
                        using var fs = new FileStream(file, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite);
                        fs.Seek(0, SeekOrigin.End);
                        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
                        fs.Write(bytes, 0, bytes.Length);
                        fs.Flush(true);
                    }
                    catch (Exception exFile)
                    {
                        Trace.TraceWarning($"Failed to write unresolved ISIN to dev file {file}: {exFile}");
                    }

                    try
                    {
                        var tempFile = Path.Combine(Path.GetTempPath(), "unresolved_isins.log");
                        using var fs2 = new FileStream(tempFile, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite);
                        fs2.Seek(0, SeekOrigin.End);
                        var bytes2 = Encoding.UTF8.GetBytes(sb.ToString());
                        fs2.Write(bytes2, 0, bytes2.Length);
                        fs2.Flush(true);

                        Trace.TraceInformation($"Also wrote unresolved ISIN to temp: {tempFile}");
                    }
                    catch (Exception exTemp)
                    {
                        Trace.TraceWarning($"Failed to write unresolved ISIN to temp: {exTemp}");
                    }
                }

                Trace.TraceInformation($"Wrote unresolved ISIN to: {file}");
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"LogUnresolvedIsin failed for {isin}: {ex}");
            }
        }
        private string GetDefaultFolder()
        {
            var baseFolder = _devFolderPath;
            try
            {
                if (string.IsNullOrWhiteSpace(baseFolder))
                {
                    baseFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "Trade", "TradeMVVM.Trading");
                }
            }
            catch { baseFolder = Path.Combine(".", "Trade"); }
            try { Directory.CreateDirectory(baseFolder); } catch { }
            return baseFolder;
        }
    }
}
