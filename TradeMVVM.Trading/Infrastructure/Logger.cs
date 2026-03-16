using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace TradeMVVM.Trading.Infrastructure
{
    // Minimal logger for unhandled exceptions and throttled info messages.
    public static class Logger
    {
        private static readonly object _fileLock = new object();
        private static readonly ConcurrentDictionary<string, DateTime> _lastShown = new ConcurrentDictionary<string, DateTime>();
        private static readonly TimeSpan _defaultThrottle = TimeSpan.FromSeconds(1);

        // Log size controls
        private const long MaxLogFileSizeBytes = 1 * 1024 * 1024; // 1 MB
        private const int MaxBackupFiles = 3;
        private const int MaxEntryChars = 32 * 1024; // truncate very large exception dumps

        public static void LogException(Exception ex, string context = null)
        {
            try
            {
                var dir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory ?? string.Empty, "logs");
                Directory.CreateDirectory(dir);
                var file = Path.Combine(dir, "unhandled.log");

                // Throttle identical/very similar exceptions to avoid log spam on rapid repeats
                var key = "file:" + (context ?? "") + ":" + (ex?.GetType().FullName ?? "") + ":" + (ex?.Message ?? "");
                var now = DateTime.UtcNow;
                var last = _lastShown.GetOrAdd(key, DateTime.MinValue);
                if ((now - last) < TimeSpan.FromSeconds(2))
                {
                    // skip writing nearly-duplicate exceptions within short interval
                    return;
                }
                _lastShown[key] = now;

                var sb = new StringBuilder();
                sb.AppendLine("-----");
                sb.AppendLine(DateTime.UtcNow.ToString("o") + " UTC");
                if (!string.IsNullOrEmpty(context)) sb.AppendLine("Context: " + context);
                if (ex != null)
                {
                    var text = ex.ToString() ?? string.Empty;
                    if (text.Length > MaxEntryChars)
                    {
                        text = text.Substring(0, MaxEntryChars) + "\n[truncated]";
                    }
                    sb.AppendLine(text);
                }
                else
                {
                    sb.AppendLine("Exception object was null");
                }

                lock (_fileLock)
                {
                    EnsureRotate(file);
                    File.AppendAllText(file, sb.ToString());
                }

                try { Trace.WriteLine(sb.ToString()); } catch { }
            }
            catch { }
        }

        public static void LogUnhandled(object exceptionObject, string context = null)
        {
            try
            {
                Exception ex = exceptionObject as Exception;
                if (ex != null)
                    LogException(ex, context);
                else
                {
                    var dir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory ?? string.Empty, "logs");
                    Directory.CreateDirectory(dir);
                    var file = Path.Combine(dir, "unhandled.log");
                    var line = DateTime.UtcNow.ToString("o") + " UTC - Unhandled object: " + exceptionObject?.ToString();
                    if (line.Length > MaxEntryChars) line = line.Substring(0, MaxEntryChars) + " [truncated]";
                    var key = "file:unhandled_object:" + (exceptionObject?.ToString() ?? "");
                    var now = DateTime.UtcNow;
                    var last = _lastShown.GetOrAdd(key, DateTime.MinValue);
                    if ((now - last) < TimeSpan.FromSeconds(2))
                    {
                        return;
                    }
                    _lastShown[key] = now;

                    lock (_fileLock)
                    {
                        EnsureRotate(file);
                        File.AppendAllText(file, line + Environment.NewLine);
                    }
                    try { Trace.WriteLine(line); } catch { }
                }
            }
            catch { }
        }

        public static void ThrottledInfo(string key, string message, TimeSpan? minInterval = null)
        {
            try
            {
                var interval = minInterval ?? _defaultThrottle;
                var now = DateTime.UtcNow;
                var last = _lastShown.GetOrAdd(key, DateTime.MinValue);
                if ((now - last) >= interval)
                {
                    _lastShown[key] = now;
                    try { Trace.WriteLine(message); } catch { }
                }
            }
            catch { }
        }

        private static void EnsureRotate(string file)
        {
            try
            {
                try
                {
                    var fi = new FileInfo(file);
                    if (fi.Exists && fi.Length > MaxLogFileSizeBytes)
                    {
                        // rotate backups: unhandled.log.2 -> .3, .1 -> .2, . -> .1
                        for (int i = MaxBackupFiles - 1; i >= 1; i--)
                        {
                            var src = file + "." + i;
                            var dst = file + "." + (i + 1);
                            try { if (File.Exists(dst)) File.Delete(dst); } catch { }
                            try { if (File.Exists(src)) File.Move(src, dst); } catch { }
                        }
                        // move current to .1
                        try { var first = file + ".1"; if (File.Exists(first)) File.Delete(first); } catch { }
                        try { File.Move(file, file + ".1"); } catch { }
                    }
                }
                catch { }
            }
            catch { }
        }
    }
}
