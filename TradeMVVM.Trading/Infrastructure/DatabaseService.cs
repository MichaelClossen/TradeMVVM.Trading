using System;
using System.Diagnostics;
using TradeMVVM.Trading.Infrastructure;
using System.Collections.Generic;
using System.Linq;
using System.Data.SQLite;
using System.Threading;
using System.Threading.Tasks;
using TradeMVVM.Domain;
using System.Globalization;
using System.IO;

namespace TradeMVVM.Trading.Services
{
    public class DatabaseService
    {
        private readonly string _connection;
        private static int _summaryScheduled = 0;
        private const int DbLockedRetryCount = 5;
        private static readonly TimeSpan LoadByIsinCacheTtl = TimeSpan.FromSeconds(5);
        private static readonly object _initLock = new object();
        private static bool _isInitialized = false;
        private readonly object _loadByIsinCacheLock = new object();
        private readonly Dictionary<string, (DateTime fetchedUtc, List<StockPoint> rows)> _loadByIsinCache
            = new Dictionary<string, (DateTime, List<StockPoint>)>(StringComparer.OrdinalIgnoreCase);





        // Replace NEW_Holdings table to contain exactly the provided ISINs.
        // This deletes existing rows and inserts one row per ISIN using only the ISIN column
        // (case-insensitive lookup for the column name). Safe no-op if table or column missing.
        public void ReplaceNewHoldingsWithIsins(IEnumerable<string> isins)
        {
            if (isins == null) return;
            try
            {
                using var conn = new SQLiteConnection(_connection);
                conn.Open();

                using var chk = new SQLiteCommand("SELECT name FROM sqlite_master WHERE type='table' AND name='NEW_Holdings';", conn);
                var exists = chk.ExecuteScalar();
                if (exists == null) return;

                // find ISIN-like column name
                string isinCol = null;
                using (var pragma = new SQLiteCommand("PRAGMA table_info(NEW_Holdings);", conn))
                using (var rdr = pragma.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        try
                        {
                            var col = rdr.GetString(1);
                            if (string.Equals(col, "isin", StringComparison.OrdinalIgnoreCase) || string.Equals(col, "ISIN", StringComparison.OrdinalIgnoreCase) || string.Equals(col, "Isin", StringComparison.OrdinalIgnoreCase))
                            {
                                isinCol = col;
                                break;
                            }
                        }
                        catch { }
                    }

                }

                if (string.IsNullOrEmpty(isinCol))
                    return; // cannot insert without ISIN column

                using var tran = conn.BeginTransaction();
                try
                {
                    using var del = new SQLiteCommand("DELETE FROM NEW_Holdings;", conn, tran);
                    del.ExecuteNonQuery();

                    using var ins = new SQLiteCommand($"INSERT INTO NEW_Holdings ({isinCol}) VALUES (@isin);", conn, tran);
                    var p = ins.Parameters.AddWithValue("@isin", string.Empty);

                    foreach (var raw in isins)
                    {
                        try
                        {
                            var v = raw?.Replace("\u00A0", string.Empty).Trim();
                            if (string.IsNullOrWhiteSpace(v)) continue;
                            p.Value = v;
                            ins.ExecuteNonQuery();
                        }
                        catch { }
                    }

                    tran.Commit();
                }
                catch { try { tran.Rollback(); } catch { } }
            }
            catch { }
        }


        private void InvalidateLoadByIsinCache(string isin = null)
        {
            lock (_loadByIsinCacheLock)
            {
                if (string.IsNullOrWhiteSpace(isin))
                {
                    _loadByIsinCache.Clear();
                    return;
                }

                _loadByIsinCache.Remove(isin);
            }
        }

        // Insert a single TotalPLHistory sample.
        public void InsertTotalPLHistory(DateTime time, double totalPl)
        {
            for (int attempt = 1; attempt <= DbLockedRetryCount; attempt++)
            {
                try
                {
                    using (var conn = new SQLiteConnection(_connection))
                    {
                        conn.Open();
                        using (var busyCmd = new SQLiteCommand("PRAGMA busy_timeout = 5000;", conn))
                            busyCmd.ExecuteNonQuery();

                        using (var tran = conn.BeginTransaction())
                        {
                            try
                            {
                                // check latest sample inside the transaction to avoid race conditions
                                DateTime? lastTime = null;
                                double? lastTotal = null;
                                using (var sel = new SQLiteCommand("SELECT Time, TotalPL FROM TotalPLHistory ORDER BY Time DESC LIMIT 1;", conn, tran))
                                using (var reader = sel.ExecuteReader())
                                {
                                    if (reader.Read())
                                    {
                                        lastTime = ReadNullableDateTime(reader, 0);
                                        lastTotal = reader.IsDBNull(1) ? (double?)null : reader.GetDouble(1);
                                    }
                                }

                                bool skip = false;
                                if (lastTime.HasValue && lastTotal.HasValue)
                                {
                                    if (Math.Abs((time - lastTime.Value).TotalSeconds) < 5 && Math.Abs(lastTotal.Value - totalPl) < 0.005)
                                    {
                                        skip = true;
                                    }
                                }

                                if (!skip)
                                {
                                    var cmd = new SQLiteCommand("INSERT INTO TotalPLHistory (Time, TotalPL) VALUES (@time, @total);", conn, tran);
                                    cmd.Parameters.AddWithValue("@time", time);
                                    cmd.Parameters.AddWithValue("@total", totalPl);
                                    cmd.ExecuteNonQuery();
                                }

                                tran.Commit();
                                return;
                            }
                            catch
                            {
                                try { tran.Rollback(); } catch { }
                                throw;
                            }
                        }
                    }


                }
                catch (SQLiteException ex) when (IsBusyOrLocked(ex) && attempt < DbLockedRetryCount)
                {
                    Thread.Sleep(50 * attempt);
                }
                catch (Exception ex)
                {
                    try { Logger.LogException(ex, "InsertTotalPLHistory"); } catch { }
                    return;
                }
            }
        }
        // Load TotalPL history between two timestamps (inclusive), ordered ascending.
        public List<Tuple<DateTime, double>> LoadTotalPLHistoryBetween(DateTime from, DateTime to)
        {
            var list = new List<Tuple<DateTime, double>>();
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    var sql = "SELECT Time, TotalPL FROM TotalPLHistory WHERE Time >= @from AND Time <= @to ORDER BY Time ASC";
                    var cmd = new SQLiteCommand(sql, conn);
                    cmd.Parameters.AddWithValue("@from", from);
                    cmd.Parameters.AddWithValue("@to", to);
                    using (var reader = cmd.ExecuteReader())
                    {
                            while (reader.Read())
                            {
                                var time = ReadDateTimeSafe(reader, 0);
                                var total = reader.IsDBNull(1) ? 0.0 : reader.GetDouble(1);
                                list.Add(new Tuple<DateTime, double>(time, total));
                            }
                    }
                }
            }
            catch (Exception ex)
            {
                try { Logger.LogException(ex, "LoadTotalPLHistoryBetween"); } catch { }
            }
            return list;
        }

        // Replace TotalPLHistory rows within [from,to] by deleting that range and inserting provided points.
        // Points should be ordered ascending.
        public void ReplaceTotalPLHistoryRange(DateTime from, DateTime to, List<Tuple<DateTime, double>> points)
        {
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    using (var tran = conn.BeginTransaction())
                    {
                        try
                        {
                            var del = new SQLiteCommand("DELETE FROM TotalPLHistory WHERE Time >= @from AND Time <= @to;", conn, tran);
                            del.Parameters.AddWithValue("@from", from);
                            del.Parameters.AddWithValue("@to", to);
                            del.ExecuteNonQuery();

                            if (points != null && points.Count > 0)
                            {
                                var insert = new SQLiteCommand("INSERT INTO TotalPLHistory (Time, TotalPL) VALUES (@time, @total);", conn, tran);
                                var pTime = insert.Parameters.AddWithValue("@time", DateTime.MinValue);
                                var pTotal = insert.Parameters.AddWithValue("@total", 0.0);
                                foreach (var pt in points)
                                {
                                    pTime.Value = pt.Item1;
                                    pTotal.Value = pt.Item2;
                                    insert.ExecuteNonQuery();
                                }
                            }

                            tran.Commit();
                        }
                        catch
                        {
                            try { tran.Rollback(); } catch { }
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                try { Logger.LogException(ex, "ReplaceTotalPLHistoryRange"); } catch { }
            }
        }

        private static bool IsBusyOrLocked(SQLiteException ex)
        {
            if (ex == null)
                return false;

            return ex.ResultCode == SQLiteErrorCode.Busy
                || ex.ResultCode == SQLiteErrorCode.Locked
                || (ex.Message?.IndexOf("database is locked", StringComparison.OrdinalIgnoreCase) >= 0);
        }

        // Robust helper for reading DateTime values from SQLite which may store
        // date/time as DATETIME, TEXT (various formats), INTEGER (ticks or unix seconds) or real.
        private static DateTime? ReadNullableDateTime(System.Data.SQLite.SQLiteDataReader reader, int index)
        {
            try
            {
                if (reader.IsDBNull(index))
                    return null;

                var val = reader.GetValue(index);
                if (val is DateTime dt)
                    return dt;

                if (val is long l)
                {
                    // Heuristic: very large values are ticks, smaller likely unix seconds
                    if (l > 1000000000000L) // ticks threshold (~year 33658)
                    {
                        try { return new DateTime(l); } catch { }
                    }
                    try { return DateTimeOffset.FromUnixTimeSeconds(l).DateTime; } catch { }
                }

                if (val is double d)
                {
                    // maybe stored as unix seconds with fractional part
                    try
                    {
                        var seconds = (long)d;
                        return DateTimeOffset.FromUnixTimeSeconds(seconds).DateTime;
                    }
                    catch { }
                }

                var s = val as string;
                if (!string.IsNullOrWhiteSpace(s))
                {
                    if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal | DateTimeStyles.AdjustToUniversal, out var parsed))
                        return parsed;

                    if (DateTime.TryParseExact(s, "O", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out parsed))
                        return parsed;
                }
            }
            catch { }

            return null;
        }

        private static DateTime ReadDateTimeSafe(System.Data.SQLite.SQLiteDataReader reader, int index)
        {
            var dt = ReadNullableDateTime(reader, index);
            return dt ?? DateTime.MinValue;
        }

        private string ComputeConnectionString()
        {
            try
            {
                var env = Environment.GetEnvironmentVariable("TRADE_DB_CONNECTION");
                if (!string.IsNullOrWhiteSpace(env))
                {
                    // allow full connection string or sqlite DSN
                    return env;
                }
            }
            catch { }

            // Prefer an explicit desktop "Trade" folder DB so all components use the same file.
            try
            {
                var desktopTrade = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "Trade", "trading.db");
                try { Trace.TraceInformation($"DatabaseService: preferring desktop Trade DB at {desktopTrade}"); } catch { }
                return $"Data Source={desktopTrade}";
            }
            catch { }

            // Prefer an application-local database file in the app base directory to ensure all
            // components of the application use the same file regardless of current working dir.
            try
            {
                var baseDir = AppDomain.CurrentDomain.BaseDirectory ?? Directory.GetCurrentDirectory();
                var candidate = Path.Combine(baseDir, "trading.db");
                return $"Data Source={candidate}";
            }
            catch { }

            // Prior versions stored trading.db in the app folder or in a 'Data' subfolder.
            var candidates = new List<string>();

            // 1) current working directory
            candidates.Add(Path.Combine(Directory.GetCurrentDirectory(), "trading.db"));

            // 2) search upwards from assembly location to repository/project root
            try
            {
                var exe = System.Reflection.Assembly.GetEntryAssembly()?.Location ?? System.Reflection.Assembly.GetExecutingAssembly().Location;
                if (!string.IsNullOrEmpty(exe))
                {
                    var dir = Path.GetDirectoryName(exe);
                    // walk up directories looking for trading.db
                    var cur = dir;
                    for (int i = 0; i < 10 && !string.IsNullOrEmpty(cur); i++)
                    {
                        candidates.Add(Path.Combine(cur, "trading.db"));
                        candidates.Add(Path.Combine(cur, "Data", "trading.db"));
                        var parent = Path.GetDirectoryName(cur);
                        if (string.IsNullOrEmpty(parent) || parent == cur) break;
                        cur = parent;
                    }
                }
            }
            catch { }

            // 3) application data (previous installs may have placed DB here)
            try { candidates.Add(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "TradeMVVM", "trading.db")); } catch { }

            // 3.1) legacy build output folders (net48 etc.) often contain the old DB
            try
            {
                var asm = System.Reflection.Assembly.GetEntryAssembly()?.Location ?? System.Reflection.Assembly.GetExecutingAssembly().Location;
                var repoDir = Path.GetDirectoryName(asm);
                if (!string.IsNullOrEmpty(repoDir))
                {
                    var root = repoDir;
                    for (int i = 0; i < 6 && !string.IsNullOrEmpty(root); i++)
                    {
                        // candidate: <root>\TradeMVVM.Trading\bin\Debug\net48\trading.db
                        var candidate = Path.Combine(root, "TradeMVVM.Trading", "bin", "Debug", "net48", "trading.db");
                        candidates.Add(candidate);
                        candidate = Path.Combine(root, "TradeMVVM.Trading", "bin", "Debug", "net48", "trading.db");
                        candidates.Add(candidate);

                        // also check Release
                        candidate = Path.Combine(root, "TradeMVVM.Trading", "bin", "Release", "net48", "trading.db");
                        candidates.Add(candidate);

                        var parent = Path.GetDirectoryName(root);
                        if (string.IsNullOrEmpty(parent) || parent == root) break;
                        root = parent;
                    }
                }
            }
            catch { }

            // 4) fallback: current directory file name
            string chosen = null;
            foreach (var c in candidates)
            {
                try { if (File.Exists(c)) { chosen = c; break; } } catch { }
            }

            if (chosen == null)
            {
                // use working dir by default
                chosen = Path.Combine(Directory.GetCurrentDirectory(), "trading.db");
            }

            try { Trace.TraceInformation($"DatabaseService: using DB path {chosen}"); } catch { }
            return $"Data Source={chosen}";
        }

        public DatabaseService()
        {
            _connection = ComputeConnectionString();

            // ensure directory for default path exists
            try
            {
                var cs = new SQLiteConnectionStringBuilder(_connection);
                var file = cs.DataSource;
                var dir = Path.GetDirectoryName(file);
                if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                    Directory.CreateDirectory(dir);
            }
            catch { }

            // Log DB path and a quick summary (row count, min/max times) asynchronously to avoid blocking UI startup.
            try
            {
                if (System.Threading.Interlocked.CompareExchange(ref _summaryScheduled, 1, 0) == 0)
                {
                    var connStr = _connection;
                    Task.Run(() =>
                    {
                        try
                        {
                            var csb = new SQLiteConnectionStringBuilder(connStr);
                            var dbFile = csb.DataSource;
                            try { Trace.TraceInformation($"DatabaseService: using DB file '{dbFile}'"); } catch { }

                            // diagnostic: look for other trading.db files in parent directories to detect multiple DB copies
                            try
                            {
                                var dir = Path.GetDirectoryName(dbFile) ?? AppDomain.CurrentDomain.BaseDirectory;
                                var found = new List<string>();
                                var cur = dir;
                                for (int i = 0; i < 6 && !string.IsNullOrEmpty(cur); i++)
                                {
                                    try
                                    {
                                        var p = Path.Combine(cur, "trading.db");
                                        if (File.Exists(p) && !string.Equals(Path.GetFullPath(p), Path.GetFullPath(dbFile), StringComparison.OrdinalIgnoreCase))
                                            found.Add(p);
                                        var data = Path.Combine(cur, "Data", "trading.db");
                                        if (File.Exists(data) && !string.Equals(Path.GetFullPath(data), Path.GetFullPath(dbFile), StringComparison.OrdinalIgnoreCase))
                                            found.Add(data);
                                    }
                                    catch { }
                                    var parent = Path.GetDirectoryName(cur);
                                    if (string.IsNullOrEmpty(parent) || parent == cur) break;
                                    cur = parent;
                                }
                                if (found.Count > 0)
                                {
                                    foreach (var f in found.Distinct())
                                    {
                                        try { Trace.TraceInformation($"DatabaseService: found other trading.db candidate: {f} (size={new FileInfo(f).Length} bytes)"); } catch { }
                                    }
                                }
                            }
                            catch { }

                            using (var conn = new SQLiteConnection(connStr))
                            {
                                conn.Open();
                                try
                                {
                                    using (var c = new SQLiteCommand("SELECT COUNT(*) FROM Prices;", conn))
                                    {
                                        var cnt = Convert.ToInt32(c.ExecuteScalar());
                                        Trace.TraceInformation($"DatabaseService: Prices rows={cnt}");
                                    }
                            try
                            {
                                using (var c3 = new SQLiteCommand("SELECT COUNT(*) FROM NEW_Holdings;", conn))
                                {
                                    var cnt3 = Convert.ToInt32(c3.ExecuteScalar());
                                    Trace.TraceInformation($"DatabaseService: NEW_Holdings rows={cnt3}");
                                }
                            }
                            catch { /* table might not exist or other DB state - ignore */ }
                                }
                                catch { }

                                try
                                {
                                    using (var c2 = new SQLiteCommand("SELECT MIN(Time), MAX(Time) FROM Prices;", conn))
                                    using (var r = c2.ExecuteReader())
                                    {
                                        if (r.Read())
                                        {
                                            var min = r.IsDBNull(0) ? "NULL" : r.GetValue(0).ToString();
                                            var max = r.IsDBNull(1) ? "NULL" : r.GetValue(1).ToString();
                                            Trace.TraceInformation($"DatabaseService: Prices time range: min={min} max={max}");
                                        }
                                    }
                                }
                                catch { }
                            }
                        }
                        catch { }
                    });
                }
            }
            catch { }

            if (_isInitialized)
                return;

            lock (_initLock)
            {
                if (_isInitialized)
                    return;

                for (int attempt = 1; attempt <= DbLockedRetryCount; attempt++)
                {
                    try
                    {
                        using (var conn = new SQLiteConnection(_connection))
                        {
                            conn.Open();

                            using (var busyCmd = new SQLiteCommand("PRAGMA busy_timeout = 5000;", conn))
                            {
                                busyCmd.ExecuteNonQuery();
                            }

                            // create Prices table if missing
                            var cmd = new SQLiteCommand(@"
                    CREATE TABLE IF NOT EXISTS Prices (
                        ISIN TEXT NOT NULL,
                        Time DATETIME NOT NULL,
                        Price REAL,
                        Percent REAL,
                        Provider TEXT,
                        ProviderTime DATETIME,
                        Forecast TEXT,
                        PredictedPrice REAL
                    );", conn);
                            cmd.ExecuteNonQuery();

                            // index
                            var indexCmd = new SQLiteCommand(@"
                    CREATE INDEX IF NOT EXISTS idx_prices_isin_time
                    ON Prices (ISIN, Time);", conn);
                            indexCmd.ExecuteNonQuery();

                            // create holding totals and aggregates and history tables
                            var totalsCmd = new SQLiteCommand(@"
                    CREATE TABLE IF NOT EXISTS HoldingTotals (
                        ISIN TEXT PRIMARY KEY,
                        Currency TEXT,
                        RealizedPL REAL,
                        UnrealizedPL REAL,
                        TotalPL REAL,
                        TotalPLEUR REAL,
                        Shares REAL,
                        Updated DATETIME
                    );", conn);
                            totalsCmd.ExecuteNonQuery();

                            var aggrCmd = new SQLiteCommand(@"
                    CREATE TABLE IF NOT EXISTS Aggregates (
                        Key TEXT PRIMARY KEY,
                        Value REAL,
                        Updated DATETIME
                    );", conn);
                            aggrCmd.ExecuteNonQuery();

                            var histCmd = new SQLiteCommand(@"
                    CREATE TABLE IF NOT EXISTS TotalPLHistory (
                        Time DATETIME NOT NULL,
                        TotalPL REAL
                    );", conn);
                            histCmd.ExecuteNonQuery();

                            // Ensure optional columns exist in Prices table
                            try
                            {
                                var existingCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                using (var pragma = new SQLiteCommand("PRAGMA table_info(Prices);", conn))
                                using (var reader = pragma.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        var colName = reader.GetString(1);
                                        existingCols.Add(colName);
                                    }
                                }

                                if (!existingCols.Contains("Provider"))
                                {
                                    try { var alter = new SQLiteCommand("ALTER TABLE Prices ADD COLUMN Provider TEXT;", conn); alter.ExecuteNonQuery(); } catch { }
                                }

                                if (!existingCols.Contains("ProviderTime"))
                                {
                                    try { var alter2 = new SQLiteCommand("ALTER TABLE Prices ADD COLUMN ProviderTime DATETIME;", conn); alter2.ExecuteNonQuery(); } catch { }
                                }

                                if (!existingCols.Contains("Forecast"))
                                {
                                    try { var alter3 = new SQLiteCommand("ALTER TABLE Prices ADD COLUMN Forecast TEXT;", conn); alter3.ExecuteNonQuery(); } catch { }
                                }

                                if (!existingCols.Contains("PredictedPrice"))
                                {
                                    try { var alter4 = new SQLiteCommand("ALTER TABLE Prices ADD COLUMN PredictedPrice REAL;", conn); alter4.ExecuteNonQuery(); } catch { }
                                }
                                if (!existingCols.Contains("Shares"))
                                {
                                    try { var alter5 = new SQLiteCommand("ALTER TABLE HoldingTotals ADD COLUMN Shares REAL;", conn); alter5.ExecuteNonQuery(); } catch { }
                                }
                            }
                            catch { }
                        }

                        _isInitialized = true;
                        return;
                    }
                    catch (SQLiteException ex) when (IsBusyOrLocked(ex) && attempt < DbLockedRetryCount)
                    {
                        Thread.Sleep(50 * attempt);
                    }
                }

                Debug.WriteLine("DB: Initialization failed: database remained locked after retries.");
            }

            // Ensure NEW_Holdings table is recreated on startup to avoid stale entries
            try
            {
                RecreateNewHoldingsTable();
            }
            catch { }
        }

        // Drop and recreate NEW_Holdings as a simple table with an ISIN column.
        // This enforces a clean state on application startup when requested.
        public void RecreateNewHoldingsTable()
        {
            try
            {
                using var conn = new SQLiteConnection(_connection);
                conn.Open();
                using var tran = conn.BeginTransaction();
                try
                {
                    using (var drop = new SQLiteCommand("DROP TABLE IF EXISTS NEW_Holdings;", conn, tran))
                        drop.ExecuteNonQuery();

                    using (var create = new SQLiteCommand("CREATE TABLE IF NOT EXISTS NEW_Holdings (ISIN TEXT);", conn, tran))
                        create.ExecuteNonQuery();

                    tran.Commit();
                }
                catch
                {
                    try { tran.Rollback(); } catch { }
                    throw;
                }
            }
            catch { }
        }

        // Failure counters persistence for UI thresholds
        public void EnsureFailureCountsTable()
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                var cmd = new SQLiteCommand(@"
                    CREATE TABLE IF NOT EXISTS FailureCounts (
                        Key TEXT NOT NULL,
                        Type TEXT NOT NULL,
                        Count INTEGER NOT NULL,
                        Updated DATETIME,
                        PRIMARY KEY (Key, Type)
                    );", conn);
                cmd.ExecuteNonQuery();
            }
        }

        // Ensure NEW_CSV_ACTIVE table exists. Used to track which CSV is currently active in the UI.
        public void EnsureNewCsvActiveTable()
        {
            try
            {
                using var conn = new SQLiteConnection(_connection);
                conn.Open();
                using var cmd = new SQLiteCommand(@"
                    CREATE TABLE IF NOT EXISTS NEW_CSV_ACTIVE (
                        CSV TEXT PRIMARY KEY,
                        Active INTEGER NOT NULL DEFAULT 0,
                        Created DATETIME
                    );", conn);
                cmd.ExecuteNonQuery();
            }
            catch { }
        }

        // Mark the provided CSV (path or name) as the single active CSV. Stores only the filename.
        public void SetActiveCsv(string csvPathOrName)
        {
            if (string.IsNullOrWhiteSpace(csvPathOrName)) return;
            string csvName;
            try { csvName = Path.GetFileName(csvPathOrName); } catch { csvName = csvPathOrName; }

            for (int attempt = 1; attempt <= DbLockedRetryCount; attempt++)
            {
                try
                {
                    using var conn = new SQLiteConnection(_connection);
                    conn.Open();
                    using var tran = conn.BeginTransaction();
                    try
                    {
                        using var create = new SQLiteCommand(@"
                            CREATE TABLE IF NOT EXISTS NEW_CSV_ACTIVE (
                                CSV TEXT PRIMARY KEY,
                                Active INTEGER NOT NULL DEFAULT 0,
                                Created DATETIME
                            );", conn, tran);
                        create.ExecuteNonQuery();

                        using var updAll = new SQLiteCommand("UPDATE NEW_CSV_ACTIVE SET Active = 0;", conn, tran);
                        updAll.ExecuteNonQuery();

                        using var ins = new SQLiteCommand(@"
                            INSERT OR REPLACE INTO NEW_CSV_ACTIVE (CSV, Active, Created)
                            VALUES (@csv, 1, @created);", conn, tran);
                        ins.Parameters.AddWithValue("@csv", csvName ?? string.Empty);
                        ins.Parameters.AddWithValue("@created", DateTime.Now);
                        ins.ExecuteNonQuery();

                        tran.Commit();
                        return;
                    }
                    catch
                    {
                        try { tran.Rollback(); } catch { }
                        throw;
                    }
                }
                catch (SQLiteException ex) when (IsBusyOrLocked(ex) && attempt < DbLockedRetryCount)
                {
                    Thread.Sleep(50 * attempt);
                }
                catch { return; }
            }
        }

        public Dictionary<string, int> LoadFailureCounts(string type)
        {
            var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                EnsureFailureCountsTable();
                var cmd = new SQLiteCommand("SELECT Key, Count FROM FailureCounts WHERE Type = @type;", conn);
                cmd.Parameters.AddWithValue("@type", type ?? string.Empty);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var key = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
                        var count = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
                        if (!string.IsNullOrWhiteSpace(key)) dict[key] = count;
                    }
                }
            }
            return dict;
        }

        // Return latest stored HoldingTotals records for all ISINs.
        // Value: (updated, totalEur, shares)
        public Dictionary<string, (DateTime updated, double totalEur, double shares)> LoadHoldingTotalRecords()
        {
            var dict = new Dictionary<string, (DateTime updated, double totalEur, double shares)>(StringComparer.OrdinalIgnoreCase);
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    var cmd = new SQLiteCommand("SELECT ISIN, TotalPLEUR, Shares, Updated FROM HoldingTotals;", conn);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var isin = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
                            if (string.IsNullOrWhiteSpace(isin))
                                continue;

                            if (reader.IsDBNull(1))
                                continue;

                            var totalEur = reader.GetDouble(1);
                            var shares = reader.FieldCount > 2 && !reader.IsDBNull(2) ? reader.GetDouble(2) : double.NaN;
                            var updated = ReadNullableDateTime(reader, 3) ?? DateTime.MinValue;
                            dict[isin] = (updated, totalEur, shares);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DB: LoadHoldingTotalRecords failed: {ex.Message}");
            }

            return dict;
        }

        public void SetFailureCount(string key, int count, string type)
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                EnsureFailureCountsTable();
                var cmd = new SQLiteCommand(@"
                    INSERT OR REPLACE INTO FailureCounts (Key, Type, Count, Updated)
                    VALUES (@key, @type, @count, @updated);", conn);
                cmd.Parameters.AddWithValue("@key", key ?? string.Empty);
                cmd.Parameters.AddWithValue("@type", type ?? string.Empty);
                cmd.Parameters.AddWithValue("@count", count);
                cmd.Parameters.AddWithValue("@updated", DateTime.Now);
                cmd.ExecuteNonQuery();
            }
        }

        public void DeleteFailureCount(string key, string type)
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                EnsureFailureCountsTable();
                var cmd = new SQLiteCommand("DELETE FROM FailureCounts WHERE Key = @key AND Type = @type;", conn);
                cmd.Parameters.AddWithValue("@key", key ?? string.Empty);
                cmd.Parameters.AddWithValue("@type", type ?? string.Empty);
                cmd.ExecuteNonQuery();
            }
        }

        public void DeleteFailureCountsByType(string type)
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                EnsureFailureCountsTable();
                var cmd = new SQLiteCommand("DELETE FROM FailureCounts WHERE Type = @type;", conn);
                cmd.Parameters.AddWithValue("@type", type ?? string.Empty);
                cmd.ExecuteNonQuery();
            }
        }

        // ========================================
        // CLEANUP HELPERS
        // ========================================
        public void ClearAll()
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                using (var tran = conn.BeginTransaction())
                {
                    try
                    {
                        // remove price history
                        var cmd = new SQLiteCommand("DELETE FROM Prices;", conn, tran);
                        cmd.ExecuteNonQuery();

                        // remove persisted holding totals
                        var cmd2 = new SQLiteCommand("DELETE FROM HoldingTotals;", conn, tran);
                        cmd2.ExecuteNonQuery();

                        // remove aggregates (overall totals)
                        var cmd3 = new SQLiteCommand("DELETE FROM Aggregates;", conn, tran);
                        cmd3.ExecuteNonQuery();

                        // remove TotalPL history snapshots
                        var cmd4 = new SQLiteCommand("DELETE FROM TotalPLHistory;", conn, tran);
                        cmd4.ExecuteNonQuery();

                        tran.Commit();
                    }
                    catch
                    {
                        try { tran.Rollback(); } catch { }
                        throw;
                    }
                }
            }
        }

        // Persist per-holding totals (insert or update)
        public void UpsertHoldingTotal(string isin, string currency, double realized, double unrealized, double total, double totalEur, double shares = double.NaN)
        {
            for (int attempt = 1; attempt <= DbLockedRetryCount; attempt++)
            {
                try
                {
                    using (var conn = new SQLiteConnection(_connection))
                    {
                        conn.Open();

                        using (var busyCmd = new SQLiteCommand("PRAGMA busy_timeout = 5000;", conn))
                        {
                            busyCmd.ExecuteNonQuery();
                        }

                        var cmd = new SQLiteCommand(@"
                        INSERT OR REPLACE INTO HoldingTotals (ISIN, Currency, RealizedPL, UnrealizedPL, TotalPL, TotalPLEUR, Shares, Updated)
                        VALUES (@isin, @cur, @realized, @unreal, @total, @totalEur, @shares, @updated);", conn);
                        cmd.CommandTimeout = 5;
                        cmd.Parameters.AddWithValue("@isin", isin ?? string.Empty);
                        cmd.Parameters.AddWithValue("@cur", currency ?? string.Empty);
                        cmd.Parameters.AddWithValue("@realized", realized);
                        cmd.Parameters.AddWithValue("@unreal", unrealized);
                        cmd.Parameters.AddWithValue("@total", total);
                        cmd.Parameters.AddWithValue("@totalEur", totalEur);
                        cmd.Parameters.AddWithValue("@shares", double.IsNaN(shares) ? (object)DBNull.Value : shares);
                        cmd.Parameters.AddWithValue("@updated", DateTime.Now);
                        cmd.ExecuteNonQuery();
                        return;
                    }
                }
                catch (SQLiteException ex) when (IsBusyOrLocked(ex) && attempt < DbLockedRetryCount)
                {
                    Thread.Sleep(50 * attempt);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"DB: UpsertHoldingTotal failed for {isin}: {ex.Message}");
                    return;
                }
            }

            Debug.WriteLine($"DB: UpsertHoldingTotal failed for {isin}: database remained locked after retries.");
        }

        public void UpsertAggregate(string key, double value)
        {
            for (int attempt = 1; attempt <= DbLockedRetryCount; attempt++)
            {
                try
                {
                    using (var conn = new SQLiteConnection(_connection))
                    {
                        conn.Open();

                        using (var busyCmd = new SQLiteCommand("PRAGMA busy_timeout = 5000;", conn))
                        {
                            busyCmd.ExecuteNonQuery();
                        }

                        var cmd = new SQLiteCommand(@"
                        INSERT OR REPLACE INTO Aggregates (Key, Value, Updated)
                        VALUES (@key, @value, @updated);", conn);
                        cmd.CommandTimeout = 5;
                        cmd.Parameters.AddWithValue("@key", key ?? string.Empty);
                        cmd.Parameters.AddWithValue("@value", value);
                        cmd.Parameters.AddWithValue("@updated", DateTime.Now);
                        cmd.ExecuteNonQuery();
                        return;
                    }
                }
                catch (SQLiteException ex) when (IsBusyOrLocked(ex) && attempt < DbLockedRetryCount)
                {
                    Thread.Sleep(50 * attempt);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"DB: UpsertAggregate failed for {key}: {ex.Message}");
                    return;
                }
            }

            Debug.WriteLine($"DB: UpsertAggregate failed for {key}: database remained locked after retries.");
        }

        public void DeleteByIsin(string isin)
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                var cmd = new SQLiteCommand("DELETE FROM Prices WHERE ISIN = @isin;", conn);
                cmd.Parameters.AddWithValue("@isin", isin);
                cmd.ExecuteNonQuery();
            }

            InvalidateLoadByIsinCache(isin);
        }

        // Backfill Percent values for rows where Price != 0 but Percent is 0 or NULL.
        // Returns number of rows updated.
        public int BackfillPercentWhereZero()
        {
            int updated = 0;
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();

                    // get distinct ISINs to process
                    var isins = new List<string>();
                    using (var isinsCmd = new SQLiteCommand("SELECT DISTINCT ISIN FROM Prices;", conn))
                    using (var r = isinsCmd.ExecuteReader())
                    {
                        while (r.Read())
                        {
                            if (!r.IsDBNull(0))
                                isins.Add(r.GetString(0));
                        }
                    }

                    // prepare commands
                    using (var selectCmd = new SQLiteCommand("SELECT rowid, Price, Percent FROM Prices WHERE ISIN = @isin ORDER BY Time ASC;", conn))
                    using (var updateCmd = new SQLiteCommand("UPDATE Prices SET Percent = @percent WHERE rowid = @rowid;", conn))
                    {
                        var pIsin = selectCmd.Parameters.AddWithValue("@isin", string.Empty);
                        var pPercent = updateCmd.Parameters.AddWithValue("@percent", 0.0);
                        var pRow = updateCmd.Parameters.AddWithValue("@rowid", 0L);

                        foreach (var isin in isins)
                        {
                            pIsin.Value = isin ?? string.Empty;
                            double? lastPrice = null;
                            using (var reader = selectCmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var rowid = reader.IsDBNull(0) ? 0L : reader.GetInt64(0);
                                    var price = reader.IsDBNull(1) ? 0.0 : reader.GetDouble(1);
                                    double percent = reader.IsDBNull(2) ? double.NaN : reader.GetDouble(2);

                                    if (price != 0.0 && (double.IsNaN(percent) || percent == 0.0))
                                    {
                                        double newPercent = 0.0;
                                        if (lastPrice.HasValue && lastPrice.Value != 0.0)
                                        {
                                            newPercent = (price - lastPrice.Value) / lastPrice.Value * 100.0;
                                        }

                                        // only perform update if value differs (avoid touching unchanged rows)
                                        if (double.IsNaN(percent) || Math.Abs(newPercent - percent) > 1e-9)
                                        {
                                            pPercent.Value = newPercent;
                                            pRow.Value = rowid;
                                            try
                                            {
                                                var affected = updateCmd.ExecuteNonQuery();
                                                if (affected > 0) updated += affected;
                                            }
                                            catch { }
                                        }
                                    }

                                    // advance lastPrice if current price is non-zero
                                    if (price != 0.0)
                                        lastPrice = price;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DB: BackfillPercentWhereZero failed: {ex.Message}");
            }

            if (updated > 0)
                InvalidateLoadByIsinCache();

            return updated;
        }

        public void RemoveZeroOrNullPrices()
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                var cmd = new SQLiteCommand("DELETE FROM Prices WHERE Price IS NULL OR Price = 0;", conn);
                cmd.ExecuteNonQuery();
            }

            InvalidateLoadByIsinCache();
        }

        // Helper: delete rows with invalid prices and return number deleted
        public int DeleteZeroOrNullPricesCount()
        {
            int affected = 0;
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                var cmd = new SQLiteCommand("DELETE FROM Prices WHERE Price IS NULL OR Price = 0;", conn);
                affected = cmd.ExecuteNonQuery();
            }
            return affected;
        }

        public void RemoveDuplicateEntries()
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                // keep the earliest rowid per (ISIN, Time)
                var cmd = new SQLiteCommand(@"
                    DELETE FROM Prices
                    WHERE rowid NOT IN (
                        SELECT MIN(rowid) FROM Prices GROUP BY ISIN, Time
                    );", conn);
                cmd.ExecuteNonQuery();
            }

            InvalidateLoadByIsinCache();
        }

        // Remove duplicates and return number of rows deleted
        public int RemoveDuplicateEntriesCount()
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                var cmd = new SQLiteCommand(@"
                    DELETE FROM Prices
                    WHERE rowid NOT IN (
                        SELECT MIN(rowid) FROM Prices GROUP BY ISIN, Time
                    );", conn);
                return cmd.ExecuteNonQuery();
            }
        }

        public void Vacuum()
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                var cmd = new SQLiteCommand("VACUUM;", conn);
                cmd.ExecuteNonQuery();
            }
        }

        public void CleanDatabase()
        {
            try
            {
                RemoveZeroOrNullPrices();
                RemoveDuplicateEntries();
                Vacuum();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DB: CleanDatabase failed: {ex.Message}");
                throw;
            }
        }

        // Return last stored TotalPLEUR for an ISIN, or null if not present
        public double? GetHoldingTotalEur(string isin)
        {
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    var cmd = new SQLiteCommand("SELECT TotalPLEUR FROM HoldingTotals WHERE ISIN = @isin;", conn);
                    cmd.Parameters.AddWithValue("@isin", isin ?? string.Empty);
                    var res = cmd.ExecuteScalar();
                    if (res == null || res == DBNull.Value) return null;
                    return Convert.ToDouble(res);
                }
            }
            catch { return null; }
        }

        // Return last stored TotalPLEUR and Updated timestamp for an ISIN, or (null,null) if not present
        public (DateTime? updated, double? totalEur) GetHoldingTotalRecord(string isin)
        {
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    var cmd = new SQLiteCommand("SELECT TotalPLEUR, Updated FROM HoldingTotals WHERE ISIN = @isin;", conn);
                    cmd.Parameters.AddWithValue("@isin", isin ?? string.Empty);
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var total = reader.IsDBNull(0) ? (double?)null : reader.GetDouble(0);
                            var updated = ReadNullableDateTime(reader, 1);
                            return (updated, total);
                        }
                    }
                }
                return (null, null);
            }
            catch { return (null, null); }
        }

        // Return the latest valid price (Price > 0) per ISIN in a single efficient query.
        // Returns a dictionary keyed by ISIN with the most recent StockPoint for each.
        public Dictionary<string, StockPoint> LoadLatestPricePerIsin()
        {
            var dict = new Dictionary<string, StockPoint>(StringComparer.OrdinalIgnoreCase);
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    var cmd = new SQLiteCommand(@"
                        SELECT p.ISIN, p.Time, p.Price, p.Percent, p.Provider, p.ProviderTime
                        FROM Prices p
                        INNER JOIN (
                            SELECT ISIN, MAX(Time) AS MaxTime
                            FROM Prices
                            WHERE Price IS NOT NULL AND Price > 0
                            GROUP BY ISIN
                        ) latest ON p.ISIN = latest.ISIN AND p.Time = latest.MaxTime
                        WHERE p.Price IS NOT NULL AND p.Price > 0;", conn);
                    using (var reader = cmd.ExecuteReader())
                    {
                            while (reader.Read())
                        {
                            var sp = new StockPoint
                            {
                                ISIN = reader.GetString(0),
                                Time = ReadDateTimeSafe(reader, 1),
                                Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                                Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                                Provider = reader.FieldCount > 4 && !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                                ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? ReadNullableDateTime(reader, 5) : null
                            };
                            dict[sp.ISIN] = sp;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DB: LoadLatestPricePerIsin failed: {ex.Message}");
            }
            return dict;
        }

        // ========================================
        // INSERT
        // ========================================
        public void Insert(StockPoint point)
        {
            try
            {
                // delete only trailing equal rows (by Price and Percent) for this ISIN, then insert new row
                const double EPS = 1e-2;
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    using (var tran = conn.BeginTransaction())
                    {
                        try
                        {
                            var block = new List<long>();
                            using (var sel = new SQLiteCommand("SELECT rowid, Price, Percent FROM Prices WHERE ISIN = @isin ORDER BY Time DESC;", conn, tran))
                            {
                                sel.Parameters.AddWithValue("@isin", point.ISIN ?? string.Empty);
                                using var reader = sel.ExecuteReader();
                                while (reader.Read())
                                {
                                    var rowid = reader.IsDBNull(0) ? 0L : reader.GetInt64(0);
                                    double price = reader.IsDBNull(1) ? double.NaN : reader.GetDouble(1);
                                    double percent = reader.IsDBNull(2) ? double.NaN : reader.GetDouble(2);
                                    bool priceEq = !double.IsNaN(price) && !double.IsNaN(point.Price) ? Math.Abs(price - point.Price) <= EPS : (double.IsNaN(price) && double.IsNaN(point.Price));
                                    bool percentEq = !double.IsNaN(percent) && !double.IsNaN(point.Percent) ? Math.Abs(percent - point.Percent) <= EPS : (double.IsNaN(percent) && double.IsNaN(point.Percent));
                                    if (priceEq && percentEq && rowid > 0)
                                    {
                                        block.Add(rowid);
                                    }
                                    else
                                    {
                                        break; // stop at first non-equal row
                                    }
                                }
                            }

                            if (block.Count > 0)
                            {
                                // update latest row to new values and delete remaining in the block
                                var latest = block[0];
                                using var upd = new SQLiteCommand(@"UPDATE Prices SET Time = @time, Price = @price, Percent = @percent, Provider = @provider, ProviderTime = @providertime, Forecast = @forecast, PredictedPrice = @predicted WHERE rowid = @rowid;", conn, tran);
                                upd.Parameters.AddWithValue("@time", point.Time);
                                upd.Parameters.AddWithValue("@price", point.Price);
                                upd.Parameters.AddWithValue("@percent", point.Percent);
                                upd.Parameters.AddWithValue("@provider", point.Provider ?? string.Empty);
                                upd.Parameters.AddWithValue("@providertime", (object)point.ProviderTime ?? DBNull.Value);
                                upd.Parameters.AddWithValue("@forecast", point.Forecast ?? string.Empty);
                                upd.Parameters.AddWithValue("@predicted", point.PredictedPrice);
                                upd.Parameters.AddWithValue("@rowid", latest);
                                upd.ExecuteNonQuery();

                                if (block.Count > 1)
                                {
                                    var others = block.Skip(1).ToList();
                                    var inParam = string.Join(",", others.Select((_, i) => $"@p{i}"));
                                    using var del = new SQLiteCommand($"DELETE FROM Prices WHERE rowid IN ({inParam});", conn, tran);
                                    for (int i = 0; i < others.Count; i++)
                                        del.Parameters.AddWithValue($"@p{i}", others[i]);
                                    del.ExecuteNonQuery();
                                }
                            }
                            else
                            {
                                using var cmd = new SQLiteCommand(
                                    "INSERT INTO Prices (ISIN, Time, Price, Percent, Provider, ProviderTime, Forecast, PredictedPrice) VALUES (@isin, @time, @price, @percent, @provider, @providertime, @forecast, @predicted)",
                                    conn, tran);

                                cmd.Parameters.AddWithValue("@isin", point.ISIN);
                                cmd.Parameters.AddWithValue("@time", point.Time);
                                cmd.Parameters.AddWithValue("@price", point.Price);
                                cmd.Parameters.AddWithValue("@percent", point.Percent);
                                cmd.Parameters.AddWithValue("@provider", point.Provider ?? string.Empty);
                                cmd.Parameters.AddWithValue("@providertime", (object)point.ProviderTime ?? DBNull.Value);
                                cmd.Parameters.AddWithValue("@forecast", point.Forecast ?? string.Empty);
                                cmd.Parameters.AddWithValue("@predicted", point.PredictedPrice);

                                cmd.ExecuteNonQuery();
                            }

                            tran.Commit();
                        }
                        catch
                        {
                            try { tran.Rollback(); } catch { }
                            throw;
                        }
                    }
                }

                Debug.WriteLine($"DB: Inserted {point.ISIN} @ {point.Time:O} price={point.Price} percent={point.Percent} provider={point.Provider}");
                InvalidateLoadByIsinCache(point?.ISIN);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DB: Insert failed for {point.ISIN} at {point.Time:O}: {ex.Message}");
                throw;
            }
        }



        // ========================================
        // LOAD ALL
        // ========================================
        public List<StockPoint> LoadAll()
        {
            var list = new List<StockPoint>();

            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();

                var cmd = new SQLiteCommand(@"
                     SELECT ISIN, Time, Price, Percent, Provider, ProviderTime, Forecast, PredictedPrice
                     FROM Prices
                     ORDER BY Time ASC;", conn);

                using (var reader = cmd.ExecuteReader())
                {
                        while (reader.Read())
                        {
                            list.Add(new StockPoint
                            {
                                ISIN = reader.GetString(0),
                                Time = ReadDateTimeSafe(reader, 1),
                                Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                                Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                                Provider = reader.FieldCount > 4 && !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                                ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? ReadNullableDateTime(reader, 5) : null,
                                Forecast = reader.FieldCount > 6 && !reader.IsDBNull(6) ? reader.GetString(6) : string.Empty,
                                PredictedPrice = reader.FieldCount > 7 && !reader.IsDBNull(7) ? reader.GetDouble(7) : 0.0
                            });
                        }
                }
            }

            return list;
        }

        // Load TotalPL history (time, total) ordered ascending by time. Limit to maxRows when > 0.
        public List<Tuple<DateTime, double>> LoadTotalPLHistory(int maxRows = 0)
        {
            var list = new List<Tuple<DateTime, double>>();
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                // When a maxRows limit is requested, prefer returning the most recent rows.
                // To keep the API consistent (ascending order), we fetch descending and then reverse.
                string sql;
                if (maxRows > 0)
                    sql = "SELECT Time, TotalPL FROM TotalPLHistory ORDER BY Time DESC LIMIT " + maxRows.ToString();
                else
                    sql = "SELECT Time, TotalPL FROM TotalPLHistory ORDER BY Time ASC";

                var cmd = new SQLiteCommand(sql, conn);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var time = ReadDateTimeSafe(reader, 0);
                        var total = reader.IsDBNull(1) ? 0.0 : reader.GetDouble(1);
                        list.Add(new Tuple<DateTime, double>(time, total));
                    }
                }

                if (maxRows > 0)
                {
                    // currently list is in DESC order (newest first) — reverse to ASC to preserve previous behavior
                    list.Reverse();
                }
            }
            return list;
        }

        // Load all points inserted after a given timestamp (for incremental refresh)
        public List<StockPoint> LoadSince(DateTime since)
        {
            var list = new List<StockPoint>();
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                var cmd = new SQLiteCommand(@"
                     SELECT ISIN, Time, Price, Percent, Provider, ProviderTime, Forecast, PredictedPrice
                     FROM Prices
                     WHERE Time >= @since
                     ORDER BY Time ASC;", conn);
                cmd.Parameters.AddWithValue("@since", since);
                using (var reader = cmd.ExecuteReader())
                {
                                while (reader.Read())
                                {
                                    list.Add(new StockPoint
                                    {
                                        ISIN = reader.GetString(0),
                                        Time = ReadDateTimeSafe(reader, 1),
                                        Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                                        Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                                        Provider = reader.FieldCount > 4 && !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                                        ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? ReadNullableDateTime(reader, 5) : null,
                                        Forecast = reader.FieldCount > 6 && !reader.IsDBNull(6) ? reader.GetString(6) : string.Empty,
                                        PredictedPrice = reader.FieldCount > 7 && !reader.IsDBNull(7) ? reader.GetDouble(7) : 0.0
                                    });
                                }
                }
            }
            return list;
        }

        // ========================================
        // OPTIONAL: Load by ISIN (empfohlen!)
        // ========================================
        public List<StockPoint> LoadByIsin(string isin)
        {
            if (string.IsNullOrWhiteSpace(isin))
                return new List<StockPoint>();

            var cacheKey = isin.Trim();
            lock (_loadByIsinCacheLock)
            {
                var nowUtc = DateTime.UtcNow;
                if (_loadByIsinCache.TryGetValue(cacheKey, out var cached)
                    && (nowUtc - cached.fetchedUtc) <= LoadByIsinCacheTtl)
                {
                    return new List<StockPoint>(cached.rows);
                }
            }

            var list = new List<StockPoint>();

            for (int attempt = 1; attempt <= DbLockedRetryCount; attempt++)
            {
                try
                {
                    using (var conn = new SQLiteConnection(_connection))
                    {
                        conn.Open();

                        using (var busyCmd = new SQLiteCommand("PRAGMA busy_timeout = 5000;", conn))
                        {
                            busyCmd.ExecuteNonQuery();
                        }

                        var cmd = new SQLiteCommand(@"
                     SELECT ISIN, Time, Price, Percent, Provider, ProviderTime, Forecast, PredictedPrice
                     FROM Prices
                     WHERE ISIN = @isin
                     ORDER BY Time ASC;", conn);
                        cmd.CommandTimeout = 5;
                        cmd.Parameters.AddWithValue("@isin", cacheKey);

                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                list.Add(new StockPoint
                                {
                                    ISIN = reader.GetString(0),
                                    Time = ReadDateTimeSafe(reader, 1),
                                    Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                                    Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                                    Provider = reader.FieldCount > 4 && !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                                    ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? ReadNullableDateTime(reader, 5) : null,
                                    Forecast = reader.FieldCount > 6 && !reader.IsDBNull(6) ? reader.GetString(6) : string.Empty,
                                    PredictedPrice = reader.FieldCount > 7 && !reader.IsDBNull(7) ? reader.GetDouble(7) : 0.0
                                });
                            }
                        }

                        lock (_loadByIsinCacheLock)
                        {
                            _loadByIsinCache[cacheKey] = (DateTime.UtcNow, list);
                        }

                        return new List<StockPoint>(list);
                    }
                }
                catch (SQLiteException ex) when (IsBusyOrLocked(ex) && attempt < DbLockedRetryCount)
                {
                    Thread.Sleep(50 * attempt);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"DB: LoadByIsin failed for {isin}: {ex.Message}");
                    return list;
                }
            }

            Debug.WriteLine($"DB: LoadByIsin failed for {isin}: database remained locked after retries.");

            return list;
        }
    }
}
