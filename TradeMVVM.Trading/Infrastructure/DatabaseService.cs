using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Threading;
using TradeMVVM.Domain;

namespace TradeMVVM.Trading.Services
{
    public class DatabaseService
    {
        private readonly string _connection = "Data Source=trading.db";
        private const int DbLockedRetryCount = 5;
        private static readonly TimeSpan LoadByIsinCacheTtl = TimeSpan.FromSeconds(5);
        private static readonly object _initLock = new object();
        private static bool _isInitialized = false;
        private readonly object _loadByIsinCacheLock = new object();
        private readonly Dictionary<string, (DateTime fetchedUtc, List<StockPoint> rows)> _loadByIsinCache
            = new Dictionary<string, (DateTime, List<StockPoint>)>(StringComparer.OrdinalIgnoreCase);

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

                        var cmd = new SQLiteCommand("INSERT INTO TotalPLHistory (Time, TotalPL) VALUES (@time, @total);", conn);
                        cmd.Parameters.AddWithValue("@time", time);
                        cmd.Parameters.AddWithValue("@total", totalPl);
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
                    Debug.WriteLine($"DB: InsertTotalPLHistory failed: {ex.Message}");
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
                            var time = reader.IsDBNull(0) ? DateTime.MinValue : reader.GetDateTime(0);
                            var total = reader.IsDBNull(1) ? 0.0 : reader.GetDouble(1);
                            list.Add(new Tuple<DateTime, double>(time, total));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DB: LoadTotalPLHistoryBetween failed: {ex.Message}");
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
                Debug.WriteLine($"DB: ReplaceTotalPLHistoryRange failed: {ex.Message}");
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

        public DatabaseService()
        {
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
        public Dictionary<string, (DateTime updated, double totalEur)> LoadHoldingTotalRecords()
        {
            var dict = new Dictionary<string, (DateTime updated, double totalEur)>(StringComparer.OrdinalIgnoreCase);
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    var cmd = new SQLiteCommand("SELECT ISIN, TotalPLEUR, Updated FROM HoldingTotals;", conn);
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
                            var updated = reader.IsDBNull(2) ? DateTime.MinValue : reader.GetDateTime(2);
                            dict[isin] = (updated, totalEur);
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
        public void UpsertHoldingTotal(string isin, string currency, double realized, double unrealized, double total, double totalEur)
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
                        INSERT OR REPLACE INTO HoldingTotals (ISIN, Currency, RealizedPL, UnrealizedPL, TotalPL, TotalPLEUR, Updated)
                        VALUES (@isin, @cur, @realized, @unreal, @total, @totalEur, @updated);", conn);
                        cmd.CommandTimeout = 5;
                        cmd.Parameters.AddWithValue("@isin", isin ?? string.Empty);
                        cmd.Parameters.AddWithValue("@cur", currency ?? string.Empty);
                        cmd.Parameters.AddWithValue("@realized", realized);
                        cmd.Parameters.AddWithValue("@unreal", unrealized);
                        cmd.Parameters.AddWithValue("@total", total);
                        cmd.Parameters.AddWithValue("@totalEur", totalEur);
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
                            var updated = reader.IsDBNull(1) ? (DateTime?)null : reader.GetDateTime(1);
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
                                Time = reader.GetDateTime(1),
                                Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                                Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                                Provider = reader.FieldCount > 4 && !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                                ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? (DateTime?)reader.GetDateTime(5) : null
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
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();

                    var cmd = new SQLiteCommand(
                        "INSERT INTO Prices (ISIN, Time, Price, Percent, Provider, ProviderTime, Forecast, PredictedPrice) VALUES (@isin, @time, @price, @percent, @provider, @providertime, @forecast, @predicted)",
                        conn);

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
                            Time = reader.GetDateTime(1),
                            Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                            Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                            Provider = reader.FieldCount > 4 && !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                            ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? (DateTime?)reader.GetDateTime(5) : null,
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
                string sql = "SELECT Time, TotalPL FROM TotalPLHistory ORDER BY Time ASC";
                if (maxRows > 0) sql += " LIMIT " + maxRows.ToString();
                var cmd = new SQLiteCommand(sql, conn);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var time = reader.IsDBNull(0) ? DateTime.MinValue : reader.GetDateTime(0);
                        var total = reader.IsDBNull(1) ? 0.0 : reader.GetDouble(1);
                        list.Add(new Tuple<DateTime, double>(time, total));
                    }
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
                            Time = reader.GetDateTime(1),
                            Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                            Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                            Provider = reader.FieldCount > 4 && !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                            ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? (DateTime?)reader.GetDateTime(5) : null,
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
                                    Time = reader.GetDateTime(1),
                                    Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                                    Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                                    Provider = reader.FieldCount > 4 && !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                                    ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? (DateTime?)reader.GetDateTime(5) : null,
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
