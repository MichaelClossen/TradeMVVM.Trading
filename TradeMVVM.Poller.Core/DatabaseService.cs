using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Data.SQLite;
using Npgsql;
using System.Data;
using TradeMVVM.Domain;

namespace TradeMVVM.Poller.Core
{
    public class DatabaseService
    {
        private readonly string _connection;

        private bool _usePostgres = false;

        public DatabaseService()
        {
            // allow overriding via environment variable or appsettings when hosted
            var env = Environment.GetEnvironmentVariable("TRADE_DB_CONNECTION");
            _connection = string.IsNullOrWhiteSpace(env) ? "Data Source=trading.db" : env;
            // detect provider: simple heuristic - if connection string contains "Host=" or "Username=" assume Postgres
            if (!string.IsNullOrWhiteSpace(env) && (env.Contains("Host=", StringComparison.OrdinalIgnoreCase) || env.Contains("Username=", StringComparison.OrdinalIgnoreCase)))
            {
                _usePostgres = true;
            }

            if (_usePostgres)
            {
                using (var conn = new NpgsqlConnection(_connection))
                {
                    conn.Open();

                    using var cmd = new NpgsqlCommand(@"
                        CREATE TABLE IF NOT EXISTS Prices (
                            ISIN TEXT NOT NULL,
                            Time TIMESTAMP WITH TIME ZONE NOT NULL,
                            Price DOUBLE PRECISION,
                            Percent DOUBLE PRECISION,
                            Provider TEXT,
                            ProviderTime TIMESTAMP WITH TIME ZONE,
                            Forecast TEXT,
                            PredictedPrice DOUBLE PRECISION
                        );", conn);
                    cmd.ExecuteNonQuery();

                    using var idx = new NpgsqlCommand(@"
                        CREATE INDEX IF NOT EXISTS idx_prices_isin_time ON Prices (ISIN, Time);", conn);
                    idx.ExecuteNonQuery();

                    // create control table
                    using var ctrl = new NpgsqlCommand(@"
                        CREATE TABLE IF NOT EXISTS PollingControl (
                            Id INTEGER PRIMARY KEY,
                            PollingEnabled INTEGER NOT NULL DEFAULT 1,
                            LastHeartbeat TIMESTAMP WITH TIME ZONE
                        );", conn);
                    ctrl.ExecuteNonQuery();

                    using var ensure = new NpgsqlCommand("INSERT INTO PollingControl (Id, PollingEnabled) SELECT 1,1 WHERE NOT EXISTS (SELECT 1 FROM PollingControl WHERE Id = 1);", conn);
                    ensure.ExecuteNonQuery();
                }
            }
            else
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();

                var cmd = new SQLiteCommand(@"
                    CREATE TABLE IF NOT EXISTS Prices (
                        ISIN TEXT NOT NULL,
                        Time DATETIME NOT NULL,
                        Price REAL,
                        Percent REAL,
                        Provider TEXT,
                        ProviderTime DATETIME
                    );", conn);

                cmd.ExecuteNonQuery();

                // Optional: Index für Performance
                var indexCmd = new SQLiteCommand(@"
                    CREATE INDEX IF NOT EXISTS idx_prices_isin_time
                    ON Prices (ISIN, Time);", conn);

                indexCmd.ExecuteNonQuery();

                // Ensure Provider column exists for backward compatibility
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
                }
                catch { }
                // Ensure control table exists for Poller server control
                try
                {
                    var ctrl = new SQLiteCommand(@"
                        CREATE TABLE IF NOT EXISTS PollingControl (
                            Id INTEGER PRIMARY KEY CHECK (Id = 1),
                            PollingEnabled INTEGER NOT NULL DEFAULT 1,
                            LastHeartbeat DATETIME
                        );", conn);
                    ctrl.ExecuteNonQuery();

                    // ensure single row exists
                    var ensure = new SQLiteCommand("INSERT OR IGNORE INTO PollingControl (Id, PollingEnabled) VALUES (1, 1);", conn);
                    ensure.ExecuteNonQuery();
                }
                catch { }
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

                        // also remove any persisted holding totals and aggregates if present
                        try { new SQLiteCommand("DELETE FROM HoldingTotals;", conn, tran).ExecuteNonQuery(); } catch { }
                        try { new SQLiteCommand("DELETE FROM Aggregates;", conn, tran).ExecuteNonQuery(); } catch { }
                        try { new SQLiteCommand("DELETE FROM TotalPLHistory;", conn, tran).ExecuteNonQuery(); } catch { }

                        tran.Commit();
                    }

        // ========================================
        // Poller control helpers
        // ========================================
        public bool IsPollingEnabled()
        {
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    var cmd = new SQLiteCommand("SELECT PollingEnabled FROM PollingControl WHERE Id = 1;", conn);
                    var v = cmd.ExecuteScalar();
                    if (v == null) return true;
                    return Convert.ToInt32(v) != 0;
                }
            }
            catch { return true; }
        }

        public void SetHeartbeat(DateTime dt)
        {
            try
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();
                    var cmd = new SQLiteCommand("UPDATE PollingControl SET LastHeartbeat = @t WHERE Id = 1;", conn);
                    cmd.Parameters.AddWithValue("@t", dt);
                    cmd.ExecuteNonQuery();
                }
            }
            catch { }
        }
                    catch
                    {
                        try { tran.Rollback(); } catch { }
                        throw;
                    }
                }
            }
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
        }

        public void RemoveZeroOrNullPrices()
        {
            using (var conn = new SQLiteConnection(_connection))
            {
                conn.Open();
                var cmd = new SQLiteCommand("DELETE FROM Prices WHERE Price IS NULL OR Price = 0;", conn);
                cmd.ExecuteNonQuery();
            }
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

        // ========================================
        // INSERT
        // ========================================
        public void Insert(StockPoint point)
        {
            try
            {
                if (_usePostgres)
                {
                    using var conn = new NpgsqlConnection(_connection);
                    conn.Open();
                    using var cmd = new NpgsqlCommand("INSERT INTO Prices (ISIN, Time, Price, Percent, Provider, ProviderTime, Forecast, PredictedPrice) VALUES (@isin, @time, @price, @percent, @provider, @providertime, @forecast, @predicted)", conn);
                    cmd.Parameters.AddWithValue("@isin", NpgsqlTypes.NpgsqlDbType.Text, point.ISIN);
                    cmd.Parameters.AddWithValue("@time", NpgsqlTypes.NpgsqlDbType.TimestampTz, point.Time);
                    cmd.Parameters.AddWithValue("@price", NpgsqlTypes.NpgsqlDbType.Double, point.Price);
                    cmd.Parameters.AddWithValue("@percent", NpgsqlTypes.NpgsqlDbType.Double, point.Percent);
                    cmd.Parameters.AddWithValue("@provider", NpgsqlTypes.NpgsqlDbType.Text, (object)point.Provider ?? string.Empty);
                    cmd.Parameters.AddWithValue("@providertime", (object)point.ProviderTime ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@forecast", NpgsqlTypes.NpgsqlDbType.Text, (object)point.Forecast ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@predicted", NpgsqlTypes.NpgsqlDbType.Double, point.PredictedPrice);
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    using (var conn = new SQLiteConnection(_connection))
                    {
                        conn.Open();

                        var cmd = new SQLiteCommand(
                            "INSERT INTO Prices (ISIN, Time, Price, Percent, Provider, ProviderTime) VALUES (@isin, @time, @price, @percent, @provider, @providertime)",
                            conn);

                        cmd.Parameters.AddWithValue("@isin", point.ISIN);
                        cmd.Parameters.AddWithValue("@time", point.Time);
                        cmd.Parameters.AddWithValue("@price", point.Price);
                        cmd.Parameters.AddWithValue("@percent", point.Percent);
                        cmd.Parameters.AddWithValue("@provider", point.Provider ?? string.Empty);
                        cmd.Parameters.AddWithValue("@providertime", (object)point.ProviderTime ?? DBNull.Value);

                        cmd.ExecuteNonQuery();
                    }
                }

                Debug.WriteLine($"DB: Inserted {point.ISIN} @ {point.Time:O} price={point.Price} percent={point.Percent} provider={point.Provider}");
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

            if (_usePostgres)
            {
                using var conn = new NpgsqlConnection(_connection);
                conn.Open();
                using var cmd = new NpgsqlCommand("SELECT ISIN, Time, Price, Percent, Provider, ProviderTime FROM Prices ORDER BY Time ASC", conn);
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    list.Add(new StockPoint
                    {
                        ISIN = reader.GetString(0),
                        Time = reader.GetDateTime(1),
                        Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                        Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                        Provider = !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                        ProviderTime = !reader.IsDBNull(5) ? (DateTime?)reader.GetDateTime(5) : null
                    });
                }
            }
            else
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();

                    var cmd = new SQLiteCommand(@"
                         SELECT ISIN, Time, Price, Percent, Provider, ProviderTime
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
                                ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? (DateTime?)reader.GetDateTime(5) : null
                            });
                        }
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
            var list = new List<StockPoint>();
            if (_usePostgres)
            {
                using var conn = new NpgsqlConnection(_connection);
                conn.Open();
                using var cmd = new NpgsqlCommand("SELECT ISIN, Time, Price, Percent, Provider, ProviderTime FROM Prices WHERE ISIN = @isin ORDER BY Time ASC", conn);
                cmd.Parameters.AddWithValue("@isin", isin);
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    list.Add(new StockPoint
                    {
                        ISIN = reader.GetString(0),
                        Time = reader.GetDateTime(1),
                        Price = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                        Percent = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                        Provider = !reader.IsDBNull(4) ? reader.GetString(4) : string.Empty,
                        ProviderTime = !reader.IsDBNull(5) ? (DateTime?)reader.GetDateTime(5) : null
                    });
                }
            }
            else
            {
                using (var conn = new SQLiteConnection(_connection))
                {
                    conn.Open();

                    var cmd = new SQLiteCommand(@"
                         SELECT ISIN, Time, Price, Percent, Provider, ProviderTime
                         FROM Prices
                         WHERE ISIN = @isin
                         ORDER BY Time ASC;", conn);

                    cmd.Parameters.AddWithValue("@isin", isin);

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
                                ProviderTime = reader.FieldCount > 5 && !reader.IsDBNull(5) ? (DateTime?)reader.GetDateTime(5) : null
                            });
                        }
                    }
                }
            }

            return list;
        }
    }
}
