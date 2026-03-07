using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Data.SQLite;
using TradeMVVM.Domain;

namespace TradeMVVM.Poller.Core
{
    public class DatabaseService
    {
        private readonly string _connection = "Data Source=trading.db";

        public DatabaseService()
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

            return list;
        }

        // ========================================
        // OPTIONAL: Load by ISIN (empfohlen!)
        // ========================================
        public List<StockPoint> LoadByIsin(string isin)
        {
            var list = new List<StockPoint>();

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

            return list;
        }
    }
}
