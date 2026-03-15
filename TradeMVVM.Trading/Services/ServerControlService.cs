using System;
using System.Data.SQLite;
using Npgsql;

namespace TradeMVVM.Trading.Services
{
    // Minimal service to check poller server heartbeat and set PollingEnabled flag.
    public class ServerControlService
    {
        private readonly string _connection;
        private readonly bool _usePostgres;

        // expose connection for diagnostics
        public string ConnectionString => _connection;

        public ServerControlService()
        {
            var env = Environment.GetEnvironmentVariable("TRADE_DB_CONNECTION");
            // Default to the absolute development DB path so GUI and poller use the same file during development
            _connection = string.IsNullOrWhiteSpace(env) ? "Data Source=C:\\Users\\micha\\Desktop\\Trade\\trading.db" : env;
            _usePostgres = !string.IsNullOrWhiteSpace(env) && (env.Contains("Host=", StringComparison.OrdinalIgnoreCase) || env.Contains("Username=", StringComparison.OrdinalIgnoreCase));
        }

        public DateTime? GetLastHeartbeat()
        {
            try
            {
                if (_usePostgres)
                {
                    using var conn = new NpgsqlConnection(_connection);
                    conn.Open();
                    using var cmd = new NpgsqlCommand("SELECT LastHeartbeat FROM PollingControl WHERE Id = 1", conn);
                    var v = cmd.ExecuteScalar();
                    if (v == null || v == DBNull.Value) return null;
                    // prefer parsing as DateTimeOffset (handles offsets like +01:00 or trailing Z), then return UTC DateTime
                    var s = v.ToString();
                    // parse as DateTimeOffset first so offsets are respected, otherwise parse as local ISO format
                    // Prefer parsing as DateTimeOffset. Treat unspecified offsets as UTC so legacy values without 'Z' are
                    // interpreted as UTC timestamps rather than local time (the DB historically stored UTC).
                    // Parse and return a local DateTime so the GUI and DB show the same human-readable time.
                    if (System.DateTimeOffset.TryParse(s, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var dto))
                        return dto.LocalDateTime;
                    if (DateTime.TryParse(s, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeLocal, out var dtLocal))
                        return DateTime.SpecifyKind(dtLocal, DateTimeKind.Local);
                    return Convert.ToDateTime(v);
                }
                else
                {
                    using var conn = new SQLiteConnection(_connection);
                    conn.Open();
                    using var cmd = new SQLiteCommand("SELECT LastHeartbeat FROM PollingControl WHERE Id = 1", conn);
                    var v = cmd.ExecuteScalar();
                    if (v == null || v == DBNull.Value) return null;
                    var s = v.ToString();
                    if (System.DateTimeOffset.TryParse(s, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var dto))
                        return dto.LocalDateTime;
                    if (DateTime.TryParse(s, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeLocal, out var dtLocal))
                        return DateTime.SpecifyKind(dtLocal, DateTimeKind.Local);
                    return Convert.ToDateTime(v);
                }
            }
            catch { return null; }
        }

        public bool IsPollingEnabled()
        {
            try
            {
                if (_usePostgres)
                {
                    using var conn = new NpgsqlConnection(_connection);
                    conn.Open();
                    using var cmd = new NpgsqlCommand("SELECT PollingEnabled FROM PollingControl WHERE Id = 1", conn);
                    var v = cmd.ExecuteScalar();
                    if (v == null) return true;
                    return Convert.ToInt32(v) != 0;
                }
                else
                {
                    using var conn = new SQLiteConnection(_connection);
                    conn.Open();
                    using var cmd = new SQLiteCommand("SELECT PollingEnabled FROM PollingControl WHERE Id = 1", conn);
                    var v = cmd.ExecuteScalar();
                    if (v == null) return true;
                    return Convert.ToInt32(v) != 0;
                }
            }
            catch { return true; }
        }

        public void SetPollingEnabled(bool enabled)
        {
            try
            {
                if (_usePostgres)
                {
                    using var conn = new NpgsqlConnection(_connection);
                    conn.Open();
                    using var cmd = new NpgsqlCommand("UPDATE PollingControl SET PollingEnabled = @v WHERE Id = 1", conn);
                    cmd.Parameters.AddWithValue("@v", enabled ? 1 : 0);
                    if (cmd.ExecuteNonQuery() == 0)
                    {
                        using var ins = new NpgsqlCommand("INSERT INTO PollingControl (Id, PollingEnabled) VALUES (1, @v)", conn);
                        ins.Parameters.AddWithValue("@v", enabled ? 1 : 0);
                        ins.ExecuteNonQuery();
                    }
                }
                else
                {
                    using var conn = new SQLiteConnection(_connection);
                    conn.Open();
                    using var cmd = new SQLiteCommand("UPDATE PollingControl SET PollingEnabled = @v WHERE Id = 1", conn);
                    cmd.Parameters.AddWithValue("@v", enabled ? 1 : 0);
                    if (cmd.ExecuteNonQuery() == 0)
                    {
                        using var ins = new SQLiteCommand("INSERT OR REPLACE INTO PollingControl (Id, PollingEnabled) VALUES (1, @v)", conn);
                        ins.Parameters.AddWithValue("@v", enabled ? 1 : 0);
                        ins.ExecuteNonQuery();
                    }
                }
            }
            catch { }
        }
    }
}
