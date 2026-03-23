using Microsoft.Data.Sqlite;
using System;
using System.IO;

static class DbHelpers
{
    public static int ReadIsinsCountFromDatabase(string dbPath)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) return 0;
            var csb = new SqliteConnectionStringBuilder { DataSource = dbPath, Mode = SqliteOpenMode.ReadOnly };
            using var conn = new SqliteConnection(csb.ToString());
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(1) FROM NEW_Holdings WHERE isin IS NOT NULL;";
            var v = cmd.ExecuteScalar();
            conn.Close();
            if (v == null || v == DBNull.Value) return 0;
            try { return Convert.ToInt32(v); } catch { return 0; }
        }
        catch { return 0; }
    }
}
