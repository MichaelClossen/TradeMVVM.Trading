using System;
using System.Data.SQLite;
using System.IO;

string connection;
string chosen = null;
if (args.Length > 0 && !string.IsNullOrWhiteSpace(args[0]))
{
    chosen = args[0];
}
else
{
    // Try to find legacy path
    var candidates = new string[] {
        Path.Combine(Directory.GetCurrentDirectory(), "trading.db"),
        Path.Combine(Directory.GetCurrentDirectory(), "..", "TradeMVVM.Trading", "bin", "Debug", "net48", "trading.db"),
        Path.Combine(Directory.GetCurrentDirectory(), "TradeMVVM.Trading", "bin", "Debug", "net48", "trading.db"),
        Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "TradeMVVM.Trading", "bin", "Debug", "net48", "trading.db")
    };
    foreach (var c in candidates)
    {
        if (File.Exists(c)) { chosen = c; break; }
    }
}
if (chosen == null) chosen = Path.Combine(Directory.GetCurrentDirectory(), "trading.db");
connection = $"Data Source={chosen}";
Console.WriteLine("Using DB: " + chosen);

try
{
    using var conn = new SQLiteConnection(connection);
    conn.Open();
    using var cmd = new SQLiteCommand("SELECT COUNT(*) FROM Prices;", conn);
    var count = Convert.ToInt32(cmd.ExecuteScalar());
    Console.WriteLine($"Prices rows: {count}");

    using var cmd2 = new SQLiteCommand("SELECT MIN(Time), MAX(Time) FROM Prices;", conn);
    using var reader = cmd2.ExecuteReader();
    if (reader.Read())
    {
        Console.WriteLine($"Min Time: {(reader.IsDBNull(0) ? "NULL" : reader.GetValue(0).ToString())}");
        Console.WriteLine($"Max Time: {(reader.IsDBNull(1) ? "NULL" : reader.GetValue(1).ToString())}");
    }
}
catch (Exception ex)
{
    Console.WriteLine("DB check failed: " + ex.Message);
}
