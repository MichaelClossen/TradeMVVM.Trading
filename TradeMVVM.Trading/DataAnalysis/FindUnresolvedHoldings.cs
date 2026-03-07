using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace TradeMVVM.Trading.DataAnalysis
{
    // Simple command-line utility to find holdings that appear in unresolved_isins.log
    // Usage:
    //  - Build as a small console app or run the compiled exe from the repository root.
    //  - Optional args: [holdingsCsv] [unresolvedLog] [outputPath]
    public static class FindUnresolvedHoldings
    {
        // Renamed from Main to Run to avoid defining multiple entry points in the solution.
        // Call FindUnresolvedHoldings.Run(...) from a small console project or from tests.
        public static int Run(string[] args)
        {
            try
            {
                string repoRoot = Directory.GetCurrentDirectory();

                string holdingsPath = args.Length > 0 && !string.IsNullOrWhiteSpace(args[0])
                    ? args[0]
                    : Path.Combine(repoRoot, "TradeMVVM.Trading", "DataAnalysis", "HoldingsReport.csv");

                string unresolvedLog = args.Length > 1 && !string.IsNullOrWhiteSpace(args[1])
                    ? args[1]
                    : Path.Combine(repoRoot, "TradeMVVM.Trading", "unresolved_isins.log");

                string outputPath = args.Length > 2 && !string.IsNullOrWhiteSpace(args[2])
                    ? args[2]
                    : Path.Combine(repoRoot, "TradeMVVM.Trading", "DataAnalysis", "unresolved_holdings_cs.txt");

                if (!File.Exists(holdingsPath))
                {
                    Console.Error.WriteLine($"Holdings file not found: {holdingsPath}");
                    return 2;
                }

                if (!File.Exists(unresolvedLog))
                {
                    Console.Error.WriteLine($"Unresolved log not found: {unresolvedLog}");
                    return 3;
                }

                var holdings = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                using (var sr = new StreamReader(holdingsPath))
                {
                    var header = sr.ReadLine();
                    if (header == null) return 0;
                    var headers = header.Split(';');
                    int isinIndex = -1;
                    for (int i = 0; i < headers.Length; i++)
                    {
                        if (string.Equals(headers[i].Trim(), "isin", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(headers[i].Trim(), "ISIN", StringComparison.OrdinalIgnoreCase))
                        {
                            isinIndex = i; break;
                        }
                    }

                    if (isinIndex < 0)
                    {
                        // fallback: assume first column is ISIN
                        isinIndex = 0;
                    }

                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        if (string.IsNullOrWhiteSpace(line)) continue;
                        var parts = line.Split(';');
                        if (parts.Length <= isinIndex) continue;
                        var isin = parts[isinIndex].Trim();
                        if (!string.IsNullOrEmpty(isin)) holdings.Add(isin);
                    }
                }

                var isinRegex = new Regex("ISIN:\\s*([A-Z0-9]+)", RegexOptions.IgnoreCase);
                var unresolved = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var line in File.ReadLines(unresolvedLog))
                {
                    var m = isinRegex.Match(line);
                    if (m.Success)
                    {
                        var i = m.Groups[1].Value.Trim();
                        if (!string.IsNullOrEmpty(i)) unresolved.Add(i);
                    }
                }

                var bad = holdings.Where(h => unresolved.Contains(h)).OrderBy(x => x).ToList();

                using (var outw = new StreamWriter(outputPath, false))
                {
                    if (bad.Count == 0)
                    {
                        outw.WriteLine("No holdings appear in unresolved_isins.log");
                    }
                    else
                    {
                        foreach (var b in bad) outw.WriteLine(b);
                    }
                }

                Console.WriteLine($"Holdings: {holdings.Count} total");
                Console.WriteLine($"Unresolved ISINs in log: {unresolved.Count}");
                Console.WriteLine($"Holdings not scraped (count={bad.Count}):");
                foreach (var i in bad) Console.WriteLine(i);
                Console.WriteLine($"Wrote results to: {outputPath}");

                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex}");
                return 1;
            }
        }
    }
}
