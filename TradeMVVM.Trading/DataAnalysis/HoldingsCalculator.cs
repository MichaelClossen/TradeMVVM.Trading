using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace TradeMVVM.Trading.DataAnalysis
{
    public class Holding
    {
        public string ISIN { get; set; }
        public string Name { get; set; }
        public string Currency { get; set; }
        public double Shares { get; set; }
        public double LastPrice { get; set; }
        public DateTime? LastPriceTime { get; set; }
        public double MarketValue => (double.IsNaN(LastPrice) ? 0.0 : Shares * LastPrice);

        // additional aggregates
        public double TotalFees { get; set; }
        public double TotalTaxes { get; set; }

        public double TotalBoughtShares { get; set; }
        public double TotalBoughtAmount { get; set; }
        public double TotalSoldShares { get; set; }
        public double TotalSoldAmount { get; set; }
        // FIFO-based realized P/L and remaining cost basis
        public double RealizedPL { get; set; }
        public double RemainingBoughtShares { get; set; }
        public double RemainingBoughtAmount { get; set; }
    }

    public static class HoldingsCalculator
    {
        // internal transaction representation used to ensure chronological processing
        private class Transaction
        {
            public int Index { get; set; }
            public DateTime Timestamp { get; set; }
            public string ISIN { get; set; }
            public string Name { get; set; }
            public string Currency { get; set; }
            public double SignedShares { get; set; }
            public double Price { get; set; }
            public double Fee { get; set; }
            public double Tax { get; set; }
        }
        // internal lot representation for FIFO matching
        private class Lot
        {
            public double Shares { get; set; }
            public double Price { get; set; }
        }

        // CSV expected: date;time;status;reference;description;assetType;type;isin;shares;price;amount;fee;tax;currency
        public static Dictionary<string, Holding> ComputeHoldingsFromCsv(string csvPath)
        {
            if (!File.Exists(csvPath))
                throw new FileNotFoundException(csvPath);

            string[] lines = null;
            // Try opening the file allowing other processes to keep it open for write/read.
            // This prevents IOException when another process has the file open without shared locks.
            const int maxAttempts = 1;
            for (int attempt = 0; attempt < maxAttempts; attempt++)
            {
                try
                {
                    using var fs = new FileStream(csvPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                    using var sr = new StreamReader(fs);
                    var list = new List<string>();
                    string l;
                    while ((l = sr.ReadLine()) != null)
                        list.Add(l);
                    lines = list.ToArray();
                    break;
                }
                catch (IOException)
                {
                    if (attempt == maxAttempts - 1)
                        throw; // give up after retries
                    System.Threading.Thread.Sleep(100);
                }
            }

            if (lines == null || lines.Length <= 1)
                return new Dictionary<string, Holding>();

            var holdings = new Dictionary<string, Holding>(StringComparer.OrdinalIgnoreCase);

            var culture = CultureInfo.GetCultureInfo("de-DE");


            // header assumed on first line -> first parse all transactions into an intermediate list
            var transactions = new List<Transaction>();
            int lineIndex = 0;
            for (int i = 1; i < lines.Length; i++)
            {
                var line = lines[i];
                lineIndex++;
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                // simple split by semicolon
                var parts = line.Split(';');
                if (parts.Length < 14)
                    continue;

                var dateText = parts[0].Trim();
                var timeText = parts[1].Trim();
                var status = parts[2].Trim();
                if (!string.Equals(status, "Executed", StringComparison.OrdinalIgnoreCase))
                    continue;

                var name = parts[4].Trim('"');
                var type = parts[6].Trim(); // original type value from CSV (Buy / Sell / SellShort / Short / ...)
                var isin = parts[7].Trim();
                var sharesText = parts[8].Trim();
                var priceText = parts[9].Trim();
                var amountText = parts.Length > 10 ? parts[10].Trim() : "";
                var feeText = parts.Length > 11 ? parts[11].Trim() : "";
                var taxText = parts.Length > 12 ? parts[12].Trim() : "";
                var currency = parts.Length > 13 ? parts[13].Trim() : "";

                if (string.IsNullOrWhiteSpace(isin))
                    continue;

                DateTime timestamp = DateTime.MinValue;
                try
                {
                    timestamp = DateTime.ParseExact(dateText + " " + timeText, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                }
                catch
                {
                    // ignore, leave min
                }

                if (!double.TryParse(sharesText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double shares))
                    shares = 0;

                if (!double.TryParse(priceText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double price))
                    price = double.NaN;

                if (!double.TryParse(feeText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double fee))
                    fee = 0.0;

                if (!double.TryParse(taxText.Replace("\u00A0", ""), NumberStyles.Any, culture, out double tax))
                    tax = 0.0;

                // determine sign — prefer using the transaction amount sign (positive/negative)
                // If amount is not available, fall back to keyword-based normalization.
                double signedShares = 0;
                double amount = double.NaN;
                if (!string.IsNullOrWhiteSpace(amountText))
                {
                    double.TryParse(amountText.Replace("\u00A0", ""), NumberStyles.Any, culture, out amount);
                }

                if (!double.IsNaN(amount))
                {
                    // common banking convention: negative amount = cash out (you paid) -> buy
                    // positive amount = cash in (you received) -> sell
                    if (amount < 0)
                        signedShares = shares;
                    else if (amount > 0)
                        signedShares = -shares;
                    else
                        signedShares = 0;
                }
                else
                {
                    // fallback to type keywords when amount is not parseable
                    if (!string.IsNullOrWhiteSpace(type))
                    {
                        var t = type.ToLowerInvariant();
                        if (t.Contains("buy") || t.Contains("kauf") || t.Contains("acq") || t.Contains("purchase"))
                        {
                            signedShares = shares;
                        }
                        else if (t.Contains("sell") || t.Contains("verkauf") || t.Contains("short") || t.Contains("leer"))
                        {
                            signedShares = -shares;
                        }
                        else
                        {
                            signedShares = 0;
                        }
                    }
                }

                transactions.Add(new Transaction
                {
                    Index = lineIndex,
                    Timestamp = timestamp,
                    ISIN = isin,
                    Name = name,
                    Currency = currency,
                    SignedShares = signedShares,
                    Price = price,
                    Fee = fee,
                    Tax = tax
                });
            }

            // process transactions in chronological order (stable by original file index)
            var ordered = transactions.OrderBy(t => t.Timestamp).ThenBy(t => t.Index).ToList();
            // prepare FIFO lots per ISIN
            var lotsByIsin = new Dictionary<string, Queue<Lot>>(StringComparer.OrdinalIgnoreCase);

            foreach (var tx in ordered)
            {
                var isin = tx.ISIN;
                if (!holdings.TryGetValue(isin, out var h))
                {
                    h = new Holding { ISIN = isin, Name = tx.Name, Currency = tx.Currency, Shares = 0, LastPrice = double.NaN, LastPriceTime = null };
                    holdings[isin] = h;
                }
                // update aggregates in chronological order
                h.Shares += tx.SignedShares;
                h.TotalFees += tx.Fee;
                h.TotalTaxes += tx.Tax;

                // ensure lots queue exists
                if (!lotsByIsin.TryGetValue(isin, out var queue))
                {
                    queue = new Queue<Lot>();
                    lotsByIsin[isin] = queue;
                }

                // update last price only if timestamp is newer or if LastPriceTime is null
                if (!double.IsNaN(tx.Price))
                {
                    if (!h.LastPriceTime.HasValue || tx.Timestamp >= h.LastPriceTime.Value)
                    {
                        h.LastPrice = tx.Price;
                        h.LastPriceTime = tx.Timestamp == DateTime.MinValue ? DateTime.Now : tx.Timestamp;
                    }
                }

                if (tx.SignedShares > 0)
                {
                    // buy -> add FIFO lot
                    var buyShares = tx.SignedShares;
                    var lotPrice = double.IsNaN(tx.Price) ? 0.0 : tx.Price;
                    queue.Enqueue(new Lot { Shares = buyShares, Price = lotPrice });

                    h.TotalBoughtShares += buyShares;
                    if (!double.IsNaN(tx.Price))
                        h.TotalBoughtAmount += buyShares * tx.Price;

                    h.RemainingBoughtShares += buyShares;
                    h.RemainingBoughtAmount += buyShares * lotPrice;
                }
                else if (tx.SignedShares < 0)
                {
                    // sell -> consume FIFO lots
                    var sellSharesRemaining = -tx.SignedShares;
                    double proceeds = double.IsNaN(tx.Price) ? 0.0 : tx.Price * (-tx.SignedShares);
                    double cost = 0.0;

                    while (sellSharesRemaining > 0 && queue.Count > 0)
                    {
                        var lot = queue.Peek();
                        if (lot.Shares <= sellSharesRemaining)
                        {
                            // consume full lot
                            cost += lot.Shares * lot.Price;
                            sellSharesRemaining -= lot.Shares;
                            queue.Dequeue();
                        }
                        else
                        {
                            // partial consume
                            cost += sellSharesRemaining * lot.Price;
                            lot.Shares -= sellSharesRemaining;
                            sellSharesRemaining = 0;
                        }
                    }

                    // if not enough lots, remaining sellSharesRemaining represent shorting/unknown cost -> cost += 0

                    // realized P/L = proceeds - cost - fees - taxes
                    double feesAndTaxes = tx.Fee + tx.Tax;
                    double realized = proceeds - cost - feesAndTaxes;
                    h.RealizedPL += realized;

                    // Update sold aggregates
                    var soldShares = -tx.SignedShares;
                    h.TotalSoldShares += soldShares;
                    if (!double.IsNaN(tx.Price))
                        h.TotalSoldAmount += soldShares * tx.Price;

                    // adjust remaining bought amounts/shares
                    // recompute remaining amounts as sum of queue to avoid drift
                    double remShares = 0.0, remAmount = 0.0;
                    foreach (var l in queue)
                    {
                        remShares += l.Shares;
                        remAmount += l.Shares * l.Price;
                    }
                    h.RemainingBoughtShares = remShares;
                    h.RemainingBoughtAmount = remAmount;
                }
            }

            return holdings;
        }

        public static string GenerateReportCsv(string csvPath, string outPath)
        {
            var holdings = ComputeHoldingsFromCsv(csvPath);

            var lines = new List<string>
            {
                "ISIN;Name;Currency;Shares;LastPrice;MarketValue;TotalBoughtShares;AvgBuyPrice;TotalSoldShares;AvgSellPrice;Fees;Taxes;RealizedPL;UnrealizedPL;TotalPL"
            };

            var cultureOut = CultureInfo.GetCultureInfo("de-DE");

            foreach (var kv in holdings.OrderBy(k => k.Key))
            {
                var h = kv.Value;
                // use FIFO-derived remaining cost basis for average buy price and unrealized P/L
                double avgBuy = h.RemainingBoughtShares > 0 ? h.RemainingBoughtAmount / h.RemainingBoughtShares : double.NaN;
                double avgSell = h.TotalSoldShares > 0 ? h.TotalSoldAmount / h.TotalSoldShares : double.NaN;

                double realized = double.NaN;
                // use FIFO realized P/L if available
                if (!double.IsNaN(h.RealizedPL) && h.RealizedPL != 0.0)
                    realized = h.RealizedPL;

                double unrealized = double.NaN;
                if (!double.IsNaN(avgBuy) && !double.IsNaN(h.LastPrice))
                    unrealized = h.Shares * (h.LastPrice - avgBuy);

                double total = (double.IsNaN(realized) ? 0.0 : realized) + (double.IsNaN(unrealized) ? 0.0 : unrealized);

                lines.Add(string.Join(";", new[] {
                    h.ISIN,
                    EscapeCsv(h.Name),
                    h.Currency,
                    h.Shares.ToString(cultureOut),
                    (double.IsNaN(h.LastPrice) ? "" : h.LastPrice.ToString(cultureOut)),
                    (double.IsNaN(h.LastPrice) ? "" : h.MarketValue.ToString(cultureOut)),
                    h.TotalBoughtShares.ToString(cultureOut),
                    (double.IsNaN(avgBuy) ? "" : avgBuy.ToString(cultureOut)),
                    h.TotalSoldShares.ToString(cultureOut),
                    (double.IsNaN(avgSell) ? "" : avgSell.ToString(cultureOut)),
                    h.TotalFees.ToString(cultureOut),
                    h.TotalTaxes.ToString(cultureOut),
                    (double.IsNaN(realized) ? "" : realized.ToString(cultureOut)),
                    (double.IsNaN(unrealized) ? "" : unrealized.ToString(cultureOut)),
                    total.ToString(cultureOut)
                }));
            }

            File.WriteAllLines(outPath, lines);
            return outPath;
        }

        private static string EscapeCsv(string s)
        {
            if (s == null) return "";
            if (s.Contains(";") || s.Contains('"') || s.Contains('\n'))
                return '"' + s.Replace("\"", "\"\"") + '"';
            return s;
        }
    }
}
