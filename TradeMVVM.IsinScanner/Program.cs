using System;
using System.Collections.Generic;
using System.IO;


using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Net;
using System.Text;
using System.Diagnostics;
using System.Threading;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;
using Microsoft.Data.Sqlite;
using System.Collections.Concurrent;

class Program
{
    static HttpClient http = new HttpClient();
    static bool Verbose = false;
    // throttle and driver usage semaphores for Gettex provider logic
    static SemaphoreSlim GettexThrottle = new SemaphoreSlim(1, 1);
    static SemaphoreSlim GettexDriverUsage = new SemaphoreSlim(1, 1);
    static ConcurrentDictionary<string, DateTime> GettexLastTradeTimes = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
    // heartbeat timer removed

    // Shared browser instance to avoid creating a new ChromeDriver per ISIN.
    // Reusing a single driver warms DNS/TCP and browser caches and is much faster for bulk scans.
    static class SharedBrowser
    {
        static object sync = new object();
        static IWebDriver? driver;

        public static IWebDriver GetOrCreateDriver()
        {
            lock (sync)
            {
                if (driver != null)
                    return driver;

                var options = new ChromeOptions();
                options.AddArgument("--headless=new");
                options.AddArgument("--window-size=1200,800");
                options.AddArgument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
                options.AddArgument("--no-sandbox");
                options.AddArgument("--disable-gpu");
                options.AddExcludedArgument("enable-automation");
                options.AddAdditionalOption("useAutomationExtension", false);
                options.AddArgument("--disable-blink-features=AutomationControlled");
                options.AddArgument("--disable-dev-shm-usage");

                var service = ChromeDriverService.CreateDefaultService();
                service.HideCommandPromptWindow = true;

                driver = new ChromeDriver(service, options);

                try
                {
                    // shorter timeouts
                    driver.Manage().Timeouts().PageLoad = TimeSpan.FromSeconds(10);
                    driver.Manage().Timeouts().AsynchronousJavaScript = TimeSpan.FromSeconds(5);
                    driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(0);
                }
                catch
                {
                    // optional logging
                }
                return driver;
            }



        }
        // TryFallbackScrape moved to top-level (outside SharedBrowser) to avoid scope issues.
        public static bool IsIsinInLookup(string dbPath, string isin)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath) || string.IsNullOrWhiteSpace(isin)) return false;
                var cs = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
                using var conn = new Microsoft.Data.Sqlite.SqliteConnection(cs);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 1 FROM NEW_ISIN_WKN WHERE Isin = @isin LIMIT 1;";
                cmd.Parameters.AddWithValue("@isin", isin);
                var v = cmd.ExecuteScalar();
                conn.Close();
                return v != null;
            }
            catch { return false; }
        }

        public static string GetWknFromIsinOnline(string isin)
        {
            try
            {
                var driver = SharedBrowser.GetOrCreateDriver();

                var url = $"https://www.onvista.de/suche/?searchValue={isin}";
                driver.Navigate().GoToUrl(url);

                var wait = new WebDriverWait(driver, TimeSpan.FromSeconds(10))
                {
                    PollingInterval = TimeSpan.FromMilliseconds(250)
                };

                wait.IgnoreExceptionTypes(
                    typeof(NoSuchElementException),
                    typeof(StaleElementReferenceException),
                    typeof(WebDriverException));

                try
                {
                    // Strategy 1: Try known table column
                    var element = wait.Until(d =>
                    {
                        try
                        {
                            var els = d.FindElements(
                                By.XPath("//*[contains(text(),'WKN')]"));

                            if (els.Count > 0)
                                return els[0];

                            return null;
                        }
                        catch
                        {
                            return null;
                        }
                    });

                    if (element != null)
                    {
                        var text = element.Text;

                        // Extract 6-char WKN
                        var match = System.Text.RegularExpressions.Regex
                            .Match(text, @"\b[A-Z0-9]{6}\b");

                        if (match.Success)
                            return match.Value;
                    }
                }
                catch (WebDriverTimeoutException)
                {
                    if (Verbose)
                        Console.WriteLine($"Timeout: {isin}");
                }

                // Strategy 2: fallback → search full page text
                try
                {
                    var body = driver.FindElement(By.TagName("body"));
                    var text = body.Text;

                    var match = System.Text.RegularExpressions.Regex
                        .Match(text, @"\b[A-Z0-9]{6}\b");

                    if (match.Success)
                        return match.Value;
                }
                catch { }
            }
            catch (Exception ex)
            {
                if (Verbose)
                    Console.WriteLine($"Lookup error {isin}: {ex.Message}");
            }

            return null;
        }
























        public static void QuitDriver()
        {
            lock (sync)
            {
                if (driver == null) return;
                try { driver.Quit(); } catch { }
                try { driver.Dispose(); } catch { }
                driver = null;

                // Try to kill any leftover chromedriver processes to ensure a truly fresh start next cycle
                try
                {
                    foreach (var p in System.Diagnostics.Process.GetProcessesByName("chromedriver"))
                    {
                        try { p.Kill(); } catch { }
                    }
                }
                catch { }
                // small pause to allow OS to reclaim handles
                try { Thread.Sleep(200); } catch { }
            }
        }
    }

    // Read lightweight WKN lookup table
    static string? GetWknFromDb(string dbPath, string isin)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) return null;
            var cs = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
            using var conn = new Microsoft.Data.Sqlite.SqliteConnection(cs);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT WKN FROM NEW_ISIN_WKN WHERE Isin = @isin LIMIT 1;";
            cmd.Parameters.AddWithValue("@isin", isin ?? (object)DBNull.Value);
            var v = cmd.ExecuteScalar();
            conn.Close();
            if (v == null || v == DBNull.Value) return null;
            return v.ToString()?.Trim();
        }
        catch { return null; }
    }

    // Minimal fallback scrape: try a small set of HTTP-based scrapers (fast) and a Selenium fallback for ariva using wkn if necessary.
    static async Task<Result?> TryFallbackScrapeAriva(string isin, string? knownWkn)
    {
        //TODO
        var fb = new Result() { Price = 0, Change = "", Source = "" };

        // Get Price and Change here
        // .....


        // .....

        if (fb.Price ==0 ||fb.Change=="")
        {            
            return null;
        }

        fb.Source = "Ariva";
        return fb;
    }

    static bool IsValidIsin(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return false;
        s = s.Trim().ToUpperInvariant();
        if (s.Length != 12) return false;
        if (!System.Text.RegularExpressions.Regex.IsMatch(s, "^[A-Z0-9]{12}$")) return false;
        try
        {
            string converted = string.Concat(s.Take(11).Select(c => char.IsLetter(c) ? (c - 'A' + 10).ToString() : c.ToString()));
            int sum = 0; int mul = 1;
            for (int i = converted.Length - 1; i >= 0; i--)
            {
                foreach (char d in converted[i].ToString())
                {
                    int v = d - '0';
                    int t = v * mul;
                    sum += t / 10 + t % 10;
                    mul = (mul == 1) ? 2 : 1;
                }
            }
            int checkDigit = (10 - (sum % 10)) % 10;
            return s[11] - '0' == checkDigit;
        }
        catch { return false; }
    }

    static List<string> ReadIsinsFromDatabase(string dbPath)
    {
        var list = new List<string>();
        try
        {
            if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) return list;

            var csb = new SqliteConnectionStringBuilder { DataSource = dbPath, Mode = SqliteOpenMode.ReadOnly };
            using var conn = new SqliteConnection(csb.ToString());
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT isin FROM NEW_Holdings WHERE isin IS NOT NULL";

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                try
                {
                    var raw = reader.IsDBNull(0) ? null : reader.GetString(0)?.Trim();
                    if (string.IsNullOrWhiteSpace(raw)) continue;
                    raw = raw.Trim('\uFEFF', '\u200B');
                    var first = raw.Split(new[] { ',', ';', '\t' }, StringSplitOptions.RemoveEmptyEntries).First().Trim();
                    if (string.IsNullOrWhiteSpace(first)) continue;
                    if (IsValidIsin(first)) list.Add(first.ToUpperInvariant());
                    else
                    {
                        var up = first.ToUpperInvariant();
                        if (!list.Contains(up)) list.Add(up);
                    }
                }
                catch { }
            }

            conn.Close();
        }
        catch { }

        return list.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
    }

    static async Task Main(string[] args)
    {
        // Default to the workspace IsinScanner isins.csv path when no argument provided
        string defaultPath = @"C:\Users\micha\Desktop\Trade\TradeMVVM.IsinScanner\isins.csv";
        // support optional -v/--verbose flag
        Verbose = args.Any(a => string.Equals(a, "-v", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--verbose", StringComparison.OrdinalIgnoreCase));

        // support optional timeout flag: --timeout=<seconds> or -t <seconds>
        int timeoutSeconds = 5; // default per-ISIN timeout in seconds (increased so slower providers can finish)
        var timeoutArg = args.FirstOrDefault(a => a.StartsWith("--timeout=", StringComparison.OrdinalIgnoreCase));
        if (timeoutArg != null)
        {
            var tok = timeoutArg.Substring("--timeout=".Length);
            if (int.TryParse(tok, out var tv) && tv >= 0) timeoutSeconds = tv;
        }
        else
        {
            var tIndex = Array.FindIndex(args, a => string.Equals(a, "-t", StringComparison.OrdinalIgnoreCase));
            if (tIndex >= 0 && tIndex + 1 < args.Length)
            {
                if (int.TryParse(args[tIndex + 1], out var tv) && tv >= 0) timeoutSeconds = tv;
            }
        }

        string file = args.FirstOrDefault(a => !a.StartsWith("-")) ?? defaultPath;
        if (Verbose) Console.WriteLine($"Per-ISIN timeout: {timeoutSeconds}s");

        if (!File.Exists(file))
        {
            // ensure directory exists for the default path
            try
            {
                var dir = Path.GetDirectoryName(file);
                if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                    Directory.CreateDirectory(dir);
            }
            catch { }

            File.WriteAllText(file, "DE000HC64J94\nDE000A1EWWW0");
            Console.WriteLine($"CSV Datei nicht gefunden: {file}");
            Console.WriteLine("Beispiel isins.csv erstellt. Bitte erneut starten.");
            return;
        }

        // ensure console can print the euro sign
        Console.OutputEncoding = Encoding.UTF8;
        Console.InputEncoding = Encoding.UTF8;

        // local poller heartbeat removed

        // ensure shared browser is quit on process exit
        AppDomain.CurrentDomain.ProcessExit += (s, e) => { try { SharedBrowser.QuitDriver(); } catch { } };

        // Ctrl+C should abort immediately
        Console.CancelKeyPress += (s, e) =>
        {
            Console.WriteLine("\nAbbruch angefordert: beende sofort...");
            e.Cancel = true; // prevent the runtime from terminating so we can cleanup
            try { SharedBrowser.QuitDriver(); } catch { }
            Environment.Exit(0);
        };

        Console.WriteLine($"Lese ISINs aus: {file}");



        var rawLines = ReadAllLinesShared(file)
                        .Where(x => !string.IsNullOrWhiteSpace(x))
                        .ToList();

        // ... helper to read file even when another process has it open (shared read)
        static string[] ReadAllLinesShared(string path, int maxAttempts = 5, int delayMs = 200)
        {
            if (path == null) return Array.Empty<string>();
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                    using var sr = new StreamReader(fs);
                    var lines = new List<string>();
                    string? line;
                    while ((line = sr.ReadLine()) != null)
                        lines.Add(line);
                    return lines.ToArray();
                }
                catch (IOException)
                {
                    if (attempt == maxAttempts) throw;
                    Thread.Sleep(delayMs);
                }
                catch (UnauthorizedAccessException)
                {
                    if (attempt == maxAttempts) throw;
                    Thread.Sleep(delayMs);
                }
            }
            return Array.Empty<string>();
        }

        var isins = new List<string>();
        var badLines = new List<string>();
        foreach (var line in rawLines)
        {
            var l = line.Trim();
            // remove common BOM/zero-width chars
            l = l.Trim('\uFEFF', '\u200B');
            if (string.IsNullOrWhiteSpace(l)) continue;

            // support CSV-like lines: ISIN[,name] or ISIN;name or ISIN\tname
            var first = l.Split(new[] { ',', ';', '\t' }, StringSplitOptions.RemoveEmptyEntries).First().Trim();
            first = first.Trim();
            if (IsValidIsin(first))
            {
                isins.Add(first.ToUpperInvariant());
            }
            else
            {
                badLines.Add(l);
            }
        }

        // ensure we still process all lines from the CSV: include bad/unknown lines as best-effort entries
        foreach (var b in badLines)
        {
            var tok = b.Split(new[] { ',', ';', '\t' }, StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()?.Trim();
            if (!string.IsNullOrWhiteSpace(tok))
            {
                var up = tok.ToUpperInvariant();
                if (!isins.Contains(up)) isins.Add(up);
            }
        }

        // if none parsed as ISINs, fall back to raw trimmed lines (best-effort)
        if (isins.Count == 0)
        {
            // try fallback: treat whole trimmed lines as entries
            foreach (var line in rawLines)
            {
                var l = line.Trim().Trim('\uFEFF', '\u200B');
                if (!string.IsNullOrWhiteSpace(l)) isins.Add(l);
            }
        }

        if (isins.Count == 0)
        {
            Console.WriteLine($"Keine gültigen ISINs in Datei: {file}");
            if (badLines.Count > 0)
            {
                Console.WriteLine("Unverarbeitete Zeilen (Beispiele):");
                foreach (var b in badLines.Take(5)) Console.WriteLine("  " + b);
            }
            return;
        }

        // path to sqlite DB containing NEW_Holdings table
        string dbPath = @"C:\Users\micha\Desktop\Trade\trading.db";

        int cycle = 0;
        while (true)
        {
            cycle++;
            Console.WriteLine($"\n=== Scan-Durchlauf #{cycle} - Start: {DateTime.Now} ===");
            // attempt to read ISIN list from database before each run so changes are picked up
            try
            {
                var dbIsins = ReadIsinsFromDatabase(dbPath);
                if (dbIsins != null && dbIsins.Count > 0)
                {
                    isins = dbIsins;
                    Console.WriteLine($"Lade {isins.Count} ISINs aus DB: {dbPath}");
                }
                else
                {
                    Console.WriteLine($"Keine ISINs in DB gefunden oder DB nicht vorhanden -> verwende CSV: {file}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fehler beim Lesen der DB: {ex.Message} -> verwende CSV: {file}");
            }
            var sw = Stopwatch.StartNew();
            var allResults = new List<PerIsinReport>();
            // warm up network, HTTP and Selenium so the first ISIN query is not affected by cold start
            try { await WarmUpProvidersAsync(); } catch { }
            for (int i = 0; i < isins.Count; i++)
            {
                var isin = isins[i];
                // you can change the preferred provider here if needed
                var task = GetSmartData(isin, preferredProvider: "BNP");
                // Always use the configured per-ISIN timeout. Previously we shortened non-ISIN entries
                // to 2s which caused valid-looking ISINs (with hidden chars) to be aborted early.
                var timeoutMs = timeoutSeconds * 1000; // per-ISIN timeout
                var swIsin = Stopwatch.StartNew();

                var finished = await Task.WhenAny(task, Task.Delay(timeoutMs));

                Console.WriteLine($"\nISIN {i + 1}/{isins.Count}: {isin}");

                if (finished != task)
                {
                    // Before acknowledging the timeout, attempt a short best-effort fallback scrape
                    // if we have either a valid ISIN or an existing WKN mapping in the DB.
                    try
                    {
                        var knownWkn = GetWknFromDb(dbPath, isin);
                        var haveIsin = !string.IsNullOrWhiteSpace(isin) && IsValidIsin(isin);
                        if (haveIsin || !string.IsNullOrWhiteSpace(knownWkn))
                        {
                            if (Verbose) Console.WriteLine("Timeout: starte Fallback-Scrape (kurze Versuche bei mehreren Providern)");


                            // Best-effort: try to resolve and persist WKN for this ISIN so the DB is enriched
                            if (knownWkn == null)
                            {
                                try
                                {
                                    // only attempt WKN lookup if ISIN not already present in the lookup table
                                    bool exists = false;
                                    try { exists = SharedBrowser.IsIsinInLookup(dbPath, isin); } catch { exists = false; }

                                    if (!exists)
                                    {
                                        // perform a bounded WKN lookup (don't block the main loop indefinitely)
                                        var tryWkn = await TryFetchWknWithTimeout(isin, 8000);
                                        if (!string.IsNullOrWhiteSpace(tryWkn))
                                        {
                                            if (Verbose) Console.WriteLine($"WKN ermittelt: {tryWkn} -> schreibe in DB");
                                            try { UpsertWknInDb(dbPath, isin, tryWkn); } catch { }
                                            knownWkn = tryWkn;
                                        }
                                        else
                                        {
                                            if (Verbose) Console.WriteLine("WKN nicht ermittelt (Timeout oder Fehler)");
                                        }
                                    }
                                    else
                                    {
                                        if (Verbose) Console.WriteLine("WKN-Lookup übersprungen: ISIN bereits in NEW_ISIN_WKN");
                                    }
                                }
                                catch { }

                            }

                            try
                            {
                                var fb = await TryFallbackScrapeAriva(isin, knownWkn);
                                if (fb != null)
                                {
                                    // Persist found values and report — treat as successful result
                                    var updatedStrFb = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                                    UpsertNewHolding(dbPath, isin, fb.Price, fb.Change != null && !fb.Change.StartsWith("n/a") ? (double?)ParseDecimalToDouble(fb.Change.Replace("%", "")) : null, updatedStrFb, fb.Source);
                                    if (Verbose) Console.WriteLine($"Fallback: gefunden {fb.Price} € ({fb.Change}) von {fb.Source} — in DB eingetragen");
                                    allResults.Add(new PerIsinReport { Isin = isin, Result = new SmartResult { BestPrice = fb.Price.ToString("0.000") + " €", BestProvider = fb.Source, Change = fb.Change ?? "n/a", ProviderStatusLines = new List<string> { "Fallback: '" + fb.Source + "'" } }, TimedOut = false, ElapsedSeconds = swIsin.Elapsed.TotalSeconds });
                                    swIsin.Stop();
                                    continue;
                                }
                            }
                            catch { }
                        }
                    }
                    catch { }
                    Console.WriteLine($"Timeout nach {timeoutMs / 1000}s — nächste ISIN");
                    // If the provider task timed out it may be stuck in Selenium or a blocking HTTP call.
                    // Quit the shared browser to attempt to abort any hanging Selenium work and
                    // ensure subsequent ISINs start with a fresh driver instance.
                    try
                    {
                        if (Verbose) Console.WriteLine("Timeout: versuche SharedBrowser zu beenden, um blockierte Selenium-Operationen abzubrechen");
                        SharedBrowser.QuitDriver();
                    }
                    catch { }
                    swIsin.Stop();


                    allResults.Add(new PerIsinReport { Isin = isin, Result = null, TimedOut = true, ElapsedSeconds = swIsin.Elapsed.TotalSeconds });
                    continue;
                }
                var result = await task; // already completed

                if (result == null)
                {
                    swIsin.Stop();
                    Console.WriteLine("Keine Daten");
                    allResults.Add(new PerIsinReport { Isin = isin, Result = null, TimedOut = false, ElapsedSeconds = swIsin.Elapsed.TotalSeconds });
                    continue;
                }

                // Best/Change output deferred until after provider status is printed
                // Versuch: Preis und Prozent in NEW_Holdings in der DB aktualisieren
                try
                {
                    if (!string.IsNullOrWhiteSpace(result.BestPrice))
                    {
                        // BestPrice enthält meist eine Zahl und ein Euro-Symbol, z.B. "12.345 €"
                        var priceClean = result.BestPrice.Replace("€", "").Replace("EUR", "").Trim();
                        var priceNum = ParseDecimalToDouble(priceClean);

                        double? pct = null;
                        if (!string.IsNullOrWhiteSpace(result.Change) && !result.Change.Trim().StartsWith("n/a", StringComparison.OrdinalIgnoreCase))
                        {
                            var pctClean = result.Change.Replace("%", "").Trim();
                            var pctNum = ParseDecimalToDouble(pctClean);
                            if (!double.IsNaN(pctNum)) pct = pctNum;
                        }

                        if (!double.IsNaN(priceNum))
                        {
                            var updatedStr = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                            // include provider information when writing to NEW_Holdings
                            UpsertNewHolding(dbPath, isin, priceNum, pct, updatedStr, result.BestProvider);
                            Console.WriteLine($"DB: NEW_Holdings aktualisiert für {isin}: purchaseValue={priceNum} percent={(pct.HasValue ? pct.Value.ToString("0.00") : "NULL")} updated={updatedStr} provider={result.BestProvider}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Fehler beim Schreiben in DB für {isin}: {ex.Message}");
                }
                // print provider status collected during GetSmartData
                try
                {
                    Console.WriteLine("Provider-Status:");
                    if (result.ProviderStatusLines != null)
                    {
                        foreach (var line in result.ProviderStatusLines)
                            Console.WriteLine(line);
                    }
                }
                catch { }

                // now print Best/Change last as requested
                Console.WriteLine($"Best: {result.BestPrice} ({result.BestProvider})");
                Console.WriteLine($"Change: {result.Change}");
                // Spread removed from output
                allResults.Add(new PerIsinReport { Isin = isin, Result = result, TimedOut = false, ElapsedSeconds = swIsin.Elapsed.TotalSeconds });
            }

            sw.Stop();
            Console.WriteLine($"\nErmittelt: {allResults.Count} ISINs in {sw.Elapsed.TotalSeconds:0.00}s");
            Console.WriteLine("Ergebnisübersicht:");
            for (int i = 0; i < allResults.Count; i++)
            {
                var e = allResults[i];
                var idx = i + 1;
                if (e.TimedOut)
                {
                    Console.WriteLine($"{idx,3}: {e.Isin} -> TIMEOUT (Dauer {e.ElapsedSeconds:0.##} s)");
                }
                else if (e.Result == null)
                {
                    Console.WriteLine($"{idx,3}: {e.Isin} -> keine Daten (Dauer {e.ElapsedSeconds:0.##} s)");
                }
                else
                {
                    Console.WriteLine($"{idx,3}: {e.Isin} -> {e.Result.BestPrice} ({e.Result.BestProvider}) | Change: {e.Result.Change} (Dauer {e.ElapsedSeconds:0.##} s)");
                }
            }
            // shutdown shared browser after completing this full run so next run starts with a fresh Chrome/Selenium
            // compute and persist aggregate header values to NEW_TotalValues table
            try { UpsertTotalValues(dbPath); } catch { }

            try { SharedBrowser.QuitDriver(); } catch { }
            // small pause to allow Chrome to exit cleanly before next cycle
            try { await Task.Delay(500); } catch { }

            // Do not warm-start Chrome/Selenium for the next run — quit and let providers create a fresh driver on demand

            // Immediately start next run
        }
    }



    static async Task WarmUpProvidersAsync()
    {
        // perform lightweight GETs to provider root pages to warm DNS/TCP and caches
        var urls = new[]
        {
            "https://derivate.bnpparibas.com/",
            "https://www.gettex.de/",
            "https://www.wallstreet-online.de/",
            "https://www.ariva.de/"
        };

        foreach (var u in urls)
        {
            try
            {
                using var cts = new CancellationTokenSource(5000);
                var resp = await http.GetAsync(u, cts.Token);
                // touch content length to ensure connection established
                _ = resp.Content.Headers.ContentLength;
                if (Verbose) Console.WriteLine($"Warmup: fetched {u} -> {(int)resp.StatusCode}");
            }
            catch (Exception ex)
            {
                if (Verbose) Console.WriteLine($"Warmup: {u} failed: {ex.Message}");
            }
        }

        // start shared Selenium driver once to avoid startup latency for first ISIN that needs it
        try
        {
            var drv = SharedBrowser.GetOrCreateDriver();
            try { drv.Navigate().GoToUrl("about:blank"); } catch { }
            await Task.Delay(200);
            if (Verbose) Console.WriteLine("Warmup: shared Selenium driver started");
        }
        catch (Exception ex)
        {
            if (Verbose) Console.WriteLine($"Warmup: selenium start failed: {ex.Message}");
        }
    }

    static async Task<SmartResult?> GetSmartData(string isin, string preferredProvider = "BNP")
    {
        // fixed provider order per user request
        var providerNames = new[] { "BNP", "Gettex", "Wallstreet", "Ariva" };

        // map provider name -> function
        var providerMap = new Dictionary<string, Func<Task<Result?>>>(StringComparer.OrdinalIgnoreCase)
        {
            ["BNP"] = () => TryBNP(isin),
            ["Gettex"] = () => TryGettex(isin),
            ["Ariva"] = () => TryAriva(isin),
            ["Wallstreet"] = () => TryWallstreet(isin)
        };

        var statusLines = new List<string>();

        var successful = new List<Result>();

        bool IsComplete(Result? r) => r != null && !double.IsNaN(r.Price) && r.Price > 0 && !string.IsNullOrWhiteSpace(r.Change) && !r.Change.Trim().StartsWith("n/a", StringComparison.OrdinalIgnoreCase);
        bool IsPartial(Result? r) => r != null && !double.IsNaN(r.Price) && r.Price > 0;

        // query providers sequentially in order
        foreach (var name in providerNames)
        {
            try
            {
                Result? res = null;
                try { res = await (providerMap.ContainsKey(name) ? providerMap[name]() : Task.FromResult<Result?>(null)); } catch { res = null; }

                if (res == null)
                {
                    statusLines.Add($"  {name,-10}: keine Daten");
                }
                else
                {
                    var priceStr = res.Price.ToString("0.000", CultureInfo.GetCultureInfo("de-DE"));
                    statusLines.Add($"  {name,-10}: {priceStr} € | {res.Change}");

                    if (IsComplete(res))
                    {
                        // complete data: accept and stop querying further providers for this ISIN
                        successful.Add(res);
                        var remaining = providerNames.SkipWhile(n => !string.Equals(n, name, StringComparison.OrdinalIgnoreCase)).Skip(1);
                        foreach (var r in remaining)
                            statusLines.Add($"  {r,-10}: nicht abgefragt");
                        break;
                    }

                    if (IsPartial(res))
                    {
                        successful.Add(res);
                        // stop if we have two partial/complete providers
                        if (successful.Count >= 2)
                        {
                            var remaining = providerNames.SkipWhile(n => !string.Equals(n, name, StringComparison.OrdinalIgnoreCase)).Skip(1);
                            foreach (var r in remaining)
                                statusLines.Add($"  {r,-10}: nicht abgefragt");
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                statusLines.Add($"  {name,-10}: fehler ({ex.Message})");
            }
        }

        if (successful.Count == 0)
            return null;

        var best = successful.OrderByDescending(x => x.Price).First();
        var min = successful.Min(x => x.Price);
        var max = successful.Max(x => x.Price);

        return new SmartResult
        {
            BestPrice = best.Price.ToString("0.000") + " €",
            BestProvider = best.Source,
            Change = best.Change ?? "n/a",
            ProviderStatusLines = statusLines
        };
    }

    class SmartResult
    {
        public string BestPrice { get; set; }
        public string BestProvider { get; set; }
        public string Change { get; set; }
        public List<string> ProviderStatusLines { get; set; }
    }

    class Result
    {
        public double Price { get; set; }
        public string Change { get; set; }
        public string Source { get; set; }
    }

    class PerIsinReport
    {
        public string Isin { get; set; }
        public SmartResult? Result { get; set; }
        public bool TimedOut { get; set; }
        public double ElapsedSeconds { get; set; }
    }

    // =========================
    // BNP (BEST SOURCE)
    // =========================
    static async Task<Result?> TryBNP(string isin)
    {
        try
        {
            var url = $"https://derivate.bnpparibas.com/product-details/{isin}/";
            var html = await http.GetStringAsync(url);

            var priceMatch = Regex.Match(html,
                $@"data-field=""bid""[^>]*data-item=""[^""]*{isin}""[^>]*>\s*(?<price>\d+[.,]\d+)",
                RegexOptions.IgnoreCase);

            if (!priceMatch.Success)
                return null;

            var priceStr = priceMatch.Groups["price"].Value.Trim();

            var percentMatch = Regex.Match(html,
                $@"data-field=""changePercent""[^>]*data-item=""[^""]*{isin}""[^>]*>\s*(?<percent>[+-]?\d+[.,]\d+)",
                RegexOptions.IgnoreCase);

            var percentStr = percentMatch.Success ? percentMatch.Groups["percent"].Value.Trim() : null;

            priceStr = WebUtility.HtmlDecode(priceStr).Replace("\u00A0", "").Trim();
            if (percentStr != null)
                percentStr = WebUtility.HtmlDecode(percentStr).Replace("\u00A0", "").Replace("%", "").Trim();

            // parse price and percent
            var price = ParseDecimalToDouble(priceStr);
            string change = "n/a";
            if (!double.IsNaN(price) && percentStr != null)
            {
                if (double.TryParse(percentStr.Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out double pct))
                    change = pct.ToString("0.00", CultureInfo.InvariantCulture) + " %";
            }

            if (double.IsNaN(price)) return null;

            return new Result
            {
                Price = price,
                Change = change,
                Source = "BNP"
            };
        }
        catch { return null; }
    }

    // =========================
    // WALLSTREET-ONLINE
    // =========================
    static async Task<Result?> TryWallstreet(string isin)
    {
        try
        {
            // candidate URL patterns to try
            var candidates = new[]
            {
                $"https://www.wallstreet-online.de/suche?query={isin}",
                $"https://www.wallstreet-online.de/aktie/{isin}",
                $"https://www.wallstreet-online.de/boerse/isin/{isin}",
                $"https://www.wallstreet-online.de/aktien/{isin}",
                $"https://www.wallstreet-online.de/{isin}"
            };

            foreach (var url in candidates)
            {
                try
                {
                    var html = await http.GetStringAsync(url);
                    // wait a bit to allow delayed/JS-driven content to become available
                    await Task.Delay(2000);

                    // try site-specific quote box: price in a span inside an element with class quoteValue
                    double price = double.NaN;
                    string change = "n/a";

                    var priceBoxMatch = Regex.Match(html, @"\bquoteValue\b[\s\S]*?<span[^>]*>(?<price>[\d\.,]+)</span>", RegexOptions.IgnoreCase | RegexOptions.Singleline);
                    if (priceBoxMatch.Success)
                    {
                        var priceStr = priceBoxMatch.Groups["price"].Value.Trim();
                        price = ParseDecimalToDouble(priceStr);
                    }

                    // percent in perfRel block
                    var perfMatch = Regex.Match(html, @"\bperfRel\b[\s\S]*?(?<pct>[-+]?\d+[\.,]\d+)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
                    if (perfMatch.Success)
                    {
                        var pct = perfMatch.Groups["pct"].Value.Trim();
                        if (double.TryParse(pct.Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out double pv))
                            change = pv.ToString("0.00", CultureInfo.GetCultureInfo("de-DE")) + " %";
                        else
                            change = pct + " %";
                    }

                    // fallback: find euro amounts with € symbol
                    if (double.IsNaN(price))
                    {
                        var priceMatch = Regex.Match(html, @"\b\d{1,3}(?:[\.,]\d{3})*[\.,]\d+\s*€\b");
                        if (priceMatch.Success)
                        {
                            var priceStr = priceMatch.Value.Replace("€", "").Replace("\u00A0", "").Trim();
                            price = ParseDecimalToDouble(priceStr);
                        }
                    }

                    if (double.IsNaN(price)) continue;

                    return new Result { Price = price, Change = change, Source = "Wallstreet" };
                }
                catch { /* try next candidate */ }
            }

            return null;
        }
        catch { return null; }
    }

    // =========================
    // GETTEX
    // =========================
    static string TryFindPercent(IWebDriver driver)
    {
        if (driver == null) return null;
        try
        {
            var script = @"(function(){
  var cls = ['.fidlet', '.fidlet.float.percentage.change', '.percentage', '.change', '.float', '.price-change'];
  for (var i=0;i<cls.length;i++){
    try{
      var els = document.querySelectorAll(cls[i]);
      for(var j=0;j<els.length;j++){ var t=(els[j].innerText||'').trim(); if(t.indexOf('%')>-1) return t; }
    }catch(e){}
  }
  // fallback: any span containing a percent sign
  var spans = document.querySelectorAll('span');
  for(var k=0;k<spans.length;k++){ try{ var tt=(spans[k].innerText||'').trim(); if(tt.indexOf('%')>-1) return tt;}catch(e){} }
  return null; })();";

            var res = ((IJavaScriptExecutor)driver).ExecuteScript(script);
            var s = res?.ToString();
            return string.IsNullOrWhiteSpace(s) ? null : s.Trim();
        }
        catch { return null; }
    }

    static async Task<Result?> TryGettex(string isin)
    {
        try
        {
            var shared = SharedBrowser.GetOrCreateDriver();
            var js = (IJavaScriptExecutor)shared;

            try { ((IWebDriver)shared).Manage().Timeouts().PageLoad = TimeSpan.FromSeconds(10); } catch { }
            try { ((IWebDriver)shared).Manage().Timeouts().AsynchronousJavaScript = TimeSpan.FromSeconds(5); } catch { }
            try { ((IWebDriver)shared).Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(0); } catch { }

            // clear session state before navigation
            try { ((IWebDriver)shared).Navigate().GoToUrl("about:blank"); } catch { }
            try { ((IWebDriver)shared).Manage().Cookies.DeleteAllCookies(); } catch { }
            try { js.ExecuteScript("try{localStorage.clear(); sessionStorage.clear();}catch(e){}"); } catch { }

            var candidates = new[] { $"https://www.gettex.de/fond/{isin}/", $"https://www.gettex.de/zertifikat/{isin}/", $"https://www.gettex.de/aktie/{isin}/" };

            var wait = new WebDriverWait((IWebDriver)shared, TimeSpan.FromSeconds(40)) { PollingInterval = TimeSpan.FromMilliseconds(20) };
            wait.IgnoreExceptionTypes(typeof(WebDriverException));


            string TryWaitForText(IWebDriver driver, WebDriverWait w, string selector)
            {

                if (driver == null || w == null) return null;
                try
                {
                    return w.Until(d =>
                    {
                        try
                        {
                            var r = ((IJavaScriptExecutor)d).ExecuteScript("var el=document.querySelector(arguments[0]); return el ? el.innerText : null;", selector);
                            var t = r as string;
                            return string.IsNullOrWhiteSpace(t) ? null : t;
                        }
                        catch { return null; }
                    });
                }
                catch { return null; }
            }

            bool TryWaitForReady(IWebDriver driver, WebDriverWait w)
            {
                if (driver == null || w == null) return false;
                try
                {
                    return w.Until(d =>
                    {
                        try { var res = ((IJavaScriptExecutor)d).ExecuteScript("return document.readyState"); if (res == null) return false; var s = res as string ?? res.ToString(); return string.Equals(s, "complete", StringComparison.OrdinalIgnoreCase); }
                        catch { return false; }
                    });
                }
                catch { return false; }
            }

            // try each candidate url similar to Poller GettexProvider
            foreach (var url in candidates)
            {
                bool navSucceeded = false;
                for (int navAttempt = 0; navAttempt < 2 && !navSucceeded; navAttempt++)
                {
                    try
                    {
                        ((IWebDriver)shared).Navigate().GoToUrl(url);
                        navSucceeded = true;
                    }
                    catch (WebDriverException)
                    {
                        // restart shared browser and retry once
                        try { SharedBrowser.QuitDriver(); } catch { }
                        try { shared = SharedBrowser.GetOrCreateDriver(); js = (IJavaScriptExecutor)shared; } catch { }
                    }
                    catch { break; }
                }

                if (!navSucceeded) continue;

                TryWaitForReady(shared, wait);

                string priceText = TryWaitForText(shared, wait, ".last-price .v-tick-flash") ?? TryWaitForText(shared, wait, ".v-tick-flash") ?? TryWaitForText(shared, wait, ".price") ?? TryWaitForText(shared, wait, ".price__value");
                // try several selectors and a heuristic finder that looks for fidlet/percentage spans
                string percentText = TryWaitForText(shared, wait, ".changes .percentage.change") ?? TryWaitForText(shared, wait, ".changes .percentage") ?? TryWaitForText(shared, wait, ".change") ?? TryWaitForText(shared, wait, ".percent");
                if (string.IsNullOrWhiteSpace(percentText))
                {
                    percentText = TryFindPercent(shared);
                }

                string dateText = null;
                try { var dt = js.ExecuteScript("var el=document.querySelector('.trade-date div'); return el ? el.innerText : null;"); dateText = dt?.ToString(); } catch { }

                // fallback: extract percent from body text
                if (string.IsNullOrWhiteSpace(percentText))
                {
                    try
                    {
                        var body = js.ExecuteScript("return document.body ? document.body.innerText : null;")?.ToString();
                        if (!string.IsNullOrWhiteSpace(body))
                        {
                            var m = Regex.Match(body, "[-+]?\\d+[\\.,]?\\d*\\s*%", RegexOptions.Singleline);
                            if (m.Success) percentText = m.Value.Trim();
                        }
                    }
                    catch { }
                }

                if (!string.IsNullOrWhiteSpace(priceText))
                {
                    decimal ParseDecimalLocal(string input)
                    {
                        if (string.IsNullOrWhiteSpace(input)) return 0;
                        input = input.Replace("\u00A0", "").Replace(".", "").Replace(",", ".");
                        decimal.TryParse(input, NumberStyles.Any, CultureInfo.InvariantCulture, out var val);
                        return val;
                    }

                    decimal lastPrice = ParseDecimalLocal(priceText);
                    decimal percent = 0;
                    if (!string.IsNullOrWhiteSpace(percentText))
                        percent = ParseDecimalLocal(percentText.Replace("%", ""));

                    string change = "n/a";
                    if (percent != 0)
                        change = ((double)percent).ToString("0.00", CultureInfo.InvariantCulture) + " %";

                    return new Result { Price = (double)lastPrice, Change = change, Source = "Gettex" };
                }

                await Task.Delay(300);
            }

            return null;
        }
        catch (WebDriverException wex)
        {
            if (Verbose) Console.WriteLine($"Gettex WebDriver error: {wex.Message}");
            try { SharedBrowser.QuitDriver(); } catch { }
            return null;
        }
        catch (Exception ex)
        {
            if (Verbose) Console.WriteLine($"Gettex (selenium) error: {ex.Message}");
            return null;
        }
    }

    // =========================
    // TRADEGATE (Fallback)
    // =========================
    static async Task<Result?> TryTradegate(string isin)
    {
        try
        {
            var html = await http.GetStringAsync(
                $"https://www.tradegate.de/orderbuch.php?isin={isin}"
            );

            var match = Regex.Match(html, @"\b\d{1,3},\d{2,3}\b");

            if (!match.Success) return null;

            var val = match.Value.Replace(",", ".");

            return new Result
            {
                Price = double.Parse(val, CultureInfo.InvariantCulture),
                Change = "n/a",
                Source = "Tradegate"
            };
        }
        catch { return null; }
    }

    // =========================
    // ARIVA (Fallback)
    // =========================
    static async Task<Result?> TryAriva(string isin)
    {
        try
        {
            // Build candidate URLs. For DE ISINs we can derive the ariva product code
            var candidates = new List<string> { $"https://www.ariva.de/{isin}" };
            if (isin != null && isin.Length >= 12 && isin.StartsWith("DE", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    var code = isin.Substring(5, 6);
                    candidates.Insert(0, $"https://www.ariva.de/hebelprodukte/{code}");
                    candidates.Insert(1, $"https://www.ariva.de/{code}");
                }
                catch { }
            }

            // Try Ariva search pages first and prefer any discovered product link
            try
            {
                var searchEndpoints = new[]
                {
                    $"https://www.ariva.de/suche/?q={Uri.EscapeDataString(isin)}",
                    $"https://www.ariva.de/suche?query={Uri.EscapeDataString(isin)}",
                    $"https://www.ariva.de/suchergebnis.asp?_search={Uri.EscapeDataString(isin)}"
                };

                foreach (var sUrl in searchEndpoints)
                {
                    try
                    {
                        var sHtml = await http.GetStringAsync(sUrl);
                        await Task.Delay(300);

                        // prefer direct product links that include the ISIN
                        var m = Regex.Match(sHtml, "href\\s*=\\s*[\"'](?<href>[^\"']*" + Regex.Escape(isin) + "[^\"']*)[\"']", RegexOptions.IgnoreCase);
                        if (!m.Success)
                        {
                            // fallback: look for hebelprodukte links or other product paths
                            m = Regex.Match(sHtml, "href\\s*=\\s*[\"'](?<href>/hebelprodukte/[^\"']*)[\"']", RegexOptions.IgnoreCase);
                            if (!m.Success)
                                m = Regex.Match(sHtml, "href\\s*=\\s*[\"'](?<href>/[a-zA-Z0-9_-]+/[^\"']*)[\"']", RegexOptions.IgnoreCase);
                        }

                        // If no link found in raw HTML, try Selenium on the search page to render JS-driven results
                        if (!m.Success || string.IsNullOrWhiteSpace(sHtml))
                        {
                            if (Verbose) Console.WriteLine($"Ariva: no link found on search page {sUrl} via HTTP — trying Selenium for search page");
                            try
                            {
                                var rendered = await FetchWithSeleniumAriva(sUrl);
                                if (!string.IsNullOrWhiteSpace(rendered))
                                {
                                    sHtml = rendered;
                                    // re-run link extraction on rendered HTML
                                    m = Regex.Match(sHtml, "href\\s*=\\s*[\"'](?<href>[^\"']*" + Regex.Escape(isin) + "[^\"']*)[\"']", RegexOptions.IgnoreCase);
                                    if (!m.Success)
                                    {
                                        m = Regex.Match(sHtml, "href\\s*=\\s*[\"'](?<href>/hebelprodukte/[^\"']*)[\"']", RegexOptions.IgnoreCase);
                                        if (!m.Success)
                                            m = Regex.Match(sHtml, "href\\s*=\\s*[\"'](?<href>/[a-zA-Z0-9_-]+/[^\"']*)[\"']", RegexOptions.IgnoreCase);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                if (Verbose) Console.WriteLine($"Ariva: Selenium search fetch error: {ex.Message}");
                            }
                        }

                        if (m.Success)
                        {
                            var href = m.Groups["href"].Value;
                            string full = href.StartsWith("http", StringComparison.OrdinalIgnoreCase) ? href : ("https://www.ariva.de" + href);
                            // prefer search-derived url at front
                            if (!candidates.Contains(full, StringComparer.OrdinalIgnoreCase))
                                candidates.Insert(0, full);
                            break;
                        }
                    }
                    catch { /* try next search endpoint */ }
                }
            }
            catch { }

            async Task<string?> FetchWithSeleniumAriva(string url)
            {
                // Use shared driver
                try
                {
                    var shared = SharedBrowser.GetOrCreateDriver();
                    try { ((IWebDriver)shared).Navigate().GoToUrl(url); } catch { }
                    var js = (IJavaScriptExecutor)shared;
                    try
                    {
                        var wait = new WebDriverWait((IWebDriver)shared, TimeSpan.FromSeconds(8)) { PollingInterval = TimeSpan.FromMilliseconds(300) };
                        wait.IgnoreExceptionTypes(typeof(WebDriverException));
                        wait.Until(d =>
                        {
                            try { return (bool)js.ExecuteScript("return !!document.querySelector('.instrument-header-quote') || !!document.querySelector('.instrument-header-numbers-description') || document.body.innerText.indexOf('€')>=0;"); }
                            catch { return false; }
                        });
                    }
                    catch { }

                    return ((IWebDriver)shared).PageSource;
                }
                catch (Exception ex)
                {
                    if (Verbose) Console.WriteLine($"FetchWithSeleniumAriva error: {ex.Message}");
                    return null;
                }
            }

            foreach (var url in candidates.Distinct())
            {
                try
                {
                    var html = await http.GetStringAsync(url);
                    // small wait for delayed/JS-driven content
                    await Task.Delay(2000);

                    // header parsing (Geld/Brief)
                    var headerMatches = Regex.Matches(html,
                        @"<div\s+class=""instrument-header-numbers-description""\s*>\s*(?<desc>[^<]+?)\s*</div>\s*<div\s+class=""instrument-header-quote""\s*>\s*(?<price>[^<]+?)\s*</div>",
                        RegexOptions.IgnoreCase);

                    string priceStr = null;
                    foreach (Match mm in headerMatches)
                    {
                        var desc = WebUtility.HtmlDecode(mm.Groups["desc"].Value).Trim();
                        var p = WebUtility.HtmlDecode(mm.Groups["price"].Value).Trim();
                        if (desc.IndexOf("Geld", StringComparison.OrdinalIgnoreCase) >= 0) { priceStr = p; break; }
                        if (desc.IndexOf("Brief", StringComparison.OrdinalIgnoreCase) >= 0 && priceStr == null) priceStr = p;
                    }

                    var pctMatch = Regex.Match(html, @"<div\s+class=""instrument-header-rel-change[^""]*""[\s\S]*?>\s*(?<pct>[-+]?\d+[,\.]\d+)\s*%?\s*</div>", RegexOptions.IgnoreCase);
                    string pct = pctMatch.Success ? pctMatch.Groups["pct"].Value.Trim() : null;

                    if (!string.IsNullOrWhiteSpace(priceStr))
                    {
                        var norm = priceStr.Replace("€", "").Replace("\u00A0", "").Trim();
                        double priceVal = ParseDecimalToDouble(norm);
                        if (!double.IsNaN(priceVal))
                        {
                            string changeStr = "n/a";
                            if (!string.IsNullOrWhiteSpace(pct) && double.TryParse(pct.Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out double pv))
                                changeStr = pv.ToString("0.00", CultureInfo.GetCultureInfo("de-DE")) + " %";
                            return new Result { Price = priceVal, Change = changeStr, Source = "Ariva" };
                        }
                    }

                    // fallback: generic euro block
                    var blockMatch = Regex.Match(html, @"(\d{1,3}(?:\.\d{3})*,\d+\s*€[\s\S]{0,100}?\d+,\d+%)");
                    if (blockMatch.Success)
                    {
                        var block = blockMatch.Value;
                        var priceMatch = Regex.Match(block, @"(\d{1,3}(?:\.\d{3})*,\d+)\s*€");
                        var percentMatch = Regex.Match(block, @"([-+]?\d+,\d+)\s*%");
                        if (priceMatch.Success)
                        {
                            double price2 = double.Parse(priceMatch.Groups[1].Value.Replace(".", "").Replace(",", "."), CultureInfo.InvariantCulture);
                            return new Result { Price = price2, Change = percentMatch.Success ? percentMatch.Groups[1].Value + " %" : "n/a", Source = "Ariva" };
                        }
                    }

                    // Selenium fallback: render page and re-parse
                    if (Verbose) Console.WriteLine($"Ariva: trying Selenium fallback for {url}");
                    var pageSource = await FetchWithSeleniumAriva(url);
                    if (!string.IsNullOrWhiteSpace(pageSource))
                    {
                        html = pageSource;
                        headerMatches = Regex.Matches(html,
                            @"<div\s+class=""instrument-header-numbers-description""\s*>\s*(?<desc>[^<]+?)\s*</div>\s*<div\s+class=""instrument-header-quote""\s*>\s*(?<price>[^<]+?)\s*</div>",
                            RegexOptions.IgnoreCase);

                        priceStr = null;
                        foreach (Match mm in headerMatches)
                        {
                            var desc = WebUtility.HtmlDecode(mm.Groups["desc"].Value).Trim();
                            var p = WebUtility.HtmlDecode(mm.Groups["price"].Value).Trim();
                            if (desc.IndexOf("Geld", StringComparison.OrdinalIgnoreCase) >= 0) { priceStr = p; break; }
                            if (desc.IndexOf("Brief", StringComparison.OrdinalIgnoreCase) >= 0 && priceStr == null) priceStr = p;
                        }

                        var pctMatch2 = Regex.Match(html, @"<div\s+class=""instrument-header-rel-change[^""]*""[\s\S]*?>\s*(?<pct>[-+]?\d+[,\.]\d+)\s*%?\s*</div>", RegexOptions.IgnoreCase);
                        string pct2 = pctMatch2.Success ? pctMatch2.Groups["pct"].Value.Trim() : null;

                        if (!string.IsNullOrWhiteSpace(priceStr))
                        {
                            var norm2 = priceStr.Replace("€", "").Replace("\u00A0", "").Trim();
                            double priceVal2 = ParseDecimalToDouble(norm2);
                            if (!double.IsNaN(priceVal2))
                            {
                                string changeStr2 = "n/a";
                                if (!string.IsNullOrWhiteSpace(pct2) && double.TryParse(pct2.Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out double pv2))
                                    changeStr2 = pv2.ToString("0.00", CultureInfo.GetCultureInfo("de-DE")) + " %";
                                return new Result { Price = priceVal2, Change = changeStr2, Source = "Ariva" };
                            }
                        }
                    }
                }
                catch { /* try next candidate */ }
            }

            return null;
        }
        catch { return null; }
    }
    static async Task<Result?> TryArivaSelenium(string isin)
    {
        try
        {
            // duplicate helper renamed to avoid collision; kept for reference
            // Build candidate URLs. For DE ISINs we can derive the ariva product code
            // Example: DE000PJ1GAU4 -> PJ1GAU (characters 6..11)
            var candidates = new List<string> { $"https://www.ariva.de/{isin}" };
            if (isin != null && isin.Length >= 12 && isin.StartsWith("DE", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    var code = isin.Substring(5, 6);
                    // try product-specific path first (hebelprodukte), then a generic product path
                    candidates.Insert(0, $"https://www.ariva.de/hebelprodukte/{code}");
                    candidates.Insert(1, $"https://www.ariva.de/{code}");
                }
                catch { }
            }

            // Selenium fallback helper for Ariva (used when initial HTTP fetch doesn't contain the price)
            async Task<string?> FetchWithSeleniumAriva(string url)
            {
                try
                {
                    // reuse shared driver to avoid creating/quitting Chrome between runs
                    var shared = SharedBrowser.GetOrCreateDriver();
                    // try to clear session state between navigations to avoid carry-over
                    try { ((IWebDriver)shared).Navigate().GoToUrl("about:blank"); } catch { }
                    try { ((IWebDriver)shared).Manage().Cookies.DeleteAllCookies(); } catch { }
                    try { ((IWebDriver)shared).Navigate().GoToUrl(url); } catch { }

                    var js = (IJavaScriptExecutor)shared;
                    try
                    {
                        var wait = new WebDriverWait((IWebDriver)shared, TimeSpan.FromSeconds(10)) { PollingInterval = TimeSpan.FromMilliseconds(300) };
                        wait.IgnoreExceptionTypes(typeof(WebDriverException));
                        wait.Until(d =>
                        {
                            try { return (bool)js.ExecuteScript("return !!document.querySelector('.instrument-header-quote') || !!document.querySelector('.instrument-header-numbers-description') || document.body.innerText.indexOf('€')>=0;"); }
                            catch { return false; }
                        });
                    }
                    catch { }

                    return ((IWebDriver)shared).PageSource;
                }
                catch (Exception ex)
                {
                    if (Verbose) Console.WriteLine($"FetchWithSeleniumAriva error: {ex.Message}");
                    return null;
                }
            }

            foreach (var url in candidates.Distinct())
            {
                try
                {
                    var html = await http.GetStringAsync(url);
                    // small wait in case ariva serves content with slight delay
                    await Task.Delay(2000);

                    // First attempt: new-style header parsing (contains Geld/Brief and instrument-header-quote)
                    var m = Regex.Matches(html,
                        @"<div\s+class=""instrument-header-numbers-description""\s*>\s*(?<desc>[^<]+?)\s*</div>\s*<div\s+class=""instrument-header-quote""\s*>\s*(?<price>[^<]+?)\s*</div>",
                        RegexOptions.IgnoreCase);

                    string priceStr = null;
                    foreach (Match mm in m)
                    {
                        var desc = WebUtility.HtmlDecode(mm.Groups["desc"].Value).Trim();
                        var p = WebUtility.HtmlDecode(mm.Groups["price"].Value).Trim();
                        if (desc.IndexOf("Geld", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            priceStr = p; // prefer Geld (bid)
                            break;
                        }
                        if (desc.IndexOf("Brief", StringComparison.OrdinalIgnoreCase) >= 0 && priceStr == null)
                        {
                            priceStr = p; // keep Brief if no Geld
                        }
                    }

                    // percent change (rel-change block)
                    var pctMatch = Regex.Match(html, @"<div\s+class=""instrument-header-rel-change[^""]*""[\s\S]*?>\s*(?<pct>[-+]?\d+[,\.]\d+)\s*%?\s*</div>", RegexOptions.IgnoreCase);
                    string pct = pctMatch.Success ? pctMatch.Groups["pct"].Value.Trim() : null;

                    if (!string.IsNullOrWhiteSpace(priceStr))
                    {
                        // normalize and parse
                        priceStr = priceStr.Replace("€", "").Replace("\u00A0", "").Trim();
                        double price = ParseDecimalToDouble(priceStr);
                        if (!double.IsNaN(price))
                        {
                            {
                                string changeStr = "n/a";
                                if (!string.IsNullOrWhiteSpace(pct))
                                {
                                    // try parse using invariant after normalizing decimal separator, then format with German culture to keep comma
                                    if (double.TryParse(pct.Replace(',', '.').Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out double pv))
                                        changeStr = pv.ToString("0.00", CultureInfo.GetCultureInfo("de-DE")) + " %";
                                    else
                                        changeStr = pct + " %";
                                }

                                return new Result
                                {
                                    Price = price,
                                    Change = changeStr,
                                    Source = "Ariva"
                                };
                            }

                            // If initial HTML didn't contain the price, try Selenium to render JS-driven content and re-parse
                            if (string.IsNullOrWhiteSpace(priceStr))
                            {
                                if (Verbose) Console.WriteLine($"Ariva: trying Selenium fallback for {url}");
                                var pageSource = await FetchWithSeleniumAriva(url);
                                if (!string.IsNullOrWhiteSpace(pageSource))
                                {
                                    html = pageSource;
                                    // re-run header parsing on rendered HTML
                                    m = Regex.Matches(html,
                                        @"<div\s+class=""instrument-header-numbers-description""\s*>\s*(?<desc>[^<]+?)\s*</div>\s*<div\s+class=""instrument-header-quote""\s*>\s*(?<price>[^<]+?)\s*</div>",
                                        RegexOptions.IgnoreCase);

                                    priceStr = null;
                                    foreach (Match mm in m)
                                    {
                                        var desc = WebUtility.HtmlDecode(mm.Groups["desc"].Value).Trim();
                                        var p = WebUtility.HtmlDecode(mm.Groups["price"].Value).Trim();
                                        if (desc.IndexOf("Geld", StringComparison.OrdinalIgnoreCase) >= 0)
                                        {
                                            priceStr = p; // prefer Geld (bid)
                                            break;
                                        }
                                        if (desc.IndexOf("Brief", StringComparison.OrdinalIgnoreCase) >= 0 && priceStr == null)
                                        {
                                            priceStr = p; // keep Brief if no Geld
                                        }
                                    }

                                    var pctMatch2 = Regex.Match(html, @"<div\s+class=""instrument-header-rel-change[^""]*""[\s\S]*?>\s*(?<pct>[-+]?\d+[,\.]\d+)\s*%?\s*</div>", RegexOptions.IgnoreCase);
                                    string pct2 = pctMatch2.Success ? pctMatch2.Groups["pct"].Value.Trim() : null;

                                    if (!string.IsNullOrWhiteSpace(priceStr))
                                    {
                                        // normalize and parse
                                        priceStr = priceStr.Replace("€", "").Replace("\u00A0", "").Trim();
                                        double priceInner = ParseDecimalToDouble(priceStr);
                                        if (!double.IsNaN(priceInner))
                                        {
                                            string changeStr = "n/a";
                                            if (!string.IsNullOrWhiteSpace(pct2))
                                            {
                                                if (double.TryParse(pct2.Replace(',', '.').Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out double pv))
                                                    changeStr = pv.ToString("0.00", CultureInfo.GetCultureInfo("de-DE")) + " %";
                                                else
                                                    changeStr = pct2 + " %";
                                            }

                                            return new Result
                                            {
                                                Price = priceInner,
                                                Change = changeStr,
                                                Source = "Ariva"
                                            };
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // fallback: try older generic block match
                    var blockMatch = Regex.Match(html,
                        @"(\d{1,3}(?:\.\d{3})*,\d+\s*€[\s\S]{0,100}?\d+,\d+%)");

                    if (!blockMatch.Success)
                        continue;

                    var block = blockMatch.Value;
                    var priceMatch = Regex.Match(block, @"(\d{1,3}(?:\.\d{3})*,\d+)\s*€");
                    var percentMatch = Regex.Match(block, @"([-+]?\d+,\d+)\s*%");
                    if (!priceMatch.Success)
                        continue;

                    double price2 = double.Parse(
                        priceMatch.Groups[1].Value.Replace(".", "").Replace(",", "."),
                        CultureInfo.InvariantCulture
                    );

                    return new Result
                    {
                        Price = price2,
                        Change = percentMatch.Success ? percentMatch.Groups[1].Value + " %" : "n/a",
                        Source = "Ariva"
                    };
                }
                catch { /* try next candidate */ }
            }

            return null;
        }
        catch { return null; }
    }

    static async Task<Result?> TryFinanzen(string isin)
    {
        try
        {
            // Use the website search endpoint first to resolve the human-friendly URL
            var searchUrl = $"https://www.finanzen.net/suchergebnis.asp?_search={Uri.EscapeDataString(isin)}";

            // local helper to perform a browser-like GET and return content and HTTP status
            async Task<(string? content, HttpStatusCode? status)> GetHtml(string url)
            {
                // try a few common desktop user-agents to reduce chance of blocking
                var ualist = new[]
                {
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:116.0) Gecko/20100101 Firefox/116.0",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"
                };

                for (int attempt = 1; attempt <= 3; attempt++)
                {
                    try
                    {
                        using var req = new HttpRequestMessage(HttpMethod.Get, url);
                        // rotate user-agent
                        req.Headers.UserAgent.ParseAdd(ualist[(attempt - 1) % ualist.Length]);
                        req.Headers.Accept.ParseAdd("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8");
                        req.Headers.AcceptLanguage.ParseAdd("de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7");
                        req.Headers.Referrer = new Uri("https://www.finanzen.net/");
                        // some servers inspect these headers; add them without validation
                        req.Headers.TryAddWithoutValidation("Accept-Encoding", "gzip, deflate, br");
                        req.Headers.TryAddWithoutValidation("Connection", "keep-alive");
                        req.Headers.TryAddWithoutValidation("Upgrade-Insecure-Requests", "1");

                        using var resp = await http.SendAsync(req);
                        var content = await resp.Content.ReadAsStringAsync();
                        if (!resp.IsSuccessStatusCode)
                        {
                            if (Verbose) Console.WriteLine($"GetHtml: non-success status {(int)resp.StatusCode} for {url} (attempt {attempt})");
                            // return body and status so caller can decide to use Selenium fallback
                            if (!string.IsNullOrWhiteSpace(content)) return (content, resp.StatusCode);
                        }
                        else
                        {
                            return (content, resp.StatusCode);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (Verbose) Console.WriteLine($"GetHtml attempt {attempt} error: {ex.Message}");
                    }

                    await Task.Delay(300 + attempt * 200);
                }

                return (null, null);
            }

            // fallback: use Selenium to fetch rendered HTML when simple requests are blocked
            async Task<string?> FetchWithSelenium(string url)
            {
                try
                {
                    // reuse shared browser to avoid creating/quitting Chrome between runs
                    var shared = SharedBrowser.GetOrCreateDriver();
                    try { ((IWebDriver)shared).Navigate().GoToUrl(url); } catch { }
                    var js = (IJavaScriptExecutor)shared;
                    try
                    {
                        var wait = new WebDriverWait((IWebDriver)shared, TimeSpan.FromSeconds(10)) { PollingInterval = TimeSpan.FromMilliseconds(300) };
                        wait.IgnoreExceptionTypes(typeof(WebDriverException));
                        wait.Until(d =>
                        {
                            try { return (bool)js.ExecuteScript("return !!document.querySelector('.snapshot__values') || !!document.querySelector('.snapshot__value') || document.readyState === 'complete';"); }
                            catch { return false; }
                        });
                    }
                    catch { }

                    return ((IWebDriver)shared).PageSource;
                }
                catch (Exception ex)
                {
                    if (Verbose) Console.WriteLine($"FetchWithSelenium error: {ex.Message}");
                    return null;
                }
            }

            var searchRes = await GetHtml(searchUrl);
            string html = searchRes.content;
            HttpStatusCode? searchStatus = searchRes.status;
            string pageHtml = null;
            if (Verbose) Console.WriteLine($"Finanzen: search status={(int?)searchStatus} contentLength={(html?.Length ?? 0)}");

            if (!string.IsNullOrWhiteSpace(html))
            {
                // prefer /fonds/ links that contain the ISIN, else take first /fonds/ link
                var m = Regex.Match(html, "href\\s*=\\s*[\"'](?<href>/fonds/[^\"']*" + Regex.Escape(isin) + "[^\"']*)[\"']", RegexOptions.IgnoreCase);
                if (!m.Success)
                    m = Regex.Match(html, "href\\s*=\\s*[\"'](?<href>/fonds/[^\"']*)[\"']", RegexOptions.IgnoreCase);

                // If search page is JS-driven and no link found, try Selenium on the search page and reparse
                if (!m.Success)
                {
                    if (Verbose) Console.WriteLine("Finanzen: no /fonds/ link found on search HTML — trying Selenium on search page");
                    var searchSelenium = await FetchWithSelenium(searchUrl);
                    if (!string.IsNullOrWhiteSpace(searchSelenium))
                    {
                        html = searchSelenium;
                        m = Regex.Match(html, "href\\s*=\\s*[\"'](?<href>/fonds/[^\"']*" + Regex.Escape(isin) + "[^\"']*)[\"']", RegexOptions.IgnoreCase);
                        if (!m.Success)
                            m = Regex.Match(html, "href\\s*=\\s*[\"'](?<href>/fonds/[^\"']*)[\"']", RegexOptions.IgnoreCase);
                    }
                }

                if (m.Success)
                {
                    var href = m.Groups["href"].Value;
                    var full = "https://www.finanzen.net" + href;
                    var pageRes = await GetHtml(full);
                    pageHtml = pageRes.content;
                    if (Verbose) Console.WriteLine($"Finanzen: fetched {full} status={(int?)pageRes.status} length={(pageHtml?.Length ?? 0)}");
                    // if page content doesn't contain expected snapshot markers, try Selenium fallback
                    if ((string.IsNullOrWhiteSpace(pageHtml) || !(pageHtml.Contains("snapshot__values") || pageHtml.Contains("snapshot__value"))) && pageRes.status == HttpStatusCode.Forbidden)
                    {
                        if (Verbose) Console.WriteLine("Finanzen: using Selenium fallback for search-result URL due to forbidden or missing snapshot");
                        pageHtml = await FetchWithSelenium(full);
                        if (Verbose) Console.WriteLine($"Finanzen: selenium length={(pageHtml?.Length ?? 0)}");
                    }
                    else if (string.IsNullOrWhiteSpace(pageHtml) || !(pageHtml.Contains("snapshot__values") || pageHtml.Contains("snapshot__value")))
                    {
                        if (Verbose) Console.WriteLine("Finanzen: fetched search-result lacks snapshot markers — trying Selenium fallback");
                        pageHtml = await FetchWithSelenium(full);
                        if (Verbose) Console.WriteLine($"Finanzen: selenium length={(pageHtml?.Length ?? 0)}");
                    }
                }
            }

            // fallback: try direct fonds URL variants
            if (string.IsNullOrWhiteSpace(pageHtml))
            {
                var direct1 = $"https://www.finanzen.net/fonds/{isin}";
                var direct2 = $"https://www.finanzen.net/fonds/{isin.ToLowerInvariant()}";
                var p1 = await GetHtml(direct1);
                pageHtml = p1.content;
                if (Verbose) Console.WriteLine($"Finanzen: direct1 status={(int?)p1.status} len={(pageHtml?.Length ?? 0)}");
                if (string.IsNullOrWhiteSpace(pageHtml) || !(pageHtml.Contains("snapshot__values") || pageHtml.Contains("snapshot__value")))
                {
                    // try Selenium when direct fetch is empty or lacks snapshot markers
                    if (Verbose) Console.WriteLine("Finanzen: direct1 missing snapshot — trying Selenium");
                    var s = await FetchWithSelenium(direct1);
                    if (!string.IsNullOrWhiteSpace(s)) pageHtml = s;
                }
                if (string.IsNullOrWhiteSpace(pageHtml))
                {
                    var p2 = await GetHtml(direct2);
                    pageHtml = p2.content;
                    if (Verbose) Console.WriteLine($"Finanzen: direct2 status={(int?)p2.status} len={(pageHtml?.Length ?? 0)}");
                    if (string.IsNullOrWhiteSpace(pageHtml) || !(pageHtml.Contains("snapshot__values") || pageHtml.Contains("snapshot__value")))
                    {
                        if (Verbose) Console.WriteLine("Finanzen: direct2 missing snapshot — trying Selenium");
                        var s2 = await FetchWithSelenium(direct2);
                        if (!string.IsNullOrWhiteSpace(s2)) pageHtml = s2;
                    }
                }
            }

            if (string.IsNullOrWhiteSpace(pageHtml))
                return null;

            // Try to extract the snapshot block if present
            var blockMatch = Regex.Match(pageHtml, "<div[^>]*class\\s*=\\s*[\"']snapshot__values[\"'][^>]*>(?<block>[\\s\\S]*?)</div>", RegexOptions.IgnoreCase);
            string block = blockMatch.Success ? blockMatch.Groups["block"].Value : pageHtml;

            // Try specific snapshot structure used on finanzen.net: price in .snapshot__value-current, percent in .snapshot__value-relative
            string priceStr = null;
            string pctStr = null;

            try
            {
                var curMatch = Regex.Match(block, "<span[^>]*class\\s*=\\s*[\"'][^\"']*snapshot__value-current[^\"']*[\"'][^>]*>[\\s\\S]*?<span[^>]*class\\s*=\\s*[\"'][^\"']*snapshot__value[^\"']*[\"'][^>]*>\\s*(?<price>[-+]?\\d+[\\.,]\\d+)\\s*</span>", RegexOptions.IgnoreCase);
                if (curMatch.Success)
                    priceStr = curMatch.Groups["price"].Value.Trim();

                var relMatch = Regex.Match(block, "<span[^>]*class\\s*=\\s*[\"'][^\"']*snapshot__value-relative[^\"']*[\"'][^>]*>[\\s\\S]*?<span[^>]*class\\s*=\\s*[\"'][^\"']*snapshot__value[^\"']*[\"'][^>]*>\\s*(?<pct>[-+]?\\d+[\\.,]\\d+)\\s*</span>", RegexOptions.IgnoreCase);
                if (relMatch.Success)
                    pctStr = relMatch.Groups["pct"].Value.Trim();
                if (Verbose) Console.WriteLine($"Finanzen: parsed price='{priceStr}' pct='{pctStr}'");
            }
            catch (Exception ex) { if (Verbose) Console.WriteLine($"Finanzen: parse error: {ex.Message}"); }

            // fallback: first snapshot__value followed by unit EUR or €
            if (string.IsNullOrWhiteSpace(priceStr))
            {
                var priceMatch = Regex.Match(block, "<span[^>]*class\\s*=\\s*[\"'][^\"']*snapshot__value[^\"']*[\"'][^>]*>\\s*(?<price>[\\d\\.,]+)\\s*</span>\\s*<span[^>]*class\\s*=\\s*[\"'][^\"']*snapshot__value-unit[^\"']*[\"'][^>]*>\\s*(?<unit>EUR|€)\\s*</span>", RegexOptions.IgnoreCase);
                if (priceMatch.Success) priceStr = priceMatch.Groups["price"].Value.Trim();
            }

            // fallback percent: any percentage in block
            if (string.IsNullOrWhiteSpace(pctStr))
            {
                var pctMatch = Regex.Match(block, "(?<pct>[-+]?\\d+[\\.,]\\d+)\\s*%", RegexOptions.IgnoreCase);
                if (pctMatch.Success) pctStr = pctMatch.Groups["pct"].Value.Trim();
            }

            // fallback: any euro amount
            if (string.IsNullOrWhiteSpace(priceStr))
            {
                var priceAny = Regex.Match(pageHtml, "\\d{1,3}(?:[\\.\\s]\\d{3})*[\\.,]\\d+\\s*(?:€|EUR)", RegexOptions.IgnoreCase);
                if (priceAny.Success)
                {
                    var pm = Regex.Match(priceAny.Value, "[\\d\\.,]+");
                    if (pm.Success) priceStr = pm.Value;
                }
            }

            if (string.IsNullOrWhiteSpace(priceStr)) return null;

            double price = ParseDecimalToDouble(priceStr);
            if (double.IsNaN(price)) return null;

            string change = "n/a";
            if (!string.IsNullOrWhiteSpace(pctStr))
            {
                if (double.TryParse(pctStr.Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out double pv))
                    change = pv.ToString("0.00", CultureInfo.InvariantCulture) + " %";
                else
                    change = pctStr + " %";
            }

            return new Result { Price = price, Change = change, Source = "Finanzen" };
        }
        catch { return null; }
    }

    static double ParseDecimalToDouble(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return double.NaN;
        input = WebUtility.HtmlDecode(input).Replace("\u00A0", "").Trim();
        // remove spaces
        input = input.Replace(" ", "");
        // if both '.' and ',' present, assume '.' thousand sep and ',' decimal
        if (input.Contains(',') && input.Contains('.'))
        {
            input = input.Replace(".", "");
            input = input.Replace(',', '.');
        }
        else if (input.Contains(','))
        {
            input = input.Replace(',', '.');
        }

        if (double.TryParse(input, NumberStyles.Any, CultureInfo.InvariantCulture, out double d))
            return d;
        return double.NaN;
    }

    static async Task<string?> TryFetchWknWithTimeout(string isin, int timeoutMs)
    {
        if (string.IsNullOrWhiteSpace(isin)) return null;
        try
        {
            var t = Task.Run(() => SharedBrowser.GetWknFromIsinOnline(isin));
            var finished = await Task.WhenAny(t, Task.Delay(timeoutMs));
            if (finished != t)
            {
                try { SharedBrowser.QuitDriver(); } catch { }
                return null;
            }
            try { return await t; }
            catch { return null; }
        }
        catch { return null; }
    }

    static void UpsertWknInDb(string dbPath, string isin, string wkn)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) return;
            var cs = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
            using var conn = new Microsoft.Data.Sqlite.SqliteConnection(cs);
            conn.Open();

            // ensure WKN column exists
            try
            {
                var cols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                using var pragma = conn.CreateCommand();
                pragma.CommandText = "PRAGMA table_info(NEW_Holdings);";
                using var reader = pragma.ExecuteReader();
                while (reader.Read()) cols.Add(reader.GetString(1));

                if (!cols.Contains("WKN"))
                {
                    try
                    {
                        using var alter = conn.CreateCommand();
                        alter.CommandText = "ALTER TABLE NEW_Holdings ADD COLUMN WKN TEXT;";
                        alter.ExecuteNonQuery();
                        cols.Add("WKN");
                    }
                    catch { }
                }
            }
            catch { }

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE NEW_Holdings SET WKN = @wkn WHERE isin = @isin;";
            cmd.Parameters.AddWithValue("@wkn", wkn ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@isin", isin ?? (object)DBNull.Value);
            var affected = 0;
            try { affected = cmd.ExecuteNonQuery(); } catch { }

            if (affected == 0)
            {
                try
                {
                    using var ins = conn.CreateCommand();
                    ins.CommandText = "INSERT OR IGNORE INTO NEW_Holdings (isin, WKN) VALUES (@isin, @wkn);";
                    ins.Parameters.AddWithValue("@isin", isin ?? (object)DBNull.Value);
                    ins.Parameters.AddWithValue("@wkn", wkn ?? (object)DBNull.Value);
                    try { ins.ExecuteNonQuery(); } catch { }
                }
                catch { }
            }

            // Also maintain a lightweight lookup table NEW_ISIN_WKN so ISIN->WKN mappings
            // are available even for entries not present in NEW_Holdings. Create table if missing
            // and insert the pair if not already present.
            try
            {
                using var create = conn.CreateCommand();
                create.CommandText = @"
CREATE TABLE IF NOT EXISTS NEW_ISIN_WKN (
    Isin TEXT PRIMARY KEY,
    WKN TEXT,
    Created TEXT
);";
                try { create.ExecuteNonQuery(); } catch { }

                using var ins2 = conn.CreateCommand();
                ins2.CommandText = "INSERT OR IGNORE INTO NEW_ISIN_WKN (Isin, WKN, Created) VALUES (@isin, @wkn, @created);";
                ins2.Parameters.AddWithValue("@isin", isin ?? (object)DBNull.Value);
                ins2.Parameters.AddWithValue("@wkn", wkn ?? (object)DBNull.Value);
                ins2.Parameters.AddWithValue("@created", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                try { ins2.ExecuteNonQuery(); } catch { }
            }
            catch { }

            conn.Close();
        }
        catch { }
    }

    static void UpsertNewHolding(string dbPath, string isin, double purchaseValue, double? percent, string updated, string provider)
    {
        try
        {
            // Only perform UPDATE. Do not INSERT to avoid failing when table has additional NOT NULL columns.
            if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) return;
            var cs = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder { DataSource = dbPath }.ToString();
            using var conn = new Microsoft.Data.Sqlite.SqliteConnection(cs);
            conn.Open();

            // read table columns and ensure optional columns exist (Provider, TodayValue)
            var cols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                using var pragma = conn.CreateCommand();
                pragma.CommandText = "PRAGMA table_info(NEW_Holdings);";
                using var reader = pragma.ExecuteReader();
                while (reader.Read()) cols.Add(reader.GetString(1));

                if (!cols.Contains("Provider"))
                {
                    try
                    {
                        using var alter = conn.CreateCommand();
                        alter.CommandText = "ALTER TABLE NEW_Holdings ADD COLUMN Provider TEXT;";
                        alter.ExecuteNonQuery();
                        cols.Add("Provider");
                    }
                    catch { }
                }
                if (!cols.Contains("TodayValue"))
                {
                    try
                    {
                        using var alter2 = conn.CreateCommand();
                        alter2.CommandText = "ALTER TABLE NEW_Holdings ADD COLUMN TodayValue REAL;";
                        alter2.ExecuteNonQuery();
                        cols.Add("TodayValue");
                    }
                    catch { }
                }
            }
            catch { }

            // attempt to read existing Shares value for this ISIN to calculate TodayValue
            double? shares = null;
            try
            {
                using var getCmd = conn.CreateCommand();
                getCmd.CommandText = "SELECT Shares FROM NEW_Holdings WHERE isin = @isin LIMIT 1;";
                getCmd.Parameters.AddWithValue("@isin", isin);
                var val = getCmd.ExecuteScalar();
                if (val != null && val != DBNull.Value)
                {
                    try
                    {
                        // try direct conversion first
                        shares = Convert.ToDouble(val, CultureInfo.InvariantCulture);
                    }
                    catch
                    {
                        if (double.TryParse(val.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double s))
                            shares = s;
                    }
                }
            }
            catch { }

            // compute TodayValue if shares known
            double? todayValue = null;
            if (shares.HasValue && !double.IsNaN(shares.Value))
            {
                try { todayValue = shares.Value * purchaseValue; } catch { todayValue = null; }
            }

            // attempt to read existing WKN if column exists; if missing, try to fetch online once
            string wknValue = null;
            try
            {
                if (cols.Contains("WKN"))
                {
                    using var getWknCmd = conn.CreateCommand();
                    getWknCmd.CommandText = "SELECT WKN FROM NEW_Holdings WHERE isin = @isin LIMIT 1;";
                    getWknCmd.Parameters.AddWithValue("@isin", isin);
                    var v = getWknCmd.ExecuteScalar();
                    if (v != null && v != DBNull.Value)
                    {
                        wknValue = v.ToString()?.Trim();
                    }

                    if (string.IsNullOrWhiteSpace(wknValue))
                    {
                        try
                        {
                            // best-effort online lookup (may be slow)
                            wknValue = SharedBrowser.GetWknFromIsinOnline(isin);
                        }
                        catch { }
                    }
                }
            }
            catch { }

            using (var cmd = conn.CreateCommand())
            {
                // include WKN in update if present
                if (cols.Contains("WKN"))
                    cmd.CommandText = "UPDATE NEW_Holdings SET purchaseValue = @pv, percent = @pct, updated = @upd, Provider = @prov, TodayValue = @tv, WKN = @wkn WHERE isin = @isin;";
                else
                    cmd.CommandText = "UPDATE NEW_Holdings SET purchaseValue = @pv, percent = @pct, updated = @upd, Provider = @prov, TodayValue = @tv WHERE isin = @isin;";

                cmd.Parameters.AddWithValue("@pv", purchaseValue);
                if (percent.HasValue) cmd.Parameters.AddWithValue("@pct", percent.Value);
                else cmd.Parameters.AddWithValue("@pct", DBNull.Value);
                cmd.Parameters.AddWithValue("@upd", updated);
                if (!string.IsNullOrWhiteSpace(provider)) cmd.Parameters.AddWithValue("@prov", provider);
                else cmd.Parameters.AddWithValue("@prov", DBNull.Value);
                if (todayValue.HasValue) cmd.Parameters.AddWithValue("@tv", todayValue.Value);
                else cmd.Parameters.AddWithValue("@tv", DBNull.Value);
                if (cols.Contains("WKN"))
                {
                    if (!string.IsNullOrWhiteSpace(wknValue)) cmd.Parameters.AddWithValue("@wkn", wknValue);
                    else cmd.Parameters.AddWithValue("@wkn", DBNull.Value);
                }

                cmd.Parameters.AddWithValue("@isin", isin);
                try
                {
                    var affected = cmd.ExecuteNonQuery();
                    if (affected == 0)
                    {
                        // Try INSERT as fallback. Only include columns that exist in the table to avoid NOT NULL constraint failures.
                        try
                        {
                            var insertCols = new List<string>();
                            var insertParams = new List<string>();

                            // ISIN should always exist
                            if (cols.Contains("isin")) { insertCols.Add("isin"); insertParams.Add("@isin"); }
                            if (cols.Contains("purchaseValue")) { insertCols.Add("purchaseValue"); insertParams.Add("@pv"); }
                            if (cols.Contains("percent")) { insertCols.Add("percent"); insertParams.Add("@pct"); }
                            if (cols.Contains("updated")) { insertCols.Add("updated"); insertParams.Add("@upd"); }
                            if (cols.Contains("Provider")) { insertCols.Add("Provider"); insertParams.Add("@prov"); }
                            if (cols.Contains("TodayValue")) { insertCols.Add("TodayValue"); insertParams.Add("@tv"); }
                            if (cols.Contains("WKN")) { insertCols.Add("WKN"); insertParams.Add("@wkn"); }

                            if (insertCols.Count >= 1)
                            {
                                var ins = conn.CreateCommand();
                                ins.CommandText = $"INSERT INTO NEW_Holdings ({string.Join(',', insertCols)}) VALUES ({string.Join(',', insertParams)});";
                                ins.Parameters.AddWithValue("@isin", isin);
                                ins.Parameters.AddWithValue("@pv", purchaseValue);
                                if (percent.HasValue) ins.Parameters.AddWithValue("@pct", percent.Value);
                                else ins.Parameters.AddWithValue("@pct", DBNull.Value);
                                ins.Parameters.AddWithValue("@upd", updated ?? (object)DBNull.Value);
                                if (!string.IsNullOrWhiteSpace(provider)) ins.Parameters.AddWithValue("@prov", provider);
                                else ins.Parameters.AddWithValue("@prov", DBNull.Value);
                                if (todayValue.HasValue) ins.Parameters.AddWithValue("@tv", todayValue.Value);
                                else ins.Parameters.AddWithValue("@tv", DBNull.Value);
                                if (cols.Contains("WKN"))
                                {
                                    if (!string.IsNullOrWhiteSpace(wknValue)) ins.Parameters.AddWithValue("@wkn", wknValue);
                                    else ins.Parameters.AddWithValue("@wkn", DBNull.Value);
                                }

                                try { ins.ExecuteNonQuery(); }
                                catch { /* best-effort insert; ignore failures */ }
                            }
                        }
                        catch { }
                    }
                }
                catch { }
            }
        }
        catch { }
    }

    static void UpsertTotalValues(string dbPath)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath))
                return;

            var cs = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder
            {
                DataSource = dbPath
            }.ToString();

            using var conn = new Microsoft.Data.Sqlite.SqliteConnection(cs);
            conn.Open();

            // If an existing NEW_TotalValues table has a CHECK(Id = 1) constraint
            // we need to migrate it to the appendable schema (Id AUTOINCREMENT).
            try
            {
                using var chk = conn.CreateCommand();
                chk.CommandText = "SELECT sql FROM sqlite_master WHERE type='table' AND name='NEW_TotalValues';";
                var existingSql = chk.ExecuteScalar() as string;
                if (!string.IsNullOrWhiteSpace(existingSql) && existingSql.IndexOf("CHECK (Id = 1)", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // perform migration: create new table, copy data, drop old, rename new
                    try
                    {
                        using var txm = conn.BeginTransaction();
                        using var c1 = conn.CreateCommand();
                        c1.CommandText = @"
CREATE TABLE IF NOT EXISTS NEW_TotalValues_new (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    TotalRows INTEGER,
    TotalShares INTEGER,
    SumAvgTotal REAL,
    SumTodayValue REAL,
    LastUpdated TEXT
);";
                        c1.ExecuteNonQuery();

                        using var c2 = conn.CreateCommand();
                        c2.CommandText = @"INSERT INTO NEW_TotalValues_new (TotalRows, TotalShares, SumAvgTotal, SumTodayValue, LastUpdated)
SELECT TotalRows, TotalShares, SumAvgTotal, SumTodayValue, LastUpdated FROM NEW_TotalValues;";
                        try { c2.ExecuteNonQuery(); } catch { /* ignore if nothing to copy */ }

                        using var c3 = conn.CreateCommand();
                        c3.CommandText = "DROP TABLE IF EXISTS NEW_TotalValues;";
                        c3.ExecuteNonQuery();

                        using var c4 = conn.CreateCommand();
                        c4.CommandText = "ALTER TABLE NEW_TotalValues_new RENAME TO NEW_TotalValues;";
                        c4.ExecuteNonQuery();

                        txm.Commit();
                    }
                    catch
                    {
                        // migration failed - leave old table in place
                    }
                }

                // ensure final table exists with desired schema
                using var cmdCreate = conn.CreateCommand();
                cmdCreate.CommandText = @"
CREATE TABLE IF NOT EXISTS NEW_TotalValues (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    TotalRows INTEGER,
    TotalShares INTEGER,
    SumAvgTotal REAL,
    SumTodayValue REAL,
    LastUpdated TEXT
);";
                cmdCreate.ExecuteNonQuery();
            }
            catch { }

            long totalRows = 0;
            long totalShares = 0;
            double sumAvgTotal = 0;
            double sumToday = 0;

            // Read aggregates
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText =
                    "SELECT COUNT(1), SUM(Shares), SUM(TotalValue), SUM(TodayValue) FROM NEW_Holdings WHERE Isin IS NOT NULL;";

                using var reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    totalRows = reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
                    if (reader.IsDBNull(1))
                    {
                        totalShares = 0;
                    }
                    else
                    {
                        try
                        {
                            // try integer conversion first
                            totalShares = Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
                        }
                        catch
                        {
                            try
                            {
                                // fallback: parse as double and round to nearest long
                                var d = Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture);
                                // remove any fractional part (truncate) so TotalShares is stored as integer
                                totalShares = (long)Math.Truncate(d);
                            }
                            catch
                            {
                                totalShares = 0;
                            }
                        }
                    }

                    sumAvgTotal = reader.IsDBNull(2) ? 0 :
                        Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture);
                    sumToday = reader.IsDBNull(3) ? 0 :
                        Convert.ToDouble(reader.GetValue(3), CultureInfo.InvariantCulture);
                }
            }
            catch { }

            // Insert new row (append)
            try
            {
                using var tx = conn.BeginTransaction();

                using var up = conn.CreateCommand();
                up.CommandText = @"
INSERT INTO NEW_TotalValues 
(TotalRows, TotalShares, SumAvgTotal, SumTodayValue, LastUpdated) 
VALUES (@r, @s, @a, @t, @u);";

                up.Parameters.AddWithValue("@r", totalRows);
                up.Parameters.AddWithValue("@s", totalShares);
                up.Parameters.AddWithValue("@a", sumAvgTotal);
                up.Parameters.AddWithValue("@t", sumToday);
                up.Parameters.AddWithValue("@u",
                    DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

                up.ExecuteNonQuery();
                tx.Commit();
            }
            catch { }

            conn.Close();
        }
        catch { }
    }
}