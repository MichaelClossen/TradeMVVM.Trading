using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Collections.Concurrent;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;
using System.Management;

namespace TradeMVVM.Trading.Services.Providers
{
    public class GettexProvider : IPriceProvider, IDisposable
    {
        // track chromedriver PIDs started by this process so we can clean them up on application exit
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<int, byte> _trackedDriverPids =
            new System.Collections.Concurrent.ConcurrentDictionary<int, byte>();

        // When true, providers should abort work quickly because the application is stopping/cleaning up
        private static volatile bool _isShuttingDown = false;
        public static bool IsShuttingDown
        {
            get => _isShuttingDown;
            set => _isShuttingDown = value;
        }

        public static void CleanupRegisteredDrivers(bool force = false)
        {
            try
            {
                foreach (var kv in _trackedDriverPids.Keys.ToList())
                {
                    try
                    {
                        var found = System.Diagnostics.Process.GetProcesses().FirstOrDefault(p => p.Id == kv);
                        if (found != null)
                        {
                            try { found.Kill(); } catch { }
                        }
                        _trackedDriverPids.TryRemove(kv, out _);
                    }
                    catch { }
                }

                if (force)
                {
                    try
                    {
                        foreach (var p in System.Diagnostics.Process.GetProcessesByName("chromedriver"))
                        {
                            try { p.Kill(); } catch { }
                            try
                            {
                                var search = new ManagementObjectSearcher($"SELECT ProcessId FROM Win32_Process WHERE ParentProcessId = {p.Id}");
                                foreach (ManagementObject mo in search.Get())
                                {
                                    try
                                    {
                                        var childId = Convert.ToInt32(mo["ProcessId"]);
                                        try { System.Diagnostics.Process.GetProcessById(childId).Kill(); } catch { }
                                    }
                                    catch { }
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

        private readonly SemaphoreSlim _throttle;
        private IWebDriver _driver;
        private WebDriverWait _wait;
        private int? _driverServicePid;
        private readonly DateTime _createdAt;
        private readonly object _driverLock = new object();
        // serialize access to the shared IWebDriver instance because IWebDriver is not thread-safe
        private readonly SemaphoreSlim _driverUsage = new SemaphoreSlim(1, 1);

        private readonly ConcurrentDictionary<string, DateTime> _lastTradeTimes =
            new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        // Expose last trade time for a given ISIN if available
        public DateTime? GetLastTradeTime(string isin)
        {
            if (string.IsNullOrWhiteSpace(isin)) return null;
            if (_lastTradeTimes.TryGetValue(isin, out var dt)) return dt;
            return null;
        }

        public GettexProvider(SemaphoreSlim throttle)
        {
            _createdAt = DateTime.UtcNow;
            _throttle = throttle;
        }

        private void EnsureDriver()
        {
            lock (_driverLock)
            {
                if (_driver != null)
                    return;

                try
                {
                    var options = new ChromeOptions();
                    options.AddArgument("--headless=new");
                    options.AddArgument("--window-size=1920,1080");
                    options.AddArgument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
                    options.AddArgument("--disable-blink-features=AutomationControlled");
                    options.AddExcludedArgument("enable-automation");
                    options.AddAdditionalOption("useAutomationExtension", false);
                    options.AddArgument("--no-sandbox");
                    options.AddArgument("--disable-gpu");
                    options.AddArgument("--disable-dev-shm-usage");
                    options.AddArgument("--disable-extensions");

                    var service = ChromeDriverService.CreateDefaultService();
                    service.HideCommandPromptWindow = true;
                    service.SuppressInitialDiagnosticInformation = true;

                    _driver = new ChromeDriver(service, options);

                    try
                    {


                        var svcPid = service.ProcessId;
                        if (svcPid > 0)
                        {
                            _driverServicePid = svcPid;
                            _trackedDriverPids.TryAdd(svcPid, 0);

                            try
                            {
                                var search = new ManagementObjectSearcher($"SELECT ProcessId FROM Win32_Process WHERE ParentProcessId = {svcPid}");
                                foreach (ManagementObject mo in search.Get())
                                {
                                    try { var childId = Convert.ToInt32(mo["ProcessId"]); _trackedDriverPids.TryAdd(childId, 0); } catch { }
                                }
                            }
                            catch { }
                        }
                    }
                    catch { }

                    try
                    {
                        var antiDetect =
                            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});" +
                            "Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});" +
                            "Object.defineProperty(navigator, 'languages', {get: () => ['de-DE','de']});" +
                            "window.chrome = window.chrome || { runtime: {} };";
                        ((IJavaScriptExecutor)_driver).ExecuteScript(antiDetect);
                    }
                    catch { }

                    _wait = new WebDriverWait(_driver, TimeSpan.FromSeconds(40)) { PollingInterval = TimeSpan.FromMilliseconds(200) };

                    try
                    {
                        _driver.Manage().Timeouts().PageLoad = TimeSpan.FromSeconds(60);
                        _driver.Manage().Timeouts().AsynchronousJavaScript = TimeSpan.FromSeconds(30);
                        _driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(2);
                    }
                    catch { }
                }
                catch (Exception ex)
                {
                    try { _driver?.Quit(); } catch { }
                    try { _driver?.Dispose(); } catch { }
                    _driver = null;
                    _wait = null;
                    throw new InvalidOperationException("Failed to initialize ChromeDriver", ex);
                }
            }
        }

        public async Task<(double, double, DateTime?)?> GetPriceAsync(string isin, List<string> attemptedUrls, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(isin))
                return null;

            var normalizedIsin = isin.Trim().ToUpperInvariant();
            var urls = new[] { $"https://www.gettex.de/fond/{normalizedIsin}/", $"https://www.gettex.de/zertifikat/{normalizedIsin}/", $"https://www.gettex.de/aktie/{normalizedIsin}/" };
            try { if (attemptedUrls != null) foreach (var u in urls) attemptedUrls.Add(u); } catch { }
            if (IsShuttingDown) return null;

            // frequently respect the caller cancellation token so Ctrl+C / shutdown is responsive
            bool throttleAcquired = false;
            try
            {
                await _throttle.WaitAsync(token);
                throttleAcquired = true;
            }
            catch (OperationCanceledException)
            {
                // cancellation requested -> return no result without logging
                return null;
            }

            try
            {
                EnsureDriver();
            }
            catch (Exception ex)
            {
                try { Trace.TraceWarning($"GettexProvider: EnsureDriver failed for ISIN {isin}: {ex}"); } catch { }
                try { _throttle.Release(); } catch { }
                return null;
            }

            if (_driver == null || _wait == null) { try { _throttle.Release(); } catch { } return null; }

            IWebDriver driver; WebDriverWait wait;
            lock (_driverLock) { driver = _driver; wait = _wait; }
            if (driver == null || wait == null) { try { _throttle.Release(); } catch { } return null; }

            bool driverUsageAcquired = false;
            try
            {
                // serialize all interactions with the shared IWebDriver instance
                await _driverUsage.WaitAsync(token);
                driverUsageAcquired = true;

                if (IsShuttingDown || token.IsCancellationRequested)
                    return null;

                foreach (var url in urls)
                {
                    if (IsShuttingDown || token.IsCancellationRequested)
                        return null;

                    bool navSucceeded = false;
                    for (int navAttempt = 0; navAttempt < 2 && !navSucceeded; navAttempt++)
                    {
                        try
                        {
                            // check cancellation just before a potentially blocking navigation
                            if (token.IsCancellationRequested || IsShuttingDown)
                                break;
                            driver.Navigate().GoToUrl(url);
                            navSucceeded = true;
                        }
                        catch (WebDriverException wex)
                        {
                            try { Trace.TraceWarning($"GettexProvider: navigation failed for {url}: {wex.Message}"); } catch { }
                            try { ResetDriver(); } catch { }
                            try { EnsureDriver(); lock (_driverLock) { driver = _driver; wait = _wait; } if (driver == null || wait == null) break; } catch { break; }
                        }
                        catch (ObjectDisposedException)
                        {
                            try { ResetDriver(); } catch { }
                            try { EnsureDriver(); lock (_driverLock) { driver = _driver; wait = _wait; } if (driver == null || wait == null) break; } catch { break; }
                        }
                    }

                    if (!navSucceeded) continue;

                    if (token.IsCancellationRequested || IsShuttingDown)
                    {
                        return null;
                    }

                    if (!TryWaitForReady(driver, wait)) { try { Trace.TraceWarning($"GettexProvider: readyState wait failed for URL {url}"); } catch { } continue; }

                    string priceText = TryWaitForText(driver, wait, ".last-price .v-tick-flash");
                    string percentText = TryWaitForText(driver, wait, ".changes .percentage");

                    string dateText = null;
                    try { var res = ((IJavaScriptExecutor)driver).ExecuteScript("var el=document.querySelector(arguments[0]); return el ? el.innerText : null;", ".trade-date div"); dateText = res as string; } catch { }

                    decimal lastPrice = ParseDecimal(priceText);
                    decimal percent = ParseDecimal((percentText ?? string.Empty).Replace("%", ""));

                    DateTime? parsedDate = null;
                    if (!string.IsNullOrWhiteSpace(dateText) && DateTime.TryParseExact(dateText.Trim(), "dd.MM.yyyy HH:mm", CultureInfo.GetCultureInfo("de-DE"), DateTimeStyles.None, out var parsed))
                    {
                        _lastTradeTimes[normalizedIsin] = parsed;
                        parsedDate = parsed;
                    }

                    return ((double)lastPrice, (double)percent, parsedDate);
                }

                return null;
            }
            catch (WebDriverTimeoutException)
            {
                return null;
            }
            catch (WebDriverException wex)
            {
                // Cancellation or disposal during shutdown may surface WebDriverExceptions — log as Information
                try { Trace.TraceInformation($"Gettex WebDriver exception for {isin}: {wex.Message}"); } catch { }
                try { ResetDriver(); } catch { }
                return null;
            }
            finally
            {
                if (driverUsageAcquired)
                {
                    try { _driverUsage.Release(); } catch { }
                }
                if (throttleAcquired)
                {
                    try { _throttle.Release(); } catch { }
                }
            }
        }

        private static decimal ParseDecimal(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return 0;

            input = input.Replace("\u00A0", "")
                         .Replace(".", "")
                         .Replace(",", ".");

            decimal.TryParse(input,
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out var val);

            return val;
        }

        public void Dispose()
        {
            try
            {
                _driver?.Quit();
                _driver?.Dispose();
            }
            catch { }
            // try to kill driver service PID if we captured it
            try
            {
                if (_driverServicePid.HasValue)
                {
                    try
                    {
                        var svcPid = _driverServicePid.Value;
                        var found = System.Diagnostics.Process.GetProcesses().FirstOrDefault(p => p.Id == svcPid);
                        if (found != null)
                        {
                            try { found.Kill(); } catch { }
                        }
                    }
                    catch { }
                    try { _trackedDriverPids.TryRemove(_driverServicePid.Value, out _); } catch { }
                    _driverServicePid = null;
                }
            }
            catch { }
            // central CleanupRegisteredDrivers will handle remaining processes at app exit
        }

        private void ResetDriver()
        {
            try
            {
                _driver?.Quit();
            }
            catch { }
            try
            {
                _driver?.Dispose();
            }
            catch { }
            // do not forcibly kill the service process here; just forget the pid and let Cleanup handle termination on app exit
            try
            {
                if (_driverServicePid.HasValue)
                {
                    try { _trackedDriverPids.TryRemove(_driverServicePid.Value, out _); } catch { }
                    _driverServicePid = null;
                }
            }
            catch { }

            _driver = null;
            _wait = null;
        }

        private string TryWaitForText(string selector)
        {
            // unused
            return null;
        }

        // helper overloads that operate on snapshot driver/wait instances
        private string TryWaitForText(IWebDriver driver, WebDriverWait wait, string selector)
        {
            if (wait == null || driver == null)
                return null;

            try
            {
                return wait.Until(d =>
                {
                    try
                    {
                        var js = (IJavaScriptExecutor)d;
                        var res = js.ExecuteScript(
                            "var el=document.querySelector(arguments[0]); return el ? el.innerText : null;",
                            selector);
                        var txt = res as string;
                        return string.IsNullOrWhiteSpace(txt) ? null : txt;
                    }
                    catch
                    {
                        return null;
                    }
                });
            }
            catch (WebDriverTimeoutException)
            {
                return null;
            }
            catch (WebDriverException wex)
            {
                try { Trace.TraceWarning($"GettexProvider: WebDriverException while waiting for selector '{selector}': {wex.Message}"); } catch { }
                try { ResetDriver(); } catch { }
                return null;
            }
            catch
            {
                return null;
            }
        }

        private bool TryWaitForReady(IWebDriver driver, WebDriverWait wait)
        {
            if (wait == null || driver == null)
                return false;

            try
            {
                return wait.Until(d =>
                {
                    try
                    {
                        var res = ((IJavaScriptExecutor)d).ExecuteScript("return document.readyState");
                        if (res == null) return false;
                        var s = res as string ?? res.ToString();
                        return string.Equals(s, "complete", StringComparison.OrdinalIgnoreCase);
                    }
                    catch
                    {
                        // transient WebDriver/HTTP errors while executing script - report not ready and let wait retry
                        return false;
                    }
                });
            }
            catch (WebDriverException wex)
            {
                try { Trace.TraceWarning($"GettexProvider: WebDriverException while waiting for readyState: {wex.Message}"); } catch { }
                try { ResetDriver(); } catch { }
                return false;
            }
            catch
            {
                return false;
            }
        }
    }
}
