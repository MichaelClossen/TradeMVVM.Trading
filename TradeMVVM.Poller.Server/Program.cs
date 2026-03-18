using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TradeMVVM.Trading.Services;
using TradeMVVM.Trading.Services.Di;
using System.Threading.Tasks;
using System.Reflection;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((ctx, cfg) =>
    {
        cfg.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
        cfg.AddEnvironmentVariables();
    })
    .ConfigureServices((ctx, services) =>
    {
        // Register the same trading services as the GUI so providers (BNP/Gettex), throttles and HttpClient are available
        services.AddTradingServices();

        // PriceRepository used by the poller
        services.AddSingleton<TradeMVVM.Trading.Data.PriceRepository>();

        services.AddHostedService<PollerBackgroundService>();
    })
    .ConfigureLogging((ctx, lb) =>
    {
        lb.ClearProviders();
        lb.AddConsole();
    })
    .Build();

// Attempt to set the GUI App static service provider via reflection so ChartDataProvider and other
// code that relies on TradeMVVM.Trading.App.Services can resolve services when running in the server
try
{
    var appType = Type.GetType("TradeMVVM.Trading.App, TradeMVVM.Trading");
    if (appType != null)
    {
        var prop = appType.GetProperty("Services", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
        if (prop != null)
        {
            try { prop.SetValue(null, host.Services); } catch { }
        }
    }
}
catch { }


// Best-effort: if an older instance of the poller is still running (started outside
// of the current supervisor), try to terminate it now so we don't end up with
// multiple competing pollers writing to the same DB or holding DLL locks.
try
{
    var loggerFactory = host.Services.GetService(typeof(ILoggerFactory)) as ILoggerFactory;
    var logger = loggerFactory?.CreateLogger("PollerStartup");
    var curId = System.Diagnostics.Process.GetCurrentProcess().Id;

    try
    {
        var procs = System.Diagnostics.Process.GetProcessesByName("TradeMVVM.Poller.Server");
        foreach (var p in procs)
        {
            try
            {
                if (p.Id == curId) continue;
                logger?.LogInformation($"Terminating stray poller process PID={p.Id}");
                p.Kill(true);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, $"Failed to kill process {p.Id}");
            }
        }
    }
    catch (Exception ex)
    {
        logger?.LogDebug(ex, "Error enumerating named poller processes");
    }

    // also check `dotnet` processes that may be running the poller via `dotnet run`
    try
    {
        foreach (var p in System.Diagnostics.Process.GetProcessesByName("dotnet"))
        {
            try
            {
                if (p.Id == curId) continue;
                string cmd = string.Empty;
                try
                {
                    using var mos = new System.Management.ManagementObjectSearcher($"SELECT CommandLine FROM Win32_Process WHERE ProcessId = {p.Id}");
                    foreach (System.Management.ManagementObject mo in mos.Get())
                        cmd = mo["CommandLine"]?.ToString() ?? string.Empty;
                }
                catch { }

                if (!string.IsNullOrWhiteSpace(cmd) && cmd.IndexOf("TradeMVVM.Poller.Server", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    logger?.LogInformation($"Terminating stray dotnet poller PID={p.Id} Cmd={cmd}");
                    try { p.Kill(true); } catch (Exception ex) { logger?.LogWarning(ex, $"Failed to kill dotnet process {p.Id}"); }
                }
            }
            catch (Exception ex)
            {
                logger?.LogDebug(ex, "Inspecting dotnet process failed");
            }
        }
    }
    catch (Exception ex)
    {
        logger?.LogDebug(ex, "Error enumerating dotnet processes");
    }

}
catch { }

await host.RunAsync();

// Ensure Ctrl+C triggers graceful shutdown promptly
try
{
    Console.CancelKeyPress += (s, e) =>
    {
        // prevent the process from terminating immediately; request host stop
        e.Cancel = true;
        try
        {
            // best-effort: request stop and wait briefly for shutdown
            var t = host.StopAsync(TimeSpan.FromSeconds(10));
            try { t.Wait(); } catch { }
        }
        catch { }
    };
}
catch { }
