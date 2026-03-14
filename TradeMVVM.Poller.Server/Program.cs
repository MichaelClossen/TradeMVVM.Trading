using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TradeMVVM.Poller.Core;
using TradeMVVM.Trading.Services;
using System.Threading.Tasks;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((ctx, cfg) =>
    {
        cfg.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
        cfg.AddEnvironmentVariables();
    })
    .ConfigureServices((ctx, services) =>
    {
        services.AddSingleton<ChartDataProvider>();
        services.AddSingleton<TradeMVVM.Trading.Data.PriceRepository>();
        services.AddSingleton<TradeMVVM.Trading.Services.DatabaseService>();
        services.AddHostedService<PollerBackgroundService>();
    })
    .ConfigureLogging((ctx, lb) =>
    {
        lb.ClearProviders();
        lb.AddConsole();
    })
    .Build();

await host.RunAsync();
