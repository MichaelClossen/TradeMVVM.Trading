using Microsoft.Extensions.DependencyInjection;
using System;

namespace TradeMVVM.Trading.Services.Di
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddTradingServices(this IServiceCollection services)
        {
            // core
            services.AddSingleton<SettingsService>();
            services.AddSingleton(provider => new System.Net.Http.HttpClient
            {
                Timeout = TimeSpan.FromSeconds(8)
            });

            // Server control service (shared instance) so UI components use the same DB control object
            services.AddSingleton<Services.ServerControlService>();

            // throttle — separate throttles so slow Selenium (Gettex) does not starve fast HTTP (BNP)
            var bnpThrottle = new System.Threading.SemaphoreSlim(4);
            var gettexThrottle = new System.Threading.SemaphoreSlim(1);
            // keep a general throttle for ChartDataProvider (legacy ctor requirement)
            services.AddSingleton(new System.Threading.SemaphoreSlim(2));

            // Register a single shared GettexProvider so ChromeDriver is reused
            // and we don't start a new chromedriver process for every request.
            services.AddSingleton<Providers.GettexProvider>(sp =>
                new Providers.GettexProvider(gettexThrottle));
            // Register BNP provider as transient (HTTP-based)
            services.AddTransient<Providers.BnpProvider>(sp =>
                new Providers.BnpProvider(
                    sp.GetRequiredService<System.Net.Http.HttpClient>(),
                    bnpThrottle));
            // Register IPriceProvider as a singleton pointing to the shared GettexProvider
            services.AddSingleton<TradeMVVM.Trading.Services.Providers.IPriceProvider>(sp =>
                sp.GetRequiredService<Providers.GettexProvider>());

            // ChartDataProvider with injected settings and logger
            // unresolved logger from settings (dev folder path)
            services.AddSingleton<Services.Infrastructure.IUnresolvedIsinLogger>(sp =>
            {
                var settings = sp.GetRequiredService<SettingsService>();
                var devFolder = settings.UnresolvedLogFolder;
                return new Services.Infrastructure.DevFolderUnresolvedIsinLogger(devFolder);
            });

            services.AddTransient<ChartDataProvider>(sp =>
            {
                var settings = sp.GetRequiredService<SettingsService>();
                var client = sp.GetRequiredService<System.Net.Http.HttpClient>();
                var throttle = sp.GetRequiredService<System.Threading.SemaphoreSlim>();
                var logger = sp.GetRequiredService<Services.Infrastructure.IUnresolvedIsinLogger>();

                // ChartDataProvider no longer requires a BNP provider parameter; pass logger as last arg
                var provider = new ChartDataProvider(settings, client, throttle, logger);
                return provider;
            });

            return services;
        }
    }
}
