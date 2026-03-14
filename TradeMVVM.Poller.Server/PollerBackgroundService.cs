using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TradeMVVM.Poller.Core;
using TradeMVVM.Domain;

public class PollerBackgroundService : BackgroundService
{
    private readonly ILogger<PollerBackgroundService> _logger;
    private readonly ChartDataProvider _provider;
    private readonly TradeMVVM.Trading.Data.PriceRepository _repo;

    public PollerBackgroundService(ILogger<PollerBackgroundService> logger, ChartDataProvider provider, TradeMVVM.Trading.Data.PriceRepository repo)
    {
        _logger = logger;
        _provider = provider;
        _repo = repo;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("PollerBackgroundService starting");

        var service = new PricePollingServiceCore(_provider, _repo, TimeSpan.FromSeconds(2), maxDegreeOfParallelism: 6);

        // sample stock list - in real deployments this should be configurable via DB or configuration
        var stocks = new List<(string isin, string name, StockType type)>
        {
            ("DE000BASF111", "BASF", StockType.Aktie),
            ("DE0007664039", "Siemens", StockType.Aktie),
        };

        try
        {
            await service.StartAsync(stocks, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // expected on shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PollerBackgroundService crashed");
        }

        _logger.LogInformation("PollerBackgroundService stopping");
    }
}
