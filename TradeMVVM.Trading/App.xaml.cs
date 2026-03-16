using System.Windows;
using System.Threading.Tasks;
using System.Threading;
using TradeMVVM.Trading.Presentation.ViewModels;
using Microsoft.Extensions.DependencyInjection;
using System;
using TradeMVVM.Trading.Services.Di;
using TradeMVVM.Trading.Infrastructure;

namespace TradeMVVM.Trading
{
    public partial class App : Application
    {
        public static IServiceProvider Services { get; private set; }
        // hold a reference to the running MainViewModel so we can call Stop on program abort
        public static TradeMVVM.Trading.Presentation.ViewModels.MainViewModel MainViewModelInstance { get; set; }

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

            var services = new ServiceCollection();
            services.AddTradingServices();
            Services = services.BuildServiceProvider();
            // Register handlers to cleanup any chromedriver processes we tracked on stop/exit/crash
            this.Exit += (s, ev) =>
            {
                TryStopAndCleanup();
            };

            this.DispatcherUnhandledException += (s, ev) =>
            {
                try { TradeMVVM.Trading.Infrastructure.Logger.LogException(ev.Exception, "DispatcherUnhandledException"); } catch { }
                TryStopAndCleanup();
            };

            AppDomain.CurrentDomain.ProcessExit += (s, ev) =>
            {
                TryStopAndCleanup();
            };
            // also handle non-UI unhandled exceptions and unobserved task exceptions
            AppDomain.CurrentDomain.UnhandledException += (s, ev) =>
            {
                try { TradeMVVM.Trading.Infrastructure.Logger.LogUnhandled(ev.ExceptionObject, "AppDomain.CurrentDomain.UnhandledException"); } catch { }
                TryStopAndCleanup();
            };

            TaskScheduler.UnobservedTaskException += (s, ev) =>
            {
                try { TradeMVVM.Trading.Infrastructure.Logger.LogException(ev.Exception, "TaskScheduler.UnobservedTaskException"); } catch { }
                TryStopAndCleanup();
            };

            // watchdog script removed: cleanup performed synchronously in TryStopAndCleanup
        }

        private void TryStopAndCleanup()
        {
            try
            {
                // Prefer calling Stop() directly on the known MainViewModel instance (synchronous)
                try
                {
                    var globalVm = MainViewModelInstance;
                    if (globalVm != null)
                    {
                        try
                        {
                            var cmd = globalVm.StopCommand;
                            if (cmd != null && cmd.CanExecute(null))
                            {
                                try { Application.Current?.Dispatcher?.Invoke(() => cmd.Execute(null)); } catch { try { cmd.Execute(null); } catch { } }
                            }
                            else
                            {
                                try { globalVm.Dispose(); } catch { }
                            }
                        }
                        catch { try { globalVm.Dispose(); } catch { } }
                    }
                }
                catch { }

                // Also attempt to call Stop on any view-bound VM (fallback)
                try
                {
                    var win = Application.Current?.MainWindow;
                    if (win != null)
                    {
                        var vm = win.DataContext as MainViewModel;
                        if (vm != null && !ReferenceEquals(vm, MainViewModelInstance))
                        {
                            try
                            {
                                var cmd = vm.StopCommand;
                                if (cmd != null && cmd.CanExecute(null))
                                {
                                    try { win.Dispatcher.Invoke(() => cmd.Execute(null)); } catch { try { cmd.Execute(null); } catch { } }
                                }
                                else
                                {
                                    try { vm.Dispose(); } catch { }
                                }
                            }
                            catch { try { vm.Dispose(); } catch { } }
                        }
                    }
                }
                catch { }

                // give a short grace period for clean shutdown
                try
                {
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    while (sw.Elapsed < TimeSpan.FromSeconds(2))
                    {
                        try
                        {
                            // attempt non-forced cleanup of any registered drivers
                            TradeMVVM.Trading.Services.Providers.GettexProvider.CleanupRegisteredDrivers(force: false);
                        }
                        catch { }
                        Thread.Sleep(200);
                    }
                }
                catch { }

                // final aggressive cleanup
                try { TradeMVVM.Trading.Services.Providers.GettexProvider.CleanupRegisteredDrivers(force: true); } catch { }
                // and as a last resort kill any chrome/chromedriver processes
                try
                {
                    foreach (var p in System.Diagnostics.Process.GetProcessesByName("chromedriver"))
                    {
                        try { p.Kill(); } catch { }
                    }
                }
                catch { }
                try
                {
                    foreach (var p in System.Diagnostics.Process.GetProcessesByName("chrome"))
                    {
                        try { p.Kill(); } catch { }
                    }
                }
                catch { }
            }
            catch (Exception ex)
            {
                try { Logger.LogException(ex, "TryStopAndCleanup"); } catch { }
            }
        }
    }
}
