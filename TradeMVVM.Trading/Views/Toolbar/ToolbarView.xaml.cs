using System;
using System.Diagnostics;
using System.Windows.Controls;
using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Windows;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using TradeMVVM.Trading.Presentation.ViewModels;

namespace TradeMVVM.Trading.Views.Toolbar
{
    public partial class ToolbarView : UserControl
    {
        private System.Windows.Threading.DispatcherTimer _statusTimer;
        private System.Threading.Timer _backgroundStatusTimer;
        private readonly TimeSpan _statusInterval = TimeSpan.FromSeconds(30);
        // toggle flag: when true the next update attempt will set the text to Red; otherwise White
        private bool _nextAttemptRed = true;

        public ToolbarView()
        {
            InitializeComponent();
            try
            {
                var txtE = this.FindName("TxtPollingEnabled") as System.Windows.Controls.TextBlock;
                if (txtE != null) txtE.Foreground = System.Windows.Media.Brushes.White;
            }
            catch { }
            Loaded += ToolbarView_Loaded;
            Unloaded += ToolbarView_Unloaded;
            // trigger an immediate status check once UI is initialized
            try { Task.Run(() => UpdateServerStatusAsync()); } catch { }
        }

        private void DbCleanupButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // run DB cleanup on background thread to avoid UI freeze
                Task.Run(() =>
                {
                    try
                    {
                        var db = new TradeMVVM.Trading.Services.DatabaseService();
                        db.RemoveZeroOrNullPrices();
                        db.RemoveDuplicateEntries();
                        db.Vacuum();
                        try { Application.Current?.Dispatcher?.BeginInvoke(new Action(() => MessageBox.Show("DB cleanup completed."))); } catch { }
                    }
                    catch (Exception ex)
                    {
                        try { Application.Current?.Dispatcher?.BeginInvoke(new Action(() => MessageBox.Show($"DB cleanup failed: {ex.Message}"))); } catch { }
                    }
                });
            }
            catch { }
        }

        private void BtnDiag_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // gather diagnostics
                var sb = new System.Text.StringBuilder();
                try
                {
                    var svc = new TradeMVVM.Trading.Services.ServerControlService();
                    sb.AppendLine($"GUI DB: {svc.ConnectionString}");
                    try { sb.AppendLine($"PollingEnabled: {svc.IsPollingEnabled()}"); } catch { sb.AppendLine("PollingEnabled: ?"); }
                    try { sb.AppendLine($"LastHeartbeat: {svc.GetLastHeartbeat()?.ToString("O") ?? "-"}"); } catch { sb.AppendLine("LastHeartbeat: ?"); }
                }
                catch { sb.AppendLine("ServerControlService: failed"); }

                sb.AppendLine();
                sb.AppendLine("Running processes (matching TradeMVVM.Poller.Server):");
                try
                {
                    foreach (var p in System.Diagnostics.Process.GetProcesses())
                    {
                        try
                        {
                            var cmd = string.Empty;
                            try
                            {
                                using (var mos = new System.Management.ManagementObjectSearcher($"SELECT CommandLine FROM Win32_Process WHERE ProcessId = {p.Id}"))
                                {
                                    foreach (System.Management.ManagementObject mo in mos.Get())
                                        cmd = mo["CommandLine"]?.ToString() ?? string.Empty;
                                }
                            }
                            catch { }

                            if (p.ProcessName.IndexOf("TradeMVVM.Poller.Server", StringComparison.OrdinalIgnoreCase) >= 0 || cmd.IndexOf("TradeMVVM.Poller.Server", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                sb.AppendLine($"PID={p.Id} Name={p.ProcessName} Cmd={cmd}");
                            }
                        }
                        catch { }
                    }
                }
                catch { sb.AppendLine("Process list failed"); }

                sb.AppendLine();
                sb.AppendLine("Last server stdout (tail 200):");
                try
                {
                    var outFile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "poller_out.txt");
                    if (System.IO.File.Exists(outFile))
                    {
                        var all = System.IO.File.ReadAllLines(outFile);
                        var lines = all.Skip(Math.Max(0, all.Length - 200));
                        foreach (var l in lines) sb.AppendLine(l);
                    }
                    else sb.AppendLine("poller_out.txt not found");
                }
                catch { sb.AppendLine("poller_out read failed"); }

                sb.AppendLine();
                sb.AppendLine("Last server stderr (tail 200):");
                try
                {
                    var errFile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "poller_err.txt");
                    if (System.IO.File.Exists(errFile))
                    {
                        var all2 = System.IO.File.ReadAllLines(errFile);
                        var lines2 = all2.Skip(Math.Max(0, all2.Length - 200));
                        foreach (var l in lines2) sb.AppendLine(l);
                    }
                    else sb.AppendLine("poller_err.txt not found");
                }
                catch { sb.AppendLine("poller_err read failed"); }

                // show in simple window
                Application.Current?.Dispatcher?.BeginInvoke(new Action(() =>
                {
                    var win = new Window
                    {
                        Title = "Poller Diagnostics",
                        Width = 900,
                        Height = 600,
                        Content = new ScrollViewer { Content = new TextBox { Text = sb.ToString(), IsReadOnly = true, AcceptsReturn = true, VerticalScrollBarVisibility = ScrollBarVisibility.Auto } }
                    };
                    win.Owner = Application.Current?.MainWindow;
                    win.WindowStartupLocation = WindowStartupLocation.CenterOwner;
                    win.ShowDialog();
                }));
            }
            catch { }
        }

        // Server start/stop removed from GUI; status only

        private void ToolbarView_Loaded(object? sender, RoutedEventArgs e)
        {
            try
            {
                // start periodic status updates
                // Start both a DispatcherTimer (for UI-thread aligned ticks) and a background timer
                _statusTimer = new System.Windows.Threading.DispatcherTimer { Interval = _statusInterval };
                _statusTimer.Tick += (s, ev) =>
                {
                    try { Trace.WriteLine($"Toolbar: dispatcher tick at {DateTime.Now:O}"); } catch { }
                    UpdateServerStatus();
                };
                _statusTimer.Start();

                // Background timer as a fallback to ensure periodic updates even if DispatcherTimer is paused
                try
                {
                    _backgroundStatusTimer?.Dispose();
                    _backgroundStatusTimer = new System.Threading.Timer(_ =>
                    {
                        try { Trace.WriteLine($"Toolbar: background timer callback at {DateTime.Now:O}"); } catch { }
                        Application.Current?.Dispatcher?.BeginInvoke(new Action(() => { try { UpdateServerStatus(); } catch { } }));
                    }, null, TimeSpan.Zero, _statusInterval);
                }
                catch { }
                // immediate lightweight status read to show times right away
                try
                {
                    var txtE = this.FindName("TxtPollingEnabled") as System.Windows.Controls.TextBlock;
                    var txtH = this.FindName("TxtLastHeartbeat") as System.Windows.Controls.TextBlock;
                    var now = DateTime.Now;
                    string hbDisplay = null;
                    bool running = false;
                    try
                    {
                        var svc = new TradeMVVM.Trading.Services.ServerControlService();
                        try { running = IsServerProcessRunning(); } catch { }
                        try
                        {
                            using var conn = new System.Data.SQLite.SQLiteConnection(svc.ConnectionString);
                            conn.Open();
                            using var cmd = new System.Data.SQLite.SQLiteCommand("SELECT LastHeartbeat FROM PollingControl WHERE Id = 1", conn);
                            var v = cmd.ExecuteScalar();
                            if (v != null && v != DBNull.Value)
                            {
                                var raw = v.ToString();
                                if (System.DateTimeOffset.TryParse(raw, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var dto))
                                    hbDisplay = dto.LocalDateTime.ToString("yyyy-MM-dd HH:mm:ss");
                                else if (DateTime.TryParse(raw, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeLocal, out var dt))
                                    hbDisplay = DateTime.SpecifyKind(dt, DateTimeKind.Local).ToString("yyyy-MM-dd HH:mm:ss");
                                else
                                    hbDisplay = raw;
                            }
                        }
                        catch { }
                    }
                    catch { }

                    try
                    {
                        if (txtE != null)
                        {
                            txtE.Text = $"Now: {now:yyyy-MM-dd HH:mm:ss}  •  Heartbeat: {(hbDisplay ?? "-")}";
                            try { txtE.Foreground = System.Windows.Media.Brushes.White; } catch { }
                        }
                        if (txtH != null)
                        {
                            txtH.Text = $"Server: {(running ? "RUNNING" : "STOPPED")}";
                            try { txtH.Foreground = running ? System.Windows.Media.Brushes.Green : System.Windows.Media.Brushes.Red; } catch { }
                        }
                    }
                    catch { }
                }
                catch { }

                try { UpdateServerStatus(); } catch { }
            }
            catch { }
        }

        private void ToolbarView_Unloaded(object? sender, RoutedEventArgs e)
        {
            try
            {
                if (_statusTimer != null)
                {
                    _statusTimer.Stop();
                    _statusTimer = null;
                }
                try
                {
                    _backgroundStatusTimer?.Dispose();
                    _backgroundStatusTimer = null;
                }
                catch { }
            }
            catch { }
        }

        private void UpdateServerStatus()
        {
            try
            {
                // Run the status update on a background thread to avoid blocking the UI thread.
                try { Task.Run(() => UpdateServerStatusAsync()); } catch { }
            }
            catch { }
        }

        private void UpdateServerStatusAsync()
        {
            try { Trace.WriteLine("Toolbar: UpdateServerStatusAsync entered"); } catch { }
            try
            {
                MainViewModel vm = null;
                try
                {
                    // DataContext is a DependencyObject property; access it on the UI thread to avoid
                    // InvalidOperationException when this method runs on a background thread.
                    Application.Current?.Dispatcher?.Invoke(new Action(() =>
                    {
                        vm = this.DataContext as MainViewModel ?? Application.Current?.MainWindow?.DataContext as MainViewModel;
                    }));
                }
                catch
                {
                    // best-effort fallback (shouldn't normally be reached)
                    try { vm = this.DataContext as MainViewModel ?? Application.Current?.MainWindow?.DataContext as MainViewModel; } catch { vm = null; }
                }
                TradeMVVM.Trading.Services.ServerControlService svc = null;
                try
                {
                    svc = vm?.GetType().GetField("_serverControl", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)?.GetValue(vm) as TradeMVVM.Trading.Services.ServerControlService;
                }
                catch { svc = null; }

                if (svc == null)
                    svc = new TradeMVVM.Trading.Services.ServerControlService();

                DateTime? hb = null;
                try { hb = svc.GetLastHeartbeat(); } catch { hb = null; }

                // indicate update attempt by toggling the 'Now' line color between Red and White
                try
                {
                    Application.Current?.Dispatcher?.BeginInvoke(new Action(() =>
                    {
                        try
                        {
                            var txtE = this.FindName("TxtPollingEnabled") as System.Windows.Controls.TextBlock ?? (this as dynamic).TxtPollingEnabled as System.Windows.Controls.TextBlock;
                            if (txtE != null)
                            {
                                txtE.Foreground = _nextAttemptRed ? System.Windows.Media.Brushes.Red : System.Windows.Media.Brushes.White;
                                _nextAttemptRed = !_nextAttemptRed;
                            }
                        }
                        catch { }
                    }));
                }
                catch { }

                // always read raw DB value too so GUI shows exactly what's stored in the DB
                string rawHeartbeat = null;
                try
                {
                    using var conn = new System.Data.SQLite.SQLiteConnection(svc.ConnectionString);
                    conn.Open();
                    using var cmd = new System.Data.SQLite.SQLiteCommand("SELECT LastHeartbeat FROM PollingControl WHERE Id = 1", conn);
                    var v = cmd.ExecuteScalar();
                    if (v != null && v != DBNull.Value)
                    {
                        rawHeartbeat = v.ToString();
                        // if parsing of svc.GetLastHeartbeat failed earlier, try to parse raw to hb for running check
                        if (hb == null)
                        {
                            if (System.DateTimeOffset.TryParse(rawHeartbeat, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var dto))
                                hb = dto.LocalDateTime;
                            else if (DateTime.TryParse(rawHeartbeat, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeLocal, out var dtLocal))
                                hb = DateTime.SpecifyKind(dtLocal, DateTimeKind.Local);
                        }
                    }
                }
                catch { }

                // consider server running if heartbeat is recent OR a server process is present
                var isRunning = false;
                try
                {
                if (hb.HasValue)
                {
                    // Treat heartbeat as local time and compare against local now to decide running state.
                    DateTime hbLocal = hb.Value.Kind == DateTimeKind.Utc ? hb.Value.ToLocalTime() : hb.Value;
                    var age = DateTime.Now - hbLocal;
                    if (age <= TimeSpan.FromSeconds(15)) isRunning = true;
                }
                }
                catch { isRunning = false; }

                try
                {
                    if (!isRunning && IsServerProcessRunning())
                        isRunning = true;
                }
                catch { }

                var brush = isRunning ? System.Windows.Media.Brushes.Green : System.Windows.Media.Brushes.Red;

                Application.Current?.Dispatcher?.BeginInvoke(new Action(() =>
                {
                    try
                    {
                        // prefer direct named fields when available to avoid namescope issues
                        var txtE = this.FindName("TxtPollingEnabled") as System.Windows.Controls.TextBlock ?? (this as dynamic).TxtPollingEnabled as System.Windows.Controls.TextBlock;
                        var txtH = this.FindName("TxtLastHeartbeat") as System.Windows.Controls.TextBlock ?? (this as dynamic).TxtLastHeartbeat as System.Windows.Controls.TextBlock;
                        try { Trace.WriteLine($"Toolbar: isRunning={isRunning} hb={(hb.HasValue ? hb.Value.ToString("O") : rawHeartbeat ?? "-")}"); } catch { }
                        if (txtE != null)
                        {
                            // show Now and Heartbeat side-by-side, heartbeat shown 1:1 (raw), Now formatted to match raw ISO style
                            var now = DateTime.Now;
                            string nowStr = now.ToString("yyyy-MM-dd HH:mm:ss");
                            if (!string.IsNullOrEmpty(rawHeartbeat))
                            {
                                // display Now in same format as DB raw and heartbeat raw 1:1
                                txtE.Text = $"Now: {nowStr}  •  Heartbeat: {rawHeartbeat}";
                                try { txtE.ToolTip = $"Heartbeat raw: {rawHeartbeat}"; } catch { }
                            }
                            else if (hb.HasValue)
                            {
                                var hbStr = hb.Value.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff");
                                txtE.Text = $"Now: {nowStr}  •  Heartbeat: {hbStr}";
                                try { txtE.ToolTip = $"Heartbeat raw: {hbStr}"; } catch { }
                            }
                            else
                            {
                                txtE.Text = $"Now: {nowStr}  •  Heartbeat: -";
                            }
                        }
                        if (txtH != null)
                        {
                            // move server running state to the second (smaller) line
                            txtH.Text = $"Server: {(isRunning ? "RUNNING" : "STOPPED")}";
                            try { txtH.Foreground = brush; } catch { }
                        }
                    }
                    catch (Exception ex) { try { Trace.WriteLine($"Toolbar: UI update failed: {ex}"); } catch { } }
                }));
            }
            catch { }
        }

                // removed duplicate handler block

        private void BtnServerStop_LeftClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            try
            {
                var vm = DataContext as MainViewModel ?? Application.Current?.MainWindow?.DataContext as MainViewModel;
                if (vm == null) return;
                var svc = vm?.GetType().GetField("_serverControl", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)?.GetValue(vm) as TradeMVVM.Trading.Services.ServerControlService;
                if (svc == null) svc = new TradeMVVM.Trading.Services.ServerControlService();

                // left click = interrupt (pause) -> set polling enabled = false but do not kill server
                try
                {
                    svc.SetPollingEnabled(false);
                    // set start button orange to indicate paused
                    var btn = this.FindName("BtnServerStart") as System.Windows.Controls.Button;
                    if (btn != null) btn.Background = System.Windows.Media.Brushes.Orange;
                }
                catch { }
            }
            catch { }
        }

        private void BtnServerStop_RightClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            try
            {
                var vm = DataContext as MainViewModel ?? Application.Current?.MainWindow?.DataContext as MainViewModel;
                if (vm == null) return;
                var svc = vm?.GetType().GetField("_serverControl", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)?.GetValue(vm) as TradeMVVM.Trading.Services.ServerControlService;
                if (svc == null) svc = new TradeMVVM.Trading.Services.ServerControlService();

                // right click = stop/terminate -> set polling disabled and mark start button red
                try
                {
                    svc.SetPollingEnabled(false);
                    var btn = this.FindName("BtnServerStart") as System.Windows.Controls.Button;
                    if (btn != null) btn.Background = System.Windows.Media.Brushes.Red;
                }
                catch { }
            }
            catch { }
        }

        private static bool IsServerProcessRunning()
        {
            try
            {
                // quick check: any process whose name contains 'poller' (case-insensitive)
                foreach (var p in System.Diagnostics.Process.GetProcesses())
                {
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(p.ProcessName) && p.ProcessName.IndexOf("poller", StringComparison.OrdinalIgnoreCase) >= 0)
                            return true;

                        // inspect command line for dotnet or other wrappers
                        try
                        {
                            using (var mos = new System.Management.ManagementObjectSearcher($"SELECT CommandLine FROM Win32_Process WHERE ProcessId = {p.Id}"))
                            {
                                foreach (System.Management.ManagementObject mo in mos.Get())
                                {
                                    var cmd = mo["CommandLine"]?.ToString() ?? string.Empty;
                                    if (cmd.IndexOf("poller", StringComparison.OrdinalIgnoreCase) >= 0 || cmd.IndexOf("TradeMVVM.Poller.Server", StringComparison.OrdinalIgnoreCase) >= 0)
                                        return true;
                                }
                            }
                        }
                        catch { }
                    }
                    catch { }
                }
            }
            catch { }
            return false;
        }

        private static void KillServerProcesses()
        {
            try
            {
                // kill named exe processes
                foreach (var p in System.Diagnostics.Process.GetProcessesByName("TradeMVVM.Poller.Server"))
                {
                    try { p.Kill(); } catch { }
                }

                // kill dotnet processes running the poller (inspect command line)
                foreach (var p in System.Diagnostics.Process.GetProcessesByName("dotnet"))
                {
                    try
                    {
                        using (var mos = new System.Management.ManagementObjectSearcher($"SELECT ProcessId, CommandLine FROM Win32_Process WHERE ProcessId = {p.Id}"))
                        {
                            foreach (System.Management.ManagementObject mo in mos.Get())
                            {
                                var cmd = mo["CommandLine"]?.ToString() ?? string.Empty;
                                if (cmd.IndexOf("TradeMVVM.Poller.Server", StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    try { p.Kill(); } catch { }
                                }
                            }
                        }
                    }
                    catch { }
                }
            }
            catch { }
        }

        private void ZoomRangeSlider_ValueChanged(object sender, RoutedPropertyChangedEventArgs<double> e)
        {
            try
            {
                // allow reacting even during initial load so the initial slider value (2h) applies
                if (!IsLoaded && e.NewValue == e.OldValue)
                    return;
                var idx = (int)System.Math.Round(e.NewValue);
                var text = GetZoomLabel(idx);
                if (TxtZoomRange != null)
                    TxtZoomRange.Text = text;

                var vm = DataContext as MainViewModel;
                if (vm == null)
                    vm = Application.Current?.MainWindow?.DataContext as MainViewModel;
                var zoom = vm?.Zoom;
                if (zoom == null)
                    return;

                try { System.Diagnostics.Debug.WriteLine($"Toolbar: executing zoom index {idx}"); } catch { }
                ExecuteCommand(GetZoomCommand(zoom, idx));

                // request an immediate refresh of the charts so the new zoom is applied now
                try
                {
                    Application.Current?.Dispatcher?.BeginInvoke(new Action(() =>
                    {
                        try
                        {
                            var mw = Application.Current?.MainWindow as TradeMVVM.Trading.Views.MainWindow;
                            mw?.TriggerRefresh();
                        }
                        catch { }
                    }));
                }
                catch { }
            }
            catch { }
        }

        private static string GetZoomLabel(int idx)
        {
            switch (idx)
            {
                case 0: return "1m";
                case 1: return "5m";
                case 2: return "15m";
                case 3: return "30m";
                case 4: return "1h";
                case 5: return "2h";
                case 6: return "4h";
                case 7: return "6h";
                case 8: return "8h";
                case 9: return "1D";
                case 10: return "1W";
                case 11: return "1M";
                case 12: return "1Y";
                default: return "Auto";
            }
        }

        private static ICommand GetZoomCommand(ZoomCommands zoom, int idx)
        {
            switch (idx)
            {
                case 0: return zoom.Zoom1MinCommand;
                case 1: return zoom.Zoom5MinCommand;
                case 2: return zoom.Zoom15MinCommand;
                case 3: return zoom.Zoom30MinCommand;
                case 4: return zoom.Zoom1HourCommand;
                case 5: return zoom.Zoom2HourCommand;
                case 6: return zoom.Zoom4HourCommand;
                case 7: return zoom.Zoom6HourCommand;
                case 8: return zoom.Zoom8HourCommand;
                case 9: return zoom.ZoomDayCommand;
                case 10: return zoom.ZoomWeekCommand;
                case 11: return zoom.ZoomMonthCommand;
                case 12: return zoom.ZoomYearCommand;
                default: return zoom.AutoCommand;
            }
        }

        private static void ExecuteCommand(ICommand command)
        {
            try
            {
                if (command != null && command.CanExecute(null))
                    command.Execute(null);
            }
            catch { }
        }

        private void ResetLayoutDefaultsButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (Application.Current?.MainWindow is TradeMVVM.Trading.Views.MainWindow mainWindow)
                    mainWindow.ResetLayoutDefaults();
            }
            catch { }
        }

        private void KillChromeButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                TradeMVVM.Trading.Services.Providers.GettexProvider.CleanupRegisteredDrivers(force: true);
                try
                {
                    foreach (var p in System.Diagnostics.Process.GetProcessesByName("chrome"))
                    {
                        try { p.Kill(); } catch { }
                    }
                }
                catch { }
            }
            catch { }
        }
    }
}

