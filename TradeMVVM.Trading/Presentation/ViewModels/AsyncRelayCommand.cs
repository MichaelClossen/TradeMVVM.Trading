using System;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Windows;

public class AsyncRelayCommand : ICommand
{
    private readonly Func<Task> _execute;
    private bool _isExecuting;

    public AsyncRelayCommand(Func<Task> execute)
    {
        _execute = execute;
    }

    public event EventHandler CanExecuteChanged;

    public bool CanExecute(object parameter)
    {
        return !_isExecuting;
    }

    public async void Execute(object parameter)
    {
        _isExecuting = true;
        RaiseCanExecuteChanged();

        try
        {
            await _execute();
        }
        finally
        {
            _isExecuting = false;
            RaiseCanExecuteChanged();
        }
    }

    private void RaiseCanExecuteChanged()
    {
        var handler = CanExecuteChanged;
        if (handler == null)
            return;

        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher != null && !dispatcher.CheckAccess())
        {
            dispatcher.BeginInvoke(new Action(() => handler(this, EventArgs.Empty)));
        }
        else
        {
            handler(this, EventArgs.Empty);
        }
    }

    // Expose a public method to force a CanExecuteChanged notification (used by callers to refresh UI)
    public void RefreshCanExecute()
    {
        RaiseCanExecuteChanged();
    }
}
