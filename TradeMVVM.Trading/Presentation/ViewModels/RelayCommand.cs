using System;
using System.Windows.Input;
using System.Windows;

public class RelayCommand : ICommand
{
    private readonly Action _execute;
    private readonly Predicate<object> _canExecute;

    public RelayCommand(Action execute)
        : this(execute, null)
    {
    }

    public RelayCommand(Action execute, Predicate<object> canExecute)
    {
        _execute = execute ?? throw new ArgumentNullException(nameof(execute));
        _canExecute = canExecute;
    }

    public event EventHandler CanExecuteChanged;

    public bool CanExecute(object parameter)
    {
        return _canExecute?.Invoke(parameter) ?? true;
    }

    public void Execute(object parameter)
    {
        _execute();
    }

    public void RaiseCanExecuteChanged()
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
}
