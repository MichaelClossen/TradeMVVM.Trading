using System;
using System.Windows;
using System.Windows.Input;
using TradeMVVM.Trading.Services;

namespace TradeMVVM.Trading.ViewModels
{
    public class SettingsViewModel : TradeMVVM.Trading.Presentation.ViewModels.BaseViewModel
    {
        private readonly SettingsService _settings;

        public string BnpPriorityKeywords
        {
            get => _settings.BnpPriorityKeywords;
            set { _settings.BnpPriorityKeywords = value; OnPropertyChanged(); }
        }

        public bool BnpPriorityEnabled
        {
            get => _settings.BnpPriorityEnabled;
            set { _settings.BnpPriorityEnabled = value; OnPropertyChanged(); }
        }
        // legacy provider keyword settings removed (Wallstreet, LangUndSchwarz, DeutscheBoerse)

        // Prediction settings exposed to UI
        public string PredictionMethod
        {
            get => _settings.PredictionMethod;
            set { _settings.PredictionMethod = value; OnPropertyChanged(); }
        }

        public int PredictionWindowSize
        {
            get => _settings.PredictionWindowSize;
            set { _settings.PredictionWindowSize = value; OnPropertyChanged(); }
        }

        public double PredictionThresholdPercent
        {
            get => _settings.PredictionThresholdPercent;
            set { _settings.PredictionThresholdPercent = value; OnPropertyChanged(); }
        }

        public int UnresolvedIsinFailureThreshold
        {
            get => _settings.UnresolvedIsinFailureThreshold;
            set { _settings.UnresolvedIsinFailureThreshold = value; OnPropertyChanged(); }
        }

        public int PriceNanFailureThreshold
        {
            get => _settings.PriceNanFailureThreshold;
            set { _settings.PriceNanFailureThreshold = value; OnPropertyChanged(); }
        }

        public double PlAxisMarginEur
        {
            get => _settings.PlAxisMarginEur;
            set { _settings.PlAxisMarginEur = value; OnPropertyChanged(); try { _settings.Save(); } catch { } }
        }

        public double OutlierThresholdPercent
        {
            get => _settings.OutlierThresholdPercent;
            set { _settings.OutlierThresholdPercent = value; OnPropertyChanged(); try { _settings.Save(); } catch { } }
        }

        public ICommand SaveCommand { get; }
        public ICommand CancelCommand { get; }
        public ICommand DeleteLogsCommand { get; }

        public SettingsViewModel(SettingsService settings, Action close)
        {
            _settings = settings;
            SaveCommand = new RelayCommand(() => { if (Validate()) { _settings.Save(); close?.Invoke(); } });
            CancelCommand = new RelayCommand(() => { close?.Invoke(); });
            DeleteLogsCommand = new RelayCommand(() => { DeleteLogs(); });
        }

        private bool Validate()
        {
            if (_settings.UnresolvedIsinFailureThreshold < 1)
            {
                MessageBox.Show("Unresolved ISIN Failure Threshold must be a positive integer (>= 1).", "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
                return false;
            }
            if (_settings.PriceNanFailureThreshold < 1)
            {
                MessageBox.Show("Price NaN Failure Threshold must be a positive integer (>= 1).", "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
                return false;
            }
            return true;
        }

        private void DeleteLogs()
        {
            try
            {
                var folder = _settings.UnresolvedLogFolder;
                var file = System.IO.Path.Combine(folder, "unresolved_isins.log");
                int deleted = 0;
                try
                {
                    if (System.IO.File.Exists(file)) { System.IO.File.Delete(file); deleted++; }
                }
                catch { }

                try
                {
                    var temp = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "unresolved_isins.log");
                    if (System.IO.File.Exists(temp)) { System.IO.File.Delete(temp); deleted++; }
                }
                catch { }

                // also clear failure counts in DB for both types
                try
                {
                    var db = App.Services?.GetService(typeof(DatabaseService)) as DatabaseService ?? new DatabaseService();
                    db.DeleteFailureCountsByType("unresolved");
                    db.DeleteFailureCountsByType("price_nan");
                }
                catch { }

                MessageBox.Show(deleted > 0 ? $"Deleted {deleted} unresolved ISIN log file(s)." : "No unresolved ISIN log files found.", "Delete Logs", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to delete log files: {ex.Message}", "Delete Logs", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private SettingsProvidersViewModel _providersVM;
        public SettingsProvidersViewModel ProvidersVM
        {
            get => _providersVM;
            set { _providersVM = value; OnPropertyChanged(); }
        }

        // Deutsche Börse priority removed from preferences - property removed
    }
}
