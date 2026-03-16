using System.Collections.ObjectModel;
using TradeMVVM.Trading.Services;
using System.Linq;
using System.Collections.Generic;

namespace TradeMVVM.Trading.ViewModels
{
    using System.ComponentModel;

    public class ProviderEntry : INotifyPropertyChanged
    {
        private string _isin;
        private string _name;
        private string _primaryProvider;
        private bool _isLocked;

        public string ISIN { get => _isin; set { _isin = value; OnPropertyChanged(nameof(ISIN)); } }
        public string Name { get => _name; set { _name = value; OnPropertyChanged(nameof(Name)); } }
        public string PrimaryProvider { get => _primaryProvider; set { _primaryProvider = value; OnPropertyChanged(nameof(PrimaryProvider)); } }
        public bool IsLocked { get => _isLocked; set { _isLocked = value; OnPropertyChanged(nameof(IsLocked)); OnPropertyChanged(nameof(IsEditable)); } }
        public bool IsEditable { get => !_isLocked; }

        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged(string propName) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propName));
    }

    public class SettingsProvidersViewModel : TradeMVVM.Trading.Presentation.ViewModels.BaseViewModel
    {
        public ObservableCollection<ProviderEntry> Providers { get; } = new ObservableCollection<ProviderEntry>();
        // Only expose relevant providers in preferences. Removed legacy providers.
        public List<string> ProviderOptions { get; } = new List<string> { "Default", "Gettex", "BNP" };

        private readonly ChartDataProvider _provider;

        public SettingsProvidersViewModel(ChartDataProvider provider)
        {
            _provider = provider;
        }

        public void Load(IEnumerable<TradeMVVM.Trading.DataAnalysis.Holding> holdings)
        {
            Providers.Clear();
            foreach (var h in holdings.OrderBy(x => x.ISIN))
            {
                try
                {
                    if (h == null || h.Shares <= 0) continue; // only include currently held ISINs
                }
                catch { continue; }

                var entry = new ProviderEntry
                {
                    ISIN = h.ISIN,
                    Name = h.Name,
                    PrimaryProvider = _provider.GetPrimaryProviderForName(h.Name)
                };

                // lock provider selection when any known provider keyword matches the name
                try
                {
                    var lower = (h.Name ?? string.Empty).ToLowerInvariant();
                    if (!string.IsNullOrWhiteSpace(lower))
                    {
                        // check configured keywords from SettingsService via ChartDataProvider
                        // if keyword match, lock selection to the provider
                        // use ChartDataProvider's detection logic by calling GetPrimaryProviderForName
                        var suggested = _provider.GetPrimaryProviderForName(h.Name);
                        if (!string.IsNullOrWhiteSpace(suggested) && suggested != "Gettex")
                        {
                            entry.PrimaryProvider = suggested;
                            entry.IsLocked = true;
                        }
                    }
                }
                catch { }

                Providers.Add(entry);
            }
        }

        // Load provider entries from DB holding totals (only ISINs that have persisted totals)
        public void LoadFromDb(TradeMVVM.Trading.Services.DatabaseService db = null)
        {
            Providers.Clear();
            try
            {
                var svc = db ?? new TradeMVVM.Trading.Services.DatabaseService();
                var recs = svc.LoadHoldingTotalRecords();
                foreach (var kv in recs.OrderBy(k => k.Key))
                {
                    try
                    {
                        // kv.Value is now (updated, totalEur, shares)
                        var shares = kv.Value.shares;
                        if (double.IsNaN(shares) || shares <= 0) continue;
                        var entry = new ProviderEntry
                        {
                            ISIN = kv.Key,
                            Name = string.Empty,
                            PrimaryProvider = _provider.GetPrimaryProviderForName(string.Empty) ?? "Gettex"
                        };
                        Providers.Add(entry);
                    }
                    catch { }
                }
            }
            catch { }
        }

        public void SetAllToDefault()
        {
            foreach (var p in Providers)
            {
                p.PrimaryProvider = "Gettex";
            }
            OnPropertyChanged(nameof(Providers));
        }
    }
}
