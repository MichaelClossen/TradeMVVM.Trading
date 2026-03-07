using System.Windows.Controls;
using System.Windows;

namespace TradeMVVM.Trading.Views.Settings
{
    public partial class ProvidersListView : UserControl
    {
        public ProvidersListView()
        {
            InitializeComponent();
        }

        private void BtnSetAllDefault_Click(object sender, RoutedEventArgs e)
        {
            if (this.DataContext is TradeMVVM.Trading.ViewModels.SettingsProvidersViewModel vm)
            {
                vm.SetAllToDefault();
            }
        }
    }
}
