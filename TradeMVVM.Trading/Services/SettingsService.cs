using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Collections.Generic;

namespace TradeMVVM.Trading.Services
{
    public class SettingsService
    {
        private readonly string _filePath;

        public event Action SettingsChanged;

        public string BnpPriorityKeywords { get; set; } = "long,short,turbo"; 
        public bool BnpPriorityEnabled { get; set; } = true; 
        // default provider for knockouts (e.g. "BNP" or "Gettex")
        public string DefaultProviderForKnockout { get; set; } = "Gettex";
        // legacy providers removed from preferences: Wallstreet, LangUndSchwarz, DeutscheBoerse
        public string UnresolvedLogFolder { get; set; } = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "Trade", "TradeMVVM.Trading");
        public string HoldingsCsvPath { get; set; } = Path.Combine(AppDomain.CurrentDomain.BaseDirectory ?? "", "DataAnalysis", "HoldingsReport.csv");
        // Prediction / analysis preferences
        public string PredictionMethod { get; set; } = "MovingAverage"; // e.g. "MovingAverage", "ML", "None"
        public int PredictionWindowSize { get; set; } = 20; // default window size (e.g. 20)
        public double PredictionThresholdPercent { get; set; } = 1.0; // threshold in percent
        public string MlParameters { get; set; } = string.Empty; // free-form ML parameter string
        // thresholds for logging behavior
        public int UnresolvedIsinFailureThreshold { get; set; } = 5; // consecutive provider failures before logging unresolved ISIN
        public int PriceNanFailureThreshold { get; set; } = 5; // consecutive NaN prices before logging info
        // PL axis margin in EUR for charts (half-span)
        public double PlAxisMarginEur { get; set; } = 10.0;
        // persisted manual Y axis limits for charts (0 means not set)
        public double ChartsStocksYMin { get; set; } = 0.0;
        public double ChartsStocksYMax { get; set; } = 0.0;
        public double ChartsStocksTopYMin { get; set; } = 0.0;
        public double ChartsStocksTopYMax { get; set; } = 0.0;
        public double ChartsKnockoutsYMin { get; set; } = 0.0;
        public double ChartsKnockoutsYMax { get; set; } = 0.0;
        // outlier detection threshold in percent (if price change > threshold -> treat as outlier)
        public double OutlierThresholdPercent { get; set; } = 50.0;
        // alert thresholds
        public double AlertPricePercentThreshold { get; set; } = 1.0;
        public double AlertPlDeltaThresholdEur { get; set; } = 10.0;
        public Dictionary<string, double> IsinAlertPercentThresholds { get; set; } = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, double> IsinTrailingStopPercentThresholds { get; set; } = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, double> IsinTrailingStopCurrentPercentThresholds { get; set; } = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, double> IsinTrailingStopLastPlPercents { get; set; } = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, DateTime> IsinTrailingStopSetAtUtc { get; set; } = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, double> IsinTrailingPeakPercents { get; set; } = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        // diagnostic logging in HoldingsReportViewModel (HoldingsSnapshot / MOV)
        public bool EnableHoldingsSnapshotLogging { get; set; } = false;
        public double MainWindowWidth { get; set; } = 2200.0;
        public double MainWindowHeight { get; set; } = 1100.0;
        public double MainWindowLeft { get; set; } = 120.0;
        public double MainWindowTop { get; set; } = 120.0;
        public double MainWindowRightPanelWidth { get; set; } = 1200.0;
        public double ChartsAlertPanelWidth { get; set; } = 320.0;
        public double HoldingsZoomPercent { get; set; } = 100.0;

        public SettingsService()
        {
            var dir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "TradeMVVM.Trading");
            Directory.CreateDirectory(dir);
            _filePath = Path.Combine(dir, "settings.json");
            Load();
        }

        public void Load()
        {
            try
            {
                if (!File.Exists(_filePath))
                    return;

                var txt = File.ReadAllText(_filePath);
                var doc = JsonSerializer.Deserialize<SettingsDoc>(txt);
                if (doc != null)
                {
                    BnpPriorityKeywords = doc.BnpPriorityKeywords ?? BnpPriorityKeywords;
                    BnpPriorityEnabled = doc.BnpPriorityEnabled;
                    DefaultProviderForKnockout = doc.DefaultProviderForKnockout ?? DefaultProviderForKnockout;
                    // legacy keyword settings removed; ignore any legacy fields in settings file
                    UnresolvedLogFolder = doc.UnresolvedLogFolder ?? UnresolvedLogFolder;
                    HoldingsCsvPath = doc.HoldingsCsvPath ?? HoldingsCsvPath;
                    // prediction settings
                    PredictionMethod = doc.PredictionMethod ?? PredictionMethod;
                    PredictionWindowSize = doc.PredictionWindowSize != 0 ? doc.PredictionWindowSize : PredictionWindowSize;
                    PredictionThresholdPercent = doc.PredictionThresholdPercent != 0 ? doc.PredictionThresholdPercent : PredictionThresholdPercent;
                    MlParameters = doc.MlParameters ?? MlParameters;
                    UnresolvedIsinFailureThreshold = doc.UnresolvedIsinFailureThreshold != 0 ? doc.UnresolvedIsinFailureThreshold : UnresolvedIsinFailureThreshold;
                    PriceNanFailureThreshold = doc.PriceNanFailureThreshold != 0 ? doc.PriceNanFailureThreshold : PriceNanFailureThreshold;
                    PlAxisMarginEur = doc.PlAxisMarginEur != 0 ? doc.PlAxisMarginEur : PlAxisMarginEur;
                    ChartsStocksYMin = doc.ChartsStocksYMin != 0 ? doc.ChartsStocksYMin : ChartsStocksYMin;
                    ChartsStocksYMax = doc.ChartsStocksYMax != 0 ? doc.ChartsStocksYMax : ChartsStocksYMax;
                    ChartsStocksTopYMin = doc.ChartsStocksTopYMin != 0 ? doc.ChartsStocksTopYMin : ChartsStocksTopYMin;
                    ChartsStocksTopYMax = doc.ChartsStocksTopYMax != 0 ? doc.ChartsStocksTopYMax : ChartsStocksTopYMax;
                    ChartsKnockoutsYMin = doc.ChartsKnockoutsYMin != 0 ? doc.ChartsKnockoutsYMin : ChartsKnockoutsYMin;
                    ChartsKnockoutsYMax = doc.ChartsKnockoutsYMax != 0 ? doc.ChartsKnockoutsYMax : ChartsKnockoutsYMax;
                    OutlierThresholdPercent = doc.OutlierThresholdPercent != 0 ? doc.OutlierThresholdPercent : OutlierThresholdPercent;
                    AlertPricePercentThreshold = doc.AlertPricePercentThreshold > 0 ? doc.AlertPricePercentThreshold : AlertPricePercentThreshold;
                    AlertPlDeltaThresholdEur = doc.AlertPlDeltaThresholdEur > 0 ? doc.AlertPlDeltaThresholdEur : AlertPlDeltaThresholdEur;
                    IsinAlertPercentThresholds = doc.IsinAlertPercentThresholds ?? IsinAlertPercentThresholds;
                    IsinTrailingStopPercentThresholds = doc.IsinTrailingStopPercentThresholds ?? IsinTrailingStopPercentThresholds;
                    IsinTrailingStopCurrentPercentThresholds = doc.IsinTrailingStopCurrentPercentThresholds ?? IsinTrailingStopCurrentPercentThresholds;
                    IsinTrailingStopLastPlPercents = doc.IsinTrailingStopLastPlPercents ?? IsinTrailingStopLastPlPercents;
                    IsinTrailingStopSetAtUtc = doc.IsinTrailingStopSetAtUtc ?? IsinTrailingStopSetAtUtc;
                    IsinTrailingPeakPercents = doc.IsinTrailingPeakPercents ?? IsinTrailingPeakPercents;
                    EnableHoldingsSnapshotLogging = doc.EnableHoldingsSnapshotLogging;
                    MainWindowWidth = doc.MainWindowWidth > 0 ? doc.MainWindowWidth : MainWindowWidth;
                    MainWindowHeight = doc.MainWindowHeight > 0 ? doc.MainWindowHeight : MainWindowHeight;
                    if (doc.MainWindowLeft.HasValue) MainWindowLeft = doc.MainWindowLeft.Value;
                    if (doc.MainWindowTop.HasValue) MainWindowTop = doc.MainWindowTop.Value;
                    MainWindowRightPanelWidth = doc.MainWindowRightPanelWidth > 0 ? doc.MainWindowRightPanelWidth : MainWindowRightPanelWidth;
                    ChartsAlertPanelWidth = doc.ChartsAlertPanelWidth > 0 ? doc.ChartsAlertPanelWidth : ChartsAlertPanelWidth;
                    HoldingsZoomPercent = doc.HoldingsZoomPercent > 0 ? doc.HoldingsZoomPercent : HoldingsZoomPercent;
                }
            }
            catch
            {
                // ignore and keep defaults
            }
        }

        public void Save()
        {
            try
            {
                // Automatically enable BNP priority when keywords are provided
                try
                {
                    if (!string.IsNullOrWhiteSpace(BnpPriorityKeywords))
                        BnpPriorityEnabled = true;
                }
                catch { }
                var doc = new SettingsDoc
                {
                    BnpPriorityKeywords = BnpPriorityKeywords,
                    BnpPriorityEnabled = BnpPriorityEnabled
                    ,DefaultProviderForKnockout = DefaultProviderForKnockout
                    ,UnresolvedLogFolder = UnresolvedLogFolder
                    ,HoldingsCsvPath = HoldingsCsvPath
                    // prediction settings
                    ,PredictionMethod = PredictionMethod
                    ,PredictionWindowSize = PredictionWindowSize
                    ,PredictionThresholdPercent = PredictionThresholdPercent
                    ,MlParameters = MlParameters
                    ,UnresolvedIsinFailureThreshold = UnresolvedIsinFailureThreshold
                    ,PriceNanFailureThreshold = PriceNanFailureThreshold
                    ,PlAxisMarginEur = PlAxisMarginEur
                    ,OutlierThresholdPercent = OutlierThresholdPercent
                    ,AlertPricePercentThreshold = AlertPricePercentThreshold
                    ,AlertPlDeltaThresholdEur = AlertPlDeltaThresholdEur
                    ,IsinAlertPercentThresholds = IsinAlertPercentThresholds
                    ,IsinTrailingStopPercentThresholds = IsinTrailingStopPercentThresholds
                     ,IsinTrailingStopCurrentPercentThresholds = IsinTrailingStopCurrentPercentThresholds
                     ,IsinTrailingStopLastPlPercents = IsinTrailingStopLastPlPercents
                     ,IsinTrailingStopSetAtUtc = IsinTrailingStopSetAtUtc
                    ,IsinTrailingPeakPercents = IsinTrailingPeakPercents
                    ,ChartsStocksYMin = ChartsStocksYMin
                    ,ChartsStocksYMax = ChartsStocksYMax
                    ,ChartsStocksTopYMin = ChartsStocksTopYMin
                    ,ChartsStocksTopYMax = ChartsStocksTopYMax
                    ,ChartsKnockoutsYMin = ChartsKnockoutsYMin
                    ,ChartsKnockoutsYMax = ChartsKnockoutsYMax
                    ,EnableHoldingsSnapshotLogging = EnableHoldingsSnapshotLogging
                    ,MainWindowWidth = MainWindowWidth
                    ,MainWindowHeight = MainWindowHeight
                    ,MainWindowLeft = MainWindowLeft
                    ,MainWindowTop = MainWindowTop
                    ,MainWindowRightPanelWidth = MainWindowRightPanelWidth
                    ,ChartsAlertPanelWidth = ChartsAlertPanelWidth
                    ,HoldingsZoomPercent = HoldingsZoomPercent
                };

                var txt = JsonSerializer.Serialize(doc, new JsonSerializerOptions { WriteIndented = true });
                File.WriteAllText(_filePath, txt);
                SettingsChanged?.Invoke();
            }
            catch
            {
                // ignore
            }
        }

        private class SettingsDoc
        {
            public string BnpPriorityKeywords { get; set; }
            public bool BnpPriorityEnabled { get; set; }
            public string DefaultProviderForKnockout { get; set; }
            // legacy fields removed: WallstreetPriorityKeywords, LangUndSchwarzPriorityKeywords, DeutscheBoersePriorityKeywords
            public string UnresolvedLogFolder { get; set; }
            public string HoldingsCsvPath { get; set; }
            // prediction settings
            public string PredictionMethod { get; set; }
            public int PredictionWindowSize { get; set; }
            public double PredictionThresholdPercent { get; set; }
            public string MlParameters { get; set; }
                public int UnresolvedIsinFailureThreshold { get; set; }
                public int PriceNanFailureThreshold { get; set; }
            public double PlAxisMarginEur { get; set; }
            public double ChartsStocksYMin { get; set; }
            public double ChartsStocksYMax { get; set; }
            public double ChartsStocksTopYMin { get; set; }
            public double ChartsStocksTopYMax { get; set; }
            public double ChartsKnockoutsYMin { get; set; }
            public double ChartsKnockoutsYMax { get; set; }
            public double OutlierThresholdPercent { get; set; }
            public double AlertPricePercentThreshold { get; set; }
            public double AlertPlDeltaThresholdEur { get; set; }
            public Dictionary<string, double> IsinAlertPercentThresholds { get; set; }
            public Dictionary<string, double> IsinTrailingStopPercentThresholds { get; set; }
            public Dictionary<string, double> IsinTrailingStopCurrentPercentThresholds { get; set; }
            public Dictionary<string, double> IsinTrailingStopLastPlPercents { get; set; }
            public Dictionary<string, DateTime> IsinTrailingStopSetAtUtc { get; set; }
            public Dictionary<string, double> IsinTrailingPeakPercents { get; set; }
            public bool EnableHoldingsSnapshotLogging { get; set; }
            public double MainWindowWidth { get; set; }
            public double MainWindowHeight { get; set; }
            public double? MainWindowLeft { get; set; }
            public double? MainWindowTop { get; set; }
            public double MainWindowRightPanelWidth { get; set; }
            public double ChartsAlertPanelWidth { get; set; }
            public double HoldingsZoomPercent { get; set; }
        }
    }
}
