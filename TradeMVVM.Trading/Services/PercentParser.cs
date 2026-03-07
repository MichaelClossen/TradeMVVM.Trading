using System.Globalization;

public static class PercentParser
{
    public static double Parse(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return double.NaN;

        text = text.Replace("%", "")
                   .Replace("+", "")
                   .Trim();

        if (double.TryParse(text.Replace(",", "."),
            System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture,
            out double result))
        {
            return result;
        }

        return double.NaN;
    }

}
