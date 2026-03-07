using System;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

class Program
{
    static async Task Main()
    {
        string url = "https://derivate.bnpparibas.com/product-details/DE000PJ6V1E2/";

        using HttpClient client = new HttpClient();

        // Wichtig: Browser-Header setzen (sonst blockt Cloudflare evtl.)
        client.DefaultRequestHeaders.Add("User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36");

        string html = await client.GetStringAsync(url);

        // =========================
        // Preis extrahieren
        // =========================
        // Sucht z.B. 200,3359 USD
        var priceMatch = Regex.Match(html, @"\d+,\d{2,4}\s?(EUR|USD)");

        string price = priceMatch.Success
            ? priceMatch.Value
            : "Preis nicht gefunden";

        // =========================
        // Prozent extrahieren
        // =========================
        // Sucht z.B. +2,86 %
        var percentMatch = Regex.Match(html, @"[+-]\d+,\d+\s?%");

        string percent = percentMatch.Success
            ? percentMatch.Value
            : "Prozent nicht gefunden";

        Console.WriteLine("Preis: " + price);
        Console.WriteLine("Änderung: " + percent);
    }
}