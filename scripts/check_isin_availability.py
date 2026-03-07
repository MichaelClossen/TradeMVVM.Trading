#!/usr/bin/env python3
"""
Best-effort ISIN-Availability-Checker for three providers:
  - Wallstreet-Online
  - LangUndSchwarz
  - derivate.bnpparibas.com (Knockouts)

Usage:
  python scripts/check_isin_availability.py --input "<path-to-transactions-file>" --output "output.csv"

Notes:
 - The script extracts ISINs by pattern from the input file if no explicit column is present.
 - It performs simple HTTP GET searches and considers an ISIN available if it appears in the HTML response.
 - Sites that render with heavy JavaScript (BNP knockouts) may require Selenium — this script is a best-effort HTTP-only check.
"""

import re
import csv
import argparse
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ISIN-checker/1.0)"}
TIMEOUT = 10

PROVIDERS = {
    "WallstreetOnline": [
        "https://www.wallstreet-online.de/suche?query={}",
        "https://www.wallstreet-online.de/suche?q={}",
        "https://www.wallstreet-online.de/suche/{}",
    ],
    "LangUndSchwarz": [
        "https://www.langundschwarz.com/de/suche/?q={}",
        "https://www.langundschwarz.com/search?q={}",
        "https://www.ls-exchange.com/search?query={}",
    ],
    "BNPParibasKnockouts": [
        "https://derivate.bnpparibas.com/knockouts/?search={}",
        "https://derivate.bnpparibas.com/knockouts/{}",
    ],
}


def extract_isins_from_file(path: Path):
    text = path.read_text(encoding='utf-8', errors='ignore')
    found = set(m.group(0) for m in ISIN_RE.finditer(text))
    return sorted(found)


def check_isin_on_provider(isin: str, provider_name: str):
    urls = PROVIDERS.get(provider_name, [])
    for u in urls:
        url = u.format(isin)
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200 and isin in r.text:
                return True, url
        except requests.RequestException:
            # ignore and try next
            pass
        time.sleep(0.3)
    return False, None


def check_isin(isin: str):
    results = {}
    for provider in PROVIDERS.keys():
        ok, url = check_isin_on_provider(isin, provider)
        results[provider] = "verfügbar" if ok else "nicht verfügbar"
    return isin, results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', required=True, help='Pfad zur Datei mit ISINs (oder beliebige Datei, aus der ISINs extrahiert werden)')
    parser.add_argument('--output', '-o', default='output/isins_availability.csv', help='CSV-Ausgabepfad')
    parser.add_argument('--workers', '-w', type=int, default=6, help='Anzahl paralleler Abfragen')
    args = parser.parse_args()

    inp = Path(args.input)
    if not inp.exists():
        print(f"Eingabedatei nicht gefunden: {inp}")
        return

    isins = extract_isins_from_file(inp)
    if not isins:
        print("Keine ISINs gefunden in der Eingabedatei.")
        return

    print(f"Gefundene ISINs: {len(isins)}")

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        future_to_isin = {ex.submit(check_isin, isin): isin for isin in isins}
        with open(outp, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['ISIN'] + list(PROVIDERS.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for fut in as_completed(future_to_isin):
                isin, results = fut.result()
                row = {'ISIN': isin}
                row.update(results)
                writer.writerow(row)
                print(f"{isin}: {results}")

    print(f"Ergebnis geschrieben nach: {outp}")


if __name__ == '__main__':
    main()
