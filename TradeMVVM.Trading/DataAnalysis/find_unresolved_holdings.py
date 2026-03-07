#!/usr/bin/env python3
"""
Find ISINs from HoldingsReport.csv that appear in unresolved_isins.log
Usage: run in repository root or from DataAnalysis folder. Outputs to DataAnalysis/unresolved_holdings.txt
"""
import os
import re
import csv

here = os.path.dirname(__file__)
holdings_path = os.path.join(here, 'HoldingsReport.csv')
unresolved_log = os.path.join(os.path.dirname(here), 'unresolved_isins.log')
output_path = os.path.join(here, 'unresolved_holdings.txt')

if not os.path.exists(holdings_path):
    print(f"Holdings report not found: {holdings_path}")
    print("Run generate_holdings.py first to produce HoldingsReport.csv")
    raise SystemExit(1)

if not os.path.exists(unresolved_log):
    print(f"Unresolved log not found: {unresolved_log}")
    raise SystemExit(1)

# read holdings ISINs
holdings = set()
with open(holdings_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter=';')
    for row in reader:
        isin = (row.get('isin') or row.get('ISIN') or '').strip()
        if isin:
            holdings.add(isin)

# parse unresolved log for lines like 'ISIN: DE000...' or entries inside
isin_regex = re.compile(r"ISIN:\s*([A-Z0-9]+)", re.IGNORECASE)
unresolved = set()
with open(unresolved_log, encoding='utf-8', errors='ignore') as f:
    for line in f:
        m = isin_regex.search(line)
        if m:
            unresolved.add(m.group(1).strip())

# intersection: holdings that are unresolved
bad = sorted(h for h in holdings if h in unresolved)

with open(output_path, 'w', encoding='utf-8') as out:
    if not bad:
        out.write('No holdings appear in unresolved_isins.log\n')
    else:
        out.write('\n'.join(bad) + '\n')

print(f"Holdings: {len(holdings)} total")
print(f"Unresolved ISINs in log: {len(unresolved)}")
print(f"Holdings not scraped (count={len(bad)}):")
for i in bad:
    print(i)

print(f"Wrote results to: {output_path}")
