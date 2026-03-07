#!/usr/bin/env python3
import csv
import os
from decimal import Decimal, getcontext

getcontext().prec = 28

here = os.path.dirname(__file__)
transactions_path = os.path.join(here, 'Transactions.csv')
output_path = os.path.join(here, 'HoldingsReport.csv')

def parse_decimal(s):
    s = s.strip()
    if s == '' or s == '0,00':
        return Decimal('0')
    # numbers in the CSV use '.' as thousands separator and ',' as decimal
    # remove dots, replace comma with dot
    s = s.replace('.', '').replace(',', '.')
    try:
        return Decimal(s)
    except Exception:
        return Decimal('0')

holdings = {}

with open(transactions_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter=';')
    for row in reader:
        status = row.get('status', '').strip()
        if status != 'Executed':
            continue
        asset_type = row.get('assetType', '').strip()
        if asset_type.lower() != 'security':
            continue
        isin = row.get('isin', '').strip()
        if not isin:
            continue
        tx_type = row.get('type', '').strip().lower()
        shares = parse_decimal(row.get('shares', '0'))
        desc = row.get('description', '').strip()
        currency = row.get('currency', '').strip()

        delta = shares if tx_type == 'buy' else -shares if tx_type == 'sell' else Decimal('0')

        if isin not in holdings:
            holdings[isin] = {'description': desc, 'shares': Decimal('0'), 'currency': currency}
        holdings[isin]['shares'] += delta

# Write report sorted by absolute position (descending)
rows = []
for isin, info in holdings.items():
    # skip zero positions
    if info['shares'] == 0:
        continue
    rows.append((isin, info['description'], info['shares'], info['currency']))

rows.sort(key=lambda r: abs(r[2]), reverse=True)

with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter=';')
    writer.writerow(['isin', 'description', 'shares', 'currency'])
    for isin, desc, shares, cur in rows:
        # normalize shares for output: use dot as decimal separator
        # strip trailing zeros
        s = format(shares.normalize(), 'f') if shares == shares.quantize(1) else format(shares.normalize(), 'f')
        writer.writerow([isin, desc, s, cur])

print(f'Holdings report written to: {output_path}')
