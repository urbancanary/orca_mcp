#!/usr/bin/env python3
"""Show all staging holdings with country breakdown"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery

# Get all staging holdings
sql = """
SELECT
    country,
    ticker,
    isin,
    description,
    par_amount,
    market_value
FROM staging_holdings_detail
WHERE staging_id = 2
ORDER BY country, ticker
"""

df = query_bigquery(sql, client_id='guinness')
print(f'\n📊 All {len(df)} Staging Holdings:\n')
print(df.to_string())

print(f'\n\n💰 Market Value by Country:\n')
country_summary = df.groupby('country').agg({
    'market_value': 'sum',
    'ticker': 'count'
}).rename(columns={'ticker': 'num_bonds'})
country_summary['pct'] = (country_summary['market_value'] / country_summary['market_value'].sum() * 100).round(1)
print(country_summary.to_string())

total_mv = df['market_value'].sum()
print(f'\n📈 Total Market Value: ${total_mv:,.2f}')
print(f'📍 Countries: {df["country"].nunique()}')
