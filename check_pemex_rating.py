#!/usr/bin/env python3
"""Check PEMEX ratings and compare with other Mexico bonds"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery

# Get Mexico bonds with ratings and return data
sql = """
WITH mex_holdings AS (
    SELECT DISTINCT isin
    FROM transactions
    WHERE portfolio_id = "wnbf"
        AND status = "settled"
        AND transaction_type = "BUY"
        AND country = "Mexico"
),
latest_prices AS (
    SELECT isin, MAX(bpdate) as max_date
    FROM agg_analysis_data
    WHERE isin IN (SELECT isin FROM mex_holdings)
    GROUP BY isin
)
SELECT
    a.isin, a.ticker, a.description,
    a.rating,
    a.rating_numeric,
    a.rating_notches,
    a.ytw,
    a.return_ytw,
    a.oad,
    a.oas,
    a.price
FROM agg_analysis_data a
JOIN latest_prices l ON a.isin = l.isin AND a.bpdate = l.max_date
ORDER BY a.return_ytw ASC
"""

df = query_bigquery(sql, client_id='guinness')
print('Mexico Bonds - Ratings and Returns:')
print(df[['ticker', 'description', 'rating', 'rating_notches', 'ytw', 'return_ytw', 'price']].to_string(index=False))

# Focus on PEMEX bonds
pemex_df = df[df['ticker'] == 'PEMEX']
print('\nPEMEX Bonds Detail:')
for idx, row in pemex_df.iterrows():
    print(f"\n{row['description']}:")
    print(f"  ISIN: {row['isin']}")
    print(f"  Rating: {row['rating']}")
    print(f"  Rating Notches: {row['rating_notches']}")
    print(f"  Price: {row['price']:.2f}")
    print(f"  YTW: {row['ytw']:.2f}%")
    print(f"  Return_YTW: {row['return_ytw']:.2f}%")
    print(f"  OAS: {row['oas']:.0f} bps")
    print(f"  Duration: {row['oad']:.2f}")

# Compare with other bonds
print('\n' + '='*80)
print('Why does PEMEX 5.95 have lowest return_ytw?')
print('='*80)
pemex_595 = df[df['description'] == 'PEMEX 5.95 01/28/31'].iloc[0]
cfelec_645 = df[df['description'] == 'CFELEC 6.45 01/24/35'].iloc[0]

print(f"\nPEMEX 5.95 01/28/31:")
print(f"  Price: {pemex_595['price']:.2f} (Duration: {pemex_595['oad']:.2f})")
print(f"  Return_YTW: {pemex_595['return_ytw']:.2f}%")

print(f"\nCFELEC 6.45 01/24/35:")
print(f"  Price: {cfelec_645['price']:.2f} (Duration: {cfelec_645['oad']:.2f})")
print(f"  Return_YTW: {cfelec_645['return_ytw']:.2f}%")

print(f"\nPrice difference: PEMEX trading at {pemex_595['price']:.2f} vs CFELEC at {cfelec_645['price']:.2f}")
print(f"Duration: PEMEX {pemex_595['oad']:.2f} years vs CFELEC {cfelec_645['oad']:.2f} years")
