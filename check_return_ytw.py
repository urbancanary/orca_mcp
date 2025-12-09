#!/usr/bin/env python3
"""Check return_ytw vs ytw for Mexico bonds"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery

# Get Mexico bonds with both ytw and return_ytw
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
    a.ytw, a.return_ytw,
    a.oas, a.return
FROM agg_analysis_data a
JOIN latest_prices l ON a.isin = l.isin AND a.bpdate = l.max_date
ORDER BY a.isin
"""

df = query_bigquery(sql, client_id='guinness')
print('Mexico Bonds - YTW vs Return_YTW:')
print(df[['ticker', 'description', 'ytw', 'return_ytw', 'return']].to_string(index=False))

# Check how many have valid return_ytw
valid_return_ytw = df['return_ytw'].notna().sum()
valid_ytw = df['ytw'].notna().sum()

print(f'\nValid return_ytw values: {valid_return_ytw}/{len(df)}')
print(f'Valid ytw values: {valid_ytw}/{len(df)}')

if valid_return_ytw > 0:
    print('\nBonds with valid return_ytw, sorted by return_ytw:')
    valid_df = df[df['return_ytw'].notna()].sort_values('return_ytw')
    print(valid_df[['ticker', 'description', 'return_ytw']].to_string(index=False))
