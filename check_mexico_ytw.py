#!/usr/bin/env python3
"""Check Mexico bonds by return_ytw to find lowest returning"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery

# Get Mexico bonds with their current return_ytw
sql = """
WITH mex_holdings AS (
    SELECT DISTINCT isin, ticker, description, country, par_amount, market_value
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
    h.isin, h.ticker, h.description, h.country,
    h.par_amount, h.market_value,
    a.ytw,
    a.return_ytw,
    a.oad as duration
FROM mex_holdings h
JOIN latest_prices l ON h.isin = l.isin
JOIN agg_analysis_data a ON h.isin = a.isin AND a.bpdate = l.max_date
ORDER BY a.return_ytw ASC
"""

df = query_bigquery(sql, client_id='guinness')
print('Mexico Bonds Ranked by Return_YTW (lowest first):')
print(df[['ticker', 'description', 'ytw', 'return_ytw']].to_string(index=False))
print(f'\n✅ Lowest return_ytw bond: {df.iloc[0]["ticker"]} ({df.iloc[0]["isin"]}) at {df.iloc[0]["return_ytw"]:.2f}%')
print(f'   (YTW: {df.iloc[0]["ytw"]:.2f}%)')

print('\nComparison: YTW vs Return_YTW')
print('Note: Lower YTW does not mean lower return_ytw!')
for idx, row in df.head(3).iterrows():
    print(f'  {row["ticker"]:8} YTW={row["ytw"]:6.2f}%  Return_YTW={row["return_ytw"]:6.2f}%')
