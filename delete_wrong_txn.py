#!/usr/bin/env python3
"""Delete incorrect CFELEC transaction"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import setup_bigquery_credentials, query_bigquery
from google.cloud import bigquery

setup_bigquery_credentials()
bq_client = bigquery.Client(project='future-footing-414610')

# Delete incorrect transaction
delete_sql = """
DELETE FROM `future-footing-414610.portfolio_data.transactions`
WHERE transaction_id = 84
  AND isin = "USP30179BR86"
  AND status = "staging"
"""

print('Deleting incorrect transaction (ID 84, ISIN USP30179BR86)...')
query_job = bq_client.query(delete_sql)
query_job.result()
print(f'✅ Deleted {query_job.num_dml_affected_rows} row(s)')

# Verify
verify_sql = """
SELECT transaction_id, transaction_type, ticker, description, par_amount, market_value
FROM transactions
WHERE portfolio_id = "wnbf" AND status = "staging"
ORDER BY transaction_type, ticker
"""
df = query_bigquery(verify_sql, client_id='guinness', use_cache=False)
print(f'\n📊 Remaining staging transactions ({len(df)}):')
print(df.to_string(index=False))

# Show final cash
cash_sql = """
SELECT
    10000000 as starting_cash,
    SUM(CASE WHEN status IN ('settled', 'staging') AND transaction_type = 'BUY'
        THEN market_value ELSE 0 END) as total_buys,
    SUM(CASE WHEN status IN ('settled', 'staging') AND transaction_type = 'SELL'
        THEN market_value ELSE 0 END) as total_sells,
    10000000
      - SUM(CASE WHEN status IN ('settled', 'staging') AND transaction_type = 'BUY' THEN market_value ELSE 0 END)
      + SUM(CASE WHEN status IN ('settled', 'staging') AND transaction_type = 'SELL' THEN market_value ELSE 0 END)
      as remaining_cash
FROM transactions
WHERE portfolio_id = 'wnbf'
"""
cash_df = query_bigquery(cash_sql, client_id="guinness", use_cache=False)
buys = float(cash_df.iloc[0]['total_buys'])
sells = float(cash_df.iloc[0]['total_sells'])
cash = float(cash_df.iloc[0]['remaining_cash'])
bonds = buys - sells

print(f"\n💵 Final Portfolio:")
print(f"   Total Buys:     ${buys:,.2f}")
print(f"   Total Sells:    ${sells:,.2f}")
print(f"   Net Bonds:      ${bonds:,.2f} ({bonds/10000000*100:.1f}%)")
print(f"   Remaining Cash: ${cash:,.2f} ({cash/10000000*100:.1f}%)")
