#!/usr/bin/env python3
"""Check columns in agg_analysis_data"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery

# Check columns in agg_analysis_data
sql = """
SELECT * FROM agg_analysis_data
WHERE isin = "USP30179CR77"
LIMIT 1
"""

df = query_bigquery(sql, client_id='guinness')
print('Columns in agg_analysis_data:')
for col in sorted(df.columns):
    print(f'  - {col}')

print('\nSample data for CFELEC 6.45:')
if 'ytw' in df.columns:
    print(f'  ytw: {df.iloc[0]["ytw"]}')
if 'return_ytw' in df.columns:
    print(f'  return_ytw: {df.iloc[0]["return_ytw"]}')
if 'ytm' in df.columns:
    print(f'  ytm: {df.iloc[0]["ytm"]}')
