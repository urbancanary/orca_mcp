#!/usr/bin/env python3
"""
Test automatic bond selection by country (lowest return_ytw)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery

def test_auto_select():
    """Test that we can auto-select the lowest return_ytw bond in Mexico"""
    print("=" * 80)
    print("Testing Auto-Select Lowest Return_YTW Bond in Mexico")
    print("=" * 80)

    country = "Mexico"
    portfolio_id = "wnbf"

    # Get the bond that should be auto-selected
    select_sql = f"""
    WITH holdings AS (
        SELECT DISTINCT isin, ticker, description, country, par_amount, market_value
        FROM transactions
        WHERE portfolio_id = '{portfolio_id}'
            AND status = 'settled'
            AND transaction_type = 'BUY'
            AND country = '{country}'
    ),
    latest_prices AS (
        SELECT isin, MAX(bpdate) as max_date
        FROM agg_analysis_data
        WHERE isin IN (SELECT isin FROM holdings)
        GROUP BY isin
    )
    SELECT
        h.isin, h.ticker, h.description, h.country,
        h.par_amount, h.market_value,
        a.return_ytw,
        a.ytw,
        a.oad as duration
    FROM holdings h
    JOIN latest_prices l ON h.isin = l.isin
    JOIN agg_analysis_data a ON h.isin = a.isin AND a.bpdate = l.max_date
    ORDER BY a.return_ytw ASC
    """

    df = query_bigquery(select_sql, client_id="guinness")

    print(f"\n{country} bonds ranked by return_ytw (lowest first):")
    print(df[['ticker', 'description', 'ytw', 'return_ytw']].to_string(index=False))

    if not df.empty:
        selected = df.iloc[0]
        print(f"\n✅ Auto-selected bond:")
        print(f"   Ticker: {selected['ticker']}")
        print(f"   ISIN: {selected['isin']}")
        print(f"   Description: {selected['description']}")
        print(f"   YTW: {selected['ytw']:.2f}%")
        print(f"   Return_YTW: {selected['return_ytw']:.2f}%")
        print(f"   Reason: Lowest return_ytw in {country}")
    else:
        print(f"\n❌ No bonds found in {country}")

if __name__ == "__main__":
    test_auto_select()
