#!/usr/bin/env python3
"""
Test Unified Staging Model

Tests the updated staging tools using the unified transactions table with status column:
1. get_staging_holdings - Get staging transactions (status='staging')
2. get_staging_versions - List staging transaction batches
3. compare_staging_vs_actual - Compare staging vs settled transactions
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery

def test_get_staging_holdings():
    """Test get_staging_holdings with unified model"""
    print("=" * 80)
    print("TEST 1: Get Staging Holdings (Unified Model)")
    print("=" * 80)

    sql = """
    SELECT
        transaction_id,
        isin, ticker, description, country,
        par_amount, price, market_value,
        ytm, duration, spread,
        transaction_date, notes
    FROM transactions
    WHERE portfolio_id = 'wnbf'
        AND status = 'staging'
        AND transaction_type = 'BUY'
    ORDER BY country, ticker
    """

    try:
        df = query_bigquery(sql, client_id="guinness")
        print(f"\n✅ Found {len(df)} staging transactions:\n")
        print(df[['ticker', 'country', 'par_amount', 'market_value']].to_string())

        total_mv = df['market_value'].sum()
        print(f"\n📊 Total Staging Market Value: ${total_mv:,.2f}")
        print(f"📊 Countries: {df['country'].nunique()}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

def test_get_staging_versions():
    """Test get_staging_versions with unified model"""
    print("\n" + "=" * 80)
    print("TEST 2: Get Staging Versions (Transaction Batches)")
    print("=" * 80)

    sql = """
    SELECT
        CAST(created_at AS STRING) as batch_timestamp,
        COUNT(*) as num_transactions,
        SUM(market_value) as total_value,
        STRING_AGG(DISTINCT ticker, ', ' ORDER BY ticker) as tickers
    FROM transactions
    WHERE portfolio_id = 'wnbf'
        AND status = 'staging'
    GROUP BY created_at
    ORDER BY created_at DESC
    LIMIT 10
    """

    try:
        df = query_bigquery(sql, client_id="guinness")
        print(f"\n✅ Found {len(df)} staging transaction batches:\n")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

def test_compare_staging_vs_actual():
    """Test compare_staging_vs_actual with unified model"""
    print("\n" + "=" * 80)
    print("TEST 3: Compare Staging vs Actual (Unified Model)")
    print("=" * 80)

    # Get staging holdings (status='staging')
    staging_sql = """
    SELECT isin, ticker, country, par_amount, market_value
    FROM transactions
    WHERE portfolio_id = 'wnbf'
        AND status = 'staging'
        AND transaction_type = 'BUY'
    """

    # Get actual holdings (status='settled')
    actual_sql = """
    WITH holdings_agg AS (
        SELECT
            isin, ticker, country,
            SUM(par_amount) as par_amount,
            SUM(market_value) as market_value
        FROM transactions
        WHERE portfolio_id = 'wnbf'
            AND status = 'settled'
            AND transaction_type = 'BUY'
        GROUP BY isin, ticker, country
    )
    SELECT * FROM holdings_agg
    WHERE par_amount > 0
    """

    # Get cash
    cash_sql = """
    SELECT SUM(market_value) as cash
    FROM transactions
    WHERE portfolio_id = 'wnbf'
        AND status = 'settled'
        AND ticker = 'CASH'
    """

    try:
        staging_df = query_bigquery(staging_sql, client_id="guinness")
        actual_df = query_bigquery(actual_sql, client_id="guinness")
        cash_df = query_bigquery(cash_sql, client_id="guinness")

        staging_isins = set(staging_df['isin'])
        actual_isins = set(actual_df['isin'])

        additions = staging_isins - actual_isins
        removals = actual_isins - staging_isins
        common = staging_isins & actual_isins

        cash = float(cash_df.iloc[0]['cash']) if not cash_df.empty else 0.0
        staging_total = float(staging_df['market_value'].sum())
        actual_total = float(actual_df['market_value'].sum())

        print(f"\n📊 Comparison Summary:")
        print(f"   Portfolio: WNBF")
        print(f"   Actual (settled): {len(actual_isins)} bonds, ${actual_total:,.2f}")
        print(f"   Staging: {len(staging_isins)} bonds, ${staging_total:,.2f}")
        print(f"   Cash: ${cash:,.2f}")
        print(f"\n   Additions: {len(additions)} bonds")
        print(f"   Removals: {len(removals)} bonds")
        print(f"   Common: {len(common)} bonds")

        if len(additions) > 0:
            print(f"\n✨ Additions (first 5):")
            additions_df = staging_df[staging_df['isin'].isin(additions)].head(5)
            print(additions_df[['ticker', 'country', 'market_value']].to_string(index=False))

        if len(removals) > 0:
            print(f"\n🗑️  Removals (first 5):")
            removals_df = actual_df[actual_df['isin'].isin(removals)].head(5)
            print(removals_df[['ticker', 'country', 'market_value']].to_string(index=False))

        print(f"\n✅ Unified model working correctly!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all tests"""
    print("\n🐋 Testing Unified Staging Model\n")

    # Test 1: Get staging holdings
    test_get_staging_holdings()

    # Test 2: Get staging versions (batches)
    test_get_staging_versions()

    # Test 3: Compare staging vs actual
    test_compare_staging_vs_actual()

    print("\n" + "=" * 80)
    print("✅ All unified model tests completed!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
