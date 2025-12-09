#!/usr/bin/env python3
"""
Test Orca MCP Staging Tools

Tests the new staging portfolio tools:
1. get_staging_holdings - Get staging portfolio bonds
2. get_staging_versions - List staging versions
3. compare_staging_vs_actual - Compare staging vs actual portfolio
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery

def test_get_staging_versions():
    """Test listing staging versions"""
    print("=" * 80)
    print("TEST 1: Get Staging Versions")
    print("=" * 80)

    sql = """
    SELECT
        staging_id,
        portfolio_id,
        version,
        status,
        created_at
    FROM staging_holdings
    WHERE portfolio_id = 'ggi_staging'
    ORDER BY created_at DESC
    LIMIT 5
    """

    try:
        df = query_bigquery(sql, client_id="guinness")
        print(f"\n✅ Found {len(df)} staging versions:\n")
        print(df.to_string())

        if len(df) > 0:
            latest_staging_id = int(df.iloc[0]['staging_id'])
            print(f"\n📝 Latest staging_id: {latest_staging_id}")
            return latest_staging_id
        else:
            print("\n⚠️  No staging versions found")
            return None

    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None

def test_get_staging_holdings(staging_id):
    """Test getting staging holdings"""
    print("\n" + "=" * 80)
    print("TEST 2: Get Staging Holdings")
    print("=" * 80)

    sql = f"""
    SELECT
        isin, ticker, description, country,
        par_amount, market_value
    FROM staging_holdings_detail
    WHERE staging_id = {staging_id}
    ORDER BY country, ticker
    LIMIT 10
    """

    try:
        df = query_bigquery(sql, client_id="guinness")
        print(f"\n✅ Found {len(df)} bonds in staging_id={staging_id}:\n")
        print(df.to_string())

        total_mv = df['market_value'].sum()
        print(f"\n📊 Total Market Value: ${total_mv:,.0f}")
        print(f"📊 Countries: {df['country'].nunique()}")

    except Exception as e:
        print(f"\n❌ Error: {e}")

def test_compare_portfolios():
    """Test comparing staging vs actual"""
    print("\n" + "=" * 80)
    print("TEST 3: Compare Staging vs Actual Portfolio")
    print("=" * 80)

    # Get latest staging
    staging_sql = """
    SELECT staging_id
    FROM staging_holdings
    WHERE portfolio_id = 'ggi_staging'
    ORDER BY created_at DESC
    LIMIT 1
    """

    try:
        staging_df = query_bigquery(staging_sql, client_id="guinness")
        if staging_df.empty:
            print("\n⚠️  No staging data found")
            return

        staging_id = int(staging_df.iloc[0]['staging_id'])

        # Get staging holdings
        staging_holdings_sql = f"""
        SELECT isin, ticker, country, par_amount
        FROM staging_holdings_detail
        WHERE staging_id = {staging_id}
        """
        staging_holdings = query_bigquery(staging_holdings_sql, client_id="guinness")

        # Get actual holdings
        actual_holdings_sql = """
        WITH holdings_agg AS (
            SELECT
                isin, ticker, country,
                SUM(par_amount) as par_amount
            FROM transactions
            WHERE portfolio_id = 'wnbf'
                AND transaction_type = 'BUY'
            GROUP BY isin, ticker, country
        )
        SELECT * FROM holdings_agg
        WHERE par_amount > 0
        """
        actual_holdings = query_bigquery(actual_holdings_sql, client_id="guinness")

        # Compare
        staging_isins = set(staging_holdings['isin'])
        actual_isins = set(actual_holdings['isin'])

        additions = staging_isins - actual_isins
        removals = actual_isins - staging_isins
        common = staging_isins & actual_isins

        print(f"\n📊 Comparison Summary:")
        print(f"   Actual Portfolio (WNBF): {len(actual_isins)} bonds")
        print(f"   Staging Portfolio: {len(staging_isins)} bonds")
        print(f"   Bonds to Add: {len(additions)}")
        print(f"   Bonds to Remove: {len(removals)}")
        print(f"   Common Bonds: {len(common)}")

        if len(additions) > 0:
            print(f"\n✨ Additions (first 5):")
            additions_df = staging_holdings[staging_holdings['isin'].isin(additions)].head(5)
            print(additions_df[['ticker', 'description', 'country']].to_string(index=False))

        if len(removals) > 0:
            print(f"\n🗑️  Removals (first 5):")
            removals_df = actual_holdings[actual_holdings['isin'].isin(removals)].head(5)
            print(removals_df[['ticker', 'country']].to_string(index=False))

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all tests"""
    print("\n🐋 Testing Orca MCP Staging Tools\n")

    # Test 1: List versions
    staging_id = test_get_staging_versions()

    if staging_id:
        # Test 2: Get holdings
        test_get_staging_holdings(staging_id)

    # Test 3: Compare portfolios
    test_compare_portfolios()

    print("\n" + "=" * 80)
    print("✅ All tests completed!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
