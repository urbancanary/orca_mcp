#!/usr/bin/env python3
"""
Test add_staging_allocation tool

Demonstrates adding a 3% allocation to a bond with proper sizing
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery, setup_bigquery_credentials
from google.cloud import bigquery
import math

def test_add_staging_allocation():
    """Test adding a staging allocation"""
    print("=" * 80)
    print("TEST: Add 3% Allocation to Israel Bond")
    print("=" * 80)

    # Pick an ISIN - let's use Israel bond
    isin = "XS2167193015"  # ISRAEL 3.8 05/13/60
    target_pct = 3.0
    portfolio_id = "wnbf"
    min_size = 200000
    increment = 50000

    # Get portfolio value
    portfolio_sql = f"""
    SELECT market_value as portfolio_value
    FROM transactions
    WHERE portfolio_id = '{portfolio_id}'
        AND ticker = 'CASH'
        AND transaction_type = 'INITIAL'
    """
    portfolio_df = query_bigquery(portfolio_sql, client_id="guinness")
    portfolio_value = float(portfolio_df.iloc[0]['portfolio_value'])
    print(f"\n💰 Portfolio Value: ${portfolio_value:,.2f}")

    # Get bond data
    bond_sql = f"""
    WITH latest AS (
        SELECT isin, MAX(bpdate) as max_date
        FROM agg_analysis_data
        WHERE isin = '{isin}'
        GROUP BY isin
    )
    SELECT a.isin, a.ticker, a.description, a.country,
           a.price, a.accrued_interest, a.ytw, a.oad
    FROM agg_analysis_data a
    JOIN latest l ON a.isin = l.isin AND a.bpdate = l.max_date
    """
    bond_df = query_bigquery(bond_sql, client_id="guinness")
    if bond_df.empty:
        print(f"❌ Bond {isin} not found")
        return

    bond = bond_df.iloc[0]
    clean_price = float(bond['price'])
    accrued = float(bond['accrued_interest']) if bond['accrued_interest'] else 0.0
    dirty_price = clean_price + accrued

    print(f"\n📊 Bond: {bond['ticker']} - {bond['description']}")
    print(f"   Clean Price: {clean_price:.4f}")
    print(f"   Accrued:     {accrued:.4f}")
    print(f"   Dirty Price: {dirty_price:.4f}")

    # Calculate sizing
    target_dollars = portfolio_value * (target_pct / 100.0)
    par_needed = (target_dollars / dirty_price) * 100

    if par_needed < min_size:
        par_amount = min_size
    else:
        par_amount = math.ceil((par_needed - min_size) / increment) * increment + min_size

    market_value = (dirty_price * par_amount) / 100
    actual_pct = (market_value / portfolio_value) * 100

    print(f"\n🎯 Target Allocation:")
    print(f"   Target %:      {target_pct}%")
    print(f"   Target $:      ${target_dollars:,.2f}")
    print(f"   Par Needed:    ${par_needed:,.2f}")
    print(f"\n📐 Sizing (min ${min_size:,}, increment ${increment:,}):")
    print(f"   Rounded Par:   ${par_amount:,.2f}")
    print(f"   Market Value:  ${market_value:,.2f}")
    print(f"   Actual %:      {actual_pct:.2f}%")

    # Get next transaction ID
    next_id_sql = """
    SELECT COALESCE(MAX(transaction_id), 0) + 1 as next_id
    FROM transactions
    """
    next_id_df = query_bigquery(next_id_sql, client_id="guinness")
    next_id = int(next_id_df.iloc[0]['next_id'])

    # Insert staging transaction
    print(f"\n📝 Creating staging transaction (ID: {next_id})...")

    setup_bigquery_credentials()
    bq_client = bigquery.Client(project="future-footing-414610")

    insert_sql = f"""
    INSERT INTO `future-footing-414610.portfolio_data.transactions`
    (transaction_id, portfolio_id, transaction_date, settlement_date,
     transaction_type, isin, ticker, description, country,
     par_amount, price, accrued_interest, dirty_price, market_value,
     ytm, duration, status, notes, created_at)
    VALUES
    ({next_id}, '{portfolio_id}', FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()),
     FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), 'BUY', '{isin}',
     '{bond["ticker"]}', '{bond["description"]}', '{bond["country"]}',
     {par_amount}, {clean_price}, {accrued}, {dirty_price}, {market_value},
     {float(bond['ytw']) if bond['ytw'] else 0}, {float(bond['oad']) if bond['oad'] else 0},
     'staging', 'Test: add 3% allocation',
     FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP()))
    """

    try:
        query_job = bq_client.query(insert_sql)
        query_job.result()
        print("✅ Staging transaction created successfully!")

        # Verify
        verify_sql = f"""
        SELECT transaction_id, ticker, country, par_amount, market_value, status
        FROM transactions
        WHERE transaction_id = {next_id}
        """
        verify_df = query_bigquery(verify_sql, client_id="guinness")
        print("\n📋 Verification:")
        print(verify_df.to_string(index=False))

        # Show new cash position
        cash_sql = f"""
        SELECT
            10000000 as starting_cash,
            SUM(CASE WHEN status IN ('settled', 'staging') AND transaction_type = 'BUY'
                THEN market_value ELSE 0 END) as total_bonds,
            10000000 - SUM(CASE WHEN status IN ('settled', 'staging') AND transaction_type = 'BUY'
                THEN market_value ELSE 0 END) as remaining_cash
        FROM transactions
        WHERE portfolio_id = '{portfolio_id}'
        """
        cash_df = query_bigquery(cash_sql, client_id="guinness")
        bonds = float(cash_df.iloc[0]['total_bonds'])
        cash = float(cash_df.iloc[0]['remaining_cash'])

        print(f"\n💵 Proposed Portfolio (settled + staging):")
        print(f"   Bonds: ${bonds:,.2f} ({bonds/10000000*100:.1f}%)")
        print(f"   Cash:  ${cash:,.2f} ({cash/10000000*100:.1f}%)")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("\n🐋 Testing add_staging_allocation Tool\n")
    test_add_staging_allocation()
    print("\n" + "=" * 80)
    print("✅ Test complete!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
