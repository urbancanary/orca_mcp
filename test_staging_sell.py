#!/usr/bin/env python3
"""
Test add_staging_sell tool

Demonstrates selling Mexico bonds to raise ~$150K cash to offset Israel bond purchase
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery, setup_bigquery_credentials
from google.cloud import bigquery
import math

def find_mexico_bonds():
    """Find Mexico bonds in portfolio"""
    print("=" * 80)
    print("STEP 1: Find Mexico Bonds to Sell")
    print("=" * 80)

    sql = """
    SELECT isin, ticker, description, country, par_amount, market_value
    FROM transactions
    WHERE portfolio_id = 'wnbf'
        AND status = 'settled'
        AND transaction_type = 'BUY'
        AND country = 'Mexico'
    ORDER BY market_value DESC
    """

    df = query_bigquery(sql, client_id="guinness")
    print(f"\n💰 Found {len(df)} Mexico bonds:")
    print(df.to_string(index=False))
    print(f"\nTotal Mexico: ${df['market_value'].sum():,.2f}")

    return df

def test_add_staging_sell(isin, cash_to_raise):
    """Test selling a bond to raise cash"""
    print("\n" + "=" * 80)
    print(f"STEP 2: Sell {isin} to Raise ${cash_to_raise:,.2f}")
    print("=" * 80)

    portfolio_id = "wnbf"
    min_size = 200000
    increment = 50000

    # Get current holdings
    holdings_sql = f"""
    SELECT
        SUM(CASE WHEN transaction_type = 'BUY' THEN par_amount ELSE 0 END) -
        SUM(CASE WHEN transaction_type = 'SELL' THEN par_amount ELSE 0 END) as net_par
    FROM transactions
    WHERE portfolio_id = '{portfolio_id}'
        AND isin = '{isin}'
        AND status IN ('settled', 'staging')
    """
    holdings_df = query_bigquery(holdings_sql, client_id="guinness")
    current_par = float(holdings_df.iloc[0]['net_par']) if not holdings_df.empty else 0.0

    print(f"\n📊 Current Holdings: ${current_par:,.2f} par")

    if current_par <= 0:
        print(f"❌ No holdings to sell!")
        return

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
        print(f"❌ Bond {isin} not found in pricing data")
        return

    bond = bond_df.iloc[0]
    clean_price = float(bond['price'])
    accrued = float(bond['accrued_interest']) if bond['accrued_interest'] else 0.0
    dirty_price = clean_price + accrued

    print(f"\n📈 Bond: {bond['ticker']} - {bond['description']}")
    print(f"   Clean Price: {clean_price:.4f}")
    print(f"   Accrued:     {accrued:.4f}")
    print(f"   Dirty Price: {dirty_price:.4f}")

    # Calculate sizing
    par_needed = (cash_to_raise / dirty_price) * 100

    if par_needed < min_size:
        par_amount = min_size
    else:
        par_amount = math.ceil((par_needed - min_size) / increment) * increment + min_size

    # Check if we have enough
    if par_amount > current_par:
        print(f"\n⚠️  Need ${par_amount:,.2f} par but only have ${current_par:,.2f}")
        par_amount = current_par
        print(f"   Selling all holdings: ${par_amount:,.2f}")

    market_value = (dirty_price * par_amount) / 100

    print(f"\n🎯 Target Cash:    ${cash_to_raise:,.2f}")
    print(f"   Par Needed:     ${par_needed:,.2f}")
    print(f"\n📐 Sizing (min ${min_size:,}, increment ${increment:,}):")
    print(f"   Rounded Par:    ${par_amount:,.2f}")
    print(f"   Cash Proceeds:  ${market_value:,.2f}")

    # Get next transaction ID
    next_id_sql = """
    SELECT COALESCE(MAX(transaction_id), 0) + 1 as next_id
    FROM transactions
    """
    next_id_df = query_bigquery(next_id_sql, client_id="guinness")
    next_id = int(next_id_df.iloc[0]['next_id'])

    # Insert staging transaction
    print(f"\n📝 Creating staging SELL transaction (ID: {next_id})...")

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
     FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), 'SELL', '{isin}',
     '{bond["ticker"]}', '{bond["description"]}', '{bond["country"]}',
     {par_amount}, {clean_price}, {accrued}, {dirty_price}, {market_value},
     {float(bond['ytw']) if bond['ytw'] else 0}, {float(bond['oad']) if bond['oad'] else 0},
     'staging', 'Test: sell to raise ${cash_to_raise:,.0f} cash',
     FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP()))
    """

    try:
        query_job = bq_client.query(insert_sql)
        query_job.result()
        print("✅ Staging SELL transaction created successfully!")

        # Verify
        verify_sql = f"""
        SELECT transaction_id, transaction_type, ticker, country, par_amount, market_value, status
        FROM transactions
        WHERE transaction_id = {next_id}
        """
        verify_df = query_bigquery(verify_sql, client_id="guinness")
        print("\n📋 Verification:")
        print(verify_df.to_string(index=False))

        return next_id

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def show_combined_portfolio():
    """Show portfolio with settled + staging transactions"""
    print("\n" + "=" * 80)
    print("STEP 3: Combined Portfolio (Settled + Staging)")
    print("=" * 80)

    # Cash calculation
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
    cash_df = query_bigquery(cash_sql, client_id="guinness")

    buys = float(cash_df.iloc[0]['total_buys'])
    sells = float(cash_df.iloc[0]['total_sells'])
    cash = float(cash_df.iloc[0]['remaining_cash'])
    bonds = buys - sells

    print(f"\n💵 Portfolio Calculation:")
    print(f"   Starting Cash:  $10,000,000")
    print(f"   Total Buys:     ${buys:,.2f}")
    print(f"   Total Sells:    ${sells:,.2f}")
    print(f"   Net Bonds:      ${bonds:,.2f} ({bonds/10000000*100:.1f}%)")
    print(f"   Remaining Cash: ${cash:,.2f} ({cash/10000000*100:.1f}%)")

    if cash < 0:
        print(f"\n⚠️  Portfolio is OVERLEVERAGED by ${-cash:,.2f}")
    else:
        print(f"\n✅ Portfolio has positive cash of ${cash:,.2f}")

    # Show staging transactions
    staging_sql = """
    SELECT transaction_type, ticker, country, par_amount, market_value
    FROM transactions
    WHERE portfolio_id = 'wnbf'
        AND status = 'staging'
    ORDER BY transaction_type, country, ticker
    """
    staging_df = query_bigquery(staging_sql, client_id="guinness")

    print(f"\n📊 Staging Transactions ({len(staging_df)}):")
    print(staging_df.to_string(index=False))

def main():
    print("\n🐋 Testing add_staging_sell Tool\n")

    # Step 1: Find Mexico bonds
    mexico_df = find_mexico_bonds()

    if mexico_df.empty:
        print("❌ No Mexico bonds found!")
        return

    # Pick the first Mexico bond
    isin = mexico_df.iloc[0]['isin']

    # Step 2: Sell to raise $150K
    result = test_add_staging_sell(isin, 150000)

    if result:
        # Step 3: Show combined portfolio
        show_combined_portfolio()

    print("\n" + "=" * 80)
    print("✅ Test complete!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
