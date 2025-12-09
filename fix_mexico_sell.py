#!/usr/bin/env python3
"""
Fix Mexico bond sell transaction
Delete wrong CFELEC and sell the correct one (lowest YTW)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery, setup_bigquery_credentials
from google.cloud import bigquery
import math

def delete_wrong_transaction():
    """Delete the incorrect CFELEC sell transaction"""
    print("=" * 80)
    print("STEP 1: Delete Incorrect CFELEC Sell Transaction")
    print("=" * 80)

    setup_bigquery_credentials()
    bq_client = bigquery.Client(project="future-footing-414610")

    # Find the staging SELL transaction for USP30179BR86
    find_sql = """
    SELECT transaction_id, isin, ticker, description, par_amount, market_value
    FROM transactions
    WHERE portfolio_id = 'wnbf'
        AND status = 'staging'
        AND transaction_type = 'SELL'
        AND isin = 'USP30179BR86'
    """
    df = query_bigquery(find_sql, client_id="guinness")

    if df.empty:
        print("⚠️  No staging SELL transaction found for USP30179BR86")
        return

    txn_id = int(df.iloc[0]['transaction_id'])
    print(f"\n📋 Found transaction to delete:")
    print(f"   ID: {txn_id}")
    print(f"   ISIN: {df.iloc[0]['isin']}")
    print(f"   Ticker: {df.iloc[0]['ticker']}")
    print(f"   Par: ${df.iloc[0]['par_amount']:,.2f}")
    print(f"   Value: ${df.iloc[0]['market_value']:,.2f}")

    # Delete the transaction
    delete_sql = f"""
    DELETE FROM transactions
    WHERE transaction_id = {txn_id}
    """

    try:
        query_job = bq_client.query(delete_sql)
        query_job.result()
        print(f"\n✅ Deleted transaction {txn_id}")
    except Exception as e:
        print(f"❌ Error deleting transaction: {e}")
        import traceback
        traceback.print_exc()

def create_correct_sell():
    """Create sell transaction for the correct CFELEC bond (lowest YTW)"""
    print("\n" + "=" * 80)
    print("STEP 2: Create Sell Transaction for Correct CFELEC (Lowest YTW)")
    print("=" * 80)

    isin = "USP30179CR77"  # CFELEC 6.45 01/24/35 with 6.10% YTW
    cash_to_raise = 150000
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

    print(f"\n📊 Bond: CFELEC 6.45 01/24/35 ({isin})")
    print(f"   Current Holdings: ${current_par:,.2f} par")
    print(f"   YTW: 6.10% (lowest in Mexico)")

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

    print(f"\n📈 Pricing:")
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
     'staging', 'Sell lowest YTW Mexico bond to raise ${cash_to_raise:,.0f}',
     FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP()))
    """

    try:
        query_job = bq_client.query(insert_sql)
        query_job.result()
        print("✅ Staging SELL transaction created successfully!")

        # Verify
        verify_sql = f"""
        SELECT transaction_id, transaction_type, ticker, description, par_amount, market_value, status
        FROM transactions
        WHERE transaction_id = {next_id}
        """
        verify_df = query_bigquery(verify_sql, client_id="guinness")
        print("\n📋 Verification:")
        print(verify_df.to_string(index=False))

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def show_final_portfolio():
    """Show final portfolio with corrected staging transactions"""
    print("\n" + "=" * 80)
    print("STEP 3: Final Portfolio (Settled + Corrected Staging)")
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
    SELECT transaction_type, ticker, description, par_amount, market_value
    FROM transactions
    WHERE portfolio_id = 'wnbf'
        AND status = 'staging'
    ORDER BY transaction_type, ticker
    """
    staging_df = query_bigquery(staging_sql, client_id="guinness")

    print(f"\n📊 Staging Transactions ({len(staging_df)}):")
    print(staging_df.to_string(index=False))

def main():
    print("\n🐋 Fixing Mexico Bond Sell Transaction\n")

    # Step 1: Delete incorrect transaction
    delete_wrong_transaction()

    # Step 2: Create correct transaction
    create_correct_sell()

    # Step 3: Show final portfolio
    show_final_portfolio()

    print("\n" + "=" * 80)
    print("✅ Fix complete!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
