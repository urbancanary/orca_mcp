#!/usr/bin/env python3
"""
Create portfolio_summary table and populate with initial data
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import setup_bigquery_credentials, query_bigquery
from google.cloud import bigquery

def create_table():
    """Create portfolio_summary table"""
    print("=" * 80)
    print("Creating portfolio_summary Table")
    print("=" * 80)

    setup_bigquery_credentials()
    bq_client = bigquery.Client(project="future-footing-414610")

    create_sql = """
    CREATE TABLE IF NOT EXISTS `future-footing-414610.portfolio_data.portfolio_summary` (
      portfolio_id STRING NOT NULL,
      starting_cash FLOAT64,
      settled_bonds_value FLOAT64,
      staging_buy_value FLOAT64,
      staging_sell_value FLOAT64,
      settled_cash FLOAT64,
      total_cash FLOAT64,
      num_settled_bonds INT64,
      num_staging_transactions INT64,
      last_transaction_id INT64,
      updated_at TIMESTAMP
    )
    """

    try:
        query_job = bq_client.query(create_sql)
        query_job.result()
        print("✅ Table created successfully")
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False

    return True

def refresh_portfolio_summary(portfolio_id="wnbf"):
    """Calculate and update portfolio summary for a given portfolio"""
    print(f"\n{'=' * 80}")
    print(f"Refreshing Portfolio Summary for {portfolio_id}")
    print("=" * 80)

    # Get starting cash (INITIAL transaction with CASH ticker)
    starting_cash_sql = f"""
    SELECT SUM(market_value) as starting_cash
    FROM transactions
    WHERE portfolio_id = '{portfolio_id}'
        AND transaction_type = 'INITIAL'
        AND ticker = 'CASH'
    """
    starting_cash_df = query_bigquery(starting_cash_sql, client_id="guinness")
    starting_cash = float(starting_cash_df.iloc[0]['starting_cash']) if not starting_cash_df.empty else 0.0

    # Get settled bonds value
    settled_bonds_sql = f"""
    SELECT
        COALESCE(SUM(market_value), 0) as settled_bonds_value,
        COUNT(*) as num_settled_bonds
    FROM transactions
    WHERE portfolio_id = '{portfolio_id}'
        AND status = 'settled'
        AND transaction_type = 'BUY'
    """
    settled_df = query_bigquery(settled_bonds_sql, client_id="guinness")
    settled_bonds_value = float(settled_df.iloc[0]['settled_bonds_value'])
    num_settled_bonds = int(settled_df.iloc[0]['num_settled_bonds'])

    # Get staging BUY value
    staging_buy_sql = f"""
    SELECT COALESCE(SUM(market_value), 0) as staging_buy_value
    FROM transactions
    WHERE portfolio_id = '{portfolio_id}'
        AND status = 'staging'
        AND transaction_type = 'BUY'
    """
    staging_buy_df = query_bigquery(staging_buy_sql, client_id="guinness")
    staging_buy_value = float(staging_buy_df.iloc[0]['staging_buy_value'])

    # Get staging SELL value
    staging_sell_sql = f"""
    SELECT COALESCE(SUM(market_value), 0) as staging_sell_value
    FROM transactions
    WHERE portfolio_id = '{portfolio_id}'
        AND status = 'staging'
        AND transaction_type = 'SELL'
    """
    staging_sell_df = query_bigquery(staging_sell_sql, client_id="guinness")
    staging_sell_value = float(staging_sell_df.iloc[0]['staging_sell_value'])

    # Get number of staging transactions
    staging_count_sql = f"""
    SELECT COUNT(*) as num_staging
    FROM transactions
    WHERE portfolio_id = '{portfolio_id}'
        AND status = 'staging'
    """
    staging_count_df = query_bigquery(staging_count_sql, client_id="guinness")
    num_staging = int(staging_count_df.iloc[0]['num_staging'])

    # Get last transaction ID
    last_txn_sql = f"""
    SELECT MAX(transaction_id) as last_id
    FROM transactions
    WHERE portfolio_id = '{portfolio_id}'
    """
    last_txn_df = query_bigquery(last_txn_sql, client_id="guinness")
    last_transaction_id = int(last_txn_df.iloc[0]['last_id']) if last_txn_df.iloc[0]['last_id'] else 0

    # Calculate cash positions
    settled_cash = starting_cash - settled_bonds_value
    total_cash = settled_cash - staging_buy_value + staging_sell_value

    print(f"\n📊 Summary Calculation:")
    print(f"   Starting Cash:      ${starting_cash:,.2f}")
    print(f"   Settled Bonds:      ${settled_bonds_value:,.2f} ({num_settled_bonds} bonds)")
    print(f"   Staging Buys:       ${staging_buy_value:,.2f}")
    print(f"   Staging Sells:      ${staging_sell_value:,.2f}")
    print(f"   Settled Cash:       ${settled_cash:,.2f}")
    print(f"   Total Cash:         ${total_cash:,.2f}")
    print(f"   Staging Txns:       {num_staging}")
    print(f"   Last Txn ID:        {last_transaction_id}")

    # Upsert into portfolio_summary
    setup_bigquery_credentials()
    bq_client = bigquery.Client(project="future-footing-414610")

    # Delete existing row for this portfolio
    delete_sql = f"""
    DELETE FROM `future-footing-414610.portfolio_data.portfolio_summary`
    WHERE portfolio_id = '{portfolio_id}'
    """
    query_job = bq_client.query(delete_sql)
    query_job.result()

    # Insert new summary
    insert_sql = f"""
    INSERT INTO `future-footing-414610.portfolio_data.portfolio_summary`
    (portfolio_id, starting_cash, settled_bonds_value, staging_buy_value, staging_sell_value,
     settled_cash, total_cash, num_settled_bonds, num_staging_transactions,
     last_transaction_id, updated_at)
    VALUES
    ('{portfolio_id}', {starting_cash}, {settled_bonds_value}, {staging_buy_value},
     {staging_sell_value}, {settled_cash}, {total_cash}, {num_settled_bonds},
     {num_staging}, {last_transaction_id}, CURRENT_TIMESTAMP())
    """

    try:
        query_job = bq_client.query(insert_sql)
        query_job.result()
        print(f"\n✅ Portfolio summary updated for {portfolio_id}")
        return True
    except Exception as e:
        print(f"\n❌ Error updating summary: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_summary(portfolio_id="wnbf"):
    """Verify the summary was created correctly"""
    print(f"\n{'=' * 80}")
    print("Verifying Portfolio Summary")
    print("=" * 80)

    verify_sql = f"""
    SELECT * FROM `future-footing-414610.portfolio_data.portfolio_summary`
    WHERE portfolio_id = '{portfolio_id}'
    """
    df = query_bigquery(verify_sql, client_id="guinness")

    if df.empty:
        print(f"❌ No summary found for {portfolio_id}")
        return False

    print("\n📋 Portfolio Summary:")
    for col in df.columns:
        val = df.iloc[0][col]
        if isinstance(val, (int, float)) and col != 'portfolio_id':
            print(f"   {col:30s} {val:>20,.2f}")
        else:
            print(f"   {col:30s} {str(val):>20s}")

    return True

def main():
    print("\n🐋 Creating Portfolio Summary Table\n")

    # Step 1: Create table
    if not create_table():
        print("\n❌ Failed to create table")
        return False

    # Step 2: Refresh summary for WNBF
    if not refresh_portfolio_summary("wnbf"):
        print("\n❌ Failed to refresh summary")
        return False

    # Step 3: Verify
    if not verify_summary("wnbf"):
        print("\n❌ Failed to verify summary")
        return False

    print("\n" + "=" * 80)
    print("✅ Portfolio summary table created and populated!")
    print("=" * 80 + "\n")

    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
