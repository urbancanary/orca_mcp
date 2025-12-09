#!/usr/bin/env python3
"""
Migrate to Unified Staging Model

This script:
1. Adds 'status' column to transactions table
2. Updates existing transactions to status='settled'
3. Migrates staging_holdings data to transactions with status='staging'

Transaction Status Lifecycle:
- staging: proposed ideas (can delete)
- input: planning to trade
- executed: trade executed
- settled: trade settled
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orca_mcp.tools.data_access import query_bigquery
from google.cloud import bigquery

def get_bigquery_client():
    """Get authenticated BigQuery client"""
    from orca_mcp.tools.data_access import setup_bigquery_credentials
    setup_bigquery_credentials()
    return bigquery.Client(project="future-footing-414610")

def step1_add_status_column():
    """Add status column to transactions table"""
    print("=" * 80)
    print("STEP 1: Add 'status' column to transactions table")
    print("=" * 80)

    client = get_bigquery_client()

    sql = """
    ALTER TABLE `future-footing-414610.portfolio_data.transactions`
    ADD COLUMN IF NOT EXISTS status STRING
    """

    try:
        query_job = client.query(sql)
        query_job.result()
        print("✅ Status column added successfully")
        return True
    except Exception as e:
        print(f"❌ Error adding status column: {e}")
        return False

def step2_update_existing_transactions():
    """Update existing transactions to status='settled'"""
    print("\n" + "=" * 80)
    print("STEP 2: Update existing transactions to status='settled'")
    print("=" * 80)

    client = get_bigquery_client()

    sql = """
    UPDATE `future-footing-414610.portfolio_data.transactions`
    SET status = 'settled'
    WHERE status IS NULL
    """

    try:
        query_job = client.query(sql)
        result = query_job.result()
        print(f"✅ Updated {query_job.num_dml_affected_rows} transactions to status='settled'")

        # Check status breakdown
        check_sql = """
        SELECT
            status,
            COUNT(*) as count
        FROM transactions
        GROUP BY status
        """
        df = query_bigquery(check_sql, client_id="guinness")
        print(f"\n📊 Status breakdown:")
        print(df.to_string(index=False))
        return True
    except Exception as e:
        print(f"❌ Error updating transactions: {e}")
        import traceback
        traceback.print_exc()
        return False

def step3_migrate_staging_data():
    """Migrate staging_holdings to transactions with status='staging'"""
    print("\n" + "=" * 80)
    print("STEP 3: Migrate staging_holdings to transactions table")
    print("=" * 80)

    client = get_bigquery_client()

    # Get staging holdings count
    count_sql = """
    SELECT COUNT(*) as count
    FROM `future-footing-414610.portfolio_data.staging_holdings_detail` shd
    JOIN `future-footing-414610.portfolio_data.staging_holdings` sh
        ON shd.staging_id = sh.staging_id
    WHERE sh.portfolio_id = 'ggi_staging'
    """

    count_result = client.query(count_sql).result()
    staging_count = list(count_result)[0]['count']
    print(f"\n📊 Found {staging_count} staging holdings to migrate")

    if staging_count == 0:
        print("⚠️  No staging data to migrate")
        return True

    # Get next transaction_id
    next_id_sql = """
    SELECT MAX(transaction_id) as max_id
    FROM `future-footing-414610.portfolio_data.transactions`
    """
    max_id_result = client.query(next_id_sql).result()
    next_id = list(max_id_result)[0]['max_id'] + 1

    print(f"📝 Next transaction_id: {next_id}")

    # Insert staging transactions
    insert_sql = """
    INSERT INTO `future-footing-414610.portfolio_data.transactions`
    (
        transaction_id,
        portfolio_id,
        transaction_date,
        settlement_date,
        transaction_type,
        isin,
        ticker,
        description,
        country,
        par_amount,
        price,
        market_value,
        ytm,
        duration,
        spread,
        status,
        notes,
        created_at,
        created_by
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY shd.isin) + {next_id} - 1 as transaction_id,
        'wnbf' as portfolio_id,
        FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()) as transaction_date,
        FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()) as settlement_date,
        'BUY' as transaction_type,
        shd.isin,
        shd.ticker,
        shd.description,
        shd.country,
        shd.par_amount,
        (shd.market_value / shd.par_amount * 100) as price,
        shd.market_value,
        shd.ytm,
        shd.duration,
        shd.spread,
        'staging' as status,
        CONCAT('Migrated from staging_id=', CAST(shd.staging_id AS STRING), ', version=', sh.version) as notes,
        sh.created_at,
        CAST(sh.created_by AS STRING) as created_by
    FROM `future-footing-414610.portfolio_data.staging_holdings_detail` shd
    JOIN `future-footing-414610.portfolio_data.staging_holdings` sh
        ON shd.staging_id = sh.staging_id
    WHERE sh.portfolio_id = 'ggi_staging'
        AND NOT EXISTS (
            SELECT 1 FROM `future-footing-414610.portfolio_data.transactions` t
            WHERE t.isin = shd.isin
                AND t.portfolio_id = 'wnbf'
                AND t.status = 'staging'
        )
    """.format(next_id=next_id)

    try:
        query_job = client.query(insert_sql)
        query_job.result()
        print(f"✅ Inserted {query_job.num_dml_affected_rows} rows")

        # Verify migration
        verify_sql = """
        SELECT COUNT(*) as count
        FROM `future-footing-414610.portfolio_data.transactions`
        WHERE portfolio_id = 'wnbf' AND status = 'staging'
        """
        verify_result = client.query(verify_sql).result()
        migrated_count = list(verify_result)[0]['count']

        print(f"✅ Migrated {migrated_count} staging transactions to WNBF portfolio")
        return True
    except Exception as e:
        print(f"❌ Error migrating staging data: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all migration steps"""
    print("\n🐋 Migrating to Unified Staging Model\n")

    # Step 1: Add status column
    if not step1_add_status_column():
        print("\n❌ Migration failed at step 1")
        return False

    # Step 2: Update existing transactions
    if not step2_update_existing_transactions():
        print("\n❌ Migration failed at step 2")
        return False

    # Step 3: Migrate staging data
    if not step3_migrate_staging_data():
        print("\n❌ Migration failed at step 3")
        return False

    print("\n" + "=" * 80)
    print("✅ Migration completed successfully!")
    print("=" * 80)
    print("\nNext steps:")
    print("  1. Update Orca MCP tools to use new status-based queries")
    print("  2. Test staging tools with unified model")
    print("  3. Consider archiving old staging_holdings tables")
    print()

    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
