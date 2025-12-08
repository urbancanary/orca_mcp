"""
Transaction Management Tools for Orca MCP

Unified API for managing portfolio transactions across both:
- PORT mode: BigQuery (production/live transactions)
- STAGING mode: Cloudflare D1 (hypothetical/sandbox transactions)
"""

from typing import Dict, Any, Literal
import pandas as pd

from .data_access import query_bigquery
from .cloudflare_d1 import (
    get_staging_transactions,
    save_staging_transaction,
    delete_staging_transaction,
    clear_all_staging_transactions
)


PortfolioMode = Literal['PORT', 'STAGING']
TransactionStatus = Literal['settled', 'staging', 'executed', 'input', 'pending']


def get_transactions(
    portfolio_id: str,
    mode: PortfolioMode = 'PORT',
    status_filter: str = None,
    client_id: str = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Get transactions for a portfolio with mode support

    Args:
        portfolio_id: Portfolio identifier
        mode: 'PORT' for BigQuery transactions, 'STAGING' for Cloudflare D1
        status_filter: Optional filter by status (e.g., 'settled', 'staging', 'executed')
                      If None, returns all statuses
        client_id: Client identifier
        use_cache: Whether to use Redis cache (for PORT mode only)

    Returns:
        DataFrame with transactions
    """
    if mode == 'PORT':
        # Query BigQuery for transactions (cached in Redis)
        status_clause = f"AND status = '{status_filter}'" if status_filter else ""
        sql = f"""
        SELECT
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
            accrued_interest,
            days_accrued,
            dirty_price,
            market_value,
            ytm,
            duration,
            spread,
            notes,
            status,
            created_at,
            created_by
        FROM transactions
        WHERE portfolio_id = '{portfolio_id}'
        {status_clause}
        ORDER BY transaction_date DESC, created_at DESC
        """
        return query_bigquery(sql, client_id=client_id, use_cache=use_cache)

    elif mode == 'STAGING':
        # Fetch staging transactions from Cloudflare D1
        return get_staging_transactions(portfolio_id, client_id=client_id)

    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'PORT' or 'STAGING'")


def get_all_transactions(
    portfolio_id: str,
    client_id: str = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Get ALL transactions (PORT + STAGING combined) for a portfolio

    Useful for STAGING mode compliance checks where you want to see
    the impact of staging trades on the real portfolio.

    Args:
        portfolio_id: Portfolio identifier
        client_id: Client identifier
        use_cache: Whether to use Redis cache for PORT transactions

    Returns:
        DataFrame with all transactions (PORT + STAGING combined)
    """
    # Get PORT transactions from BigQuery
    port_df = get_transactions(portfolio_id, mode='PORT', client_id=client_id, use_cache=use_cache)

    # Get STAGING transactions from Cloudflare D1
    staging_df = get_transactions(portfolio_id, mode='STAGING', client_id=client_id)

    # Add port_mode column to identify source
    if not port_df.empty:
        port_df['port_mode'] = 'PORT'

    if not staging_df.empty:
        staging_df['port_mode'] = 'STAGING'

    # Combine both DataFrames
    if port_df.empty and staging_df.empty:
        return pd.DataFrame()
    elif port_df.empty:
        return staging_df
    elif staging_df.empty:
        return port_df
    else:
        # Concatenate and sort by date
        combined_df = pd.concat([port_df, staging_df], ignore_index=True)
        combined_df = combined_df.sort_values(['transaction_date', 'created_at'], ascending=[False, False])
        return combined_df


def save_transaction(
    transaction_data: Dict[str, Any],
    mode: PortfolioMode = 'PORT',
    client_id: str = None
) -> Dict[str, Any]:
    """
    Save a transaction with mode support

    Args:
        transaction_data: Transaction data dictionary
        mode: 'PORT' for live transactions, 'STAGING' for hypothetical
        client_id: Client identifier

    Returns:
        Result dictionary with transaction_id
    """
    if mode == 'PORT':
        # Save to BigQuery
        # TODO: Implement BigQuery write via data_upload.py
        raise NotImplementedError("PORT mode transaction saving not yet implemented via Orca MCP")

    elif mode == 'STAGING':
        # Save to Cloudflare D1
        return save_staging_transaction(transaction_data, client_id=client_id)

    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'PORT' or 'STAGING'")


def delete_transaction(
    transaction_id: int,
    mode: PortfolioMode = 'STAGING',
    client_id: str = None
) -> Dict[str, Any]:
    """
    Delete a transaction (STAGING mode only)

    Args:
        transaction_id: Transaction ID to delete
        mode: Must be 'STAGING' (PORT transactions should use proper workflow)
        client_id: Client identifier

    Returns:
        Result dictionary
    """
    if mode != 'STAGING':
        raise ValueError("Can only delete STAGING transactions via this API")

    return delete_staging_transaction(transaction_id, client_id=client_id)


def clear_staging_portfolio(
    portfolio_id: str,
    client_id: str = None
) -> Dict[str, Any]:
    """
    Clear all staging transactions for a portfolio

    Useful for resetting the sandbox environment.

    Args:
        portfolio_id: Portfolio identifier
        client_id: Client identifier

    Returns:
        Result with count of deleted transactions
    """
    return clear_all_staging_transactions(portfolio_id, client_id=client_id)


def update_transaction(
    transaction_id: int,
    updates: Dict[str, Any],
    client_id: str = None
) -> Dict[str, Any]:
    """
    Update a PORT transaction in BigQuery

    Allows editing of price, accrued_interest, dirty_price, market_value,
    settlement_date, and notes fields.

    Args:
        transaction_id: Transaction ID to update
        updates: Dictionary of field:value pairs to update
            Allowed fields: price, accrued_interest, dirty_price, market_value,
                           settlement_date, par_amount, notes
        client_id: Client identifier

    Returns:
        Result dictionary with success status
    """
    from google.cloud import bigquery

    # Validate allowed fields
    allowed_fields = {
        'price', 'accrued_interest', 'dirty_price', 'market_value',
        'settlement_date', 'par_amount', 'notes', 'transaction_date', 'status',
        'days_accrued'
    }

    invalid_fields = set(updates.keys()) - allowed_fields
    if invalid_fields:
        raise ValueError(f"Cannot update fields: {invalid_fields}. Allowed: {allowed_fields}")

    if not updates:
        raise ValueError("No updates provided")

    # Build the SET clause
    set_clauses = []
    for field, value in updates.items():
        if isinstance(value, str):
            set_clauses.append(f"{field} = '{value}'")
        elif value is None:
            set_clauses.append(f"{field} = NULL")
        else:
            set_clauses.append(f"{field} = {value}")

    set_clause = ", ".join(set_clauses)

    # Execute the update
    client = bigquery.Client(project='future-footing-414610')

    sql = f"""
    UPDATE `future-footing-414610.portfolio_data.transactions`
    SET {set_clause}
    WHERE transaction_id = {transaction_id}
    """

    try:
        query_job = client.query(sql)
        query_job.result()  # Wait for completion

        return {
            'success': True,
            'transaction_id': transaction_id,
            'updated_fields': list(updates.keys()),
            'message': f'Transaction {transaction_id} updated successfully'
        }
    except Exception as e:
        return {
            'success': False,
            'transaction_id': transaction_id,
            'error': str(e)
        }
