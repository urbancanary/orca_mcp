"""
Orca MCP - Master Orchestrator for Portfolio Management

Provides unified API for multi-client portfolio data access.
"""

from .client_config import get_client_config, ClientConfig
from .tools.data_access import (
    query_bigquery,
    fetch_credentials_from_auth_mcp,
    setup_bigquery_credentials,
    get_client_database_registry
)
from .tools.performance import get_portfolio_performance
from .tools.transactions import (
    get_transactions,
    get_all_transactions,
    save_transaction,
    delete_transaction,
    clear_staging_portfolio
)
from .tools.cloudflare_d1 import (
    get_watchlist,
    get_watchlist_complete,
    get_holdings,
    get_holdings_summary,
    get_analytics,
    get_analytics_batch,
    get_period_prices,
    get_transactions as get_transactions_d1,
    get_cashflows as get_cashflows_d1,
)
from .tools.backfill import backfill_watchlist_analytics, get_backfill_status, refresh_bond_analytics

__version__ = "1.0.0"

__all__ = [
    # Client config
    "get_client_config",
    "ClientConfig",
    # BigQuery (background sync only)
    "query_bigquery",
    "fetch_credentials_from_auth_mcp",
    "setup_bigquery_credentials",
    "get_client_database_registry",
    "get_portfolio_performance",
    # Transactions (BigQuery - for staging)
    "get_transactions",
    "get_all_transactions",
    "save_transaction",
    "delete_transaction",
    "clear_staging_portfolio",
    # D1 Edge Database (fast user queries)
    "get_watchlist",
    "get_watchlist_complete",
    "get_holdings",
    "get_holdings_summary",
    "get_analytics",
    "get_analytics_batch",
    "get_period_prices",
    "get_transactions_d1",
    "get_cashflows_d1",
    # Background jobs
    "backfill_watchlist_analytics",
    "get_backfill_status",
    "refresh_bond_analytics",
]
