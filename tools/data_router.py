"""
Data Source Router for Orca MCP

Routes portfolio data requests to the appropriate backend:
- Supabase: New Orion portfolios
- Cloudflare D1: Existing Athena portfolios (default)

Configuration via environment:
- SUPABASE_PORTFOLIOS: Comma-separated list of portfolio IDs that use Supabase
  Example: "wnbf,acme_fund,client_x"
- SUPABASE_URL: Supabase project URL
- SUPABASE_KEY: Supabase service role key
"""

import os
from typing import Dict, Any, List, Optional
import pandas as pd

# Portfolio routing configuration
# Portfolios listed here will use Supabase, all others use D1
SUPABASE_PORTFOLIOS = set(
    p.strip() for p in os.environ.get("SUPABASE_PORTFOLIOS", "").split(",") if p.strip()
)

# Check if Supabase is configured
SUPABASE_ENABLED = bool(os.environ.get("SUPABASE_KEY"))


def uses_supabase(portfolio_id: str) -> bool:
    """Check if a portfolio should use Supabase backend."""
    if not SUPABASE_ENABLED:
        return False
    # If no specific portfolios configured, use Supabase for all
    if not SUPABASE_PORTFOLIOS:
        return True
    return portfolio_id in SUPABASE_PORTFOLIOS


def get_holdings(portfolio_id: str, staging_id: int = 1, client_id: str = None) -> pd.DataFrame:
    """
    Get holdings from appropriate backend.
    Returns DataFrame in consistent format regardless of source.
    """
    import numpy as np

    if uses_supabase(portfolio_id):
        from .supabase_client import get_holdings as sb_get_holdings
        holdings = sb_get_holdings(portfolio_id)
        # Convert to DataFrame format expected by display_endpoints
        if not holdings:
            return pd.DataFrame()
        df = pd.DataFrame(holdings)
        # Map Supabase fields to expected D1 field names
        column_map = {
            'face_value': 'par_amount',
            'current_price': 'price',
            'yield_to_worst': 'ytw',
            'duration': 'oad',
            'spread': 'oas',
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
        # Replace NaN/Inf with 0 to avoid JSON serialization errors
        df = df.replace([np.inf, -np.inf], 0)
        df = df.fillna(0)
        return df
    else:
        from .cloudflare_d1 import get_holdings as d1_get_holdings
        return d1_get_holdings(portfolio_id, staging_id, client_id)


def get_holdings_summary(portfolio_id: str, staging_id: int = 1, client_id: str = None) -> Dict[str, Any]:
    """
    Get holdings summary from appropriate backend.
    """
    if uses_supabase(portfolio_id):
        from .supabase_client import get_portfolio_summary
        summary = get_portfolio_summary(portfolio_id)
        # Map to expected D1 format
        return {
            'total_market_value': summary.get('total_market_value', 0),
            'cash': summary.get('cash_balance', 0),
            'weighted_duration': 0,  # TODO: Calculate from holdings
            'weighted_yield': 0,     # TODO: Calculate from holdings
            'num_holdings': summary.get('holdings_count', 0),
            'country_breakdown': {
                c['country']: c['value']
                for c in summary.get('country_allocation', [])
            }
        }
    else:
        from .cloudflare_d1 import get_holdings_summary as d1_get_summary
        return d1_get_summary(portfolio_id, staging_id, client_id)


def get_transactions(portfolio_id: str, client_id: str = None) -> pd.DataFrame:
    """
    Get transactions from appropriate backend.
    Returns DataFrame in consistent format.
    """
    if uses_supabase(portfolio_id):
        from .supabase_client import get_transactions as sb_get_transactions
        txns = sb_get_transactions(portfolio_id)
        if not txns:
            return pd.DataFrame()
        return pd.DataFrame(txns)
    else:
        from .cloudflare_d1 import get_transactions as d1_get_transactions
        return d1_get_transactions(portfolio_id, client_id)


def get_cashflows(portfolio_id: str, client_id: str = None) -> pd.DataFrame:
    """
    Get cashflows from appropriate backend.
    """
    if uses_supabase(portfolio_id):
        # TODO: Implement cashflows in Supabase
        # For now, return empty DataFrame
        return pd.DataFrame()
    else:
        from .cloudflare_d1 import get_cashflows as d1_get_cashflows
        return d1_get_cashflows(portfolio_id, client_id)


def get_analytics_batch(isins: List[str], client_id: str = None) -> pd.DataFrame:
    """
    Get bond analytics for a list of ISINs.
    """
    # Always use D1 for now since it has the analytics data
    from .cloudflare_d1 import get_analytics_batch as d1_get_analytics
    return d1_get_analytics(isins)


# Export info about routing for debugging
def get_routing_info() -> Dict[str, Any]:
    """Get current routing configuration."""
    return {
        "supabase_enabled": SUPABASE_ENABLED,
        "supabase_portfolios": list(SUPABASE_PORTFOLIOS) if SUPABASE_PORTFOLIOS else "all",
        "supabase_url": os.environ.get("SUPABASE_URL", "not set"),
    }
