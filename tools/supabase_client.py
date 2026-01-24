"""
Supabase Client for Orca MCP

Clean Postgres backend for Orion portfolios.
Replaces BigQuery/D1 for new client deployments.

Environment variables:
    SUPABASE_URL: Project URL (e.g., https://xxx.supabase.co)
    SUPABASE_KEY: Service role key (for server-side access)
"""

import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, date
import requests

# Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://iociqthaxysqqqamonqa.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def _headers() -> Dict[str, str]:
    """Get request headers with auth."""
    if not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_KEY environment variable not set")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def _get(endpoint: str, params: Dict[str, Any] = None) -> List[Dict]:
    """GET request to Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    resp = requests.get(url, headers=_headers(), params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _post(endpoint: str, data: Dict[str, Any]) -> Dict:
    """POST request to Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {**_headers(), "Prefer": "return=representation"}
    resp = requests.post(url, headers=headers, json=data, timeout=10)
    resp.raise_for_status()
    result = resp.json()
    return result[0] if isinstance(result, list) and result else result


# ==============================================
# HOLDINGS
# ==============================================

def get_holdings(portfolio_id: str) -> List[Dict[str, Any]]:
    """
    Get current holdings for a portfolio.

    Uses current_holdings view (computed from transactions).
    Bond reference data is not stored locally - it comes from Andy's MCPs.
    Returns holdings with basic fields; enrichment happens in display layer.
    """
    # Query the current_holdings view
    params = {
        "portfolio_id": f"eq.{portfolio_id}",
    }
    holdings = _get("current_holdings", params)

    # Format for display (basic fields from view)
    result = []
    for h in holdings:
        par = float(h.get("par_amount", 0) or 0)
        cost = float(h.get("total_cost_basis", 0) or 0)

        result.append({
            "isin": h.get("isin"),
            "ticker": h.get("ticker", ""),
            "description": h.get("description", ""),
            "country": h.get("country", ""),

            # Position
            "par_amount": par,
            "face_value": par,
            "face_value_fmt": f"${par:,.0f}",
            "cost_basis": cost,
            "cost_basis_fmt": f"${cost:,.0f}",

            # Transaction info
            "transaction_count": h.get("transaction_count", 0),
            "first_transaction_date": h.get("first_transaction_date"),
            "last_transaction_date": h.get("last_transaction_date"),
        })

    return result


def get_holdings_display(portfolio_id: str, include_staging: bool = False) -> Dict[str, Any]:
    """
    Get holdings formatted for display with weights and totals.

    Note: Market values and prices must be enriched from Andy's MCPs.
    This returns cost-basis weighted data.
    """
    holdings = get_holdings(portfolio_id)

    # Calculate total par value for weights (market value requires price enrichment)
    total_par = sum(h.get("par_amount", 0) for h in holdings)
    total_cost = sum(h.get("cost_basis", 0) for h in holdings)

    # Add weights based on par value
    for h in holdings:
        weight = (h.get("par_amount", 0) / total_par * 100) if total_par else 0
        h["weight_pct"] = weight
        h["weight_pct_fmt"] = f"{weight:.1f}%"

    # Sort by par value descending
    holdings.sort(key=lambda x: x.get("par_amount", 0), reverse=True)

    return {
        "portfolio_id": portfolio_id,
        "holdings_count": len(holdings),
        "total_par_value": total_par,
        "total_par_value_fmt": f"${total_par:,.0f}",
        "total_cost_basis": total_cost,
        "total_cost_basis_fmt": f"${total_cost:,.0f}",
        "holdings": holdings,
        "source": "supabase"
    }


# ==============================================
# TRANSACTIONS
# ==============================================

def get_transactions(
    portfolio_id: str,
    status: str = None,
    start_date: str = None,
    end_date: str = None,
    limit: int = None
) -> List[Dict[str, Any]]:
    """
    Get transactions for a portfolio.

    Args:
        portfolio_id: Portfolio identifier
        status: Filter by status (settled, pending, staging, etc.)
        start_date: Filter transactions on or after this date (YYYY-MM-DD)
        end_date: Filter transactions on or before this date (YYYY-MM-DD)
        limit: Maximum number of transactions to return
    """
    params = {
        "portfolio_id": f"eq.{portfolio_id}",
        "order": "transaction_date.desc,created_at.desc"
    }

    if status:
        params["status"] = f"eq.{status}"
    if start_date:
        params["transaction_date"] = f"gte.{start_date}"
    if end_date:
        params["transaction_date"] = f"lte.{end_date}"
    if limit:
        params["limit"] = limit

    return _get("transactions", params)


def get_transactions_display(portfolio_id: str, **kwargs) -> Dict[str, Any]:
    """Get transactions formatted for display."""
    txns = get_transactions(portfolio_id, **kwargs)

    # Format for display
    for t in txns:
        t["par_amount_fmt"] = f"${float(t.get('par_amount', 0)):,.0f}"
        t["market_value_fmt"] = f"${float(t.get('market_value', 0)):,.0f}"
        t["price_fmt"] = f"{float(t.get('price', 0)):.4f}"

    return {
        "portfolio_id": portfolio_id,
        "transaction_count": len(txns),
        "transactions": txns,
        "source": "supabase"
    }


def save_transaction(transaction: Dict[str, Any]) -> Dict[str, Any]:
    """
    Save a new transaction.

    The holdings_current table is automatically updated via database trigger.
    """
    # Ensure required fields
    required = ["portfolio_id", "transaction_date", "settlement_date",
                "transaction_type", "isin", "par_amount"]
    for field in required:
        if field not in transaction:
            raise ValueError(f"Missing required field: {field}")

    # Set defaults
    transaction.setdefault("status", "staged")
    transaction.setdefault("created_at", datetime.utcnow().isoformat())

    result = _post("transactions", transaction)
    return {"success": True, "transaction": result}


def update_transaction(transaction_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing transaction.

    Args:
        transaction_id: The transaction ID to update
        updates: Dictionary of fields to update
    """
    url = f"{SUPABASE_URL}/rest/v1/transactions"
    headers = {**_headers(), "Prefer": "return=representation"}
    params = {"id": f"eq.{transaction_id}"}

    # Add updated_at timestamp
    updates["updated_at"] = datetime.utcnow().isoformat()

    resp = requests.patch(url, headers=headers, params=params, json=updates, timeout=10)

    if resp.status_code == 200:
        result = resp.json()
        return {"success": True, "transaction": result[0] if result else None}
    else:
        return {"success": False, "error": f"Update failed: {resp.status_code} - {resp.text}"}


# ==============================================
# PORTFOLIO SUMMARY
# ==============================================

def get_portfolio(portfolio_id: str) -> Dict[str, Any]:
    """Get portfolio metadata."""
    params = {"portfolio_id": f"eq.{portfolio_id}"}
    results = _get("portfolios", params)
    return results[0] if results else None


def get_portfolio_summary(portfolio_id: str) -> Dict[str, Any]:
    """
    Get portfolio summary with aggregated metrics.

    Note: Market values and P&L require price enrichment from Andy's MCPs.
    """
    portfolio = get_portfolio(portfolio_id)
    holdings = get_holdings(portfolio_id)

    if not portfolio:
        return {"error": f"Portfolio {portfolio_id} not found"}

    # Aggregate metrics (par and cost - market value requires price enrichment)
    total_par = sum(h.get("par_amount", 0) for h in holdings)
    total_cost = sum(h.get("cost_basis", 0) for h in holdings)

    # Country allocation (by par value)
    country_alloc = {}
    for h in holdings:
        country = h.get("country") or "Unknown"
        country_alloc[country] = country_alloc.get(country, 0) + h.get("par_amount", 0)

    country_list = [
        {"country": k, "value": v, "pct": v / total_par * 100 if total_par else 0}
        for k, v in sorted(country_alloc.items(), key=lambda x: -x[1])
    ]

    return {
        "portfolio_id": portfolio_id,
        "name": portfolio.get("name"),
        "client_id": portfolio.get("client_id"),

        "total_par_value": total_par,
        "total_par_value_fmt": f"${total_par:,.0f}",
        "total_cost_basis": total_cost,
        "total_cost_basis_fmt": f"${total_cost:,.0f}",

        "holdings_count": len(holdings),
        "country_allocation": country_list,

        "source": "supabase"
    }


def get_portfolio_dashboard(portfolio_id: str) -> Dict[str, Any]:
    """
    Get full dashboard data for a portfolio.
    Combines summary, holdings, and allocations.

    Note: Market values and ratings require enrichment from Andy's MCPs.
    """
    holdings = get_holdings(portfolio_id)
    portfolio = get_portfolio(portfolio_id)

    if not portfolio:
        return {"error": f"Portfolio {portfolio_id} not found"}

    total_par = sum(h.get("par_amount", 0) for h in holdings)
    total_cost = sum(h.get("cost_basis", 0) for h in holdings)

    # Country allocation (by par value)
    country_agg = {}
    for h in holdings:
        country = h.get("country") or "Unknown"
        country_agg[country] = country_agg.get(country, 0) + h.get("par_amount", 0)

    by_country = [
        {"country": k, "value": v, "pct": round(v / total_par * 100, 1) if total_par else 0}
        for k, v in sorted(country_agg.items(), key=lambda x: -x[1])
    ]

    return {
        "portfolio_id": portfolio_id,
        "name": portfolio.get("name"),

        "summary": {
            "total_par_value": total_par,
            "total_cost_basis": total_cost,
            "holdings_count": len(holdings),
        },

        "allocation": {
            "by_country": by_country,
        },

        "source": "supabase"
    }


# ==============================================
# BONDS (Reference Data)
# ==============================================
# Note: Bond reference data comes from Andy's MCPs (static_data, pricing)
# not from Guinness's Supabase. These functions are placeholders
# that route to the data router for MCP access.

def get_bond(isin: str) -> Dict[str, Any]:
    """
    Get bond reference data by ISIN.
    Routes to Andy's MCPs for static bond data.
    """
    # Bond data comes from Andy's static data MCP, not local Supabase
    from .cloudflare_d1 import get_analytics_for_isin
    return get_analytics_for_isin(isin)


# ==============================================
# HEALTH CHECK
# ==============================================

def health_check() -> Dict[str, Any]:
    """Check Supabase connectivity."""
    try:
        # Simple query to verify connection
        params = {"limit": 1}
        _get("portfolios", params)
        return {
            "status": "healthy",
            "supabase_url": SUPABASE_URL,
            "key_configured": bool(SUPABASE_KEY)
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "supabase_url": SUPABASE_URL,
            "key_configured": bool(SUPABASE_KEY)
        }
