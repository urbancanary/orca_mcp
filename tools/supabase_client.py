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
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ttkcqogfbklodhgfmiac.supabase.co")
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

    Joins holdings_current with bonds for full details.
    Returns display-ready format with formatted fields.
    """
    # Query holdings with bond details via Supabase's foreign key joins
    params = {
        "portfolio_id": f"eq.{portfolio_id}",
        "select": "*, bonds(*)"
    }
    holdings = _get("holdings_current", params)

    # Format for display
    result = []
    for h in holdings:
        bond = h.get("bonds", {}) or {}
        par = float(h.get("par_amount", 0) or 0)
        price = float(h.get("current_price", 0) or 0)
        mv = float(h.get("market_value", 0) or 0)
        cost = float(h.get("cost_basis", 0) or 0)
        pnl = mv - cost if mv and cost else 0
        pnl_pct = (pnl / cost * 100) if cost else 0

        result.append({
            "isin": h.get("isin"),
            "ticker": bond.get("ticker", ""),
            "description": bond.get("description", ""),
            "country": bond.get("country", ""),
            "currency": bond.get("currency", "USD"),
            "coupon": bond.get("coupon"),
            "maturity_date": bond.get("maturity_date"),
            "rating": bond.get("rating_sp") or bond.get("rating_moody"),
            "sector": bond.get("sector"),

            # Position
            "face_value": par,
            "face_value_fmt": f"${par:,.0f}",
            "avg_cost": float(h.get("avg_price", 0) or 0),
            "current_price": price,
            "current_price_fmt": f"{price:.2f}",
            "cost_basis": cost,
            "market_value": mv,
            "market_value_fmt": f"${mv:,.0f}",

            # P&L
            "unrealized_pnl": pnl,
            "unrealized_pnl_fmt": f"${pnl:+,.0f}",
            "unrealized_pnl_pct": pnl_pct,
            "unrealized_pnl_pct_fmt": f"{pnl_pct:+.2f}%",

            # Dates
            "first_purchase_date": h.get("first_purchase_date"),
            "last_transaction_date": h.get("last_transaction_date"),
        })

    return result


def get_holdings_display(portfolio_id: str, include_staging: bool = False) -> Dict[str, Any]:
    """
    Get holdings formatted for display with weights and totals.
    """
    holdings = get_holdings(portfolio_id)

    # Calculate total market value for weights
    total_mv = sum(h["market_value"] for h in holdings)

    # Add weights
    for h in holdings:
        weight = (h["market_value"] / total_mv * 100) if total_mv else 0
        h["weight_pct"] = weight
        h["weight_pct_fmt"] = f"{weight:.1f}%"

    # Sort by market value descending
    holdings.sort(key=lambda x: x["market_value"], reverse=True)

    return {
        "portfolio_id": portfolio_id,
        "holdings_count": len(holdings),
        "total_market_value": total_mv,
        "total_market_value_fmt": f"${total_mv:,.0f}",
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
    """
    portfolio = get_portfolio(portfolio_id)
    holdings = get_holdings(portfolio_id)

    if not portfolio:
        return {"error": f"Portfolio {portfolio_id} not found"}

    # Aggregate metrics
    total_mv = sum(h["market_value"] for h in holdings)
    total_cost = sum(h["cost_basis"] for h in holdings)
    total_pnl = total_mv - total_cost
    cash = float(portfolio.get("cash_balance", 0) or 0)

    # Country allocation
    country_alloc = {}
    for h in holdings:
        country = h.get("country", "Unknown")
        country_alloc[country] = country_alloc.get(country, 0) + h["market_value"]

    country_list = [
        {"country": k, "value": v, "pct": v / total_mv * 100 if total_mv else 0}
        for k, v in sorted(country_alloc.items(), key=lambda x: -x[1])
    ]

    return {
        "portfolio_id": portfolio_id,
        "name": portfolio.get("name"),
        "client_id": portfolio.get("client_id"),

        "total_market_value": total_mv,
        "total_market_value_fmt": f"${total_mv:,.0f}",
        "cash_balance": cash,
        "cash_balance_fmt": f"${cash:,.0f}",
        "total_value": total_mv + cash,
        "total_value_fmt": f"${total_mv + cash:,.0f}",

        "total_cost": total_cost,
        "unrealized_pnl": total_pnl,
        "unrealized_pnl_pct": (total_pnl / total_cost * 100) if total_cost else 0,

        "holdings_count": len(holdings),
        "country_allocation": country_list,

        "source": "supabase"
    }


def get_portfolio_dashboard(portfolio_id: str) -> Dict[str, Any]:
    """
    Get full dashboard data for a portfolio.
    Combines summary, holdings, and allocations.
    """
    holdings = get_holdings(portfolio_id)
    portfolio = get_portfolio(portfolio_id)

    if not portfolio:
        return {"error": f"Portfolio {portfolio_id} not found"}

    total_mv = sum(h["market_value"] for h in holdings)
    cash = float(portfolio.get("cash_balance", 0) or 0)

    # Country allocation
    country_agg = {}
    for h in holdings:
        country = h.get("country", "Unknown")
        country_agg[country] = country_agg.get(country, 0) + h["market_value"]

    by_country = [
        {"country": k, "value": v, "pct": round(v / total_mv * 100, 1) if total_mv else 0}
        for k, v in sorted(country_agg.items(), key=lambda x: -x[1])
    ]

    # Rating allocation (placeholder - would need rating data)
    rating_agg = {}
    for h in holdings:
        rating = h.get("rating") or "NR"
        rating_agg[rating] = rating_agg.get(rating, 0) + h["market_value"]

    by_rating = [
        {"rating": k, "value": v, "pct": round(v / total_mv * 100, 1) if total_mv else 0}
        for k, v in sorted(rating_agg.items(), key=lambda x: -x[1])
    ]

    return {
        "portfolio_id": portfolio_id,
        "name": portfolio.get("name"),

        "summary": {
            "total_value": total_mv + cash,
            "bond_value": total_mv,
            "cash_balance": cash,
            "holdings_count": len(holdings),
        },

        "allocation": {
            "by_country": by_country,
            "by_rating": by_rating,
        },

        "source": "supabase"
    }


# ==============================================
# BONDS (Reference Data)
# ==============================================

def get_bond(isin: str) -> Dict[str, Any]:
    """Get bond reference data by ISIN."""
    params = {"isin": f"eq.{isin}"}
    results = _get("bonds", params)
    return results[0] if results else None


def search_bonds(
    country: str = None,
    sector: str = None,
    min_coupon: float = None,
    max_coupon: float = None
) -> List[Dict[str, Any]]:
    """Search bonds by criteria."""
    params = {}
    if country:
        params["country"] = f"eq.{country}"
    if sector:
        params["sector"] = f"eq.{sector}"
    if min_coupon:
        params["coupon"] = f"gte.{min_coupon}"
    if max_coupon:
        params["coupon"] = f"lte.{max_coupon}"

    return _get("bonds", params)


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
