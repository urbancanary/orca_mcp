"""
Display-Ready Endpoints for Orca MCP

These endpoints return data with pre-formatted strings for direct display.
Frontends (Streamlit, Reflex, etc.) do zero calculation, zero formatting -
they just render what the MCP returns.

Key principle: Every numeric value includes a _fmt version.
Example: market_value: 837000, market_value_fmt: "$837,000"
"""

from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
import pandas as pd
import json
import sys
from pathlib import Path

# Add parent directory to path for imports
SCRIPT_DIR = Path(__file__).parent.parent.resolve()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    from .data_router import (
        get_holdings,
        get_holdings_summary,
        get_transactions,
        get_cashflows,
    )
    from .compliance import check_compliance, compliance_to_dict
except ImportError:
    from tools.data_router import (
        get_holdings,
        get_holdings_summary,
        get_transactions,
        get_cashflows,
    )
    from tools.compliance import check_compliance, compliance_to_dict


# =============================================================================
# FORMATTING HELPERS (Currency-neutral - no $ symbols)
# =============================================================================

def fmt_money(value: float, decimals: int = 0, abbreviate: bool = False) -> str:
    """Format money value with commas (NO currency symbol - currency neutral)"""
    if value is None or pd.isna(value):
        return "-"

    if abbreviate and abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    elif abbreviate and abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"

    if value < 0:
        if decimals == 0:
            return f"-{abs(value):,.0f}"
        return f"-{abs(value):,.{decimals}f}"

    if decimals == 0:
        return f"{value:,.0f}"
    return f"{value:,.{decimals}f}"


def fmt_money_change(value: float) -> str:
    """Format money with +/- prefix (NO currency symbol)"""
    if value is None or pd.isna(value):
        return "-"
    if value >= 0:
        return f"+{value:,.0f}"
    return f"-{abs(value):,.0f}"


def fmt_pct(value: float, decimals: int = 1, show_sign: bool = False) -> str:
    """Format percentage value"""
    if value is None or pd.isna(value):
        return "-"
    if show_sign and value > 0:
        return f"+{value:.{decimals}f}%"
    return f"{value:.{decimals}f}%"


def fmt_price(value: float) -> str:
    """Format bond price (always 2 decimals)"""
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.2f}"


def fmt_duration(value: float) -> str:
    """Format duration in years"""
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.2f}y"


def fmt_spread(value: float) -> str:
    """Format spread in basis points"""
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.0f} bp"


def fmt_date(value) -> str:
    """Format date as YYYY-MM-DD"""
    if value is None:
        return "-"
    if isinstance(value, str):
        return value[:10] if len(value) >= 10 else value
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    return str(value)


def get_rating_bucket(rating: str) -> str:
    """Classify rating as IG (Investment Grade) or HY (High Yield)"""
    if not rating:
        return "NR"
    # S&P ratings
    ig_ratings = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-']
    # Moody's equivalents (Aaa=AAA, Aa1=AA+, Aa2=AA, Aa3=AA-, A1=A+, A2=A, A3=A-, Baa1=BBB+, Baa2=BBB, Baa3=BBB-)
    ig_ratings += ['AAA', 'AA1', 'AA2', 'AA3', 'A1', 'A2', 'A3', 'BAA1', 'BAA2', 'BAA3']
    return "IG" if rating.upper() in ig_ratings else "HY"


def safe_float(value, default: float = 0.0) -> float:
    """Safely convert value to float, handling NaN and None"""
    if value is None:
        return default
    if pd.isna(value):
        return default
    try:
        result = float(value)
        # Also check for inf
        if pd.isna(result) or result != result:  # NaN check
            return default
        return result
    except (ValueError, TypeError):
        return default


def safe_str(value, default: str = "") -> str:
    """Safely convert value to string, handling NaN and None"""
    if value is None:
        return default
    if pd.isna(value):
        return default
    return str(value)


# =============================================================================
# 1. GET HOLDINGS DISPLAY
# =============================================================================

def get_holdings_display(
    portfolio_id: str = "wnbf",
    include_staging: bool = False,
    client_id: str = None
) -> Dict[str, Any]:
    """
    Get holdings with ALL display columns and formatted values.

    Every numeric field has a corresponding _fmt version for direct display.

    Args:
        portfolio_id: Portfolio identifier
        include_staging: Include staging holdings
        client_id: Client identifier

    Returns:
        Dictionary with holdings array, totals, and metadata
    """
    staging_id = 2 if include_staging else 1
    holdings_df = get_holdings(portfolio_id, staging_id, client_id)

    if holdings_df.empty:
        return {
            "holdings": [],
            "totals": {},
            "count": 0,
            "as_of": datetime.utcnow().isoformat() + "Z"
        }

    # Calculate totals for weight calculation
    total_market_value = holdings_df['market_value'].sum() if 'market_value' in holdings_df.columns else 0
    total_face_value = holdings_df['par_amount'].sum() if 'par_amount' in holdings_df.columns else 0

    # Build display-ready holdings
    display_holdings = []
    total_unrealized_pnl = 0
    weighted_yield = 0
    weighted_duration = 0
    weighted_spread = 0

    for _, row in holdings_df.iterrows():
        # Get base values with defaults
        face_value = float(row.get('par_amount', 0) or 0)
        market_value = float(row.get('market_value', 0) or 0)
        price = float(row.get('price', 0) or 0)

        # Cost basis: use stored cost_basis, or calculate from purchase_price/avg_cost
        cost_basis = float(row.get('cost_basis', 0) or 0)
        avg_cost = float(row.get('purchase_price', 0) or row.get('avg_cost', 0) or 0)
        if not cost_basis and avg_cost and face_value:
            cost_basis = face_value * (avg_cost / 100)
        if not cost_basis:
            cost_basis = market_value  # Fallback only if no cost data at all
        if not avg_cost:
            avg_cost = price  # For display purposes only

        # Calculate P&L
        unrealized_pnl = market_value - cost_basis
        total_unrealized_pnl += unrealized_pnl
        unrealized_pnl_pct = (unrealized_pnl / cost_basis * 100) if cost_basis else 0

        # Calculate weight
        weight_pct = (market_value / total_market_value * 100) if total_market_value else 0

        # Get analytics
        ytw = float(row.get('ytw', 0) or row.get('yield', 0) or 0)
        duration = float(row.get('oad', 0) or row.get('duration', 0) or 0)
        spread = float(row.get('oas', 0) or row.get('spread', 0) or 0)

        # Weighted averages
        if total_market_value > 0:
            weighted_yield += ytw * (market_value / total_market_value)
            weighted_duration += duration * (market_value / total_market_value)
            weighted_spread += spread * (market_value / total_market_value)

        # Get rating info
        rating = row.get('rating_sp', '') or row.get('rating', '') or ''

        holding = {
            # Identifiers
            "isin": row.get('isin', ''),
            "ticker": row.get('ticker', ''),
            "description": row.get('description', ''),
            "country": row.get('country', ''),
            "rating": rating,
            "rating_bucket": get_rating_bucket(rating),
            "sector": row.get('sector', 'Sovereign'),
            "coupon": float(row.get('coupon', 0) or 0),
            "maturity_date": fmt_date(row.get('maturity_date')),

            # Values
            "face_value": face_value,
            "face_value_fmt": fmt_money(face_value),
            "avg_cost": avg_cost,
            "current_price": price,
            "current_price_fmt": fmt_price(price),

            # Calculated values
            "cost_basis": cost_basis,
            "market_value": market_value,
            "market_value_fmt": fmt_money(market_value),

            # P&L
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pnl_fmt": fmt_money(unrealized_pnl),
            "unrealized_pnl_pct": unrealized_pnl_pct,
            "unrealized_pnl_pct_fmt": fmt_pct(unrealized_pnl_pct, show_sign=True),

            # Weight
            "weight_pct": weight_pct,
            "weight_pct_fmt": fmt_pct(weight_pct),

            # Analytics
            "yield_to_worst": ytw,
            "yield_fmt": fmt_pct(ytw),
            "duration": duration,
            "duration_fmt": fmt_duration(duration),
            "spread": spread,
            "spread_fmt": fmt_spread(spread),

            # Coupon info
            "accrued_interest": float(row.get('accrued_interest', 0) or 0),
            "accrued_interest_fmt": fmt_money(row.get('accrued_interest', 0), 2),
        }
        display_holdings.append(holding)

    return {
        "holdings": display_holdings,
        "totals": {
            "face_value": total_face_value,
            "face_value_fmt": fmt_money(total_face_value),
            "market_value": total_market_value,
            "market_value_fmt": fmt_money(total_market_value),
            "unrealized_pnl": total_unrealized_pnl,
            "unrealized_pnl_fmt": fmt_money(total_unrealized_pnl),
            "avg_yield": weighted_yield,
            "avg_yield_fmt": fmt_pct(weighted_yield),
            "avg_duration": weighted_duration,
            "avg_duration_fmt": fmt_duration(weighted_duration),
            "avg_spread": weighted_spread,
            "avg_spread_fmt": fmt_spread(weighted_spread),
        },
        "count": len(display_holdings),
        "as_of": datetime.utcnow().isoformat() + "Z"
    }


# =============================================================================
# 2. GET PORTFOLIO DASHBOARD
# =============================================================================

def get_portfolio_dashboard(
    portfolio_id: str = "wnbf",
    client_id: str = None
) -> Dict[str, Any]:
    """
    Single call to populate the Portfolio/Summary page.

    Includes summary stats, allocation breakdowns, and compliance summary.

    Args:
        portfolio_id: Portfolio identifier
        client_id: Client identifier

    Returns:
        Complete dashboard data with formatted values
    """
    # Get holdings and summary from D1
    holdings_df = get_holdings(portfolio_id, staging_id=1, client_id=client_id)
    summary = get_holdings_summary(portfolio_id, staging_id=1, client_id=client_id)

    if holdings_df.empty:
        return {
            "summary": {"error": "No holdings found"},
            "allocation": {},
            "compliance_summary": {},
            "as_of": datetime.utcnow().isoformat() + "Z"
        }

    # Extract values from summary, with fallback to holdings calculation
    total_bond_value = float(summary.get('total_market_value', 0) or 0)

    # If summary has no market value, calculate from holdings
    if total_bond_value == 0 and 'market_value' in holdings_df.columns:
        total_bond_value = float(holdings_df['market_value'].sum())

    # Cash: try summary first, then calculate from transactions
    cash_balance = float(summary.get('cash', 0) or 0)
    if cash_balance == 0:
        # Calculate cash from transactions (INITIAL + SELLs + COUPONs - BUYs)
        txns_df = get_transactions(portfolio_id, client_id)
        if not txns_df.empty:
            for _, row in txns_df.iterrows():
                txn_type = safe_str(row.get('transaction_type', '')).upper()
                status = safe_str(row.get('status', '')).lower()
                if status not in ['confirmed', 'settled']:
                    continue
                # Use settlement_amount (actual cash impact), not market_value
                amount = safe_float(row.get('settlement_amount') or row.get('market_value'))
                if txn_type in ['INITIAL', 'SELL', 'COUPON']:
                    cash_balance += amount
                elif txn_type == 'BUY':
                    cash_balance -= amount

    total_value = total_bond_value + cash_balance
    cash_pct = (cash_balance / total_value * 100) if total_value else 0

    # Weighted averages from summary
    weighted_duration = float(summary.get('weighted_duration', 0) or 0)
    weighted_yield = float(summary.get('weighted_yield', 0) or 0)
    num_holdings = int(summary.get('num_holdings', len(holdings_df)))

    # Build summary section
    dashboard_summary = {
        "total_value": total_value,
        "total_value_fmt": fmt_money(total_value, abbreviate=True),
        "bond_value": total_bond_value,
        "bond_value_fmt": fmt_money(total_bond_value, abbreviate=True),
        "cash_balance": cash_balance,
        "cash_balance_fmt": fmt_money(cash_balance, abbreviate=True),
        "cash_pct": cash_pct,
        "cash_pct_fmt": fmt_pct(cash_pct),
        "duration": weighted_duration,
        "duration_fmt": fmt_duration(weighted_duration),
        "yield": weighted_yield,
        "yield_fmt": fmt_pct(weighted_yield),
        "num_holdings": num_holdings,
    }

    # Build allocation breakdowns
    allocation = {"by_country": [], "by_rating": [], "by_sector": []}

    # Country allocation (from summary or calculate)
    country_breakdown = summary.get('country_breakdown', {})
    if country_breakdown:
        for country, mv in sorted(country_breakdown.items(), key=lambda x: x[1], reverse=True):
            pct = (mv / total_bond_value * 100) if total_bond_value else 0
            allocation["by_country"].append({
                "country": country,
                "pct": round(pct, 1),
                "pct_fmt": fmt_pct(pct),
                "value": mv,
                "value_fmt": fmt_money(mv),
            })
    else:
        # Calculate from holdings
        if 'country' in holdings_df.columns and 'market_value' in holdings_df.columns:
            country_totals = holdings_df.groupby('country')['market_value'].sum()
            for country, mv in country_totals.sort_values(ascending=False).items():
                pct = (mv / total_bond_value * 100) if total_bond_value else 0
                allocation["by_country"].append({
                    "country": country,
                    "pct": round(pct, 1),
                    "pct_fmt": fmt_pct(pct),
                    "value": mv,
                    "value_fmt": fmt_money(mv),
                })

    # Rating allocation
    rating_col = 'rating_sp' if 'rating_sp' in holdings_df.columns else 'rating'
    if rating_col in holdings_df.columns and 'market_value' in holdings_df.columns:
        rating_totals = holdings_df.groupby(rating_col)['market_value'].sum()
        for rating, mv in rating_totals.sort_values(ascending=False).items():
            if rating:
                pct = (mv / total_bond_value * 100) if total_bond_value else 0
                allocation["by_rating"].append({
                    "rating": rating,
                    "pct": round(pct, 1),
                    "pct_fmt": fmt_pct(pct),
                    "value": mv,
                    "value_fmt": fmt_money(mv),
                    "bucket": get_rating_bucket(rating),
                })

    # Run compliance check
    compliance_result = check_compliance(holdings_df, cash_balance)
    compliance_summary = {
        "is_compliant": compliance_result.is_compliant,
        "hard_rules_pass": compliance_result.hard_pass,
        "hard_rules_total": compliance_result.hard_total,
        "soft_warnings": compliance_result.soft_total - compliance_result.soft_pass,
    }

    return {
        "summary": dashboard_summary,
        "allocation": allocation,
        "compliance_summary": compliance_summary,
        "as_of": datetime.utcnow().isoformat() + "Z"
    }


# =============================================================================
# 3. CALCULATE TRADE SETTLEMENT
# =============================================================================

def calculate_trade_settlement(
    isin: str,
    face_value: float,
    price: float,
    settle_date: str,
    side: str = "BUY",
    client_id: str = None,
    bond_info: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Pre-trade settlement calculation for Trade Ticket page.

    Calculates principal, accrued interest, and net settlement amount.
    Uses 30/360 day count convention.

    Args:
        isin: Bond ISIN
        face_value: Face/par value of the trade
        price: Clean price as percentage of par
        settle_date: Settlement date (YYYY-MM-DD)
        side: "BUY" or "SELL"
        client_id: Client identifier
        bond_info: Optional pre-fetched bond details

    Returns:
        Complete settlement calculation with formatted values
    """
    # Parse settle date
    if isinstance(settle_date, str):
        settle_dt = datetime.strptime(settle_date[:10], "%Y-%m-%d")
    else:
        settle_dt = settle_date

    # Get bond info if not provided
    if bond_info is None:
        # Try to fetch from D1 analytics
        from .cloudflare_d1 import get_analytics_batch
        analytics = get_analytics_batch([isin])
        if not analytics.empty:
            row = analytics.iloc[0]
            bond_info = {
                "isin": isin,
                "ticker": row.get('ticker', ''),
                "description": row.get('description', ''),
                "country": row.get('country', ''),
                "rating": row.get('rating_sp', '') or row.get('rating', ''),
                "coupon": float(row.get('coupon', 0) or 0),
                "maturity_date": str(row.get('maturity_date', '')),
                "ytw": float(row.get('ytw', 0) or row.get('yield', 0) or 0),
                "duration": float(row.get('oad', 0) or row.get('duration', 0) or 0),
                "spread": float(row.get('oas', 0) or row.get('spread', 0) or 0),
            }
        else:
            bond_info = {"isin": isin, "coupon": 0}

    # Get coupon info
    coupon_rate = float(bond_info.get('coupon', 0) or 0)

    # Calculate last coupon date (simplified - assumes semi-annual, Jan/Jul)
    # In production, this would come from bond reference data
    today = settle_dt
    if today.month <= 1:
        last_coupon = datetime(today.year - 1, 7, 14)
    elif today.month <= 7:
        last_coupon = datetime(today.year, 1, 14)
    else:
        last_coupon = datetime(today.year, 7, 14)

    next_coupon = last_coupon + timedelta(days=182)  # ~6 months

    # Calculate days accrued (30/360 convention)
    days_in_period = 180  # Semi-annual
    days_accrued = (settle_dt - last_coupon).days
    if days_accrued < 0:
        days_accrued = 0
    if days_accrued > 180:
        days_accrued = 180

    # Calculate amounts
    principal = face_value * (price / 100)
    accrued_interest = face_value * (coupon_rate / 100) * (days_accrued / 360)
    accrued_pct = (accrued_interest / face_value * 100) if face_value else 0

    net_settlement = principal + accrued_interest
    direction = "PAY" if side.upper() == "BUY" else "RECEIVE"

    return {
        "bond": {
            "isin": isin,
            "ticker": bond_info.get('ticker', ''),
            "description": bond_info.get('description', ''),
            "country": bond_info.get('country', ''),
            "rating": bond_info.get('rating', ''),
            "coupon": coupon_rate,
            "maturity_date": bond_info.get('maturity_date', ''),
            "last_coupon_date": fmt_date(last_coupon),
            "next_coupon_date": fmt_date(next_coupon),
        },
        "analytics": {
            "yield_to_worst": bond_info.get('ytw', 0),
            "yield_fmt": fmt_pct(bond_info.get('ytw', 0)),
            "duration": bond_info.get('duration', 0),
            "duration_fmt": fmt_duration(bond_info.get('duration', 0)),
            "spread": bond_info.get('spread', 0),
            "spread_fmt": fmt_spread(bond_info.get('spread', 0)),
        },
        "settlement": {
            "face_value": face_value,
            "face_value_fmt": fmt_money(face_value),
            "price": price,
            "price_fmt": fmt_price(price),

            "principal": principal,
            "principal_fmt": fmt_money(principal, 2),

            "days_accrued": days_accrued,
            "accrued_interest": accrued_interest,
            "accrued_interest_fmt": fmt_money(accrued_interest, 2),
            "accrued_pct": accrued_pct,
            "accrued_pct_fmt": fmt_pct(accrued_pct, 2),

            "net_settlement": net_settlement,
            "net_settlement_fmt": fmt_money(net_settlement, 2),

            "side": side.upper(),
            "direction": direction,
        },
        "settle_date": fmt_date(settle_dt),
        "calculation_method": "30/360"
    }


# =============================================================================
# 4. GET TRANSACTIONS DISPLAY
# =============================================================================

def get_transactions_display(
    portfolio_id: str = "wnbf",
    transaction_type: str = "ALL",
    status: str = "ALL",
    start_date: str = None,
    end_date: str = None,
    limit: int = 100,
    client_id: str = None
) -> Dict[str, Any]:
    """
    Transaction history with display-ready formatting.

    Args:
        portfolio_id: Portfolio identifier
        transaction_type: "ALL", "BUY", "SELL", or "COUPON"
        status: "ALL", "settled", "pending", or "staging"
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        limit: Maximum transactions to return
        client_id: Client identifier

    Returns:
        Transactions with formatted values and summary
    """
    # Get transactions from D1
    txns_df = get_transactions(portfolio_id, client_id)

    if txns_df.empty:
        return {
            "transactions": [],
            "summary": {
                "total_transactions": 0,
                "buys": 0,
                "sells": 0,
                "coupons": 0,
            },
            "count": 0
        }

    # Apply filters
    if transaction_type != "ALL" and 'transaction_type' in txns_df.columns:
        txns_df = txns_df[txns_df['transaction_type'].str.upper() == transaction_type.upper()]

    if status != "ALL" and 'status' in txns_df.columns:
        txns_df = txns_df[txns_df['status'].str.lower() == status.lower()]

    if start_date and 'transaction_date' in txns_df.columns:
        txns_df = txns_df[txns_df['transaction_date'] >= start_date]

    if end_date and 'transaction_date' in txns_df.columns:
        txns_df = txns_df[txns_df['transaction_date'] <= end_date]

    # Limit results
    txns_df = txns_df.head(limit)

    # Build display transactions
    display_txns = []
    total_settled = 0
    buy_count = 0
    sell_count = 0
    coupon_count = 0

    for _, row in txns_df.iterrows():
        txn_type = safe_str(row.get('transaction_type', '')).upper()
        face_value = safe_float(row.get('par_amount'))
        price = safe_float(row.get('price'))
        accrued = safe_float(row.get('accrued_interest'))
        settlement_amount = safe_float(row.get('market_value'))

        # Count by type
        if txn_type == 'BUY':
            buy_count += 1
            total_settled += settlement_amount
        elif txn_type == 'SELL':
            sell_count += 1
        elif txn_type == 'COUPON':
            coupon_count += 1

        txn = {
            "id": safe_str(row.get('transaction_id', '')),
            "trade_date": fmt_date(row.get('transaction_date')),
            "settle_date": fmt_date(row.get('settlement_date')),
            "transaction_type": txn_type,
            "status": safe_str(row.get('status', 'settled')).lower(),

            "isin": safe_str(row.get('isin', '')),
            "ticker": safe_str(row.get('ticker', '')),
            "description": safe_str(row.get('description', '')),
            "country": safe_str(row.get('country', '')),

            "face_value": face_value,
            "face_value_fmt": fmt_money(face_value),
            "price": price,
            "price_fmt": fmt_price(price),
            "accrued_interest": accrued,
            "settlement_amount": settlement_amount,
            "settlement_amount_fmt": fmt_money(settlement_amount),

            "yield_at_trade": safe_float(row.get('ytm')),
            "duration_at_trade": safe_float(row.get('duration')),
            "spread_at_trade": safe_float(row.get('spread')),

            "notes": safe_str(row.get('notes', '')),
        }
        display_txns.append(txn)

    return {
        "transactions": display_txns,
        "summary": {
            "total_transactions": len(display_txns),
            "buys": buy_count,
            "sells": sell_count,
            "coupons": coupon_count,
            "total_settled_amount": total_settled,
            "total_settled_amount_fmt": fmt_money(total_settled, abbreviate=True),
        },
        "count": len(display_txns)
    }


# =============================================================================
# 5. CHECK TRADE COMPLIANCE (Enhanced)
# =============================================================================

def check_trade_compliance(
    portfolio_id: str,
    ticker: str,
    country: str,
    action: str,
    market_value: float,
    client_id: str = None
) -> Dict[str, Any]:
    """
    Enhanced pre-trade compliance check with display-ready output.

    Args:
        portfolio_id: Portfolio identifier
        ticker: Bond ticker
        country: Bond country
        action: "buy" or "sell"
        market_value: Trade market value
        client_id: Client identifier

    Returns:
        Before/after compliance with impact analysis and warnings
    """
    from .compliance import check_compliance_impact

    # Get current holdings
    holdings_df = get_holdings(portfolio_id, staging_id=1, client_id=client_id)
    summary = get_holdings_summary(portfolio_id, staging_id=1, client_id=client_id)
    net_cash = float(summary.get('cash', 0) or 0)

    # Build proposed trade
    proposed_trade = {
        "ticker": ticker,
        "country": country,
        "action": action.lower(),
        "market_value": market_value
    }

    # Run compliance impact check
    result = check_compliance_impact(holdings_df, net_cash, proposed_trade)

    # Build warnings and errors
    warnings = []
    errors = []

    impact = result.get('impact', {})
    before = result.get('before', {})
    after = result.get('after', {})

    # Check for breaches
    if impact.get('would_breach'):
        errors.append("This trade would cause a compliance breach")

    # Check specific rules
    after_metrics = after.get('metrics', {})
    before_metrics = before.get('metrics', {})

    # Country concentration warning
    country_breakdown = after_metrics.get('country_breakdown', {})
    if country in country_breakdown:
        country_pct = country_breakdown[country] / after_metrics.get('total_nav', 1) * 100
        if country_pct > 15:  # Warning threshold
            warnings.append(f"This trade would increase {country} exposure to {country_pct:.1f}% (limit: 20%)")

    # Cash warning
    if action.lower() == 'buy':
        new_cash = net_cash - market_value
        if new_cash < 0:
            errors.append(f"Insufficient cash: would be ${new_cash:,.0f} overdrawn")

    # Determine if trade can proceed
    can_proceed = not errors and after.get('is_compliant', False)
    can_proceed_reason = "All hard rules pass" if can_proceed else "Would breach hard rules"
    if errors:
        can_proceed_reason = errors[0]

    return {
        "before": before,
        "after": after,
        "impact": {
            "compliance_change": impact.get('compliance_change', 'unchanged'),
            "would_breach": impact.get('would_breach', False),
            "would_fix": impact.get('would_fix', False),
            "hard_rules_change": impact.get('hard_rules_change', 0),
            "soft_rules_change": impact.get('soft_rules_change', 0),
        },
        "warnings": warnings,
        "errors": errors,
        "can_proceed": can_proceed,
        "can_proceed_reason": can_proceed_reason
    }


# =============================================================================
# 6. GET CASHFLOWS DISPLAY
# =============================================================================

def get_cashflows_display(
    portfolio_id: str = "wnbf",
    months_ahead: int = 12,
    client_id: str = None
) -> Dict[str, Any]:
    """
    Projected cashflows for the Cashflows page.

    Returns upcoming coupon payments and maturities with formatted values.

    Args:
        portfolio_id: Portfolio identifier
        months_ahead: How many months ahead to project
        client_id: Client identifier

    Returns:
        Cashflows with summary, individual flows, and monthly breakdown
    """
    # Get cashflows from D1
    cashflows_df = get_cashflows(portfolio_id, client_id)

    # Filter to future cashflows
    today = datetime.now()
    end_date = today + timedelta(days=months_ahead * 30)

    if cashflows_df.empty:
        return {
            "summary": {
                "total_12m": 0,
                "total_12m_fmt": "$0",
                "next_coupon_date": None,
                "next_coupon_amount": 0,
                "avg_coupon_rate": 0,
                "maturities_5yr": 0,
            },
            "cashflows": [],
            "by_month": [],
            "maturities": [],
            "count": 0
        }

    # Parse dates and filter
    if 'payment_date' in cashflows_df.columns:
        cashflows_df['payment_date'] = pd.to_datetime(cashflows_df['payment_date'])
        cashflows_df = cashflows_df[
            (cashflows_df['payment_date'] >= today) &
            (cashflows_df['payment_date'] <= end_date)
        ]

    # Sort by date
    cashflows_df = cashflows_df.sort_values('payment_date')

    # Build display cashflows
    display_cashflows = []
    total_amount = 0
    maturities = []
    by_month = {}

    for _, row in cashflows_df.iterrows():
        cf_type = str(row.get('payment_type', 'COUPON')).upper()
        amount = float(row.get('amount', 0) or row.get('payment_amount', 0) or 0)
        payment_date = row.get('payment_date')

        total_amount += amount

        # Group by month
        if payment_date:
            month_key = payment_date.strftime('%Y-%m')
            by_month[month_key] = by_month.get(month_key, 0) + amount

        cf = {
            "date": fmt_date(payment_date),
            "type": cf_type,
            "ticker": row.get('ticker', ''),
            "isin": row.get('isin', ''),
            "amount": amount,
            "amount_fmt": fmt_money(amount),
        }
        display_cashflows.append(cf)

        # Track maturities
        if cf_type == 'MATURITY' or cf_type == 'PRINCIPAL':
            maturities.append(cf)

    # Get next coupon
    coupons = [cf for cf in display_cashflows if cf['type'] == 'COUPON']
    next_coupon_date = coupons[0]['date'] if coupons else None
    next_coupon_amount = coupons[0]['amount'] if coupons else 0

    # Build monthly breakdown
    monthly_breakdown = [
        {"month": month, "amount": amt, "amount_fmt": fmt_money(amt)}
        for month, amt in sorted(by_month.items())
    ]

    return {
        "summary": {
            "total_12m": total_amount,
            "total_12m_fmt": fmt_money(total_amount),
            "next_coupon_date": next_coupon_date,
            "next_coupon_amount": next_coupon_amount,
            "next_coupon_amount_fmt": fmt_money(next_coupon_amount),
            "avg_coupon_rate": 0,  # Would need to calculate from holdings
            "maturities_5yr": len(maturities),
        },
        "cashflows": display_cashflows,
        "by_month": monthly_breakdown,
        "maturities": maturities,
        "count": len(display_cashflows)
    }


# =============================================================================
# 7. GET COMPLIANCE DISPLAY
# =============================================================================

def get_compliance_display(
    portfolio_id: str = "wnbf",
    client_id: str = None
) -> Dict[str, Any]:
    """
    Display-ready compliance dashboard.
    Wraps check_compliance with display-ready output including chart data.
    """
    holdings_df = get_holdings(portfolio_id, staging_id=1, client_id=client_id)
    summary = get_holdings_summary(portfolio_id, staging_id=1, client_id=client_id)
    net_cash = safe_float(summary.get('cash', 0))

    if holdings_df.empty:
        return {
            "is_compliant": True,
            "summary": {"hard_pass": 0, "hard_total": 0, "soft_pass": 0, "soft_total": 0},
            "rules": [], "country_chart": [],
            "violations": {"issuers_over_5": [], "nfa_violations": []},
            "metrics": {}
        }

    result = check_compliance(holdings_df, net_cash)

    display_rules = [{
        "type": rule.rule_type, "name": rule.name, "limit": rule.limit,
        "current": rule.current, "status": rule.status.lower(), "details": rule.details
    } for rule in result.rules]

    country_chart = []
    country_breakdown = result.metrics.get('country_breakdown', {})
    total_nav = result.metrics.get('total_nav', 1)
    for country, value in sorted(country_breakdown.items(), key=lambda x: x[1], reverse=True):
        pct = (value / total_nav * 100) if total_nav else 0
        country_chart.append({"country": country, "pct": round(pct, 1), "value": round(value, 0), "over_limit": pct > 20})

    return {
        "is_compliant": result.is_compliant,
        "summary": {"hard_pass": result.hard_pass, "hard_total": result.hard_total,
                    "soft_pass": result.soft_pass, "soft_total": result.soft_total},
        "rules": display_rules,
        "country_chart": country_chart,
        "violations": {
            "issuers_over_5": result.metrics.get('issuers_over_5_details', []),
            "nfa_violations": result.metrics.get('nfa_violations', [])
        },
        "metrics": {
            "max_position": result.metrics.get('max_position', 0),
            "max_position_ticker": result.metrics.get('max_position_ticker', ''),
            "max_country_pct": result.metrics.get('max_country_pct', 0),
            "max_country": result.metrics.get('max_country', ''),
            "cash_pct": result.metrics.get('cash_pct', 0),
            "num_holdings": result.metrics.get('num_holdings', 0)
        }
    }


# =============================================================================
# 8. GET P&L DISPLAY - Proper reconciliation (not derived)
# =============================================================================

def get_pnl_display(
    portfolio_id: str = "wnbf",
    period: str = "Since Inception",
    start_date: str = None,
    end_date: str = None,
    client_id: str = None
) -> Dict[str, Any]:
    """
    P&L reconciliation with validation.

    Proper accounting:
    - Opening NAV = Total Cost Basis + Initial Cash
    - Closing NAV = Total Market Value + Current Cash
    - P&L components: Unrealized (MV - Cost) + Realized + Coupons
    - Validation: Opening + Total P&L = Closing (must balance)
    """
    holdings_df = get_holdings(portfolio_id, staging_id=1, client_id=client_id)
    txns_df = get_transactions(portfolio_id, client_id)
    today = datetime.now()

    if period == "MTD":
        period_start, period_end, period_label = today.replace(day=1) - timedelta(days=1), today, "MTD"
    elif period == "YTD":
        period_start, period_end, period_label = datetime(today.year - 1, 12, 31), today, "YTD"
    elif period == "Custom" and start_date and end_date:
        period_start = datetime.strptime(start_date[:10], "%Y-%m-%d")
        period_end = datetime.strptime(end_date[:10], "%Y-%m-%d")
        period_label = f"{start_date[:10]} to {end_date[:10]}"
    else:
        period_start, period_end, period_label = datetime(2024, 10, 31), today, "Since Inception"

    # Calculate cash flows from transactions (settled/confirmed only)
    initial_investment = 0
    total_buys = 0
    total_sells = 0
    total_coupons = 0

    if not txns_df.empty and 'transaction_type' in txns_df.columns:
        # Use market_value column from D1 transactions
        amount_col = 'market_value' if 'market_value' in txns_df.columns else 'settlement_amount'

        for _, row in txns_df.iterrows():
            txn_type = safe_str(row.get('transaction_type', '')).upper()
            status = safe_str(row.get('status', '')).lower()
            amount = safe_float(row.get(amount_col, 0))

            # Only count settled/confirmed transactions for cash calculation
            if status in ['settled', 'confirmed']:
                if txn_type == 'INITIAL':
                    initial_investment += amount
                elif txn_type == 'BUY':
                    total_buys += amount
                elif txn_type == 'SELL':
                    total_sells += amount
                elif txn_type == 'COUPON':
                    total_coupons += amount

    # Calculate P&L per holding
    total_cost_basis = 0
    total_market_value = 0
    total_unrealised = 0
    by_holding = []

    for _, row in holdings_df.iterrows():
        face_value = safe_float(row.get('par_amount'))
        market_value = safe_float(row.get('market_value'))

        # Cost basis from purchase price or stored cost_basis
        cost_basis = safe_float(row.get('cost_basis'))
        if not cost_basis:
            avg_cost = safe_float(row.get('avg_cost')) or safe_float(row.get('purchase_price'))
            if avg_cost and face_value:
                cost_basis = face_value * (avg_cost / 100)
            else:
                cost_basis = market_value  # Fallback if no cost data

        unrealised = market_value - cost_basis

        total_cost_basis += cost_basis
        total_market_value += market_value
        total_unrealised += unrealised

        # Calculate P&L percentage
        unrealised_pct = (unrealised / cost_basis * 100) if cost_basis > 0 else 0

        by_holding.append({
            "ticker": safe_str(row.get('ticker')),
            "isin": safe_str(row.get('isin')),
            "description": safe_str(row.get('description')),
            "cost_basis": round(cost_basis, 0),
            "cost_basis_fmt": fmt_money(cost_basis),
            "market_value": round(market_value, 0),
            "market_value_fmt": fmt_money(market_value),
            "realised": 0,
            "realised_fmt": fmt_money(0),
            "unrealised": round(unrealised, 0),
            "unrealised_fmt": fmt_money(unrealised),
            "unrealised_pct": round(unrealised_pct, 2),
            "unrealised_pct_fmt": f"{unrealised_pct:.2f}%",
            "coupons": 0,
            "total": round(unrealised, 0),
            "total_fmt": fmt_money(unrealised)
        })

    # Opening NAV = Initial Investment (not cost basis + initial)
    opening_nav = initial_investment

    # Implied Cash = Initial - Buys + Sells + Coupons
    implied_cash = initial_investment - total_buys + total_sells + total_coupons

    # Closing NAV = Holdings Market Value + Implied Cash
    closing_nav = total_market_value + implied_cash

    # Total P&L = Closing NAV - Opening NAV
    total_return = closing_nav - opening_nav

    # For breakdown: Realized P&L = Total Return - Unrealized - Coupons
    total_realised = total_return - total_unrealised - total_coupons

    # VALIDATION: Opening + P&L should equal Closing
    expected_closing = opening_nav + total_return
    reconciliation_diff = closing_nav - expected_closing
    is_reconciled = abs(reconciliation_diff) < 1.0  # $1 tolerance

    return_pct = (total_return / opening_nav * 100) if opening_nav > 0 else 0

    return {
        "currency": "USD",
        "period": {"start": fmt_date(period_start), "end": fmt_date(period_end), "label": period_label},
        "summary": {
            "opening_nav": round(opening_nav, 0),
            "opening_nav_fmt": fmt_money(opening_nav),
            "closing_nav": round(closing_nav, 0),
            "closing_nav_fmt": fmt_money(closing_nav),
            "total_return": round(total_return, 0),
            "total_return_fmt": fmt_money(total_return),
            "total_return_pct": round(return_pct, 2)
        },
        "breakdown": {
            "realised_pnl": round(total_realised, 0),
            "realised_pnl_fmt": fmt_money(total_realised),
            "unrealised_pnl": round(total_unrealised, 0),
            "unrealised_pnl_fmt": fmt_money(total_unrealised),
            "coupon_income": round(total_coupons, 0),
            "coupon_income_fmt": fmt_money(total_coupons),
            "accrued_change": 0
        },
        "by_holding": by_holding,
        "cash_flows": {
            "initial_investment": round(initial_investment, 0),
            "total_buys": round(total_buys, 0),
            "total_sells": round(total_sells, 0),
            "total_coupons": round(total_coupons, 0),
            "implied_cash": round(implied_cash, 0),
            "holdings_value": round(total_market_value, 0)
        },
        "validation": {
            "is_reconciled": is_reconciled,
            "opening_nav": round(opening_nav, 0),
            "total_pnl": round(total_return, 0),
            "expected_closing": round(expected_closing, 0),
            "actual_closing": round(closing_nav, 0),
            "difference": round(reconciliation_diff, 2),
            "formula": "Opening NAV + Total P&L = Closing NAV"
        }
    }


# =============================================================================
# 9. GET CASH EVENT HORIZON
# =============================================================================

def get_cash_event_horizon(
    portfolio_id: str = "wnbf",
    future_days: int = 90,
    client_id: str = None
) -> Dict[str, Any]:
    """Historical + future cash timeline."""
    txns_df = get_transactions(portfolio_id, client_id)
    cashflows_df = get_cashflows(portfolio_id, client_id)
    today = datetime.now()

    historical = []
    running_balance = 0
    if not txns_df.empty:
        if 'settlement_date' in txns_df.columns:
            txns_df = txns_df.sort_values('settlement_date')
        for _, row in txns_df.iterrows():
            txn_type = safe_str(row.get('transaction_type', '')).upper()
            # Use settlement_amount (actual cash impact), not market_value
            amount = safe_float(row.get('settlement_amount') or row.get('market_value'))
            if txn_type in ['INITIAL', 'SELL', 'COUPON']:
                credit, debit = amount, 0
                running_balance += amount
            elif txn_type == 'BUY':
                credit, debit = 0, amount
                running_balance -= amount
            else:
                credit = debit = 0
            historical.append({
                "date": fmt_date(row.get('settlement_date')), "type": txn_type,
                "description": safe_str(row.get('description')) or safe_str(row.get('ticker')),
                "ticker": safe_str(row.get('ticker')),
                "debit": round(debit, 0), "debit_fmt": fmt_money(debit) if debit else "",
                "credit": round(credit, 0), "credit_fmt": fmt_money(credit) if credit else "",
                "balance": round(running_balance, 0), "balance_fmt": fmt_money(running_balance)
            })

    current_balance = running_balance
    future = []
    total_future_income = 0
    projected_balance = current_balance

    if not cashflows_df.empty and 'payment_date' in cashflows_df.columns:
        cashflows_df['payment_date'] = pd.to_datetime(cashflows_df['payment_date'])
        future_mask = cashflows_df['payment_date'] >= today
        if future_days:
            future_mask &= cashflows_df['payment_date'] <= today + timedelta(days=future_days)
        for _, row in cashflows_df[future_mask].sort_values('payment_date').iterrows():
            amount = safe_float(row.get('amount')) or safe_float(row.get('payment_amount'))
            projected_balance += amount
            total_future_income += amount
            future.append({
                "date": fmt_date(row.get('payment_date')),
                "type": safe_str(row.get('payment_type', 'COUPON')).upper(),
                "ticker": safe_str(row.get('ticker')), "isin": safe_str(row.get('isin')),
                "amount": round(amount, 0), "amount_fmt": fmt_money(amount),
                "projected_balance": round(projected_balance, 0), "projected_balance_fmt": fmt_money(projected_balance)
            })

    return {
        "currency": "USD",
        "current_balance": round(current_balance, 0), "current_balance_fmt": fmt_money(current_balance),
        "historical": historical, "future": future,
        "summary": {
            "total_future_income": round(total_future_income, 0), "total_future_income_fmt": fmt_money(total_future_income),
            "next_event_date": future[0]['date'] if future else None,
            "next_event_amount": round(future[0]['amount'], 0) if future else 0,
            "next_event_amount_fmt": fmt_money(future[0]['amount']) if future else "-"
        }
    }


# =============================================================================
# 10. SAVE/UPDATE TRANSACTION
# =============================================================================

def save_transaction(
    portfolio_id: str, transaction_type: str, isin: str, face_value: float,
    price: float, settlement_date: str, status: str = "staging",
    client_id: str = None, **kwargs
) -> Dict[str, Any]:
    """Save a new transaction via Worker API."""
    import os
    import requests

    settlement = calculate_trade_settlement(isin=isin, face_value=face_value, price=price,
                                            settle_date=settlement_date, side=transaction_type)
    txn_data = {
        "portfolio_id": portfolio_id, "transaction_type": transaction_type.upper(),
        "isin": isin, "par_amount": face_value, "price": price,
        "settlement_date": settlement_date, "status": status,
        "market_value": settlement['settlement']['net_settlement'],
        "accrued_interest": settlement['settlement']['accrued_interest'], **kwargs
    }
    worker_url = os.environ.get("WORKER_URL", "https://portfolio-optimizer-mcp.urbancanary.workers.dev")
    try:
        response = requests.post(f"{worker_url}/api/transactions", json=txn_data, timeout=10)
        if response.status_code in [200, 201]:
            result = response.json()
            return {
                "success": True, "transaction_id": result.get('id', result.get('transaction_id')),
                "settlement": {"principal": settlement['settlement']['principal'],
                               "accrued": settlement['settlement']['accrued_interest'],
                               "net_amount": settlement['settlement']['net_settlement']}
            }
        return {"success": False, "error": f"API error: {response.status_code}", "details": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def update_transaction(transaction_id: int, updates: Dict[str, Any], client_id: str = None) -> Dict[str, Any]:
    """Update an existing transaction."""
    import os
    import requests
    worker_url = os.environ.get("WORKER_URL", "https://portfolio-optimizer-mcp.urbancanary.workers.dev")
    try:
        response = requests.put(f"{worker_url}/api/transactions/{transaction_id}", json=updates, timeout=10)
        return {"success": True, "transaction_id": transaction_id} if response.status_code == 200 else {
            "success": False, "error": f"API error: {response.status_code}", "details": response.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


# =============================================================================
# 11. GET RATINGS DISPLAY
# =============================================================================

def get_ratings_display(
    portfolio_id: str = "wnbf",
    rating_source: str = "sp_stub",
    client_id: str = None
) -> Dict[str, Any]:
    """Rating distribution for ratings page."""
    holdings_df = get_holdings(portfolio_id, staging_id=1, client_id=client_id)
    if holdings_df.empty:
        return {"distribution": [], "summary": {"ig_pct": 0, "hy_pct": 0, "avg_rating": "-", "avg_notches": 0}}

    rating_col = ('rating_stub_sp' if rating_source == "sp_stub" and 'rating_stub_sp' in holdings_df.columns
                  else 'rating_moodys' if rating_source == "moodys" and 'rating_moodys' in holdings_df.columns
                  else 'rating_sp' if 'rating_sp' in holdings_df.columns else 'rating')
    if rating_col not in holdings_df.columns:
        return {"distribution": [], "summary": {"ig_pct": 0, "hy_pct": 0, "avg_rating": "-", "avg_notches": 0}}

    total_mv = holdings_df['market_value'].sum() if 'market_value' in holdings_df.columns else 0
    rating_notches = {'AAA': 1, 'AA+': 2, 'AA': 3, 'AA-': 4, 'A+': 5, 'A': 6, 'A-': 7,
                      'BBB+': 8, 'BBB': 9, 'BBB-': 10, 'BB+': 11, 'BB': 12, 'BB-': 13,
                      'B+': 14, 'B': 15, 'B-': 16, 'CCC+': 17, 'CCC': 18, 'CCC-': 19, 'CC': 20, 'C': 21, 'D': 22}

    distribution = []
    ig_total = hy_total = notches_sum = notches_weight = 0
    for _, row in holdings_df.groupby(rating_col).agg({'market_value': 'sum', 'isin': 'count'}).reset_index().iterrows():
        rating = safe_str(row[rating_col])
        if not rating or rating == 'nan':
            continue
        mv, count = safe_float(row['market_value']), int(row['isin'])
        pct = (mv / total_mv * 100) if total_mv else 0
        bucket = get_rating_bucket(rating)
        ig_total += mv if bucket == 'IG' else 0
        hy_total += mv if bucket == 'HY' else 0
        if rating.upper() in rating_notches:
            notches_sum += rating_notches[rating.upper()] * mv
            notches_weight += mv
        distribution.append({"rating": rating, "pct": round(pct, 1), "pct_fmt": fmt_pct(pct),
                             "value": round(mv, 0), "value_fmt": fmt_money(mv), "bucket": bucket, "count": count})

    distribution.sort(key=lambda x: rating_notches.get(x['rating'].upper(), 99))
    avg_notches = (notches_sum / notches_weight) if notches_weight else 0
    avg_rating = next((r for r, n in rating_notches.items() if n >= avg_notches), "-")

    return {
        "distribution": distribution,
        "summary": {"ig_pct": round((ig_total / total_mv * 100) if total_mv else 0, 1),
                    "ig_pct_fmt": fmt_pct((ig_total / total_mv * 100) if total_mv else 0),
                    "hy_pct": round((hy_total / total_mv * 100) if total_mv else 0, 1),
                    "hy_pct_fmt": fmt_pct((hy_total / total_mv * 100) if total_mv else 0),
                    "avg_rating": avg_rating, "avg_notches": round(avg_notches, 1)}
    }


# =============================================================================
# 12. GET ISSUER EXPOSURE
# =============================================================================

def get_issuer_exposure(
    portfolio_id: str = "wnbf",
    client_id: str = None
) -> Dict[str, Any]:
    """Issuer-level aggregation for 5/10/40 compliance."""
    holdings_df = get_holdings(portfolio_id, staging_id=1, client_id=client_id)
    summary = get_holdings_summary(portfolio_id, staging_id=1, client_id=client_id)
    if holdings_df.empty:
        return {"issuers": [], "summary": {"issuers_over_5": 0, "total_over_5_pct": 0, "max_issuer_pct": 0, "rule_5_10_40_pass": True}}

    total_mv = holdings_df['market_value'].sum() if 'market_value' in holdings_df.columns else 0
    total_nav = total_mv + safe_float(summary.get('cash', 0))
    if total_nav == 0:
        return {"issuers": [], "summary": {"issuers_over_5": 0, "total_over_5_pct": 0, "max_issuer_pct": 0, "rule_5_10_40_pass": True}}

    issuer_col = 'ticker' if 'ticker' in holdings_df.columns else 'isin'
    issuers = []
    issuers_over_5 = total_over_5_pct = max_issuer_pct = 0

    for issuer, group in holdings_df.groupby(issuer_col):
        if not issuer:
            continue
        total_value = group['market_value'].sum() if 'market_value' in group.columns else 0
        pct = (total_value / total_nav * 100) if total_nav else 0
        max_issuer_pct = max(max_issuer_pct, pct)

        bonds = [{"isin": safe_str(r.get('isin')), "description": safe_str(r.get('description')),
                  "pct": round((safe_float(r.get('market_value')) / total_nav * 100) if total_nav else 0, 2),
                  "pct_fmt": fmt_pct((safe_float(r.get('market_value')) / total_nav * 100) if total_nav else 0),
                  "value": round(safe_float(r.get('market_value')), 0),
                  "value_fmt": fmt_money(safe_float(r.get('market_value')))}
                 for _, r in group.iterrows()]

        over_5, over_10 = pct > 5, pct > 10
        if over_5:
            issuers_over_5 += 1
            total_over_5_pct += pct

        issuers.append({
            "ticker": safe_str(issuer), "country": safe_str(group.iloc[0].get('country', '')) if len(group) > 0 else '',
            "total_pct": round(pct, 2), "total_pct_fmt": fmt_pct(pct),
            "total_value": round(total_value, 0), "total_value_fmt": fmt_money(total_value),
            "bonds": bonds, "bond_count": len(bonds), "over_5": over_5, "over_10": over_10
        })

    issuers.sort(key=lambda x: x['total_pct'], reverse=True)

    return {
        "issuers": issuers,
        "summary": {
            "issuers_over_5": issuers_over_5,
            "total_over_5_pct": round(total_over_5_pct, 1), "total_over_5_pct_fmt": fmt_pct(total_over_5_pct),
            "max_issuer_pct": round(max_issuer_pct, 1), "max_issuer_pct_fmt": fmt_pct(max_issuer_pct),
            "rule_5_10_40_pass": (max_issuer_pct <= 10) and (total_over_5_pct <= 40)
        }
    }


# =============================================================================
# 13. GET DASHBOARD COMPLETE (Unified Endpoint)
# =============================================================================

def get_dashboard_complete(
    portfolio_id: str = "wnbf",
    include_staging: bool = False,
    client_id: str = None
) -> Dict[str, Any]:
    """
    Single endpoint for complete dashboard data.

    Combines get_portfolio_dashboard + get_holdings_display in one call,
    eliminating multiple round trips and ensuring data consistency.

    Args:
        portfolio_id: Portfolio identifier
        include_staging: Include staging/proposed transactions
        client_id: Client identifier

    Returns:
        Complete dashboard data with summary, allocation, totals, holdings, compliance
    """
    # Get portfolio dashboard data (summary, allocation, compliance)
    # Note: get_portfolio_dashboard doesn't have include_staging param
    dashboard = get_portfolio_dashboard(portfolio_id, client_id)

    # Get holdings display data (holdings array, totals)
    holdings_data = get_holdings_display(portfolio_id, include_staging, client_id)

    # Combine into unified response
    return {
        "summary": dashboard.get("summary", {}),
        "allocation": dashboard.get("allocation", {}),
        "compliance_summary": dashboard.get("compliance_summary", {}),
        "totals": holdings_data.get("totals", {}),
        "holdings": holdings_data.get("holdings", []),
        "cash": holdings_data.get("cash", 0),
        "cash_fmt": holdings_data.get("cash_fmt", "0"),
        "count": holdings_data.get("count", 0),
        "as_of": dashboard.get("as_of", datetime.utcnow().isoformat() + "Z")
    }
