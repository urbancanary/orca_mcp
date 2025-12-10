"""
Compliance checking for UCITS 5/10/40 and portfolio rules.

This module provides stateless compliance calculations that can be used by:
- Streamlit compliance page
- Agents via James MCP
- Any other client needing compliance checks

Rules:
- Hard (must comply):
  - Max Single Issuer: 10% (5/10/40 rule)
  - Sum of Issuers >5%: 40% (5/10/40 rule)
  - NFA 3*+ Countries: 100% (all holdings must be in 3*+ rated countries)
  - Cash not overdrawn: >= 0

- Soft (targets):
  - Cash Level: 0-5%
  - Max Country: 20%
  - Diversification: 10+ holdings
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import pandas as pd
import json
from pathlib import Path


# Load NFA country ratings from james_mcp data
def _load_nfa_ratings() -> Dict[str, int]:
    """Load NFA country ratings from JSON file."""
    # Try multiple paths for flexibility
    possible_paths = [
        Path(__file__).parent.parent.parent / "james_mcp" / "data" / "country_eligibility.json",
        Path("/Users/andyseaman/Notebooks/mcp_central/james_mcp/data/country_eligibility.json"),
    ]

    for path in possible_paths:
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                # Extract country -> nfa_rating mapping
                return {
                    country: info.get('nfa_rating', 0)
                    for country, info in data.get('countries', {}).items()
                }

    # Fallback: return empty dict (will treat all countries as unknown)
    return {}


# Global NFA ratings lookup (loaded once)
NFA_RATINGS = _load_nfa_ratings()


@dataclass
class ComplianceRule:
    """A single compliance rule with its evaluation result."""
    rule_type: str  # 'Hard' or 'Soft'
    name: str
    limit: str
    current: str
    status: str  # 'Pass', 'Fail', 'Warning'
    details: str


@dataclass
class ComplianceResult:
    """Complete compliance check result."""
    is_compliant: bool  # True if all hard rules pass
    hard_pass: int
    hard_total: int
    soft_pass: int
    soft_total: int
    rules: List[ComplianceRule]
    metrics: Dict[str, Any]


def check_compliance(
    holdings: pd.DataFrame,
    net_cash: float,
    limits: Optional[Dict[str, float]] = None
) -> ComplianceResult:
    """
    Check portfolio compliance against UCITS 5/10/40 and soft rules.

    Args:
        holdings: DataFrame with columns: ticker, country, market_value
        net_cash: Net cash position (can be negative if overdrawn)
        limits: Optional custom limits (defaults below if not provided)
            - max_single_issuer: 10.0
            - max_sum_over_5: 40.0
            - max_cash_pct: 5.0
            - max_country_pct: 20.0
            - min_holdings: 10

    Returns:
        ComplianceResult with all rules evaluated and summary
    """
    # Default limits
    limits = limits or {}
    max_single_issuer = limits.get('max_single_issuer', 10.0)
    max_sum_over_5 = limits.get('max_sum_over_5', 40.0)
    max_cash_pct = limits.get('max_cash_pct', 5.0)
    max_country_pct = limits.get('max_country_pct', 20.0)
    min_holdings = limits.get('min_holdings', 10)

    # Calculate total NAV
    total_nav = holdings['market_value'].sum() + net_cash

    if total_nav <= 0:
        # Edge case: no NAV
        return ComplianceResult(
            is_compliant=False,
            hard_pass=0,
            hard_total=4,
            soft_pass=0,
            soft_total=3,
            rules=[],
            metrics={'total_nav': 0, 'error': 'No NAV'}
        )

    # Calculate weights
    holdings = holdings.copy()
    holdings['pct_nav'] = (holdings['market_value'] / total_nav * 100)

    # === ISSUER CONCENTRATION (5/10/40) ===
    issuer_weights = holdings.groupby('ticker')['pct_nav'].sum()
    max_position = issuer_weights.max()
    max_position_ticker = issuer_weights.idxmax()

    # Sum of issuers over 5%
    issuers_over_5 = issuer_weights[issuer_weights > 5]
    sum_over_5_pct = issuers_over_5.sum()
    num_issuers_over_5 = len(issuers_over_5)

    # === CASH ===
    is_overdrawn = net_cash < 0
    cash_pct = (net_cash / total_nav * 100)

    # === COUNTRY CONCENTRATION ===
    country_totals = holdings.groupby('country')['market_value'].sum()
    country_pcts = (country_totals / total_nav * 100)
    max_country_pct_actual = country_pcts.max()
    max_country = country_pcts.idxmax()

    # === DIVERSIFICATION ===
    num_holdings = len(holdings)
    avg_position = holdings['pct_nav'].mean()

    # === NFA COUNTRY ELIGIBILITY (3*+ required) ===
    # Check each holding's country against NFA ratings
    nfa_violations = []
    if 'country' in holdings.columns:
        for _, row in holdings.iterrows():
            country = row.get('country', 'Unknown')
            nfa_rating = NFA_RATINGS.get(country, 0)
            if nfa_rating < 3:
                nfa_violations.append({
                    'ticker': row.get('ticker', 'Unknown'),
                    'isin': row.get('isin', ''),
                    'description': row.get('description', ''),
                    'country': country,
                    'nfa_rating': nfa_rating,
                    'pct_nav': row.get('pct_nav', 0),
                    'market_value': row.get('market_value', 0),
                    'par_amount': row.get('par_amount', 0)
                })

    nfa_violation_pct = sum(v['pct_nav'] for v in nfa_violations)
    nfa_compliant = len(nfa_violations) == 0

    # === BUILD VIOLATION DETAILS FOR 5/10/40 ===
    # List issuers over 5% for the expander - include all bonds for that issuer
    issuers_over_5_details = []
    for ticker, pct in issuers_over_5.items():
        # Get all holdings for this issuer
        issuer_holdings = holdings[holdings['ticker'] == ticker]
        issuer_country = issuer_holdings['country'].iloc[0] if len(issuer_holdings) > 0 else 'Unknown'
        issuer_total_mv = issuer_holdings['market_value'].sum()

        # List of bonds for this issuer
        bonds = []
        for _, row in issuer_holdings.iterrows():
            bonds.append({
                'isin': row['isin'] if 'isin' in row.index else '',
                'description': row['description'] if 'description' in row.index else '',
                'market_value': row['market_value'] if 'market_value' in row.index else 0,
                'par_amount': row['par_amount'] if 'par_amount' in row.index else 0,
                'pct_nav': row['pct_nav'] if 'pct_nav' in row.index else 0  # Individual holding weight
            })

        issuers_over_5_details.append({
            'ticker': ticker,
            'pct_nav': pct,
            'country': issuer_country,
            'total_market_value': issuer_total_mv,
            'bonds': bonds
        })
    # Sort by weight descending
    issuers_over_5_details = sorted(issuers_over_5_details, key=lambda x: x['pct_nav'], reverse=True)

    # Build rules list
    rules = []

    # Hard Rule 1: Max Single Issuer
    rules.append(ComplianceRule(
        rule_type='Hard',
        name='Max Single Issuer (5/10/40)',
        limit=f'{max_single_issuer}%',
        current=f'{max_position:.1f}%',
        status='Pass' if max_position <= max_single_issuer else 'Fail',
        details=str(max_position_ticker)
    ))

    # Hard Rule 2: Sum of Issuers >5%
    rules.append(ComplianceRule(
        rule_type='Hard',
        name='Sum Issuers >5% (5/10/40)',
        limit=f'{max_sum_over_5}%',
        current=f'{sum_over_5_pct:.1f}%',
        status='Pass' if sum_over_5_pct <= max_sum_over_5 else 'Fail',
        details=f'{num_issuers_over_5} issuers'
    ))

    # Hard Rule 3: Cash Overdrawn
    rules.append(ComplianceRule(
        rule_type='Hard',
        name='Cash Overdrawn',
        limit='>= $0',
        current=f'${net_cash:,.0f}',
        status='Fail' if is_overdrawn else 'Pass',
        details='OVERDRAWN!' if is_overdrawn else 'OK'
    ))

    # Hard Rule 4: NFA 3*+ Countries
    rules.append(ComplianceRule(
        rule_type='Hard',
        name='NFA 3*+ Countries',
        limit='100%',
        current=f'{100 - nfa_violation_pct:.1f}%',
        status='Pass' if nfa_compliant else 'Fail',
        details=f'{len(nfa_violations)} ineligible' if nfa_violations else 'All eligible'
    ))

    # Soft Rule 1: Cash Level
    cash_in_range = 0 <= cash_pct <= max_cash_pct
    rules.append(ComplianceRule(
        rule_type='Soft',
        name='Cash Level',
        limit=f'0-{max_cash_pct}%',
        current=f'{cash_pct:.1f}%',
        status='Pass' if cash_in_range else 'Warning',
        details=f'${net_cash:,.0f}'
    ))

    # Soft Rule 2: Max Country
    rules.append(ComplianceRule(
        rule_type='Soft',
        name='Max Country',
        limit=f'{max_country_pct}%',
        current=f'{max_country_pct_actual:.1f}%',
        status='Pass' if max_country_pct_actual <= max_country_pct else 'Warning',
        details=str(max_country)
    ))

    # Soft Rule 3: Diversification
    rules.append(ComplianceRule(
        rule_type='Soft',
        name='Diversification',
        limit=f'{min_holdings}+',
        current=str(num_holdings),
        status='Pass' if num_holdings >= min_holdings else 'Warning',
        details=f'{num_holdings} bonds'
    ))

    # Count pass/fail
    hard_rules = [r for r in rules if r.rule_type == 'Hard']
    soft_rules = [r for r in rules if r.rule_type == 'Soft']

    hard_pass = len([r for r in hard_rules if r.status == 'Pass'])
    hard_total = len(hard_rules)
    soft_pass = len([r for r in soft_rules if r.status == 'Pass'])
    soft_total = len(soft_rules)

    is_compliant = hard_pass == hard_total

    # Build metrics dict
    metrics = {
        'total_nav': total_nav,
        'net_cash': net_cash,
        'cash_pct': cash_pct,
        'num_holdings': num_holdings,
        'avg_position': avg_position,
        'max_position': max_position,
        'max_position_ticker': max_position_ticker,
        'sum_over_5_pct': sum_over_5_pct,
        'num_issuers_over_5': num_issuers_over_5,
        'max_country_pct': max_country_pct_actual,
        'max_country': max_country,
        'country_breakdown': country_pcts.to_dict(),
        'issuer_weights': issuer_weights.to_dict(),
        # Violation details for expanders
        'issuers_over_5_details': issuers_over_5_details,  # List of {ticker, pct_nav, country}
        'nfa_violations': nfa_violations,  # List of {ticker, country, nfa_rating, pct_nav, market_value}
        'nfa_violation_pct': nfa_violation_pct,
    }

    return ComplianceResult(
        is_compliant=is_compliant,
        hard_pass=hard_pass,
        hard_total=hard_total,
        soft_pass=soft_pass,
        soft_total=soft_total,
        rules=rules,
        metrics=metrics
    )


def compliance_to_dict(result: ComplianceResult) -> Dict[str, Any]:
    """Convert ComplianceResult to a dictionary for JSON serialization."""
    return {
        'is_compliant': result.is_compliant,
        'hard_pass': result.hard_pass,
        'hard_total': result.hard_total,
        'soft_pass': result.soft_pass,
        'soft_total': result.soft_total,
        'rules': [
            {
                'type': r.rule_type,
                'name': r.name,
                'limit': r.limit,
                'current': r.current,
                'status': r.status,
                'details': r.details
            }
            for r in result.rules
        ],
        'metrics': result.metrics
    }


def check_compliance_impact(
    holdings: pd.DataFrame,
    net_cash: float,
    proposed_trade: Dict[str, Any],
    limits: Optional[Dict[str, float]] = None
) -> Dict[str, Any]:
    """
    Check how a proposed trade would impact compliance.

    Args:
        holdings: Current holdings DataFrame
        net_cash: Current net cash
        proposed_trade: Dict with keys:
            - ticker: str
            - country: str
            - action: 'buy' or 'sell'
            - market_value: float (positive)
        limits: Optional custom limits

    Returns:
        Dict with 'before', 'after' compliance results and 'impact' summary
    """
    # Check current compliance
    before = check_compliance(holdings, net_cash, limits)

    # Apply proposed trade
    new_holdings = holdings.copy()
    new_cash = net_cash

    trade_ticker = proposed_trade['ticker']
    trade_country = proposed_trade['country']
    trade_value = proposed_trade['market_value']
    action = proposed_trade['action']

    if action == 'buy':
        # Add to holdings, reduce cash
        new_cash -= trade_value
        existing = new_holdings[new_holdings['ticker'] == trade_ticker]
        if len(existing) > 0:
            idx = existing.index[0]
            new_holdings.loc[idx, 'market_value'] += trade_value
        else:
            new_row = pd.DataFrame([{
                'ticker': trade_ticker,
                'country': trade_country,
                'market_value': trade_value
            }])
            new_holdings = pd.concat([new_holdings, new_row], ignore_index=True)

    elif action == 'sell':
        # Remove from holdings, increase cash
        new_cash += trade_value
        existing = new_holdings[new_holdings['ticker'] == trade_ticker]
        if len(existing) > 0:
            idx = existing.index[0]
            new_holdings.loc[idx, 'market_value'] -= trade_value
            # Remove if fully sold
            if new_holdings.loc[idx, 'market_value'] <= 0:
                new_holdings = new_holdings.drop(idx)

    # Check new compliance
    after = check_compliance(new_holdings, new_cash, limits)

    # Determine impact
    impact = {
        'compliance_change': 'improved' if (after.is_compliant and not before.is_compliant)
                           else 'worsened' if (not after.is_compliant and before.is_compliant)
                           else 'unchanged',
        'hard_rules_change': after.hard_pass - before.hard_pass,
        'soft_rules_change': after.soft_pass - before.soft_pass,
        'would_breach': not after.is_compliant and before.is_compliant,
        'would_fix': after.is_compliant and not before.is_compliant,
    }

    return {
        'before': compliance_to_dict(before),
        'after': compliance_to_dict(after),
        'impact': impact
    }
