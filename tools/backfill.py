"""
Backfill Tools for Orca MCP

SAFETY: This module ALWAYS fetches bonds from D1 watchlist.
There is NO way to override the bond universe - this prevents accidental quota waste.
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd

from ..client_config import get_client_config
from .cloudflare_d1 import get_watchlist
from .data_access import setup_bigquery_credentials, query_bigquery
from .cache_manager import invalidate_cache
from google.cloud import bigquery

# Add auth_mcp to path
AUTH_MCP_PATH = Path(__file__).parent.parent.parent / "auth_mcp"
if str(AUTH_MCP_PATH) not in sys.path:
    sys.path.insert(0, str(AUTH_MCP_PATH))

from auth_client import get_api_key

# GA10 Configuration - URLs from environment variables
import os
GA10_API_URL = os.getenv('GA10_GATEWAY_URL', 'https://ga10-gateway.example.com')
GA10_API_KEY = get_api_key("GA10_API_KEY", fallback_env=True, requester="orca_backfill")

# RVM Configuration
RVM_DATA_URL = os.getenv('GA10_RVM_DATA_URL', 'https://ga10-rvm-data.example.com')


def _fetch_ga10_analytics(description: str, price: float, settlement_date: Optional[str] = None) -> Optional[Dict]:
    """
    Fetch bond analytics from GA10 V2 API

    Args:
        description: Bond description (e.g., "MUBAUH 5 ½ 04/28/33")
        price: Bond price
        settlement_date: Settlement date (default: today)

    Returns:
        Dict with analytics: {ytw, duration, spread, accrued, ...}
    """
    try:
        payload = {
            'description': description,
            'price': float(price),
            'settlement_date': settlement_date or datetime.now().strftime('%Y-%m-%d')
        }

        cmd = [
            'curl', '-s',
            f'{GA10_API_URL}/api/v2/bond/analysis',
            '-H', 'Content-Type: application/json',
            '-H', f'X-API-Key: {GA10_API_KEY}',
            '-d', json.dumps(payload)
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0 and result.stdout:
            data = json.loads(result.stdout)
            if data.get('analytics'):
                return data.get('analytics')
            elif data.get('success') and data.get('data'):
                return data.get('data')

        return None
    except Exception as e:
        print(f"   ❌ GA10 error: {e}")
        return None


def _fetch_historical_prices_cbonds(isin: str, days: int, client_id: str = 'guinness') -> List[dict]:
    """
    Fetch historical prices from CBonds MCP (authenticated)

    Args:
        isin: Bond ISIN
        days: Number of days to fetch (max 40)
        client_id: Client identifier for authentication

    Returns:
        List of dicts: [{'date': '2025-11-20', 'price': 98.5}, ...]
    """
    import urllib.request
    import urllib.error

    # Get auth token for CBonds access
    config = get_client_config(client_id)
    auth_token = config.get_auth_token('auth_mcp')

    prices = []
    cbonds_url = os.getenv('CBONDS_MCP_URL', 'https://cbonds-mcp.example.com') + "/cbonds/prices"

    today = datetime.now().date()
    dates_to_query = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]

    for settlement_date in dates_to_query:
        try:
            payload = {
                "isin": isin,
                "settlement_date": settlement_date
            }

            req = urllib.request.Request(
                cbonds_url,
                data=json.dumps(payload).encode('utf-8'),
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {auth_token}',
                    'X-Client-ID': client_id
                },
                method='POST'
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())
                if data.get('success') and data.get('price'):
                    prices.append({
                        'date': settlement_date,
                        'price': float(data['price'])
                    })

        except urllib.error.HTTPError as e:
            if e.code != 404:  # Ignore 404s (no price for that date)
                pass
        except Exception:
            pass

    return prices


def _insert_bond_analytics(isin: str, analytics: Dict, bpdate: str, price: float, client_id: str = 'guinness'):
    """
    Insert bond analytics into bond_analytics_daily table

    Args:
        isin: Bond ISIN
        analytics: Dict with calculated analytics from GA10
        bpdate: Business/pricing date
        price: Bond price
        client_id: Client identifier
    """
    setup_bigquery_credentials()
    config = get_client_config(client_id)
    bq_service = config.get_service('bigquery')
    dataset = config.get_bigquery_dataset()

    client = bigquery.Client(project=bq_service['project'])
    table_id = f"{bq_service['project']}.{dataset}.bond_analytics_daily"

    # Extract analytics fields
    ytw = analytics.get('ytw') or analytics.get('yield')
    oad = analytics.get('duration') or analytics.get('oad')
    oas = analytics.get('spread') or analytics.get('oas')
    accrued = analytics.get('accrued') or analytics.get('accrued_interest')
    convexity = analytics.get('convexity')
    dv01 = analytics.get('dv01')

    if not any([ytw, oad, oas]):
        return

    # Get rating_notches from agg_analysis_data
    rating_query = f"SELECT rating_notches FROM agg_analysis_data WHERE isin = '{isin}' LIMIT 1"
    rating_df = query_bigquery(rating_query, client_id)
    rating_notches = rating_df['rating_notches'].iloc[0] if not rating_df.empty else None

    # Calculate derived fields
    rating_notches_rounded = round(rating_notches) if rating_notches else None

    # Prepare row
    row = {
        'isin': isin,
        'price_date': bpdate,
        'price': float(price),
        'accrued_interest': float(accrued) if accrued else None,
        'ytw': float(ytw) if ytw else None,
        'oad': float(oad) if oad else None,
        'oas': float(oas) if oas else None,
        'convexity': float(convexity) if convexity else None,
        'dv01': float(dv01) if dv01 else None,
        'oas_predicted': None,  # Will be filled by RVM model
        'regression_residual': None,  # Will be filled by RVM model
        'return': None,  # Will be filled by RVM model
        'rating_notches': float(rating_notches) if rating_notches else None,
        'rating_notches_rounded': rating_notches_rounded,
        'oas_zscore': None,
        'oad_zscore': None,
        'ytw_zscore': None,
        'analysis_timestamp': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat()
    }

    # Insert row
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        print(f"   ⚠️  BigQuery insert errors: {errors}")


def backfill_watchlist_analytics(days: int = 30, limit: Optional[int] = None, client_id: str = 'guinness') -> Dict:
    """
    Backfill bond analytics for ALL watchlist bonds

    SAFETY: This function ALWAYS fetches bonds from D1 watchlist.
    There is NO way to override the bond list - prevents quota waste.

    Args:
        days: Number of days to backfill (max 40)
        limit: Optional limit on number of bonds (for testing)
        client_id: Client identifier

    Returns:
        Dict with results: {
            'bonds_processed': int,
            'api_calls': int,
            'records_inserted': int,
            'errors': List[str]
        }
    """
    setup_bigquery_credentials()

    # SAFETY: ALWAYS fetch from D1 watchlist - no overrides
    print(f"🔒 Fetching watchlist from D1 (LOCKED - cannot override)")
    watchlist_df = get_watchlist()

    if watchlist_df.empty:
        return {
            'bonds_processed': 0,
            'api_calls': 0,
            'records_inserted': 0,
            'errors': ['No watchlist bonds found']
        }

    print(f"✅ Watchlist loaded: {len(watchlist_df)} bonds")

    # Apply limit if specified (for testing)
    if limit:
        watchlist_df = watchlist_df.head(limit)
        print(f"⚠️  TESTING MODE: Limited to {limit} bonds")

    # Get ISINs from watchlist
    isins = watchlist_df['isin'].unique().tolist()
    isin_list = "', '".join(isins)

    # Query agg_analysis_data for bond descriptions and prices
    sql = f"""
    SELECT
        isin,
        description,
        price,
        bpdate
    FROM agg_analysis_data
    WHERE isin IN ('{isin_list}')
    AND price IS NOT NULL
    """

    bonds_df = query_bigquery(sql, client_id)
    print(f"📊 Found {len(bonds_df)} bonds with pricing data")

    if bonds_df.empty:
        return {
            'bonds_processed': 0,
            'api_calls': 0,
            'records_inserted': 0,
            'errors': ['No bonds with pricing data found']
        }

    # Backfill each bond
    results = {
        'bonds_processed': 0,
        'api_calls': 0,
        'records_inserted': 0,
        'errors': []
    }

    total_bonds = len(bonds_df)

    for idx, bond in bonds_df.iterrows():
        print(f"\n[{idx+1}/{total_bonds}] {bond['isin']} - {bond['description']}")

        try:
            # Fetch historical prices (authenticated)
            prices = _fetch_historical_prices_cbonds(bond['isin'], days, client_id)
            results['api_calls'] += days  # Each date is an API call attempt

            if not prices:
                print(f"   ⚠️  No historical prices available")
                continue

            print(f"   ✅ Found {len(prices)} prices")

            # Process each price
            for price_record in prices:
                analytics = _fetch_ga10_analytics(
                    bond['description'],
                    price_record['price'],
                    price_record['date']
                )

                if analytics:
                    _insert_bond_analytics(
                        bond['isin'],
                        analytics,
                        price_record['date'],
                        price_record['price'],
                        client_id
                    )
                    results['records_inserted'] += 1

            results['bonds_processed'] += 1

        except Exception as e:
            error_msg = f"Error processing {bond['isin']}: {str(e)}"
            print(f"   ❌ {error_msg}")
            results['errors'].append(error_msg)

    # Invalidate cache after backfill
    invalidate_cache(f"query:{client_id}:*")

    print(f"\n{'='*60}")
    print(f"Backfill Complete")
    print(f"{'='*60}")
    print(f"Bonds processed: {results['bonds_processed']}/{total_bonds}")
    print(f"API calls made: {results['api_calls']}")
    print(f"Records inserted: {results['records_inserted']}")
    print(f"Errors: {len(results['errors'])}")

    return results


def _fetch_rvm_model() -> Optional[Dict]:
    """
    Fetch latest RVM model coefficients

    Returns:
        Dict with model coefficients: {intercept, oad, rating_num, r_squared, bpdate}
    """
    import urllib.request

    try:
        req = urllib.request.Request(
            f'{RVM_DATA_URL}/query/agg_model_coeffs?limit=1',
            headers={'Content-Type': 'application/json'}
        )

        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())

        if data.get('success') and data.get('data'):
            model = data['data'][0]
            return {
                'intercept': float(model['intercept']),
                'oad_coeff': float(model['oad']),
                'rating_coeff': float(model['rating_num']),
                'r_squared': float(model['r_squared']),
                'bpdate': model['bpdate']
            }

        return None
    except Exception as e:
        print(f"   ⚠️  RVM model fetch error: {e}")
        return None


def _fetch_bond_rating(isin: str, client_id: str = 'guinness') -> float:
    """
    Fetch bond rating_numeric from BigQuery agg_analysis_data

    Args:
        isin: Bond ISIN
        client_id: Client identifier

    Returns:
        rating_numeric value (default 0 if not found)
    """
    from .data_access import query_bigquery

    try:
        # Query BigQuery agg_analysis_data for rating
        query = f"""
        SELECT rating_numeric
        FROM agg_analysis_data
        WHERE isin = '{isin}'
        LIMIT 1
        """

        df = query_bigquery(query, client_id)

        if not df.empty and 'rating_numeric' in df.columns:
            rating_num = df['rating_numeric'].iloc[0]
            return float(rating_num) if rating_num else 0.0

        return 0.0  # Default rating if not found
    except Exception as e:
        print(f"    ⚠️  Rating fetch error: {e}")
        return 0.0  # Default rating on error


def _calculate_rvm_analytics(oas: float, oad: float, ytw: float, rating_num: float, model: Dict) -> Dict:
    """
    Calculate RVM analytics (expected return, notches, predicted spread)

    Args:
        oas: Option-adjusted spread (bps)
        oad: Option-adjusted duration (years)
        ytw: Yield to worst (%)
        rating_num: Rating numeric value
        model: RVM model coefficients

    Returns:
        Dict with: {oas_predicted, rating_notches, expected_return}
    """
    import math

    try:
        # 1. Calculate predicted OAS (what spread should bond have given its rating)
        # Formula: ln(OAS) = intercept + (ln_duration_coeff × ln(OAD)) + (rating_coeff × rating_numeric)
        ln_oas_pred = (
            model['intercept'] +
            (model['oad_coeff'] * math.log(oad)) +
            (model['rating_coeff'] * rating_num)
        )
        oas_predicted = math.exp(ln_oas_pred)

        # 2. Calculate implied rating (what rating would justify current spread)
        # Rearrange: rating_num = [ln(OAS) - intercept - (ln_duration_coeff × ln(OAD))] / rating_coeff
        implied_rating = (
            (math.log(oas) - model['intercept'] - (model['oad_coeff'] * math.log(oad)))
            / model['rating_coeff']
        )

        # 3. Calculate rating notches (difference between implied and actual rating)
        # Positive = bond trading wider than rating suggests (undervalued)
        # Negative = bond trading tighter than rating suggests (overvalued)
        rating_notches = implied_rating - rating_num

        # 4. Calculate regression residual (spread difference in bps)
        regression_residual = oas - oas_predicted

        # 5. Calculate expected return
        # Expected return = (regression_residual × OAD) / 100
        expected_return_addon = (regression_residual * oad) / 100
        expected_return = ytw + expected_return_addon

        return {
            'oas_predicted': oas_predicted,
            'rating_notches': rating_notches,
            'expected_return': expected_return,
            'regression_residual': regression_residual,
            'implied_rating': implied_rating
        }
    except Exception as e:
        print(f"   ⚠️  RVM calculation error: {e}")
        return {
            'oas_predicted': 0,
            'rating_notches': 0,
            'expected_return': 0,
            'regression_residual': 0,
            'implied_rating': 0
        }


def refresh_bond_analytics(isins: Optional[List[str]] = None, client_id: str = 'guinness') -> Dict:
    """
    Refresh bond analytics with CURRENT prices (today only, not historical)

    Fetches today's price from CBonds, calculates current analytics with GA10,
    and stores in bond_analytics_daily table (bond_analytics_latest is a view on this).

    Args:
        isins: List of ISINs to refresh. If None, refreshes all watchlist bonds.
        client_id: Client identifier

    Returns:
        Dict with results: {
            'bonds_processed': int,
            'bonds_updated': int,
            'errors': List[str]
        }
    """
    setup_bigquery_credentials()

    # If no ISINs provided, use watchlist
    if isins is None:
        print(f"📋 Fetching watchlist from D1...")
        watchlist_df = get_watchlist(client_id)
        if watchlist_df.empty:
            return {
                'bonds_processed': 0,
                'bonds_updated': 0,
                'errors': ['No watchlist bonds found']
            }
        isins = watchlist_df['isin'].unique().tolist()
        print(f"✅ Found {len(isins)} bonds in watchlist")
    else:
        print(f"📊 Refreshing {len(isins)} specific bonds")

    # Get bond descriptions from agg_analysis_data
    isin_list = "', '".join(isins)
    bond_query = f"""
    SELECT DISTINCT
        isin,
        description
    FROM agg_analysis_data
    WHERE isin IN ('{isin_list}')
    """

    bonds_df = query_bigquery(bond_query, client_id)

    if bonds_df.empty:
        return {
            'bonds_processed': 0,
            'bonds_updated': 0,
            'errors': ['No bond data found in agg_analysis_data']
        }

    print(f"✅ Found {len(bonds_df)} bonds with descriptions")

    # Fetch RVM model
    print(f"📈 Fetching RVM model...")
    rvm_model = _fetch_rvm_model()
    if rvm_model:
        print(f"✅ RVM model loaded (R²={rvm_model['r_squared']:.3f}, date={rvm_model['bpdate']})")
    else:
        print(f"⚠️  RVM model not available - will skip expected return calculation")

    print(f"🔄 Fetching current prices and calculating analytics...")

    errors = []
    bonds_updated = 0
    today = datetime.now().strftime('%Y-%m-%d')

    # BigQuery client for batch insert
    config = get_client_config(client_id)
    bq_service = config.get_service('bigquery')
    dataset = config.get_bigquery_dataset()

    bq_client = bigquery.Client(project=bq_service['project'])
    table_id = f"{bq_service['project']}.{dataset}.bond_analytics_daily"

    rows_to_insert = []

    for idx, bond in bonds_df.iterrows():
        isin = bond['isin']
        description = bond['description']

        print(f"\n  [{idx+1}/{len(bonds_df)}] {isin} - {description}")

        # 1. Fetch current price from CBonds (just today)
        prices = _fetch_historical_prices_cbonds(isin, days=1, client_id=client_id)

        if not prices:
            errors.append(f"{isin}: No price from CBonds")
            print(f"    ❌ No price from CBonds")
            continue

        price_data = prices[0]  # Just today's price
        price = price_data['price']
        price_date = price_data['date']

        print(f"    ✅ Price: {price:.2f} (date: {price_date})")

        # 2. Calculate analytics with GA10
        analytics = _fetch_ga10_analytics(description, price, settlement_date=price_date)

        if not analytics:
            errors.append(f"{isin}: GA10 analytics failed")
            print(f"    ❌ GA10 analytics failed")
            continue

        # Map GA10 field names to database field names
        ytw = analytics.get('ytm') or analytics.get('ytw', 0)
        oad = analytics.get('duration') or analytics.get('oad', 0)
        oas = analytics.get('spread') or analytics.get('oas', 0)
        dv01 = analytics.get('pvbp') or analytics.get('dv01', 0)

        print(f"    ✅ Analytics: ytw={ytw:.2f}%, oad={oad:.2f}y, oas={oas:.0f}bp")

        # 3. Calculate RVM analytics (expected return, notches, etc.)
        oas_predicted = 0
        rating_notches = 0
        expected_return = 0
        regression_residual = 0

        if rvm_model and ytw > 0 and oad > 0 and oas > 0:
            # Fetch bond rating
            rating_num = _fetch_bond_rating(isin, client_id)
            print(f"    📊 Rating: {rating_num:.1f}")

            # Calculate RVM analytics
            rvm_analytics = _calculate_rvm_analytics(oas, oad, ytw, rating_num, rvm_model)
            oas_predicted = rvm_analytics['oas_predicted']
            rating_notches = rvm_analytics['rating_notches']
            expected_return = rvm_analytics['expected_return']
            regression_residual = rvm_analytics['regression_residual']

            print(f"    ✅ RVM: expected_return={expected_return:.2f}%, notches={rating_notches:.1f}bp")

        # 4. Prepare row for BigQuery
        row = {
            'isin': isin,
            'price_date': price_date,
            'price': price,
            'accrued_interest': analytics.get('accrued_interest', 0),
            'ytw': ytw,
            'oad': oad,
            'oas': oas,
            'convexity': analytics.get('convexity', 0),
            'dv01': dv01,
            'oas_predicted': oas_predicted,
            'regression_residual': regression_residual,
            'return': expected_return,
            'rating_notches': rating_notches,
            'analysis_timestamp': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

        rows_to_insert.append(row)
        bonds_updated += 1

    # Batch insert to BigQuery
    if rows_to_insert:
        print(f"\n💾 Inserting {len(rows_to_insert)} records to bond_analytics_daily...")
        insert_errors = bq_client.insert_rows_json(table_id, rows_to_insert)

        if insert_errors:
            print(f"  ❌ Insert errors: {insert_errors}")
            errors.extend([str(e) for e in insert_errors])
        else:
            print(f"  ✅ Successfully inserted {len(rows_to_insert)} records")

    # Invalidate cache (for both daily table and latest view)
    invalidate_cache('bond_analytics_daily')
    invalidate_cache('bond_analytics_latest')

    return {
        'bonds_processed': len(bonds_df),
        'bonds_updated': bonds_updated,
        'errors': errors
    }


def get_backfill_status(client_id: str = 'guinness') -> Dict:
    """
    Get status of backfill for watchlist bonds

    Returns:
        Dict with status: {
            'watchlist_bonds': int,
            'bonds_with_data': int,
            'missing_bonds': int,
            'avg_days_per_bond': float,
            'date_range': {'min': str, 'max': str}
        }
    """
    setup_bigquery_credentials()

    # Get watchlist count
    watchlist_df = get_watchlist()
    watchlist_isins = set(watchlist_df['isin'].unique())

    # Get backfill status from bond_analytics_daily
    query = '''
    SELECT
        isin,
        COUNT(*) as days_count,
        MIN(price_date) as earliest_date,
        MAX(price_date) as latest_date
    FROM bond_analytics_daily
    GROUP BY isin
    '''

    backfill_df = query_bigquery(query, client_id)
    backfilled_isins = set(backfill_df['isin'].unique())

    # Calculate overlap
    watchlist_with_data = watchlist_isins & backfilled_isins
    missing_bonds = watchlist_isins - backfilled_isins

    avg_days = backfill_df['days_count'].mean() if not backfill_df.empty else 0

    return {
        'watchlist_bonds': len(watchlist_isins),
        'bonds_with_data': len(watchlist_with_data),
        'missing_bonds': len(missing_bonds),
        'missing_isins': sorted(list(missing_bonds))[:20],  # First 20
        'avg_days_per_bond': float(avg_days),
        'date_range': {
            'min': str(backfill_df['earliest_date'].min()) if not backfill_df.empty else None,
            'max': str(backfill_df['latest_date'].max()) if not backfill_df.empty else None
        }
    }
