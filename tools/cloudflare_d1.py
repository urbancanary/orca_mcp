"""
Cloudflare D1 Tools for Orca MCP

Handles staging transactions stored in Cloudflare D1 edge database.
Provides fast, globally-distributed storage for hypothetical/sandbox trades.
"""

import json
import urllib.request
import urllib.error
import urllib.parse
from typing import Any, Dict, List, Optional
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path for imports
SCRIPT_DIR = Path(__file__).parent.parent.resolve()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    from ..client_config import get_client_config
except ImportError:
    from client_config import get_client_config


def get_orca_url() -> str:
    """
    Get the Orca MCP API URL.

    This is the single source of truth for the ORCA endpoint URL.
    Client applications should import this function rather than
    hardcoding the URL.

    Returns:
        API base URL for ORCA MCP operations
    """
    import os

    # Use production worker by default, local for development only
    if os.getenv('CLOUDFLARE_WORKER_DEV', 'false').lower() in ('1', 'true', 'yes'):
        return "http://localhost:8787"
    else:
        return os.getenv('ORCA_URL', 'https://portfolio-optimizer-mcp.urbancanary.workers.dev')


# Alias for backward compatibility (internal use)
_get_d1_api_url = get_orca_url


def save_staging_transaction(transaction_data: Dict[str, Any], client_id: str = None) -> Dict[str, Any]:
    """
    Save a staging transaction to Cloudflare D1

    Args:
        transaction_data: Transaction data dictionary
        client_id: Client identifier

    Returns:
        Result with transaction_id
    """
    config = get_client_config(client_id)

    # Build request
    url = f"{_get_d1_api_url()}/api/staging/transactions"

    # Add client_id to transaction data
    transaction_data['portfolio_id'] = transaction_data.get('portfolio_id', config.client_id)

    req = urllib.request.Request(
        url,
        data=json.dumps(transaction_data).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'X-Client-ID': config.client_id
        },
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Saved staging transaction: {result.get('transaction_id')}")
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to save staging transaction: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to save staging transaction: {str(e)}")


def get_staging_transactions(portfolio_id: str, client_id: str = None) -> pd.DataFrame:
    """
    Get all staging transactions for a portfolio from Cloudflare D1

    Args:
        portfolio_id: Portfolio identifier
        client_id: Client identifier

    Returns:
        DataFrame with staging transactions
    """
    config = get_client_config(client_id)

    # Build request
    url = f"{_get_d1_api_url()}/api/staging/transactions?portfolio_id={portfolio_id}"

    req = urllib.request.Request(
        url,
        headers={
            'X-Client-ID': config.client_id
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            transactions = data.get('transactions', [])

            if not transactions:
                # Return empty DataFrame with expected columns
                return pd.DataFrame(columns=[
                    'transaction_id', 'portfolio_id', 'transaction_date', 'settlement_date',
                    'transaction_type', 'isin', 'ticker', 'description', 'country',
                    'par_amount', 'price', 'accrued_interest', 'dirty_price', 'market_value',
                    'ytm', 'duration', 'spread', 'notes', 'created_at', 'created_by'
                ])

            df = pd.DataFrame(transactions)
            print(f"✅ Fetched {len(df)} staging transactions from D1")
            return df

    except urllib.error.HTTPError as e:
        if e.code == 404:
            # No staging transactions found - return empty DataFrame
            return pd.DataFrame(columns=[
                'transaction_id', 'portfolio_id', 'transaction_date', 'settlement_date',
                'transaction_type', 'isin', 'ticker', 'description', 'country',
                'par_amount', 'price', 'accrued_interest', 'dirty_price', 'market_value',
                'ytm', 'duration', 'spread', 'notes', 'created_at', 'created_by'
            ])
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to fetch staging transactions: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to fetch staging transactions: {str(e)}")


def delete_staging_transaction(transaction_id: int, client_id: str = None) -> Dict[str, Any]:
    """
    Delete a staging transaction from Cloudflare D1

    Args:
        transaction_id: Transaction ID to delete
        client_id: Client identifier

    Returns:
        Result dictionary
    """
    config = get_client_config(client_id)

    # Build request
    url = f"{_get_d1_api_url()}/api/staging/transactions/{transaction_id}"

    req = urllib.request.Request(
        url,
        headers={
            'X-Client-ID': config.client_id
        },
        method='DELETE'
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Deleted staging transaction: {transaction_id}")
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to delete staging transaction: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to delete staging transaction: {str(e)}")


def update_transaction_d1(transaction_id: int, status: str, client_id: str = None) -> Dict[str, Any]:
    """
    Update a transaction's status in Cloudflare D1

    Args:
        transaction_id: Transaction ID to update
        status: New status value (staging, confirmed, settled)
        client_id: Client identifier

    Returns:
        Result dictionary with success status
    """
    config = get_client_config(client_id)

    # Build request
    url = f"{_get_d1_api_url()}/api/transactions/{transaction_id}"

    req = urllib.request.Request(
        url,
        data=json.dumps({'status': status}).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'X-Client-ID': config.client_id
        },
        method='PATCH'
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Updated transaction {transaction_id} to {status}")
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        return {
            'success': False,
            'transaction_id': transaction_id,
            'error': f"HTTP {e.code}: {error_body}"
        }
    except Exception as e:
        return {
            'success': False,
            'transaction_id': transaction_id,
            'error': str(e)
        }


def clear_all_staging_transactions(portfolio_id: str, client_id: str = None) -> Dict[str, Any]:
    """
    Clear all staging transactions for a portfolio

    Args:
        portfolio_id: Portfolio identifier
        client_id: Client identifier

    Returns:
        Result with count of deleted transactions
    """
    config = get_client_config(client_id)

    # Build request
    url = f"{_get_d1_api_url()}/api/staging/transactions/clear?portfolio_id={portfolio_id}"

    req = urllib.request.Request(
        url,
        headers={
            'X-Client-ID': config.client_id
        },
        method='DELETE'
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Cleared {result.get('count', 0)} staging transactions")
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to clear staging transactions: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to clear staging transactions: {str(e)}")


def get_watchlist(client_id: str = None) -> pd.DataFrame:
    """
    Get watchlist data from Cloudflare D1 API

    Args:
        client_id: Client identifier (optional)

    Returns:
        DataFrame with watchlist bonds (ISINs and metadata only)
    """
    import os
    # Watchlist API endpoint
    pricing_url = os.getenv('GA10_PRICING_URL', 'https://ga10-pricing.urbancanary.workers.dev')
    url = f"{pricing_url}/watchlist"

    req = urllib.request.Request(url)

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
            # API returns 'watchlist' not 'bonds'
            watchlist = data.get('watchlist', [])

            if not watchlist:
                print("⚠️ No bonds found in watchlist")
                return pd.DataFrame()

            df = pd.DataFrame(watchlist)
            print(f"✅ Fetched {len(df)} bonds from watchlist D1 API")
            return df

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch watchlist: {e.code} - {error_body}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Failed to fetch watchlist: {str(e)}")
        return pd.DataFrame()


def get_holdings(portfolio_id: str = 'wnbf', staging_id: int = 1, client_id: str = None) -> pd.DataFrame:
    """
    Get portfolio holdings from Cloudflare D1 (fast edge database)

    D1-First Architecture: User queries go to D1 for fast responses.
    Data is synced from BigQuery via background job.

    Args:
        portfolio_id: Portfolio identifier (default: 'wnbf')
        staging_id: 1=Live portfolio, 2=Staging portfolio
        client_id: Client identifier (optional)

    Returns:
        DataFrame with holdings data
    """
    url = f"{_get_d1_api_url()}/api/holdings?portfolio_id={portfolio_id}&staging_id={staging_id}"

    req = urllib.request.Request(url)

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            holdings = data.get('holdings', [])

            if not holdings:
                print(f"⚠️ No holdings found in D1 for {portfolio_id} staging_id={staging_id}")
                return pd.DataFrame()

            df = pd.DataFrame(holdings)
            print(f"✅ Fetched {len(df)} holdings from D1 (staging_id={staging_id})")
            return df

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch holdings from D1: {e.code} - {error_body}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Failed to fetch holdings from D1: {str(e)}")
        return pd.DataFrame()


def get_holdings_summary(portfolio_id: str = 'wnbf', staging_id: int = 1, client_id: str = None) -> Dict[str, Any]:
    """
    Get portfolio summary stats from Cloudflare D1 (fast edge database)

    Args:
        portfolio_id: Portfolio identifier (default: 'wnbf')
        staging_id: 1=Live portfolio, 2=Staging portfolio
        client_id: Client identifier (optional)

    Returns:
        Dictionary with summary stats and country breakdown
    """
    url = f"{_get_d1_api_url()}/api/holdings/summary?portfolio_id={portfolio_id}&staging_id={staging_id}"

    req = urllib.request.Request(url)

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            print(f"✅ Fetched portfolio summary from D1 (staging_id={staging_id})")
            return data

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch holdings summary from D1: {e.code} - {error_body}")
        return {}
    except Exception as e:
        print(f"❌ Failed to fetch holdings summary from D1: {str(e)}")
        return {}


def sync_holdings_to_d1(holdings_df: pd.DataFrame, portfolio_id: str = 'wnbf', staging_id: int = 1) -> Dict[str, Any]:
    """
    Sync holdings data from BigQuery to Cloudflare D1

    Called by background sync job to populate D1 with fresh data.

    Args:
        holdings_df: DataFrame with holdings data from BigQuery
        portfolio_id: Portfolio identifier
        staging_id: 1=Live portfolio, 2=Staging portfolio

    Returns:
        Result dictionary with sync status
    """
    if holdings_df.empty:
        return {"success": False, "error": "Empty DataFrame provided"}

    # Convert DataFrame to list of dicts
    holdings = holdings_df.to_dict(orient='records')

    # Handle NaN values
    for h in holdings:
        for key, value in h.items():
            if pd.isna(value):
                h[key] = None

    url = f"{_get_d1_api_url()}/api/holdings/sync"

    payload = {
        "portfolio_id": portfolio_id,
        "staging_id": staging_id,
        "holdings": holdings
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Synced {result.get('inserted', 0)} holdings to D1 (staging_id={staging_id})")
            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to sync holdings to D1: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to sync holdings to D1: {str(e)}")


def get_analytics(limit: int = 5000, offset: int = 0) -> pd.DataFrame:
    """
    Get bond analytics from Cloudflare D1 (fast edge database)

    D1-First Architecture: User queries go to D1 for fast responses.
    Data is synced from BigQuery via background job (4am daily refresh).

    Args:
        limit: Maximum number of records to return (default: 5000)
        offset: Offset for pagination (default: 0)

    Returns:
        DataFrame with bond analytics (universe data for watchlist/search)
    """
    url = f"{_get_d1_api_url()}/api/analytics?limit={limit}&offset={offset}"

    req = urllib.request.Request(url)

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
            analytics = data.get('analytics', [])

            if not analytics:
                print(f"⚠️ No analytics found in D1")
                return pd.DataFrame()

            df = pd.DataFrame(analytics)
            print(f"✅ Fetched {len(df)} bonds from D1 analytics")
            return df

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch analytics from D1: {e.code} - {error_body}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Failed to fetch analytics from D1: {str(e)}")
        return pd.DataFrame()


def get_analytics_batch(isins: List[str]) -> pd.DataFrame:
    """
    Get analytics for specific ISINs from Cloudflare D1 (batch query)

    Args:
        isins: List of ISINs to fetch

    Returns:
        DataFrame with analytics for requested ISINs
    """
    if not isins:
        return pd.DataFrame()

    url = f"{_get_d1_api_url()}/api/analytics"

    payload = {"isins": isins}

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
            analytics = data.get('analytics', [])

            if not analytics:
                print(f"⚠️ No analytics found for {len(isins)} ISINs in D1")
                return pd.DataFrame()

            df = pd.DataFrame(analytics)
            print(f"✅ Fetched analytics for {len(df)}/{len(isins)} ISINs from D1")
            return df

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch analytics batch from D1: {e.code} - {error_body}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Failed to fetch analytics batch from D1: {str(e)}")
        return pd.DataFrame()


def search_bonds(
    country: Optional[str] = None,
    maturity_year: Optional[int] = None,
    ticker: Optional[str] = None,
    coupon: Optional[float] = None,
    limit: int = 20
) -> pd.DataFrame:
    """
    Search bonds in D1 analytics by criteria (for Minerva chat)

    Uses filtered query instead of loading all bonds - avoids SQLite variable limit.
    Called by Minerva chat to find bonds matching Haiku's parsed intent.

    Args:
        country: Country name to filter by (partial match, case-insensitive)
        maturity_year: Maturity year (2-digit like 61 or 4-digit like 2061)
        ticker: Ticker or issuer name (partial match, case-insensitive)
        coupon: Coupon rate to filter by
        limit: Maximum results to return (default 20)

    Returns:
        DataFrame with matching bonds (isin, description, ticker, country, etc.)
    """
    # Build query parameters
    params = []
    if country:
        params.append(f"country={urllib.parse.quote(country)}")
    if maturity_year:
        params.append(f"maturity_year={maturity_year}")
    if ticker:
        params.append(f"ticker={urllib.parse.quote(ticker)}")
    if coupon:
        params.append(f"coupon={coupon}")
    params.append(f"limit={limit}")

    # Need at least one filter
    if not any([country, maturity_year, ticker, coupon]):
        print("⚠️ search_bonds requires at least one filter")
        return pd.DataFrame()

    url = f"{_get_d1_api_url()}/api/analytics/search?{'&'.join(params)}"

    req = urllib.request.Request(url)

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
            analytics = data.get('analytics', [])

            if not analytics:
                filters = data.get('filters', {})
                print(f"⚠️ No bonds found matching filters: {filters}")
                return pd.DataFrame()

            df = pd.DataFrame(analytics)
            print(f"✅ Found {len(df)} bonds matching search criteria")
            return df

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to search bonds in D1: {e.code} - {error_body}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Failed to search bonds in D1: {str(e)}")
        return pd.DataFrame()


def match_bond(
    query: str,
    source: str = "analytics",
    top_n: int = 5,
    portfolio_id: str = "wnbf",
    bonds: List[Dict] = None
) -> Dict[str, Any]:
    """
    Match a natural language bond query using Orca's server-side intelligence.

    This function sends raw text to the Orca API and receives structured
    matching results. The intelligence lives in Orca, not the client.

    Example queries:
        - "buy 500k colombia 61"
        - "sell ANGLAN 8.75 2025"
        - "US912810TM67"  (direct ISIN lookup)
        - "mexico 5s of 27"

    Args:
        query: Natural language bond query or ISIN
        source: Where to search ("analytics", "holdings", "watchlist", "custom")
        top_n: Number of matches to return (default: 5)
        portfolio_id: Portfolio ID for holdings/watchlist searches
        bonds: Custom list of bonds to search (when source="custom")

    Returns:
        Dictionary with:
        - intent: Parsed trade intent (action, quantity, bond_query)
        - matches: List of matching bonds with scores
        - confident_match: Single high-confidence match (if any)
        - source: Data source used
        - total_bonds_searched: Size of search universe
    """
    url = f"{_get_d1_api_url()}/api/bond_match"

    payload = {
        "query": query,
        "source": source,
        "top_n": top_n,
        "portfolio_id": portfolio_id
    }

    if bonds:
        payload["bonds"] = bonds

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())

            # Log result summary
            matches = data.get('matches', [])
            confident = data.get('confident_match')
            if confident:
                print(f"✅ Confident match: {confident.get('ticker')} {confident.get('description')}")
            elif matches:
                print(f"📊 Found {len(matches)} potential matches (best score: {matches[0].get('score', 0)})")
            else:
                print(f"⚠️ No matches found for: {query}")

            return data

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Bond match failed: {e.code} - {error_body}")
        return {
            "error": f"HTTP {e.code}: {error_body}",
            "intent": None,
            "matches": [],
            "confident_match": None
        }
    except Exception as e:
        print(f"❌ Bond match failed: {str(e)}")
        return {
            "error": str(e),
            "intent": None,
            "matches": [],
            "confident_match": None
        }


def sync_analytics_to_d1(analytics_df: pd.DataFrame, clear_first: bool = False) -> Dict[str, Any]:
    """
    Sync analytics data from BigQuery to Cloudflare D1

    Called by background sync job to populate D1 with bond analytics.

    Args:
        analytics_df: DataFrame with analytics data from BigQuery
        clear_first: If True, clear all existing analytics before insert

    Returns:
        Result dictionary with sync status
    """
    if analytics_df.empty:
        return {"success": False, "error": "Empty DataFrame provided"}

    # Convert DataFrame to list of dicts
    analytics = analytics_df.to_dict(orient='records')

    # Handle NaN values
    for a in analytics:
        for key, value in a.items():
            if pd.isna(value):
                a[key] = None

    url = f"{_get_d1_api_url()}/api/analytics/sync"

    payload = {
        "analytics": analytics,
        "clear_first": clear_first
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Synced {result.get('upserted', 0)} analytics records to D1")
            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to sync analytics to D1: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to sync analytics to D1: {str(e)}")


def get_watchlist_complete(client_id: str = None, use_cache: bool = True) -> pd.DataFrame:
    """
    Get complete watchlist with full bond details from D1

    D1-First Architecture: All data comes from D1 (synced from BigQuery at 4am).

    This function:
    1. Fetches watchlist ISINs from D1
    2. Fetches full bond analytics from D1 analytics table
    3. Joins them to return complete data

    Args:
        client_id: Client identifier (optional)
        use_cache: Whether to use cached data (default: True)

    Returns:
        DataFrame with complete watchlist bond data including:
        - All watchlist metadata (weight, portfolio_name, etc.)
        - Full bond details from D1 analytics
        - Ratings (rating_notches), prices, analytics (ytw, oad, oas)
    """
    # Get watchlist ISINs from D1
    watchlist_df = get_watchlist(client_id)

    if watchlist_df.empty:
        print("⚠️ No watchlist bonds found")
        return pd.DataFrame()

    # Extract ISINs
    isins = watchlist_df['isin'].unique().tolist()

    print(f"📊 Fetching complete data for {len(isins)} watchlist bonds from D1...")

    # Get full analytics from D1 (already synced from BigQuery)
    analytics_df = get_analytics_batch(isins)

    if analytics_df.empty:
        print("⚠️ No analytics found in D1 - returning watchlist-only data")
        print("   Ensure analytics sync job has run: python scripts/sync_analytics_to_d1.py")
        return watchlist_df

    # Rename columns to match expected format
    column_mapping = {
        'yield': 'ytw',
        'duration': 'duration',  # same
        'spread': 'spread',      # same
    }
    analytics_df = analytics_df.rename(columns=column_mapping)

    # Join watchlist metadata with analytics
    complete_df = watchlist_df.merge(
        analytics_df,
        on='isin',
        how='left'
    )

    print(f"✅ Fetched complete data for {len(complete_df)} watchlist bonds from D1")
    return complete_df


def get_period_prices(start_date: str, end_date: str, isins: List[str] = None) -> pd.DataFrame:
    """
    Get beginning and ending prices for a period from D1 (fast edge database)

    Used for P&L calculations:
    Total Return = (End Dirty + Coupons - Begin Dirty) / Begin Dirty

    Args:
        start_date: Period start date (YYYY-MM-DD)
        end_date: Period end date (YYYY-MM-DD)
        isins: Optional list of ISINs to filter

    Returns:
        DataFrame with columns:
        - isin, begin_date, begin_price, begin_accrued, begin_dirty
        - end_date, end_price, end_accrued, end_dirty
    """
    url = f"{_get_d1_api_url()}/api/price_history/period"
    params = {
        'start_date': start_date,
        'end_date': end_date
    }
    if isins:
        params['isins'] = ','.join(isins)

    req = urllib.request.Request(f"{url}?{urllib.parse.urlencode(params)}")

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
            period_prices = data.get('period_prices', [])

            if not period_prices:
                print(f"⚠️ No price history found for period {start_date} to {end_date}")
                return pd.DataFrame()

            df = pd.DataFrame(period_prices)
            print(f"✅ Fetched period prices for {len(df)} bonds from D1")
            return df

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch period prices from D1: {e.code} - {error_body}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Failed to fetch period prices from D1: {str(e)}")
        return pd.DataFrame()


def get_transactions(portfolio_id: str = 'wnbf', client_id: str = None) -> pd.DataFrame:
    """
    Get historical transactions from Cloudflare D1 (fast edge database)

    D1-First Architecture: User queries go to D1 for fast responses.
    Data is synced from BigQuery via background job (4am daily refresh).

    Args:
        portfolio_id: Portfolio identifier (default: 'wnbf')
        client_id: Client identifier (optional)

    Returns:
        DataFrame with historical transactions
    """
    url = f"{_get_d1_api_url()}/api/transactions?portfolio_id={portfolio_id}"

    req = urllib.request.Request(url)

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
            transactions = data.get('transactions', [])

            if not transactions:
                print(f"⚠️ No transactions found in D1 for {portfolio_id}")
                return pd.DataFrame()

            df = pd.DataFrame(transactions)
            print(f"✅ Fetched {len(df)} transactions from D1")
            return df

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch transactions from D1: {e.code} - {error_body}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Failed to fetch transactions from D1: {str(e)}")
        return pd.DataFrame()


def get_cashflows(portfolio_id: str = 'wnbf', client_id: str = None) -> pd.DataFrame:
    """
    Get cashflows (future coupon/principal payments) from Cloudflare D1

    D1-First Architecture: User queries go to D1 for fast responses.
    Data is synced from BigQuery via background job (4am daily refresh).

    Args:
        portfolio_id: Portfolio identifier (default: 'wnbf')
        client_id: Client identifier (optional)

    Returns:
        DataFrame with cashflows (payment_date, payment_type, isin, etc.)
    """
    url = f"{_get_d1_api_url()}/api/cashflows?portfolio_id={portfolio_id}"

    req = urllib.request.Request(url)

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
            cashflows = data.get('cashflows', [])

            if not cashflows:
                print(f"⚠️ No cashflows found in D1 for {portfolio_id}")
                return pd.DataFrame()

            df = pd.DataFrame(cashflows)
            print(f"✅ Fetched {len(df)} cashflows from D1")
            return df

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch cashflows from D1: {e.code} - {error_body}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Failed to fetch cashflows from D1: {str(e)}")
        return pd.DataFrame()


def sync_price_history_to_d1(prices_df: pd.DataFrame, clear_before_date: str = None) -> Dict[str, Any]:
    """
    Sync price history from BigQuery to D1

    Called by background sync job to populate D1 with price history.

    Args:
        prices_df: DataFrame with price history from BigQuery
        clear_before_date: Optional date to clear prices before

    Returns:
        Result dictionary with sync status
    """
    if prices_df.empty:
        return {"success": False, "error": "Empty DataFrame provided"}

    # Convert DataFrame to list of dicts
    prices = prices_df.to_dict(orient='records')

    # Handle NaN values
    for p in prices:
        for key, value in p.items():
            if pd.isna(value):
                p[key] = None

    url = f"{_get_d1_api_url()}/api/price_history/sync"

    payload = {"prices": prices}
    if clear_before_date:
        payload["clear_before_date"] = clear_before_date

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Synced {result.get('upserted', 0)} price records to D1")
            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to sync price history to D1: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to sync price history to D1: {str(e)}")


# ==============================================================================
# Remote Pages API - Hot-swappable page system
# ==============================================================================

def get_remote_pages(client_id: str = 'guinness', enabled_only: bool = True) -> List[Dict[str, Any]]:
    """
    Get remote page definitions from Cloudflare D1

    Remote pages allow hot-swapping of Streamlit pages without
    touching the client's codebase.

    Args:
        client_id: Client identifier (pages can be client-specific)
        enabled_only: If True, only return enabled pages

    Returns:
        List of page definition dictionaries with keys:
        - page_id: Unique identifier
        - menu_order: Position in sidebar
        - menu_title: Display name
        - menu_icon: Emoji/icon
        - page_code: Python code as string
        - version: Version number
        - enabled: Whether page is active
        - description: Page description
        - dependencies: JSON list of required imports
        - author: Who created/updated
        - created_at: Creation timestamp
        - updated_at: Last update timestamp
    """
    url = f"{_get_d1_api_url()}/api/remote_pages?client_id={client_id}"
    if enabled_only:
        url += "&enabled=true"

    req = urllib.request.Request(
        url,
        headers={'X-Client-ID': client_id}
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            pages = data.get('pages', [])
            print(f"✅ Fetched {len(pages)} remote pages from D1")
            return pages

    except urllib.error.HTTPError as e:
        if e.code == 404:
            # No remote pages table or no pages - this is fine
            return []
        error_body = e.read().decode() if e.fp else ""
        print(f"❌ Failed to fetch remote pages from D1: {e.code} - {error_body}")
        return []
    except Exception as e:
        print(f"❌ Failed to fetch remote pages from D1: {str(e)}")
        return []


def save_remote_page(page_data: Dict[str, Any], client_id: str = 'guinness') -> Dict[str, Any]:
    """
    Save or update a remote page in Cloudflare D1

    Args:
        page_data: Dictionary with page definition:
            - page_id: Unique identifier (required)
            - menu_order: Position in sidebar (default: 100)
            - menu_title: Display name (default: page_id)
            - menu_icon: Emoji/icon (default: 📄)
            - page_code: Python code as string (required)
            - description: Page description
            - dependencies: List of required imports
            - enabled: Whether page is active (default: True)
            - author: Who created/updated (optional)
        client_id: Client identifier

    Returns:
        Result dictionary with page_id and version
    """
    url = f"{_get_d1_api_url()}/api/remote_pages"

    # Ensure required fields
    if 'page_id' not in page_data:
        raise ValueError("page_id is required")
    if 'page_code' not in page_data:
        raise ValueError("page_code is required")

    # Add client_id
    page_data['client_id'] = client_id

    # Convert dependencies list to JSON if needed
    if 'dependencies' in page_data and isinstance(page_data['dependencies'], list):
        page_data['dependencies'] = json.dumps(page_data['dependencies'])

    req = urllib.request.Request(
        url,
        data=json.dumps(page_data).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'X-Client-ID': client_id
        },
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Saved remote page: {page_data['page_id']} (v{result.get('version', 1)})")
            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to save remote page: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to save remote page: {str(e)}")


def delete_remote_page(page_id: str, client_id: str = 'guinness') -> Dict[str, Any]:
    """
    Delete a remote page from Cloudflare D1

    Args:
        page_id: Page identifier to delete
        client_id: Client identifier

    Returns:
        Result dictionary
    """
    url = f"{_get_d1_api_url()}/api/remote_pages/{page_id}"

    req = urllib.request.Request(
        url,
        headers={'X-Client-ID': client_id},
        method='DELETE'
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            print(f"✅ Deleted remote page: {page_id}")
            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to delete remote page: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to delete remote page: {str(e)}")


def toggle_remote_page(page_id: str, enabled: bool, client_id: str = 'guinness') -> Dict[str, Any]:
    """
    Enable or disable a remote page

    Args:
        page_id: Page identifier
        enabled: Whether to enable (True) or disable (False)
        client_id: Client identifier

    Returns:
        Result dictionary
    """
    url = f"{_get_d1_api_url()}/api/remote_pages/{page_id}"

    req = urllib.request.Request(
        url,
        data=json.dumps({'enabled': enabled}).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'X-Client-ID': client_id
        },
        method='PATCH'
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            status = "enabled" if enabled else "disabled"
            print(f"✅ Remote page {page_id} {status}")
            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        raise RuntimeError(f"Failed to toggle remote page: {e.code} - {error_body}")
    except Exception as e:
        raise RuntimeError(f"Failed to toggle remote page: {str(e)}")
