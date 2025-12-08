"""
Data Access Tools for Orca MCP

Handles all data access via API calls (BigQuery, auth-mcp, etc.)
NO local file access - everything via APIs
"""

import json
import urllib.request
import urllib.error
import tempfile
import os
import re
import base64
from typing import Any, Dict
from pathlib import Path
import pandas as pd

# Optional BigQuery import - only needed when using BigQuery backend
try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    bigquery = None

try:
    from ..client_config import get_client_config
    from .cache_manager import get_cache_manager, CacheManager
except ImportError:
    # When deployed standalone (not as package)
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from client_config import get_client_config
    from tools.cache_manager import get_cache_manager, CacheManager


def _allow_local_auth_fallback() -> bool:
    return os.getenv("ALLOW_LOCAL_AUTH_MCP_FALLBACK", "false").lower() in ("1", "true", "yes")


def fetch_credentials_from_auth_mcp(credentials_key: str = "GOOGLE_CREDENTIALS") -> Dict[str, Any]:
    """
    Fetch credentials from auth-mcp service with fallback to local file

    Args:
        credentials_key: Key name in auth-mcp (e.g., 'GOOGLE_CREDENTIALS')

    Returns:
        Credentials as dictionary

    Raises:
        RuntimeError: If credentials cannot be fetched from API or local file
    """
    config = get_client_config()
    auth_service = config.get_service("auth_mcp")

    try:
        # Build request
        url = f"{auth_service['api_base']}/key/{credentials_key}"
        token = config.get_auth_token("auth_mcp")

        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "X-Requester": f"orca_mcp_{config.client_id}"
            }
        )

        # Fetch credentials from API
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            return json.loads(data.get('value', '{}'))

    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as e:
        if not _allow_local_auth_fallback():
            raise RuntimeError(
                "Auth-MCP service is unreachable. "
                "Ensure AUTH_MCP_URL/TOKEN are set so the request succeeds."
            ) from e

        # Fallback to local auth_mcp keys.json file (opt-in for local dev)
        local_keys_path = Path(
            "/Users/andyseaman/Notebooks/mcp_central/auth_mcp/keys.json"
        )

        if local_keys_path.exists():
            print(f"⚠️  Auth-MCP API unavailable, trying local keys file...")
            try:
                with open(local_keys_path, 'r') as f:
                    keys = json.load(f)
                    if credentials_key in keys:
                        creds_value = keys[credentials_key]
                        # If it's a string, parse it as JSON
                        if isinstance(creds_value, str):
                            return json.loads(creds_value)
                        return creds_value
            except Exception as file_error:
                print(f"⚠️  Error reading local keys file: {file_error}")

        # Local fallback attempted but missing key/path
        raise RuntimeError(
            "Cannot fetch credentials from auth-mcp API or local file. "
            "Set ALLOW_LOCAL_AUTH_MCP_FALLBACK=true for local dev fallback."
        ) from e


def setup_bigquery_credentials():
    """
    Set up BigQuery credentials from auth-mcp with fallback to local credentials

    Tries auth-mcp first, falls back to local credentials if unavailable
    """
    # Clear any existing credentials to avoid using old/invalid files
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        old_creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        # Only skip if it's a valid, readable credentials file
        if old_creds_path and os.path.isfile(old_creds_path):
            try:
                with open(old_creds_path, 'r') as f:
                    json.load(f)  # Validate it's valid JSON
                    return  # Already configured with valid credentials
            except:
                # Invalid or unreadable - clear it
                del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        else:
            # Not a file or doesn't exist - clear it
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    try:
        # Try auth-mcp first
        creds = fetch_credentials_from_auth_mcp("GOOGLE_CREDENTIALS")
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            json.dump(creds, f)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
        print(f"✅ Fetched BigQuery credentials from auth-mcp")
        return
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError, RuntimeError) as e:
        if not _allow_local_auth_fallback():
            raise RuntimeError(
                "Auth-MCP unavailable and fallbacks disabled. "
                "Set ALLOW_LOCAL_AUTH_MCP_FALLBACK=true only for local development."
            ) from e

        print(f"⚠️  Auth-MCP unavailable ({type(e).__name__}), trying local credentials...")

    # Try GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable (Railway deployment)
    if "GOOGLE_APPLICATION_CREDENTIALS_JSON" in os.environ:
        try:
            creds_json = os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"]
            # Try to decode as base64 first (handles escape sequences better)
            try:
                decoded = base64.b64decode(creds_json).decode('utf-8')
                creds = json.loads(decoded)
                print(f"✅ Decoded base64-encoded credentials")
            except:
                # Not base64 - try parsing directly as JSON
                creds = json.loads(creds_json)

            # Write to temp file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                json.dump(creds, f)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
            print(f"✅ Using credentials from GOOGLE_APPLICATION_CREDENTIALS_JSON")
            return
        except json.JSONDecodeError as e:
            print(f"⚠️  GOOGLE_APPLICATION_CREDENTIALS_JSON is invalid JSON: {e}")

    # Fallback to local credentials
    # Try common credential file locations
    possible_paths = [
        Path.home() / ".config" / "gcloud" / "application_default_credentials.json",
        Path(__file__).parent.parent.parent / "credentials" / "google_credentials.json",
    ]

    # Only add GOOGLE_APPLICATION_CREDENTIALS if it's actually set
    gac_env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if gac_env:
        possible_paths.insert(1, Path(gac_env))

    for creds_path in possible_paths:
        # Must be a file (not directory) and exist
        if creds_path and creds_path.exists() and creds_path.is_file():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)
            print(f"✅ Using local credentials: {creds_path.name}")
            return

    # No credentials found
    raise RuntimeError(
        "Cannot access BigQuery credentials:\n"
        "  - Auth-MCP service is unavailable\n"
        "  - GOOGLE_APPLICATION_CREDENTIALS_JSON not set or invalid\n"
        "  - No local credentials found in standard locations\n"
        "  Please run 'gcloud auth application-default login' or set GOOGLE_APPLICATION_CREDENTIALS"
    )


def query_bigquery(sql: str, client_id: str = None, use_cache: bool = True, ttl: int = None) -> pd.DataFrame:
    """
    Query BigQuery for a specific client with optional Redis caching

    Args:
        sql: SQL query (use simple table names like 'transactions')
        client_id: Client identifier (uses default if None)
        use_cache: Whether to use Redis cache (default: True)
        ttl: Cache TTL in seconds (default: auto-determined by query type)

    Returns:
        DataFrame with results
    """
    if not BIGQUERY_AVAILABLE:
        raise ImportError(
            "BigQuery is not available. Install with: pip install google-cloud-bigquery\n"
            "Or use local SQLite database instead."
        )

    # Get client config
    config = get_client_config(client_id)

    # Check cache first (if enabled)
    cache = get_cache_manager()
    cache_key = None

    if use_cache and cache.enabled:
        # Generate cache key
        query_hash = cache.query_hash(sql, {"client_id": config.client_id})
        cache_key = f"query:{config.client_id}:{query_hash}"

        # Try to get from cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            try:
                # Reconstruct DataFrame from cached dict
                df = pd.DataFrame(cached_data)
                print(f"✅ Cache HIT: Returned {len(df)} rows from cache")
                return df
            except Exception as e:
                print(f"⚠️  Cache deserialization error: {e}")
                # Continue to query BigQuery

    # Cache miss or caching disabled - query BigQuery
    print(f"📊 Cache MISS: Querying BigQuery...")

    # Set up credentials
    setup_bigquery_credentials()

    # Get BigQuery service config
    bq_service = config.get_service("bigquery")

    # Create BigQuery client
    client = bigquery.Client(project=bq_service['project'])

    # Get client's dataset
    dataset = config.get_bigquery_dataset()

    # Rewrite table names to full BigQuery paths
    sql_rewritten = sql
    tables = ['staging_holdings_detail', 'staging_holdings', 'current_holdings', 'transactions', 'agg_analysis_data', 'cashflows']

    # Sort by length (longest first) to avoid partial replacements
    # e.g., staging_holdings_detail before staging_holdings

    for table_name in tables:
        # Match table name in FROM/JOIN clauses with optional alias
        full_path = f"`{bq_service['project']}.{dataset}.{table_name}`"

        # Use regex to match table name followed by space, newline, or alias
        # Pattern: table_name followed by (space + alias) or (space/newline)
        pattern = rf'\b{table_name}\b'
        sql_rewritten = re.sub(pattern, full_path, sql_rewritten)

    print(f"🔍 BigQuery SQL (client={config.client_id}): {sql_rewritten[:100]}...")

    # Execute query
    df = client.query(sql_rewritten).to_dataframe()

    print(f"✅ BigQuery returned {len(df)} rows")

    # Cache the result (if enabled)
    if use_cache and cache.enabled and cache_key:
        # Determine TTL based on query type
        if ttl is None:
            if 'agg_analysis_data' in sql.lower():
                ttl = CacheManager.TTL_UNIVERSE  # 1 hour for universe data
            elif 'transactions' in sql.lower():
                ttl = CacheManager.TTL_TRANSACTIONS  # 15 min for transactions
            elif 'holdings' in sql.lower():
                ttl = CacheManager.TTL_HOLDINGS  # 5 min for holdings
            else:
                ttl = CacheManager.TTL_DEFAULT  # 5 min default

        try:
            # Convert DataFrame to dict for JSON serialization
            cache_data = df.to_dict(orient='records')
            cache.set(cache_key, cache_data, ttl)
            print(f"💾 Cached result (TTL: {ttl}s)")
        except Exception as e:
            print(f"⚠️  Cache write error: {e}")

    return df


def get_client_database_registry(client_id: str = None) -> Dict[str, Any]:
    """
    Load database registry for a client

    Args:
        client_id: Client identifier

    Returns:
        Database registry as dictionary
    """
    config = get_client_config(client_id)
    registry_path = config.get_database_registry_path()

    if not registry_path.exists():
        raise FileNotFoundError(f"Database registry not found: {registry_path}")

    with open(registry_path, 'r') as f:
        return json.load(f)
