"""
External MCP Gateway - Wrappers for external MCP services

These tools provide access to external MCP services via HTTP:
- NFA MCP: Net Foreign Assets star ratings
- Rating MCP: Sovereign credit ratings
- Country Mapping MCP: Country name standardization
- FRED MCP: Federal Reserve economic data
- Sovereign Classification MCP: Issuer type classification
- IMF MCP: IMF economic indicators (with AI analysis)
- World Bank MCP: World Bank development indicators
- Supabase MCP: Portfolio data gateway (holdings, transactions, watchlist)
- Sov-Quasi Reports MCP: Track pending sovereign/quasi-sovereign reports

Authentication:
- Cloudflare Workers: Self-validating tokens (no secrets needed)
- Supabase MCP: No auth required (keys handled internally)
"""

import asyncio
import hashlib
import os
import secrets
import requests
import logging
import json
import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

# Optional async HTTP client - falls back to sync if not available
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None

# Auth client for fetching service URLs/keys from auth_mcp
try:
    from auth_client import get_api_key
    AUTH_CLIENT_AVAILABLE = True
except ImportError:
    AUTH_CLIENT_AVAILABLE = False
    get_api_key = None

logger = logging.getLogger("orca-mcp.external")


def _get_supabase_mcp_url() -> str:
    """Get Supabase MCP URL from auth_mcp or environment."""
    # Try auth_mcp first
    if AUTH_CLIENT_AVAILABLE and get_api_key:
        url = get_api_key("SUPABASE_MCP_URL", fallback_env=False, requester="orca-mcp")
        if url:
            return url
    # Fallback to environment variable
    return os.environ.get("SUPABASE_MCP_URL", "http://localhost:8001")

# MCP Service URLs (supabase fetched lazily from auth_mcp)
MCP_URLS = {
    "nfa": "https://nfa-mcp.urbancanary.workers.dev",
    "rating": "https://rating-mcp.urbancanary.workers.dev",
    "country_mapping": "https://country-mapping-mcp.urbancanary.workers.dev",
    "fred": "https://fred-mcp.urbancanary.workers.dev",
    "sovereign_classification": "https://sovereign-classification-mcp.urbancanary.workers.dev",
    "imf": "https://imf-mcp.urbancanary.workers.dev",
    "worldbank": "https://worldbank-mcp.urbancanary.workers.dev",
    "reasoning": os.environ.get("REASONING_MCP_URL", "https://reasoning-mcp-production-537b.up.railway.app"),
    "sov_quasi": os.environ.get("SOV_QUASI_MCP_URL", "https://sov-quasi-list-production.up.railway.app"),
}

# Lazy-loaded URL (fetched from auth_mcp on first use)
_supabase_mcp_url_cache = None

def _get_supabase_url() -> str:
    """Get cached Supabase MCP URL."""
    global _supabase_mcp_url_cache
    if _supabase_mcp_url_cache is None:
        _supabase_mcp_url_cache = _get_supabase_mcp_url()
        logger.info(f"Supabase MCP URL: {_supabase_mcp_url_cache}")
    return _supabase_mcp_url_cache

TIMEOUT = 30  # seconds


def _generate_token() -> str:
    """Generate a self-validating token on the fly. No secrets needed."""
    random_part = secrets.token_hex(8)  # 16 hex chars
    checksum = hashlib.sha256(random_part.encode()).hexdigest()[:8]
    return f"{random_part}-{checksum}"


def _get_auth_headers() -> Dict[str, str]:
    """Get headers with fresh auth token for MCP requests."""
    return {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {_generate_token()}'
    }


def _get(url: str, params: Dict = None, timeout: int = TIMEOUT) -> requests.Response:
    """Make authenticated GET request."""
    return requests.get(url, params=params, headers=_get_auth_headers(), timeout=timeout)


def _post(url: str, json_data: Dict = None, timeout: int = TIMEOUT) -> requests.Response:
    """Make authenticated POST request."""
    return requests.post(url, json=json_data, headers=_get_auth_headers(), timeout=timeout)


# ============================================================================
# NFA MCP - Net Foreign Assets
# ============================================================================

def get_nfa_rating(country: str, year: Optional[int] = None, history: bool = False) -> Dict[str, Any]:
    """
    Get NFA (Net Foreign Assets) star rating for a country.

    Args:
        country: Country name (e.g., 'Colombia', 'Brazil')
        year: Optional specific year (default: latest available)
        history: If True, return full historical time series

    Returns:
        Dict with nfa_gdp, nfa_percentage, nfa_star_rating (1-7)
    """
    try:
        url = f"{MCP_URLS['nfa']}/nfa/{country}"
        params = {}
        if year:
            params["year"] = year
        if history:
            params["history"] = "true"

        response = _get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"NFA MCP error: {e}")
        return {"error": str(e), "country": country}


def get_nfa_batch(countries: List[str], year: Optional[int] = None) -> Dict[str, Any]:
    """
    Get NFA ratings for multiple countries.

    Args:
        countries: List of country names
        year: Optional specific year

    Returns:
        Dict with results for each country
    """
    try:
        url = f"{MCP_URLS['nfa']}/nfa"
        payload = {"countries": countries}
        if year:
            payload["year"] = year

        response = _post(url, json_data=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"NFA MCP batch error: {e}")
        return {"error": str(e)}


def search_nfa_by_rating(rating: int, min_rating: Optional[int] = None,
                         max_rating: Optional[int] = None, year: Optional[int] = None) -> Dict[str, Any]:
    """
    Search for countries by NFA star rating.

    Args:
        rating: Exact rating to search (1-7)
        min_rating: Minimum rating (inclusive)
        max_rating: Maximum rating (inclusive)
        year: Optional specific year

    Returns:
        List of countries matching the rating criteria
    """
    try:
        url = f"{MCP_URLS['nfa']}/search"
        params = {}
        if rating:
            params["rating"] = rating
        if min_rating:
            params["min_rating"] = min_rating
        if max_rating:
            params["max_rating"] = max_rating
        if year:
            params["year"] = year

        response = _get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"NFA MCP search error: {e}")
        return {"error": str(e)}


# ============================================================================
# Rating MCP - Sovereign Credit Ratings
# ============================================================================

def get_credit_rating(country: str) -> Dict[str, Any]:
    """
    Get sovereign credit rating for a country.

    Args:
        country: Country name

    Returns:
        Dict with rating, rating_numeric, agency, outlook
    """
    try:
        url = f"{MCP_URLS['rating']}/rating/{country}"
        response = _get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Rating MCP error: {e}")
        return {"error": str(e), "country": country}


def get_credit_ratings_batch(countries: List[str]) -> Dict[str, Any]:
    """
    Get credit ratings for multiple countries.

    Args:
        countries: List of country names

    Returns:
        Dict with ratings for each country
    """
    try:
        url = f"{MCP_URLS['rating']}/ratings"
        response = _post(url, json_data={"countries": countries})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Rating MCP batch error: {e}")
        return {"error": str(e)}


# ============================================================================
# Country Mapping MCP - Name Standardization
# ============================================================================

def standardize_country(country: str) -> Dict[str, Any]:
    """
    Standardize a country name to the canonical form.

    Args:
        country: Country name in any format

    Returns:
        Dict with standardized name, ISO codes, aliases
    """
    try:
        url = f"{MCP_URLS['country_mapping']}/map/{country}"
        response = _get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Country Mapping error: {e}")
        return {"error": str(e), "input": country}


def get_country_info(country: str) -> Dict[str, Any]:
    """
    Get full country information including region, codes, and aliases.

    Args:
        country: Country name

    Returns:
        Dict with country details
    """
    try:
        url = f"{MCP_URLS['country_mapping']}/country/{country}"
        response = _get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Country Mapping info error: {e}")
        return {"error": str(e), "input": country}


# ============================================================================
# FRED MCP - Federal Reserve Economic Data
# Uses POST /mcp/tools/call with tool name and arguments
# ============================================================================

def _call_fred_mcp(tool_name: str, arguments: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Internal helper to call FRED MCP tools.

    Args:
        tool_name: Name of the FRED tool (e.g., 'fred_series', 'fred_search')
        arguments: Tool arguments dict

    Returns:
        Parsed response data
    """
    try:
        url = f"{MCP_URLS['fred']}/mcp/tools/call"
        payload = {
            "name": tool_name,
            "arguments": arguments or {}
        }
        response = _post(url, json_data=payload)
        response.raise_for_status()

        result = response.json()

        # Check for error in response
        if "error" in result:
            return {"error": result["error"]}

        # Parse MCP content format: {content: [{type: "text", text: "..."}]}
        if "content" in result and result["content"]:
            text_content = result["content"][0].get("text", "")
            try:
                return json.loads(text_content)
            except json.JSONDecodeError:
                return {"text": text_content}

        return result
    except Exception as e:
        logger.error(f"FRED MCP error calling {tool_name}: {e}")
        return {"error": str(e)}


def get_fred_series(series_id: str, start_date: Optional[str] = None,
                    end_date: Optional[str] = None, analyze: bool = False) -> Dict[str, Any]:
    """
    Get FRED economic data series.

    Args:
        series_id: FRED series ID (e.g., 'DGS10' for 10Y Treasury, 'CPIAUCSL' for CPI)
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        analyze: If True, include AI analysis of the data

    Returns:
        Dict with series data and optional analysis
    """
    arguments = {"series_id": series_id}
    if start_date:
        arguments["start_date"] = start_date
    # Note: end_date not supported by current FRED MCP, but kept for API compatibility

    result = _call_fred_mcp("fred_series", arguments)
    if "error" in result:
        result["series_id"] = series_id
    return result


def search_fred_series(query: str) -> Dict[str, Any]:
    """
    Search for FRED series by keyword.

    Args:
        query: Search term (e.g., 'treasury', 'inflation', 'unemployment')

    Returns:
        List of matching series with IDs and descriptions
    """
    return _call_fred_mcp("fred_search", {"query": query})


def get_fred_timeseries(series_id: str, start_date: str = "2019-01-01") -> Dict[str, Any]:
    """
    Get full FRED time series data for charting.

    Args:
        series_id: FRED series ID (e.g., 'CPIAUCSL', 'UNRATE', 'DGS10')
        start_date: Start date (YYYY-MM-DD), default 2019-01-01

    Returns:
        Dict with chart_data array [{date, value}, ...], title, units, etc.
    """
    result = _call_fred_mcp("fred_series_timeseries", {
        "series_id": series_id,
        "start_date": start_date
    })

    # Parse the nested content if present
    if "content" in result and result["content"]:
        try:
            text_content = result["content"][0].get("text", "{}")
            parsed = json.loads(text_content)
            return parsed
        except (json.JSONDecodeError, KeyError, IndexError):
            pass

    return result


def get_treasury_rates() -> Dict[str, Any]:
    """
    Get current US Treasury rates across the curve.

    Fetches directly from US Treasury (primary source) - single API call for all tenors.

    Returns:
        Dict with rates for 1M, 2M, 3M, 6M, 1Y, 2Y, 3Y, 5Y, 7Y, 10Y, 20Y, 30Y
    """
    try:
        # Fetch from US Treasury Direct - primary source
        # Try current year first, fall back to previous year (early January edge case)
        current_year = datetime.now().year
        rows = []

        for year in [current_year, current_year - 1]:
            url = (
                f"https://home.treasury.gov/resource-center/data-chart-center/"
                f"interest-rates/daily-treasury-rates.csv/{year}/all"
                f"?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"
            )

            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()

            # Parse CSV
            reader = csv.DictReader(io.StringIO(response.text))
            rows = list(reader)

            if rows:
                break  # Found data, stop trying

        if not rows:
            return {"error": "No treasury data available"}

        # Get the most recent row (first row after header)
        latest = rows[0]

        # Map column names to standardized tenor keys
        column_mapping = {
            "1 Mo": "1M",
            "2 Mo": "2M",
            "3 Mo": "3M",
            "4 Mo": "4M",
            "6 Mo": "6M",
            "1 Yr": "1Y",
            "2 Yr": "2Y",
            "3 Yr": "3Y",
            "5 Yr": "5Y",
            "7 Yr": "7Y",
            "10 Yr": "10Y",
            "20 Yr": "20Y",
            "30 Yr": "30Y"
        }

        rates = {}
        for csv_col, tenor in column_mapping.items():
            if csv_col in latest and latest[csv_col]:
                try:
                    rates[tenor] = float(latest[csv_col])
                except ValueError:
                    pass  # Skip if not a valid number

        # Parse the date (format: MM/DD/YYYY)
        date_str = latest.get("Date", "")
        if date_str:
            try:
                parsed_date = datetime.strptime(date_str, "%m/%d/%Y")
                date_str = parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return {
            "rates": rates,
            "date": date_str,
            "source": "US Treasury Direct",
            "url": "https://home.treasury.gov/resource-center/data-chart-center/interest-rates",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Treasury rates error: {e}")
        return {"error": str(e)}


# ============================================================================
# Sovereign Classification MCP - Issuer Type
# ============================================================================

def classify_issuer(isin: str) -> Dict[str, Any]:
    """
    Classify an issuer by ISIN as sovereign, quasi-sovereign, or corporate.

    Args:
        isin: Bond ISIN

    Returns:
        Dict with issuer_type, issuer_name, country, confidence
    """
    try:
        url = f"{MCP_URLS['sovereign_classification']}/api/classify/{isin}"
        response = _get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Sovereign Classification error: {e}")
        return {"error": str(e), "isin": isin}


def classify_issuers_batch(isins: List[str]) -> Dict[str, Any]:
    """
    Classify multiple issuers by ISIN.

    Args:
        isins: List of ISINs

    Returns:
        Dict with classification for each ISIN
    """
    try:
        url = f"{MCP_URLS['sovereign_classification']}/api/classify/batch"
        response = _post(url, json_data={"isins": isins})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Sovereign Classification batch error: {e}")
        return {"error": str(e)}


def filter_by_issuer_type(issuer_type: str) -> Dict[str, Any]:
    """
    Get all issuers of a specific type.

    Args:
        issuer_type: 'sovereign', 'quasi-sovereign', or 'corporate'

    Returns:
        List of issuers matching the type
    """
    try:
        url = f"{MCP_URLS['sovereign_classification']}/api/filter"
        params = {"type": issuer_type}
        response = _get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Sovereign Classification filter error: {e}")
        return {"error": str(e)}


def get_issuer_summary(issuer: str) -> Dict[str, Any]:
    """
    Get AI-generated summary for an issuer.

    Args:
        issuer: Issuer name or ticker

    Returns:
        Dict with issuer details and AI summary
    """
    try:
        url = f"{MCP_URLS['sovereign_classification']}/api/issuer/{issuer}/summary"
        response = _get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Issuer summary error: {e}")
        return {"error": str(e), "issuer": issuer}


# ============================================================================
# IMF MCP - International Monetary Fund Data
# ============================================================================

def get_imf_indicator(indicator: str, country: str, start_year: Optional[int] = None,
                      end_year: Optional[int] = None, analyze: bool = False) -> Dict[str, Any]:
    """
    Get IMF economic indicator data with optional AI analysis.

    Args:
        indicator: Indicator code (e.g., 'NGDP_RPCH' for GDP growth, 'PCPIPCH' for inflation)
        country: Country name or ISO code
        start_year: Optional start year
        end_year: Optional end year
        analyze: If True, include AI analysis via Haiku

    Returns:
        Dict with indicator data and optional analysis
    """
    try:
        url = f"{MCP_URLS['imf']}/indicator/{indicator}"
        params = {"country": country}
        if start_year:
            params["start_year"] = start_year
        if end_year:
            params["end_year"] = end_year
        if analyze:
            params["analyze"] = "true"

        response = _get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"IMF MCP error: {e}")
        return {"error": str(e)}


def compare_imf_countries(indicator: str, countries: List[str],
                          year: Optional[int] = None) -> Dict[str, Any]:
    """
    Compare IMF indicator across multiple countries.

    Args:
        indicator: Indicator code
        countries: List of country names
        year: Optional specific year (default: latest)

    Returns:
        Comparison data for all countries
    """
    try:
        url = f"{MCP_URLS['imf']}/compare"
        payload = {
            "indicator": indicator,
            "countries": countries
        }
        if year:
            payload["year"] = year

        response = _post(url, json_data=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"IMF MCP compare error: {e}")
        return {"error": str(e)}


# ============================================================================
# World Bank MCP - Development Indicators
# ============================================================================

def _country_to_iso3(country: str, api: str = "worldbank") -> str:
    """Convert country name to ISO-3 code using country-mapping-mcp."""
    try:
        url = f"{MCP_URLS['country_mapping']}/map/{country}"
        response = _get(url, params={"api": api})
        if response.status_code == 200:
            return response.json().get("code", country.upper()[:3])
    except Exception as e:
        logger.warning(f"Country mapping failed for {country}: {e}")
    return country.upper()[:3]  # Fallback


def _call_worldbank_mcp(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Call World Bank MCP via proper MCP protocol."""
    try:
        url = f"{MCP_URLS['worldbank']}/mcp/tools/call"
        payload = {"name": tool_name, "arguments": arguments}
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"World Bank MCP error: {e}")
        return {"error": str(e)}


def get_worldbank_indicator(indicator: str, country: str,
                            start_year: Optional[int] = None,
                            end_year: Optional[int] = None) -> Dict[str, Any]:
    """
    Get World Bank development indicator data.

    Args:
        indicator: Indicator code (e.g., 'NY.GDP.PCAP.CD' for GDP per capita)
        country: Country name or ISO code
        start_year: Optional start year
        end_year: Optional end year

    Returns:
        Dict with indicator data
    """
    # Convert country name to ISO-3 code
    iso_code = _country_to_iso3(country, api="worldbank")

    args = {
        "indicator_id": indicator,
        "countries": [iso_code]
    }
    if start_year:
        args["start_year"] = str(start_year)
    if end_year:
        args["end_year"] = str(end_year)

    return _call_worldbank_mcp("wb_indicator_data", args)


def search_worldbank_indicators(query: str) -> Dict[str, Any]:
    """
    Search for World Bank indicators by keyword.

    Args:
        query: Search term (e.g., 'gdp', 'population', 'education')

    Returns:
        List of matching indicators with codes and descriptions
    """
    return _call_worldbank_mcp("wb_list_indicators", {"search": query})


def get_worldbank_country_profile(country: str) -> Dict[str, Any]:
    """
    Get comprehensive country profile from World Bank.

    Args:
        country: Country name or ISO code

    Returns:
        Dict with key development indicators for the country
    """
    # Convert country name to ISO-3 code
    iso_code = _country_to_iso3(country, api="worldbank")

    # Get key indicators for a country profile
    key_indicators = [
        "NY.GDP.PCAP.CD",  # GDP per capita
        "SP.POP.TOTL",     # Population
        "SI.POV.DDAY",     # Poverty headcount
        "SP.DYN.LE00.IN",  # Life expectancy
    ]

    results = {"country": country, "iso_code": iso_code, "indicators": {}}
    for ind in key_indicators:
        data = _call_worldbank_mcp("wb_indicator_data", {
            "indicator_id": ind,
            "countries": [iso_code]
        })
        if "error" not in data:
            results["indicators"][ind] = data

    return results


# ============================================================================
# Reasoning MCP - AI-powered analysis and reasoning
# ============================================================================

def call_reasoning(
    query: str,
    portfolio_context: Optional[Dict[str, Any]] = None,
    require_compliance: bool = False
) -> Dict[str, Any]:
    """
    Call the reasoning MCP for AI-powered analysis.

    Args:
        query: Natural language query or analysis request
        portfolio_context: Optional portfolio data to analyze
        require_compliance: If True, suggestions pass compliance checks

    Returns:
        {
            "allowed": bool,
            "response": str,
            "reasoning_trace": [...],
            "skill_used": str,
            "data": {...} (if applicable)
        }
    """
    try:
        url = f"{MCP_URLS['reasoning']}/api/reason"
        payload = {
            "query": query,
            "require_compliance": require_compliance
        }
        if portfolio_context:
            payload["portfolio_context"] = json.dumps(portfolio_context)

        response = _post(url, json_data=payload, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Reasoning MCP error: {e}")
        return {"error": str(e), "allowed": False}


def list_reasoning_skills() -> List[Dict[str, str]]:
    """
    List available reasoning skills.

    Returns:
        List of {name, description, triggers} for each skill
    """
    try:
        url = f"{MCP_URLS['reasoning']}/api/skills"
        response = _get(url)
        response.raise_for_status()
        return response.json().get("skills", [])
    except Exception as e:
        logger.error(f"Reasoning MCP skills error: {e}")
        return [{"error": str(e)}]


def analyze_data(
    data: Any,
    objective: str,
    require_compliance: bool = False
) -> Dict[str, Any]:
    """
    Analyze data using the reasoning MCP.

    Args:
        data: The data to analyze (holdings, transactions, etc.)
        objective: What kind of analysis to perform
        require_compliance: If True, suggestions pass compliance checks

    Returns:
        Analysis results with reasoning trace
    """
    query = f"{objective}\n\nData to analyze:\n{json.dumps(data, indent=2, default=str)}"
    return call_reasoning(query, require_compliance=require_compliance)


# ============================================================================
# Sov-Quasi Reports MCP - Sovereign & Quasi-Sovereign Report Tracking
# ============================================================================

def get_sov_quasi_reports(
    report_type: Optional[str] = None,
    status: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get list of sovereign/quasi-sovereign reports.

    Args:
        report_type: Filter by type ('sovereign' or 'quasi-sovereign')
        status: Filter by status ('pending', 'in-progress', 'completed')

    Returns:
        List of reports with id, name, type, status, description, attachments
    """
    try:
        url = f"{MCP_URLS['sov_quasi']}/api/reports"
        params = {}
        if report_type:
            params["type"] = report_type
        if status:
            params["status"] = status

        response = _get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Sov-Quasi MCP error: {e}")
        return [{"error": str(e)}]


def add_sov_quasi_report(
    name: str,
    report_type: str,
    status: str = "pending",
    description: Optional[str] = None
) -> Dict[str, Any]:
    """
    Add a new sovereign/quasi-sovereign report.

    Args:
        name: Report name (e.g., country or entity name)
        report_type: 'sovereign' or 'quasi-sovereign'
        status: 'pending', 'in-progress', or 'completed'
        description: Optional description

    Returns:
        Created report with generated ID
    """
    try:
        url = f"{MCP_URLS['sov_quasi']}/api/reports"
        payload = {
            "name": name,
            "type": report_type,
            "status": status
        }
        if description:
            payload["description"] = description

        response = _post(url, json_data=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Sov-Quasi MCP add error: {e}")
        return {"error": str(e)}


def get_pending_sov_quasi_reports() -> Dict[str, Any]:
    """
    Get summary of reports needing research or processing.

    Returns:
        Dict with counts and lists by status
    """
    try:
        reports = get_sov_quasi_reports()
        if isinstance(reports, list) and reports and "error" in reports[0]:
            return reports[0]

        # Include both "pending" and "needs-research" statuses
        needs_work = [r for r in reports if r.get("status") in ("pending", "needs-research")]
        raw_uploaded = [r for r in reports if r.get("status") == "raw-uploaded"]

        sovereign_needs = [r for r in needs_work if r.get("type") == "sovereign"]
        quasi_needs = [r for r in needs_work if r.get("type") == "quasi-sovereign"]

        return {
            "needs_research": [r.get("name") for r in sovereign_needs],
            "raw_uploaded": [r.get("name") for r in raw_uploaded],
            "needs_research_count": len(sovereign_needs),
            "raw_uploaded_count": len(raw_uploaded),
            "quasi_pending": [r.get("name") for r in quasi_needs],
            "total_needing_work": len(needs_work)
        }
    except Exception as e:
        logger.error(f"Sov-Quasi MCP pending error: {e}")
        return {"error": str(e)}


def get_priority_countries(priority: Optional[str] = None) -> Dict[str, Any]:
    """
    Get countries that need research, organized by priority.

    Priority is based on portfolio exposure (number of bond positions):
    - high: 20+ positions
    - medium: 10-19 positions
    - low: 5-9 positions
    - minimal: <5 positions

    Args:
        priority: Optional filter ('high', 'medium', 'low', 'minimal')

    Returns:
        Dict with countries grouped by priority, with position counts
    """
    try:
        reports = get_sov_quasi_reports()
        if isinstance(reports, list) and reports and "error" in reports[0]:
            return reports[0]

        # Filter to needs-research sovereigns
        needs_research = [
            r for r in reports
            if r.get("status") == "needs-research" and r.get("type") == "sovereign"
        ]

        # Group by priority
        by_priority = {"high": [], "medium": [], "low": [], "minimal": []}
        for r in needs_research:
            p = r.get("priority", "medium")
            positions = r.get("portfolioPositions", 0)
            by_priority.setdefault(p, []).append({
                "country": r.get("name"),
                "positions": positions,
                "description": r.get("description", "")
            })

        # Sort each priority group by positions descending
        for p in by_priority:
            by_priority[p].sort(key=lambda x: -x.get("positions", 0))

        # Filter if priority specified
        if priority and priority in by_priority:
            return {
                "priority": priority,
                "countries": by_priority[priority],
                "count": len(by_priority[priority])
            }

        return {
            "high": by_priority["high"],
            "medium": by_priority["medium"],
            "low": by_priority["low"],
            "minimal": by_priority["minimal"],
            "summary": {
                "high_count": len(by_priority["high"]),
                "medium_count": len(by_priority["medium"]),
                "low_count": len(by_priority["low"]),
                "minimal_count": len(by_priority["minimal"]),
                "total": sum(len(v) for v in by_priority.values())
            }
        }
    except Exception as e:
        logger.error(f"Sov-Quasi priority error: {e}")
        return {"error": str(e)}


def check_country_priority(country: str) -> Dict[str, Any]:
    """
    Check if a specific country needs a research report and its priority.

    Args:
        country: Country name to check

    Returns:
        Dict with country status, priority, and portfolio positions
    """
    try:
        reports = get_sov_quasi_reports()
        if isinstance(reports, list) and reports and "error" in reports[0]:
            return reports[0]

        # Normalize for comparison
        country_lower = country.lower().replace(" ", "").replace("_", "")

        for r in reports:
            name_lower = r.get("name", "").lower().replace(" ", "").replace("_", "")
            if name_lower == country_lower:
                return {
                    "country": r.get("name"),
                    "status": r.get("status"),
                    "priority": r.get("priority", "N/A"),
                    "portfolio_positions": r.get("portfolioPositions", 0),
                    "type": r.get("type"),
                    "description": r.get("description", ""),
                    "has_report": r.get("status") in ("raw-uploaded", "completed"),
                    "needs_research": r.get("status") == "needs-research"
                }

        return {
            "country": country,
            "found": False,
            "message": f"Country '{country}' not found in tracker"
        }
    except Exception as e:
        logger.error(f"Sov-Quasi check country error: {e}")
        return {"error": str(e)}


# ============================================================================
# ASYNC VERSIONS - Use httpx for concurrent API calls
# These provide 7-18x speedup when calling multiple APIs in parallel
# ============================================================================

async def _async_get(client: "httpx.AsyncClient", url: str, params: Dict = None,
                     timeout: int = TIMEOUT) -> Dict[str, Any]:
    """Make authenticated async GET request."""
    try:
        response = await client.get(url, params=params, headers=_get_auth_headers(), timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Async GET error for {url}: {e}")
        return {"error": str(e)}


async def _async_post(client: "httpx.AsyncClient", url: str, json_data: Dict = None,
                      timeout: int = TIMEOUT) -> Dict[str, Any]:
    """Make authenticated async POST request."""
    try:
        response = await client.post(url, json=json_data, headers=_get_auth_headers(), timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Async POST error for {url}: {e}")
        return {"error": str(e)}


# ============================================================================
# NFA ASYNC
# ============================================================================

async def get_nfa_rating_async(country: str, year: Optional[int] = None,
                                history: bool = False, client: "httpx.AsyncClient" = None) -> Dict[str, Any]:
    """Async version of get_nfa_rating."""
    if not HTTPX_AVAILABLE:
        return get_nfa_rating(country, year, history)

    url = f"{MCP_URLS['nfa']}/nfa/{country}"
    params = {}
    if year:
        params["year"] = year
    if history:
        params["history"] = "true"

    if client:
        return await _async_get(client, url, params)
    else:
        async with httpx.AsyncClient() as new_client:
            return await _async_get(new_client, url, params)


async def get_nfa_batch_async(countries: List[str], year: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """
    Get NFA ratings for multiple countries concurrently.

    Args:
        countries: List of country names
        year: Optional specific year

    Returns:
        Dict mapping country -> rating data
    """
    if not HTTPX_AVAILABLE:
        # Fall back to sequential calls
        results = {}
        for c in countries:
            results[c] = get_nfa_rating(c, year)
        return results

    async with httpx.AsyncClient() as client:
        tasks = [get_nfa_rating_async(c, year, client=client) for c in countries]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        results = {}
        for country, response in zip(countries, responses):
            if isinstance(response, Exception):
                results[country] = {"error": str(response), "country": country}
            else:
                results[country] = response

        return results


# ============================================================================
# CREDIT RATINGS ASYNC
# ============================================================================

async def get_credit_rating_async(country: str, client: "httpx.AsyncClient" = None) -> Dict[str, Any]:
    """Async version of get_credit_rating."""
    if not HTTPX_AVAILABLE:
        return get_credit_rating(country)

    url = f"{MCP_URLS['rating']}/rating/{country}"

    if client:
        return await _async_get(client, url)
    else:
        async with httpx.AsyncClient() as new_client:
            return await _async_get(new_client, url)


async def get_credit_ratings_batch_async(countries: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Get credit ratings for multiple countries concurrently.

    Args:
        countries: List of country names

    Returns:
        Dict mapping country -> rating data
    """
    if not HTTPX_AVAILABLE:
        # Fall back to sync batch call
        return get_credit_ratings_batch(countries)

    async with httpx.AsyncClient() as client:
        tasks = [get_credit_rating_async(c, client=client) for c in countries]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        results = {}
        for country, response in zip(countries, responses):
            if isinstance(response, Exception):
                results[country] = {"error": str(response), "country": country}
            else:
                results[country] = response

        return results


# ============================================================================
# COMBINED BATCH CALLS - Maximum parallelism
# ============================================================================

async def get_country_ratings_async(countries: List[str], include_nfa: bool = True,
                                     include_credit: bool = True, year: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """
    Get both NFA and credit ratings for multiple countries in parallel.

    This is the most efficient way to get all rating data - fires all requests
    simultaneously instead of sequentially.

    Args:
        countries: List of country names
        include_nfa: Include NFA star ratings
        include_credit: Include S&P/Moody's credit ratings
        year: Optional year for NFA ratings

    Returns:
        Dict mapping country -> {nfa: {...}, credit: {...}}

    Example:
        Sequential: 10 NFA + 10 Credit = 20 calls × 200ms = 4 seconds
        Parallel:   All 20 calls at once = 200ms (20x faster)
    """
    if not HTTPX_AVAILABLE:
        # Fall back to sequential
        results = {}
        for c in countries:
            results[c] = {}
            if include_nfa:
                results[c]["nfa"] = get_nfa_rating(c, year)
            if include_credit:
                results[c]["credit"] = get_credit_rating(c)
        return results

    async with httpx.AsyncClient() as client:
        # Build all tasks
        nfa_tasks = []
        credit_tasks = []

        if include_nfa:
            nfa_tasks = [get_nfa_rating_async(c, year, client=client) for c in countries]
        if include_credit:
            credit_tasks = [get_credit_rating_async(c, client=client) for c in countries]

        # Fire all requests in parallel
        all_tasks = nfa_tasks + credit_tasks
        all_responses = await asyncio.gather(*all_tasks, return_exceptions=True)

        # Split responses back
        nfa_responses = all_responses[:len(nfa_tasks)] if include_nfa else []
        credit_responses = all_responses[len(nfa_tasks):] if include_credit else []

        # Build results
        results = {}
        for i, country in enumerate(countries):
            results[country] = {}

            if include_nfa and i < len(nfa_responses):
                resp = nfa_responses[i]
                if isinstance(resp, Exception):
                    results[country]["nfa"] = {"error": str(resp)}
                else:
                    results[country]["nfa"] = resp

            if include_credit and i < len(credit_responses):
                resp = credit_responses[i]
                if isinstance(resp, Exception):
                    results[country]["credit"] = {"error": str(resp)}
                else:
                    results[country]["credit"] = resp

        return results


# ============================================================================
# IMF ASYNC
# ============================================================================

async def get_imf_indicator_async(indicator: str, country: str, start_year: Optional[int] = None,
                                   end_year: Optional[int] = None, analyze: bool = False,
                                   client: "httpx.AsyncClient" = None) -> Dict[str, Any]:
    """Async version of get_imf_indicator."""
    if not HTTPX_AVAILABLE:
        return get_imf_indicator(indicator, country, start_year, end_year, analyze)

    url = f"{MCP_URLS['imf']}/indicator/{indicator}"
    params = {"country": country}
    if start_year:
        params["start_year"] = start_year
    if end_year:
        params["end_year"] = end_year
    if analyze:
        params["analyze"] = "true"

    if client:
        return await _async_get(client, url, params)
    else:
        async with httpx.AsyncClient() as new_client:
            return await _async_get(new_client, url, params)


async def compare_imf_countries_async(indicator: str, countries: List[str],
                                       year: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """
    Compare IMF indicator across countries concurrently.

    Args:
        indicator: IMF indicator code
        countries: List of country names
        year: Optional specific year

    Returns:
        Dict mapping country -> indicator data
    """
    if not HTTPX_AVAILABLE:
        return compare_imf_countries(indicator, countries, year)

    async with httpx.AsyncClient() as client:
        tasks = [get_imf_indicator_async(indicator, c, year, year, client=client) for c in countries]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        results = {}
        for country, response in zip(countries, responses):
            if isinstance(response, Exception):
                results[country] = {"error": str(response)}
            else:
                results[country] = response

        return results


# ============================================================================
# WORLD BANK ASYNC
# ============================================================================

async def get_worldbank_indicator_async(indicator: str, country: str,
                                         start_year: Optional[int] = None,
                                         end_year: Optional[int] = None,
                                         client: "httpx.AsyncClient" = None) -> Dict[str, Any]:
    """Async version of get_worldbank_indicator."""
    if not HTTPX_AVAILABLE:
        return get_worldbank_indicator(indicator, country, start_year, end_year)

    # Convert country name to ISO-3 code
    iso_code = _country_to_iso3(country, api="worldbank")

    url = f"{MCP_URLS['worldbank']}/mcp/tools/call"
    payload = {
        "name": "wb_indicator_data",
        "arguments": {
            "indicator_id": indicator,
            "countries": [iso_code]
        }
    }
    if start_year:
        payload["arguments"]["start_year"] = str(start_year)
    if end_year:
        payload["arguments"]["end_year"] = str(end_year)

    if client:
        return await _async_post(client, url, payload)
    else:
        async with httpx.AsyncClient() as new_client:
            return await _async_post(new_client, url, payload)


async def get_worldbank_country_profile_async(country: str) -> Dict[str, Any]:
    """
    Async version of get_worldbank_country_profile.
    Fetches all key indicators in parallel.
    """
    if not HTTPX_AVAILABLE:
        return get_worldbank_country_profile(country)

    iso_code = _country_to_iso3(country, api="worldbank")

    key_indicators = [
        "NY.GDP.PCAP.CD",  # GDP per capita
        "SP.POP.TOTL",     # Population
        "SI.POV.DDAY",     # Poverty headcount
        "SP.DYN.LE00.IN",  # Life expectancy
    ]

    async with httpx.AsyncClient() as client:
        tasks = [get_worldbank_indicator_async(ind, country, client=client) for ind in key_indicators]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        results = {"country": country, "iso_code": iso_code, "indicators": {}}
        for ind, response in zip(key_indicators, responses):
            if isinstance(response, Exception):
                results["indicators"][ind] = {"error": str(response)}
            elif "error" not in response:
                results["indicators"][ind] = response

        return results


# ============================================================================
# SUPABASE MCP - Portfolio Data Gateway
# ============================================================================

def _call_supabase_mcp(tool: str, args: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Call a Supabase MCP tool.

    Args:
        tool: Tool name (e.g., 'get_holdings_display', 'query')
        args: Tool arguments

    Returns:
        Tool result or error dict
    """
    try:
        url = f"{_get_supabase_url()}/call"
        payload = {"tool": tool, "args": args or {}}
        response = _post(url, json_data=payload)
        response.raise_for_status()
        result = response.json()

        if result.get("success"):
            return result.get("result")
        return {"error": result.get("error", "Unknown error")}
    except Exception as e:
        logger.error(f"Supabase MCP error calling {tool}: {e}")
        return {"error": str(e)}


def get_supabase_holdings(portfolio_id: str) -> Dict[str, Any]:
    """
    Get portfolio holdings via Supabase MCP.

    Args:
        portfolio_id: Portfolio identifier (e.g., 'wnbf')

    Returns:
        Holdings with weights and totals
    """
    return _call_supabase_mcp("get_holdings_display", {"portfolio_id": portfolio_id})


def get_supabase_transactions(
    portfolio_id: str,
    status: str = None,
    limit: int = None,
) -> Dict[str, Any]:
    """
    Get portfolio transactions via Supabase MCP.

    Args:
        portfolio_id: Portfolio identifier
        status: Filter by status (settled, pending, staging)
        limit: Max transactions to return

    Returns:
        Formatted transaction list
    """
    args = {"portfolio_id": portfolio_id}
    if status:
        args["status"] = status
    if limit:
        args["limit"] = limit
    return _call_supabase_mcp("get_transactions_display", args)


def get_supabase_portfolio_summary(portfolio_id: str) -> Dict[str, Any]:
    """
    Get portfolio summary via Supabase MCP.

    Args:
        portfolio_id: Portfolio identifier

    Returns:
        Summary with aggregated metrics
    """
    return _call_supabase_mcp("get_portfolio_summary", {"portfolio_id": portfolio_id})


def get_supabase_dashboard(portfolio_id: str) -> Dict[str, Any]:
    """
    Get full dashboard data via Supabase MCP.

    Args:
        portfolio_id: Portfolio identifier

    Returns:
        Dashboard with summary and allocations
    """
    return _call_supabase_mcp("get_portfolio_dashboard", {"portfolio_id": portfolio_id})


def get_supabase_watchlist(portfolio_id: str = None) -> List[Dict[str, Any]]:
    """
    Get watchlist via Supabase MCP.

    Args:
        portfolio_id: Optional portfolio filter

    Returns:
        Watchlist items with bond details
    """
    args = {}
    if portfolio_id:
        args["portfolio_id"] = portfolio_id
    return _call_supabase_mcp("get_watchlist", args)


def add_to_supabase_watchlist(
    isin: str,
    portfolio_id: str = None,
    notes: str = None,
) -> Dict[str, Any]:
    """
    Add a bond to watchlist via Supabase MCP.

    Args:
        isin: Bond ISIN
        portfolio_id: Optional portfolio to associate
        notes: Optional notes

    Returns:
        Success status and created item
    """
    args = {"isin": isin}
    if portfolio_id:
        args["portfolio_id"] = portfolio_id
    if notes:
        args["notes"] = notes
    return _call_supabase_mcp("add_to_watchlist", args)


def remove_from_supabase_watchlist(isin: str, portfolio_id: str = None) -> Dict[str, Any]:
    """
    Remove a bond from watchlist via Supabase MCP.

    Args:
        isin: Bond ISIN
        portfolio_id: Optional portfolio filter

    Returns:
        Success status and deletion count
    """
    args = {"isin": isin}
    if portfolio_id:
        args["portfolio_id"] = portfolio_id
    return _call_supabase_mcp("remove_from_watchlist", args)


def query_supabase(
    table: str,
    select: str = "*",
    filters: Dict[str, str] = None,
    order: str = None,
    limit: int = None,
) -> List[Dict[str, Any]]:
    """
    Generic Supabase query via Supabase MCP.

    Args:
        table: Table name
        select: Columns to select (supports joins like '*, bonds(*)')
        filters: PostgREST filters (e.g., {'status': 'eq.active'})
        order: Order clause (e.g., 'created_at.desc')
        limit: Max rows

    Returns:
        Query results
    """
    args = {"table": table, "select": select}
    if filters:
        args["filters"] = filters
    if order:
        args["order"] = order
    if limit:
        args["limit"] = limit
    return _call_supabase_mcp("query", args)


# ============================================================================
# SUPABASE MCP - ASYNC VERSIONS (for parallel calls)
# ============================================================================

async def _call_supabase_mcp_async(
    tool: str,
    args: Dict[str, Any] = None,
    client: "httpx.AsyncClient" = None,
) -> Dict[str, Any]:
    """Async version of _call_supabase_mcp."""
    if not HTTPX_AVAILABLE:
        return _call_supabase_mcp(tool, args)

    try:
        url = f"{_get_supabase_url()}/call"
        payload = {"tool": tool, "args": args or {}}

        if client:
            resp = await client.post(url, json=payload, timeout=TIMEOUT)
        else:
            async with httpx.AsyncClient() as new_client:
                resp = await new_client.post(url, json=payload, timeout=TIMEOUT)

        resp.raise_for_status()
        result = resp.json()

        if result.get("success"):
            return result.get("result")
        return {"error": result.get("error", "Unknown error")}
    except Exception as e:
        logger.error(f"Supabase MCP async error calling {tool}: {e}")
        return {"error": str(e)}


async def get_supabase_holdings_async(
    portfolio_id: str,
    client: "httpx.AsyncClient" = None,
) -> Dict[str, Any]:
    """Async version of get_supabase_holdings."""
    return await _call_supabase_mcp_async(
        "get_holdings_display",
        {"portfolio_id": portfolio_id},
        client,
    )


async def get_supabase_portfolio_summary_async(
    portfolio_id: str,
    client: "httpx.AsyncClient" = None,
) -> Dict[str, Any]:
    """Async version of get_supabase_portfolio_summary."""
    return await _call_supabase_mcp_async(
        "get_portfolio_summary",
        {"portfolio_id": portfolio_id},
        client,
    )


async def get_supabase_dashboard_async(
    portfolio_id: str,
    client: "httpx.AsyncClient" = None,
) -> Dict[str, Any]:
    """Async version of get_supabase_dashboard."""
    return await _call_supabase_mcp_async(
        "get_portfolio_dashboard",
        {"portfolio_id": portfolio_id},
        client,
    )


async def get_portfolio_with_ratings_async(portfolio_id: str) -> Dict[str, Any]:
    """
    Get portfolio data with country ratings - ALL IN PARALLEL.

    This fires all requests simultaneously:
    - Holdings from Supabase
    - NFA ratings for each country
    - Credit ratings for each country

    Example speedup:
        Sequential: 500ms + (10 countries × 200ms × 2 ratings) = 4.5 seconds
        Parallel:   ~500ms total (all at once)
    """
    if not HTTPX_AVAILABLE:
        # Fallback to sequential
        holdings = get_supabase_holdings(portfolio_id)
        if "error" in holdings:
            return holdings
        countries = list(set(h.get("country") for h in holdings.get("holdings", []) if h.get("country")))
        ratings = {}
        for c in countries:
            ratings[c] = {
                "nfa": get_nfa_rating(c),
                "credit": get_credit_rating(c),
            }
        return {"holdings": holdings, "ratings": ratings}

    async with httpx.AsyncClient() as client:
        # Step 1: Get holdings
        holdings = await get_supabase_holdings_async(portfolio_id, client)
        if "error" in holdings:
            return holdings

        # Step 2: Extract unique countries
        countries = list(set(
            h.get("country") for h in holdings.get("holdings", [])
            if h.get("country")
        ))

        # Step 3: Fire ALL rating requests in parallel
        nfa_tasks = [get_nfa_rating_async(c, client=client) for c in countries]
        credit_tasks = [get_credit_rating_async(c, client=client) for c in countries]

        all_results = await asyncio.gather(
            *nfa_tasks, *credit_tasks,
            return_exceptions=True
        )

        # Split results
        nfa_results = all_results[:len(countries)]
        credit_results = all_results[len(countries):]

        # Build ratings dict
        ratings = {}
        for i, country in enumerate(countries):
            ratings[country] = {
                "nfa": nfa_results[i] if not isinstance(nfa_results[i], Exception) else {"error": str(nfa_results[i])},
                "credit": credit_results[i] if not isinstance(credit_results[i], Exception) else {"error": str(credit_results[i])},
            }

        return {
            "portfolio_id": portfolio_id,
            "holdings": holdings,
            "ratings": ratings,
        }


# ============================================================================
# Reasoning MCP ASYNC
# ============================================================================

async def call_reasoning_async(
    query: str,
    portfolio_context: Optional[Dict[str, Any]] = None,
    require_compliance: bool = False,
    client: "httpx.AsyncClient" = None
) -> Dict[str, Any]:
    """
    Async version of call_reasoning.

    Args:
        query: Natural language query or analysis request
        portfolio_context: Optional portfolio data to analyze
        require_compliance: If True, suggestions pass compliance checks
        client: Optional httpx.AsyncClient for connection pooling
    """
    if not HTTPX_AVAILABLE:
        return call_reasoning(query, portfolio_context, require_compliance)

    url = f"{MCP_URLS['reasoning']}/api/reason"
    payload = {
        "query": query,
        "require_compliance": require_compliance
    }
    if portfolio_context:
        payload["portfolio_context"] = json.dumps(portfolio_context)

    if client:
        return await _async_post(client, url, json_data=payload, timeout=60)
    else:
        async with httpx.AsyncClient() as new_client:
            return await _async_post(new_client, url, json_data=payload, timeout=60)


async def analyze_data_async(
    data: Any,
    objective: str,
    require_compliance: bool = False,
    client: "httpx.AsyncClient" = None
) -> Dict[str, Any]:
    """
    Async version of analyze_data.

    Args:
        data: The data to analyze
        objective: What kind of analysis to perform
        require_compliance: If True, suggestions pass compliance checks
        client: Optional httpx.AsyncClient for connection pooling
    """
    query = f"{objective}\n\nData to analyze:\n{json.dumps(data, indent=2, default=str)}"
    return await call_reasoning_async(query, require_compliance=require_compliance, client=client)


# ============================================================================
# Funds MCP - Fund Prospectus & Application Management
# ============================================================================

def _get_funds_mcp_url() -> str:
    """Get Funds MCP URL from auth_mcp or environment."""
    if AUTH_CLIENT_AVAILABLE and get_api_key:
        url = get_api_key("FUNDS_MCP_URL", fallback_env=False, requester="orca-mcp")
        if url:
            return url
    return os.environ.get("FUNDS_MCP_URL", "http://localhost:8002")


# Lazy-loaded URL
_funds_mcp_url_cache = None


def _get_funds_url() -> str:
    """Get cached Funds MCP URL."""
    global _funds_mcp_url_cache
    if _funds_mcp_url_cache is None:
        _funds_mcp_url_cache = _get_funds_mcp_url()
        logger.info(f"Funds MCP URL: {_funds_mcp_url_cache}")
    return _funds_mcp_url_cache


def _call_funds_mcp(tool: str, args: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Call a Funds MCP tool.

    Args:
        tool: Tool name (e.g., 'list_form_templates', 'start_form_session')
        args: Tool arguments

    Returns:
        Tool result or error dict
    """
    try:
        url = f"{_get_funds_url()}/mcp/tools/call"
        payload = {"name": tool, "arguments": args or {}}
        response = _post(url, json_data=payload)
        response.raise_for_status()
        result = response.json()

        # Parse MCP content format
        if "content" in result and result["content"]:
            text_content = result["content"][0].get("text", "")
            try:
                return json.loads(text_content)
            except json.JSONDecodeError:
                return {"text": text_content}

        return result
    except Exception as e:
        logger.error(f"Funds MCP error calling {tool}: {e}")
        return {"error": str(e)}


def list_fund_templates() -> Dict[str, Any]:
    """
    List available fund prospectus templates.

    Returns:
        List of templates with template_id, name, description, field_count
    """
    return _call_funds_mcp("list_form_templates")


def get_fund_template_fields(template_id: str) -> Dict[str, Any]:
    """
    Get detailed field information for a fund template.

    Args:
        template_id: The template identifier

    Returns:
        Template details with fields, their types, and requirements
    """
    return _call_funds_mcp("get_form_fields", {"template_id": template_id})


def start_fund_application(template_id: str) -> Dict[str, Any]:
    """
    Start a new fund application session.

    Args:
        template_id: The fund template to use

    Returns:
        Session info with session_id, first field to fill, progress
    """
    return _call_funds_mcp("start_form_session", {"template_id": template_id})


def submit_fund_answer(session_id: str, field_id: str, value: str) -> Dict[str, Any]:
    """
    Submit an answer to a fund application field.

    Args:
        session_id: The active session ID
        field_id: The field being answered
        value: The answer value

    Returns:
        Validation result, next field, updated progress
    """
    return _call_funds_mcp("answer_form_question", {
        "session_id": session_id,
        "field_id": field_id,
        "value": value
    })


def get_fund_session_status(session_id: str) -> Dict[str, Any]:
    """
    Get current status of a fund application session.

    Args:
        session_id: The session to check

    Returns:
        Session status, collected values, remaining fields, progress
    """
    return _call_funds_mcp("get_session_status", {"session_id": session_id})


def review_fund_application(session_id: str) -> Dict[str, Any]:
    """
    Review all answers before submitting a fund application.

    Args:
        session_id: The session to review

    Returns:
        All collected values organized by section
    """
    return _call_funds_mcp("review_form", {"session_id": session_id})


def generate_fund_pdf(session_id: str, flatten: bool = True) -> Dict[str, Any]:
    """
    Generate a filled PDF for a fund application.

    Args:
        session_id: The completed session
        flatten: If True, make PDF fields non-editable

    Returns:
        Path to generated PDF, base64 content if requested
    """
    return _call_funds_mcp("generate_filled_pdf", {
        "session_id": session_id,
        "flatten": flatten
    })


def send_fund_for_signature(
    session_id: str,
    signer_email: str,
    signer_name: str
) -> Dict[str, Any]:
    """
    Send a fund application for electronic signature via DocuSign.

    Args:
        session_id: The session with completed form
        signer_email: Email of the person signing
        signer_name: Name of the signer

    Returns:
        DocuSign envelope_id, signing_url, status
    """
    return _call_funds_mcp("send_for_signature", {
        "session_id": session_id,
        "signer_email": signer_email,
        "signer_name": signer_name
    })


def check_fund_signature_status(session_id: str) -> Dict[str, Any]:
    """
    Check the signature status of a fund application.

    Args:
        session_id: The session to check

    Returns:
        Signature status (pending, signed, declined), timestamps
    """
    return _call_funds_mcp("check_signature_status", {"session_id": session_id})


def send_fund_application_email(
    session_id: str,
    recipient_email: str,
    cc_emails: List[str] = None,
    subject: str = None
) -> Dict[str, Any]:
    """
    Email the completed fund application.

    Args:
        session_id: The session with completed/signed form
        recipient_email: Primary recipient
        cc_emails: Optional CC recipients
        subject: Optional custom subject line

    Returns:
        Email send status, message_id
    """
    args = {
        "session_id": session_id,
        "recipient_email": recipient_email
    }
    if cc_emails:
        args["cc_emails"] = cc_emails
    if subject:
        args["subject"] = subject

    return _call_funds_mcp("send_completed_form", args)


def list_fund_sessions(status: str = None) -> Dict[str, Any]:
    """
    List fund application sessions.

    Args:
        status: Optional filter (started, in_progress, fields_complete,
                pending_signature, signed, submitted)

    Returns:
        List of sessions with their current status
    """
    args = {}
    if status:
        args["status"] = status
    return _call_funds_mcp("list_sessions", args)


# ============================================================================
# Funds MCP - ASYNC VERSIONS
# ============================================================================

async def _call_funds_mcp_async(
    tool: str,
    args: Dict[str, Any] = None,
    client: "httpx.AsyncClient" = None,
) -> Dict[str, Any]:
    """Async version of _call_funds_mcp."""
    if not HTTPX_AVAILABLE:
        return _call_funds_mcp(tool, args)

    try:
        url = f"{_get_funds_url()}/mcp/tools/call"
        payload = {"name": tool, "arguments": args or {}}

        if client:
            resp = await client.post(url, json=payload, headers=_get_auth_headers(), timeout=TIMEOUT)
        else:
            async with httpx.AsyncClient() as new_client:
                resp = await new_client.post(url, json=payload, headers=_get_auth_headers(), timeout=TIMEOUT)

        resp.raise_for_status()
        result = resp.json()

        if "content" in result and result["content"]:
            text_content = result["content"][0].get("text", "")
            try:
                return json.loads(text_content)
            except json.JSONDecodeError:
                return {"text": text_content}

        return result
    except Exception as e:
        logger.error(f"Funds MCP async error calling {tool}: {e}")
        return {"error": str(e)}


async def list_fund_templates_async(
    client: "httpx.AsyncClient" = None
) -> Dict[str, Any]:
    """Async version of list_fund_templates."""
    return await _call_funds_mcp_async("list_form_templates", client=client)


async def start_fund_application_async(
    template_id: str,
    client: "httpx.AsyncClient" = None
) -> Dict[str, Any]:
    """Async version of start_fund_application."""
    return await _call_funds_mcp_async(
        "start_form_session",
        {"template_id": template_id},
        client
    )


async def submit_fund_answer_async(
    session_id: str,
    field_id: str,
    value: str,
    client: "httpx.AsyncClient" = None
) -> Dict[str, Any]:
    """Async version of submit_fund_answer."""
    return await _call_funds_mcp_async(
        "answer_form_question",
        {"session_id": session_id, "field_id": field_id, "value": value},
        client
    )


async def get_fund_session_status_async(
    session_id: str,
    client: "httpx.AsyncClient" = None
) -> Dict[str, Any]:
    """Async version of get_fund_session_status."""
    return await _call_funds_mcp_async(
        "get_session_status",
        {"session_id": session_id},
        client
    )
