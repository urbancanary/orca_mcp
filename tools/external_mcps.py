"""
External MCP Gateway - Wrappers for external Cloudflare Worker MCPs

These tools provide access to external MCP services via HTTP:
- NFA MCP: Net Foreign Assets star ratings
- Rating MCP: Sovereign credit ratings
- Country Mapping MCP: Country name standardization
- FRED MCP: Federal Reserve economic data
- Sovereign Classification MCP: Issuer type classification
- IMF MCP: IMF economic indicators (with AI analysis)
- World Bank MCP: World Bank development indicators

Authentication:
- Self-validating tokens generated on the fly
- Token format: {random}-{SHA256(random)[:8]}
- No secrets, no storage - just math
"""

import hashlib
import secrets
import requests
import logging
import json
import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger("orca-mcp.external")

# MCP Service URLs
MCP_URLS = {
    "nfa": "https://nfa-mcp.urbancanary.workers.dev",
    "rating": "https://rating-mcp.urbancanary.workers.dev",
    "country_mapping": "https://country-mapping-mcp.urbancanary.workers.dev",
    "fred": "https://fred-mcp.urbancanary.workers.dev",
    "sovereign_classification": "https://sovereign-classification-mcp.urbancanary.workers.dev",
    "imf": "https://imf-mcp.urbancanary.workers.dev",
    "worldbank": "https://worldbank-mcp.urbancanary.workers.dev",
}

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
        url = f"{MCP_URLS['country_mapping']}/standardize/{country}"
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
        year = datetime.now().year
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
