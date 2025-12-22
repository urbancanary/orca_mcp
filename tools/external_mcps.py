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
"""

import requests
import logging
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

        response = requests.get(url, params=params, timeout=TIMEOUT)
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

        response = requests.post(url, json=payload, timeout=TIMEOUT)
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

        response = requests.get(url, params=params, timeout=TIMEOUT)
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
        response = requests.get(url, timeout=TIMEOUT)
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
        response = requests.post(url, json={"countries": countries}, timeout=TIMEOUT)
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
        response = requests.get(url, timeout=TIMEOUT)
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
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Country Mapping info error: {e}")
        return {"error": str(e), "input": country}


# ============================================================================
# FRED MCP - Federal Reserve Economic Data
# ============================================================================

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
    try:
        url = f"{MCP_URLS['fred']}/series/{series_id}"
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if analyze:
            params["analyze"] = "true"

        response = requests.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"FRED MCP error: {e}")
        return {"error": str(e), "series_id": series_id}


def search_fred_series(query: str) -> Dict[str, Any]:
    """
    Search for FRED series by keyword.

    Args:
        query: Search term (e.g., 'treasury', 'inflation', 'unemployment')

    Returns:
        List of matching series with IDs and descriptions
    """
    try:
        url = f"{MCP_URLS['fred']}/search"
        params = {"q": query}
        response = requests.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"FRED MCP search error: {e}")
        return {"error": str(e)}


def get_treasury_rates() -> Dict[str, Any]:
    """
    Get current US Treasury rates across the curve.

    Returns:
        Dict with rates for 1M, 3M, 6M, 1Y, 2Y, 5Y, 10Y, 30Y
    """
    try:
        url = f"{MCP_URLS['fred']}/treasury/rates"
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"FRED Treasury rates error: {e}")
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
        response = requests.get(url, timeout=TIMEOUT)
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
        response = requests.post(url, json={"isins": isins}, timeout=TIMEOUT)
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
        response = requests.get(url, params=params, timeout=TIMEOUT)
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
        response = requests.get(url, timeout=TIMEOUT)
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

        response = requests.get(url, params=params, timeout=TIMEOUT)
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

        response = requests.post(url, json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"IMF MCP compare error: {e}")
        return {"error": str(e)}


# ============================================================================
# World Bank MCP - Development Indicators
# ============================================================================

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
    try:
        url = f"{MCP_URLS['worldbank']}/indicator/{indicator}"
        params = {"country": country}
        if start_year:
            params["start_year"] = start_year
        if end_year:
            params["end_year"] = end_year

        response = requests.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"World Bank MCP error: {e}")
        return {"error": str(e)}


def search_worldbank_indicators(query: str) -> Dict[str, Any]:
    """
    Search for World Bank indicators by keyword.

    Args:
        query: Search term (e.g., 'gdp', 'population', 'education')

    Returns:
        List of matching indicators with codes and descriptions
    """
    try:
        url = f"{MCP_URLS['worldbank']}/search"
        params = {"q": query}
        response = requests.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"World Bank search error: {e}")
        return {"error": str(e)}


def get_worldbank_country_profile(country: str) -> Dict[str, Any]:
    """
    Get comprehensive country profile from World Bank.

    Args:
        country: Country name or ISO code

    Returns:
        Dict with key development indicators for the country
    """
    try:
        url = f"{MCP_URLS['worldbank']}/country/{country}"
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"World Bank country profile error: {e}")
        return {"error": str(e), "country": country}
