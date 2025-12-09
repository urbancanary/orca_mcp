"""
IMF Gateway for Orca MCP

Generic gateway to IMF DataMapper API and IMF MCP.
Provides flexible access to ANY IMF indicator for ANY country/region.
"""

import requests
from typing import Dict, List, Optional, Any, Union
import os

# IMF endpoints
IMF_MCP_ENDPOINT = "https://imf-mcp.urbancanary.workers.dev"
IMF_DATAMAPPER_BASE = "https://www.imf.org/external/datamapper/api/v1"
COUNTRY_MAPPING_ENDPOINT = "https://country-mapping-mcp.urbancanary.workers.dev"

# Auth token
MCP_AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN", "")

# IMF Indicator mappings (from @isla)
INDICATOR_MAPPINGS = {
    "gdp_growth": "NGDP_RPCH",
    "gdp_per_capita": "NGDPPC",
    "inflation": "PCPIPCH",
    "unemployment": "LUR",
    "current_account": "BCA",
    "fiscal_deficit": "GGXCNL_NGDP",
    "government_debt": "GGXWDG_NGDP",
}

# Reverse mapping
INDICATOR_NAMES = {
    "NGDP_RPCH": "Real GDP Growth",
    "NGDPPC": "GDP per Capita",
    "PCPIPCH": "Inflation Rate",
    "LUR": "Unemployment Rate",
    "BCA": "Current Account Balance",
    "GGXCNL_NGDP": "Fiscal Deficit",
    "GGXWDG_NGDP": "Government Debt to GDP",
}

INDICATOR_UNITS = {
    "NGDP_RPCH": "Annual percent change",
    "NGDPPC": "U.S. dollars per capita",
    "PCPIPCH": "Annual percent change",
    "LUR": "Percent",
    "BCA": "Billions of U.S. dollars",
    "GGXCNL_NGDP": "Percent of GDP",
    "GGXWDG_NGDP": "Percent of GDP",
}

# Country groups
COUNTRY_GROUPS = {
    "G7": ["USA", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN"],
    "G20": ["USA", "CHN", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN", "BRA", "IND",
            "RUS", "AUS", "KOR", "MEX", "IDN", "TUR", "SAU", "ARG", "ZAF"],
    "EU": ["DEU", "FRA", "ITA", "ESP", "NLD", "BEL", "AUT", "PRT", "GRC", "FIN"],
    "BRICS": ["BRA", "RUS", "IND", "CHN", "ZAF"],
    "ASEAN": ["IDN", "THA", "MYS", "SGP", "PHL", "VNM", "MMR", "KHM", "LAO", "BRN"],
}


def normalize_indicator(indicator: str) -> str:
    """
    Convert user-friendly indicator name to IMF code

    Args:
        indicator: User input like "debt", "gdp growth", or direct code "GGXWDG_NGDP"

    Returns:
        IMF indicator code (e.g., "GGXWDG_NGDP")
    """
    # If already an IMF code, return as-is
    if indicator.upper() in INDICATOR_NAMES:
        return indicator.upper()

    # Try mapping
    normalized = indicator.lower().strip().replace("-", "_").replace(" ", "_")

    # Direct match
    if normalized in INDICATOR_MAPPINGS:
        return INDICATOR_MAPPINGS[normalized]

    # Partial matches
    for key, code in INDICATOR_MAPPINGS.items():
        if normalized in key or key in normalized:
            return code

    # Return original (let IMF API handle error)
    return indicator.upper()


def get_country_iso_code(country: str) -> Optional[str]:
    """Convert country name to ISO 3-letter code"""
    try:
        # If already 3-letter code, return uppercase
        if len(country) == 3:
            return country.upper()

        response = requests.get(
            f"{COUNTRY_MAPPING_ENDPOINT}/map/{country}",
            params={"api": "iso"},
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            return data.get("iso_code_3", data.get("iso_code"))

        return None

    except Exception as e:
        print(f"Error mapping country '{country}': {e}")
        return None


def expand_country_group(countries: Union[str, List[str]]) -> List[str]:
    """
    Expand country groups (G7, G20, etc.) to individual country codes

    Args:
        countries: Single country, list, or group name (e.g., "G7", ["USA", "JPN"])

    Returns:
        List of ISO 3-letter country codes
    """
    if isinstance(countries, str):
        # Check if it's a group
        group_upper = countries.upper()
        if group_upper in COUNTRY_GROUPS:
            return COUNTRY_GROUPS[group_upper]
        # Single country
        return [countries]

    # Already a list
    expanded = []
    for country in countries:
        group_upper = country.upper()
        if group_upper in COUNTRY_GROUPS:
            expanded.extend(COUNTRY_GROUPS[group_upper])
        else:
            expanded.append(country)

    return expanded


def fetch_imf_data(
    indicator: str,
    countries: Union[str, List[str]],
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    use_mcp: bool = False
) -> Dict[str, Any]:
    """
    Generic gateway to fetch ANY IMF indicator for ANY country/countries

    Args:
        indicator: Indicator name (e.g., "debt", "gdp_growth") or code (e.g., "GGXWDG_NGDP")
        countries: Country name(s), ISO code(s), or group (e.g., "G7", ["USA", "JPN"])
        start_year: Optional start year (default: 2010)
        end_year: Optional end year (default: latest)
        use_mcp: If True, use IMF MCP with AI analysis. If False, use direct DataMapper API.

    Returns:
        Dict with IMF data for requested indicator and countries

    Examples:
        >>> fetch_imf_data("debt", "G7")
        >>> fetch_imf_data("NGDP_RPCH", ["USA", "CHN"], start_year=2020)
        >>> fetch_imf_data("inflation", "Germany", use_mcp=True)
    """
    # Normalize indicator
    indicator_code = normalize_indicator(indicator)

    # Expand country groups
    country_list = expand_country_group(countries)

    # Convert country names to ISO codes
    iso_codes = []
    for country in country_list:
        if len(country) == 3:
            iso_codes.append(country.upper())
        else:
            iso_code = get_country_iso_code(country)
            if iso_code:
                iso_codes.append(iso_code)

    if not iso_codes:
        return {
            "error": "Could not map any countries to ISO codes",
            "input_countries": countries
        }

    # Set date range
    start_year = start_year or 2010
    end_year = end_year or 2030  # IMF includes projections

    # Choose fetch method
    if use_mcp:
        return _fetch_via_imf_mcp(indicator_code, iso_codes, start_year, end_year)
    else:
        return _fetch_via_datamapper(indicator_code, iso_codes, start_year, end_year)


def _fetch_via_datamapper(
    indicator_code: str,
    country_codes: List[str],
    start_year: int,
    end_year: int
) -> Dict[str, Any]:
    """
    Fetch data directly from IMF DataMapper API (fast, no AI analysis)
    """
    try:
        # Build URL
        countries_str = ",".join(country_codes)
        url = f"{IMF_DATAMAPPER_BASE}/{indicator_code}"

        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            return {
                "error": f"IMF DataMapper API returned status {response.status_code}",
                "indicator": indicator_code,
                "countries": country_codes
            }

        data = response.json()

        # Extract time series data
        indicator_data = data.get("values", {}).get(indicator_code, {})

        # Parse results
        results = {}
        for country_code in country_codes:
            country_data = indicator_data.get(country_code, {})

            # Filter by year range
            filtered_data = {
                year: value
                for year, value in country_data.items()
                if start_year <= int(year) <= end_year
            }

            # Get latest value
            sorted_years = sorted(filtered_data.keys(), reverse=True)
            latest_value = filtered_data.get(sorted_years[0]) if sorted_years else None
            latest_year = sorted_years[0] if sorted_years else None

            results[country_code] = {
                "latest_value": latest_value,
                "latest_year": latest_year,
                "time_series": filtered_data,
                "data_points": len(filtered_data)
            }

        return {
            "indicator": indicator_code,
            "indicator_name": INDICATOR_NAMES.get(indicator_code, indicator_code),
            "unit": INDICATOR_UNITS.get(indicator_code, ""),
            "countries": results,
            "source": "IMF DataMapper API",
            "method": "direct",
            "year_range": [start_year, end_year]
        }

    except Exception as e:
        return {
            "error": f"Failed to fetch from IMF DataMapper: {str(e)}",
            "indicator": indicator_code,
            "countries": country_codes
        }


def _fetch_via_imf_mcp(
    indicator_code: str,
    country_codes: List[str],
    start_year: int,
    end_year: int
) -> Dict[str, Any]:
    """
    Fetch data via IMF MCP (slower, includes AI analysis via Claude Haiku)
    """
    # Map indicator code to IMF MCP tool name
    tool_mapping = {
        "NGDP_RPCH": "imf_gdp",
        "GGXWDG_NGDP": "imf_debt",
        "PCPIPCH": "imf_inflation",
        "LUR": "imf_unemployment",
        "BCA": "imf_current_account",
        "GGXCNL_NGDP": "imf_fiscal_deficit",
        "NGDPPC": "imf_gdp_per_capita",
    }

    tool_name = tool_mapping.get(indicator_code)

    if not tool_name:
        return {
            "error": f"IMF MCP does not support indicator: {indicator_code}",
            "supported_indicators": list(tool_mapping.keys())
        }

    # Fetch data for each country
    results = {}

    for country_code in country_codes:
        try:
            headers = {}
            if MCP_AUTH_TOKEN:
                headers["Authorization"] = f"Bearer {MCP_AUTH_TOKEN}"

            response = requests.post(
                f"{IMF_MCP_ENDPOINT}/mcp/tools/call",
                json={
                    "name": tool_name,
                    "arguments": {"country": country_code}
                },
                headers=headers,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                results[country_code] = result
            else:
                results[country_code] = {
                    "error": f"IMF MCP returned status {response.status_code}"
                }

        except Exception as e:
            results[country_code] = {
                "error": f"Failed to fetch: {str(e)}"
            }

    return {
        "indicator": indicator_code,
        "indicator_name": INDICATOR_NAMES.get(indicator_code, indicator_code),
        "unit": INDICATOR_UNITS.get(indicator_code, ""),
        "countries": results,
        "source": "IMF MCP (with AI analysis)",
        "method": "mcp",
        "tool_used": tool_name
    }


def get_available_indicators() -> Dict[str, Any]:
    """
    List all available IMF indicators

    Returns:
        Dict with indicator codes, names, and units
    """
    return {
        "indicators": [
            {
                "code": code,
                "name": name,
                "unit": INDICATOR_UNITS.get(code, ""),
                "aliases": [k for k, v in INDICATOR_MAPPINGS.items() if v == code]
            }
            for code, name in INDICATOR_NAMES.items()
        ],
        "total": len(INDICATOR_NAMES)
    }


def get_available_country_groups() -> Dict[str, Any]:
    """
    List all available country groups

    Returns:
        Dict with group names and member countries
    """
    return {
        "groups": {
            group: {
                "members": codes,
                "count": len(codes)
            }
            for group, codes in COUNTRY_GROUPS.items()
        },
        "total": len(COUNTRY_GROUPS)
    }
