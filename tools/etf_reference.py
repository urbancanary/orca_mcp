#!/usr/bin/env python3
"""
ETF Reference Data - Country Allocations for common ETFs

Static reference data based on MSCI index weights.
This is not CBonds data - it's derived from MSCI factsheets.
"""

from typing import Optional

# ETF Country Allocation Data
# Source: MSCI Index factsheets, justETF, iShares (as of Nov 2025)
ETF_ALLOCATIONS = {
    "IE00B0M62Q58": {
        "name": "iShares MSCI World UCITS ETF (Dist)",
        "index": "MSCI World",
        "ter": 0.50,
        "aum_eur_m": 7489,
        "allocation": [
            {"country": "United States", "weight_pct": 71.86},
            {"country": "Japan", "weight_pct": 5.43},
            {"country": "United Kingdom", "weight_pct": 3.65},
            {"country": "France", "weight_pct": 2.89},
            {"country": "Canada", "weight_pct": 2.88},
            {"country": "Switzerland", "weight_pct": 2.35},
            {"country": "Germany", "weight_pct": 2.21},
            {"country": "Australia", "weight_pct": 1.78},
            {"country": "Netherlands", "weight_pct": 1.13},
            {"country": "Denmark", "weight_pct": 0.88},
            {"country": "Sweden", "weight_pct": 0.82},
            {"country": "Spain", "weight_pct": 0.63},
            {"country": "Italy", "weight_pct": 0.62},
            {"country": "Hong Kong", "weight_pct": 0.55},
            {"country": "Singapore", "weight_pct": 0.41},
            {"country": "Other", "weight_pct": 1.91}
        ]
    },
    "IE00B1XNHC34": {
        "name": "iShares Global Clean Energy UCITS ETF (Dist)",
        "index": "S&P Global Clean Energy",
        "ter": 0.65,
        "allocation": [
            {"country": "United States", "weight_pct": 40.5},
            {"country": "China", "weight_pct": 12.3},
            {"country": "Denmark", "weight_pct": 9.8},
            {"country": "Spain", "weight_pct": 5.6},
            {"country": "Germany", "weight_pct": 4.2},
            {"country": "Japan", "weight_pct": 3.8},
            {"country": "India", "weight_pct": 3.5},
            {"country": "Other", "weight_pct": 20.3}
        ]
    },
    "IE00BYX2JD69": {
        "name": "iShares MSCI World SRI UCITS ETF EUR (Acc)",
        "index": "MSCI World SRI",
        "ter": 0.20,
        "allocation": [
            {"country": "United States", "weight_pct": 71.86},
            {"country": "Japan", "weight_pct": 5.43},
            {"country": "United Kingdom", "weight_pct": 3.65},
            {"country": "France", "weight_pct": 2.89},
            {"country": "Canada", "weight_pct": 2.88},
            {"country": "Switzerland", "weight_pct": 2.35},
            {"country": "Germany", "weight_pct": 2.21},
            {"country": "Australia", "weight_pct": 1.78},
            {"country": "Netherlands", "weight_pct": 1.13},
            {"country": "Other", "weight_pct": 5.82}
        ]
    },
    "LU0274208692": {
        "name": "Xtrackers MSCI World Swap UCITS ETF 1C",
        "index": "MSCI World",
        "ter": 0.45,
        "aum_eur_m": 4697,
        "allocation": [
            {"country": "United States", "weight_pct": 71.86},
            {"country": "Japan", "weight_pct": 5.43},
            {"country": "United Kingdom", "weight_pct": 3.65},
            {"country": "France", "weight_pct": 2.89},
            {"country": "Canada", "weight_pct": 2.88},
            {"country": "Switzerland", "weight_pct": 2.35},
            {"country": "Germany", "weight_pct": 2.21},
            {"country": "Australia", "weight_pct": 1.78},
            {"country": "Netherlands", "weight_pct": 1.13},
            {"country": "Other", "weight_pct": 5.82}
        ]
    },
    "LU0629459743": {
        "name": "UBS MSCI World Socially Responsible UCITS ETF",
        "index": "MSCI World SRI",
        "ter": 0.22,
        "allocation": [
            {"country": "United States", "weight_pct": 71.86},
            {"country": "Japan", "weight_pct": 5.43},
            {"country": "United Kingdom", "weight_pct": 3.65},
            {"country": "France", "weight_pct": 2.89},
            {"country": "Canada", "weight_pct": 2.88},
            {"country": "Switzerland", "weight_pct": 2.35},
            {"country": "Germany", "weight_pct": 2.21},
            {"country": "Australia", "weight_pct": 1.78},
            {"country": "Netherlands", "weight_pct": 1.13},
            {"country": "Other", "weight_pct": 5.82}
        ]
    },
    "IE00BDR55927": {
        "name": "UBS MSCI ACWI Socially Responsible UCITS ETF",
        "index": "MSCI ACWI SRI",
        "ter": 0.33,
        "allocation": [
            {"country": "United States", "weight_pct": 63.5},
            {"country": "Japan", "weight_pct": 5.2},
            {"country": "United Kingdom", "weight_pct": 3.4},
            {"country": "China", "weight_pct": 2.9},
            {"country": "France", "weight_pct": 2.7},
            {"country": "Canada", "weight_pct": 2.7},
            {"country": "Switzerland", "weight_pct": 2.2},
            {"country": "Germany", "weight_pct": 2.0},
            {"country": "India", "weight_pct": 1.9},
            {"country": "Australia", "weight_pct": 1.6},
            {"country": "Taiwan", "weight_pct": 1.6},
            {"country": "South Korea", "weight_pct": 1.2},
            {"country": "Netherlands", "weight_pct": 1.1},
            {"country": "Other", "weight_pct": 8.0}
        ]
    },
    "IE00B4X9L533": {
        "name": "HSBC MSCI World UCITS ETF USD",
        "index": "MSCI World",
        "ter": 0.15,
        "allocation": [
            {"country": "United States", "weight_pct": 71.86},
            {"country": "Japan", "weight_pct": 5.43},
            {"country": "United Kingdom", "weight_pct": 3.65},
            {"country": "France", "weight_pct": 2.89},
            {"country": "Canada", "weight_pct": 2.88},
            {"country": "Switzerland", "weight_pct": 2.35},
            {"country": "Germany", "weight_pct": 2.21},
            {"country": "Australia", "weight_pct": 1.78},
            {"country": "Netherlands", "weight_pct": 1.13},
            {"country": "Other", "weight_pct": 5.82}
        ]
    },
    "IE00BYZK4552": {
        "name": "iShares Automation & Robotics UCITS ETF",
        "index": "iSTOXX FactSet Automation & Robotics",
        "ter": 0.40,
        "aum_eur_m": 3333,
        "allocation": [
            {"country": "United States", "weight_pct": 52.1},
            {"country": "Japan", "weight_pct": 19.3},
            {"country": "Germany", "weight_pct": 5.8},
            {"country": "Switzerland", "weight_pct": 4.2},
            {"country": "Taiwan", "weight_pct": 3.5},
            {"country": "South Korea", "weight_pct": 2.8},
            {"country": "Other", "weight_pct": 12.3}
        ]
    },
    "IE00B8FHGS14": {
        "name": "iShares Edge MSCI World Minimum Volatility UCITS ETF",
        "index": "MSCI World Minimum Volatility",
        "ter": 0.30,
        "aum_eur_m": 2995,
        "allocation": [
            {"country": "United States", "weight_pct": 61.5},
            {"country": "Japan", "weight_pct": 11.2},
            {"country": "Switzerland", "weight_pct": 5.8},
            {"country": "United Kingdom", "weight_pct": 4.1},
            {"country": "Canada", "weight_pct": 3.2},
            {"country": "Germany", "weight_pct": 2.9},
            {"country": "France", "weight_pct": 2.4},
            {"country": "Other", "weight_pct": 8.9}
        ]
    },
    "IE00B6R52259": {
        "name": "iShares MSCI ACWI UCITS ETF USD (Acc)",
        "index": "MSCI ACWI",
        "ter": 0.20,
        "aum_eur_m": 22517,
        "allocation": [
            {"country": "United States", "weight_pct": 63.5},
            {"country": "Japan", "weight_pct": 5.2},
            {"country": "United Kingdom", "weight_pct": 3.4},
            {"country": "China", "weight_pct": 2.9},
            {"country": "France", "weight_pct": 2.7},
            {"country": "Canada", "weight_pct": 2.7},
            {"country": "Switzerland", "weight_pct": 2.2},
            {"country": "Germany", "weight_pct": 2.0},
            {"country": "India", "weight_pct": 1.9},
            {"country": "Australia", "weight_pct": 1.6},
            {"country": "Taiwan", "weight_pct": 1.6},
            {"country": "South Korea", "weight_pct": 1.2},
            {"country": "Netherlands", "weight_pct": 1.1},
            {"country": "Other", "weight_pct": 8.0}
        ]
    }
}


def get_etf_allocation(isin: str) -> dict:
    """
    Get country allocation for a specific ETF by ISIN.

    Args:
        isin: ETF ISIN code (e.g., 'IE00B0M62Q58')

    Returns:
        dict with ETF data including country allocations
    """
    isin = isin.upper().strip()

    if isin not in ETF_ALLOCATIONS:
        return {
            "success": False,
            "error": f"ETF not found: {isin}",
            "available_isins": list(ETF_ALLOCATIONS.keys())
        }

    data = ETF_ALLOCATIONS[isin]
    return {
        "success": True,
        "isin": isin,
        "name": data["name"],
        "index": data["index"],
        "ter_pct": data["ter"],
        "aum_eur_m": data.get("aum_eur_m"),
        "allocation": data["allocation"],
        "source": "MSCI Index factsheets",
        "note": "Country allocations based on underlying index weights. Actual ETF weights may vary slightly."
    }


def list_etf_allocations() -> dict:
    """
    List all available ETFs with summary info.

    Returns:
        dict with list of all ETFs and their key metrics
    """
    etfs = []
    for isin, data in ETF_ALLOCATIONS.items():
        top_country = data["allocation"][0] if data["allocation"] else None
        etfs.append({
            "isin": isin,
            "name": data["name"],
            "index": data["index"],
            "ter_pct": data["ter"],
            "aum_eur_m": data.get("aum_eur_m"),
            "top_country": top_country["country"] if top_country else None,
            "top_country_weight": top_country["weight_pct"] if top_country else None
        })

    # Sort by AUM descending (None values at end)
    etfs.sort(key=lambda x: (x["aum_eur_m"] is None, -(x["aum_eur_m"] or 0)))

    return {
        "success": True,
        "count": len(etfs),
        "etfs": etfs,
        "source": "MSCI Index factsheets",
        "note": "Country allocations based on underlying index weights"
    }


def get_etf_country_exposure(country: str) -> dict:
    """
    Find ETFs with exposure to a specific country.

    Args:
        country: Country name (e.g., 'Japan', 'China')

    Returns:
        dict with list of ETFs that have exposure to that country
    """
    country_lower = country.lower().strip()

    exposures = []
    for isin, data in ETF_ALLOCATIONS.items():
        for alloc in data["allocation"]:
            if alloc["country"].lower() == country_lower:
                exposures.append({
                    "isin": isin,
                    "name": data["name"],
                    "index": data["index"],
                    "country_weight_pct": alloc["weight_pct"]
                })
                break

    # Sort by weight descending
    exposures.sort(key=lambda x: -x["country_weight_pct"])

    return {
        "success": True,
        "country": country,
        "count": len(exposures),
        "etfs": exposures
    }
