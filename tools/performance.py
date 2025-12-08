"""
Performance analysis tools for Orca MCP.
Handles communication with GA10 Performance API.
"""

import json
import urllib.request
import urllib.error
import ssl
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("orca-mcp.performance")

# SSL workaround for development environments
ssl._create_default_https_context = ssl._create_unverified_context

import os
GA10_PERF_API_URL = os.getenv('GA10_PERF_URL', 'https://ga10-perf.example.com') + '/portfolio/performance'

def get_portfolio_performance(
    bonds: list,
    start_date: str,
    end_date: str,
    scale_to_100: bool = True
) -> Dict[str, Any]:
    """
    Calculate portfolio performance using GA10 Performance API.

    Args:
        bonds: List of bond dictionaries with 'isin', 'weight', 'start_price', 'end_price'
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        scale_to_100: Whether to scale weights to 100%

    Returns:
        Dictionary containing performance data (summary, by_country, by_bond)
    """
    payload = {
        'bonds': bonds,
        'start_date': start_date,
        'end_date': end_date,
        'scale_to_100': scale_to_100
    }

    logger.info(f"Calling GA10 Performance API for {len(bonds)} bonds")

    try:
        req = urllib.request.Request(
            GA10_PERF_API_URL,
            data=json.dumps(payload).encode('utf-8'),
            headers={
                'Content-Type': 'application/json'
            }
        )

        # Set timeout to 60 seconds to avoid timeouts on large portfolios
        with urllib.request.urlopen(req, timeout=60) as response:
            if response.getcode() != 200:
                raise RuntimeError(f"API returned status code {response.getcode()}")

            data = json.loads(response.read().decode())
            return data

    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        logger.error(f"GA10 Performance API Error: {e.code} - {error_body}")
        raise RuntimeError(f"GA10 Performance API Error: {e.code} - {error_body}")

    except Exception as e:
        logger.error(f"Error calling GA10 Performance API: {str(e)}")
        raise RuntimeError(f"Error calling GA10 Performance API: {str(e)}")
