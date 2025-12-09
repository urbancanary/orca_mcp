#!/usr/bin/env python3
"""
Orca MCP - HTTP API Server

HTTP wrapper for Orca MCP tools, designed for Railway deployment.
Exposes the same functionality as the MCP server but via REST API.
"""

import os
import sys
import json
import logging
from pathlib import Path

# Add parent directory to path for imports when running from orca_mcp directory
# This allows both package imports (from orca_mcp.tools) and direct imports (from tools)
SCRIPT_DIR = Path(__file__).parent.resolve()
if str(SCRIPT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR.parent))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from flask import Flask, request, jsonify
from functools import wraps

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orca-api")

# Import tool implementations - try both import styles for flexibility
try:
    # When deployed to Railway (working directory is orca_mcp/)
    from tools.data_access import query_bigquery, fetch_credentials_from_auth_mcp
    from tools.data_upload import (
        upload_table,
        delete_records,
        invalidate_cache_pattern,
        get_cache_stats
    )
    from tools.imf_gateway import (
        fetch_imf_data,
        get_available_indicators,
        get_available_country_groups
    )
    from tools.etf_reference import (
        get_etf_allocation,
        list_etf_allocations,
        get_etf_country_exposure
    )
    from client_config import get_client_config
except ImportError:
    # When run from parent directory (local dev)
    from orca_mcp.tools.data_access import query_bigquery, fetch_credentials_from_auth_mcp
    from orca_mcp.tools.data_upload import (
        upload_table,
        delete_records,
        invalidate_cache_pattern,
        get_cache_stats
    )
    from orca_mcp.tools.imf_gateway import (
        fetch_imf_data,
        get_available_indicators,
        get_available_country_groups
    )
    from orca_mcp.tools.etf_reference import (
        get_etf_allocation,
        list_etf_allocations,
        get_etf_country_exposure
    )
    from orca_mcp.client_config import get_client_config

app = Flask(__name__)

# API Key authentication
API_KEY = os.getenv("ORCA_API_KEY", "orca_demo_key_2024")


def require_api_key(f):
    """Decorator to require API key authentication"""
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get("X-API-Key") or request.args.get("api_key")
        if not api_key or api_key != API_KEY:
            return jsonify({"error": "Invalid or missing API key"}), 401
        return f(*args, **kwargs)
    return decorated


@app.route("/")
def index():
    """API info endpoint"""
    return jsonify({
        "service": "Orca MCP API",
        "version": "1.0.0",
        "description": "Gateway & Orchestrator for Portfolio Management",
        "endpoints": {
            "health": "GET /health",
            "client_info": "GET /client/info",
            "portfolios": "GET /client/portfolios",
            "holdings": "GET /client/holdings/<portfolio_id>",
            "transactions": "GET /client/transactions/<portfolio_id>",
            "query": "POST /client/query",
            "staging": "GET /staging/<portfolio_id>",
            "cash": "GET /portfolio/<portfolio_id>/cash",
            "imf_data": "POST /imf/data",
            "imf_indicators": "GET /imf/indicators",
            "etf_allocation": "GET /etf/<isin>",
            "etf_list": "GET /etf",
            "cache_stats": "GET /cache/stats",
            "cache_invalidate": "POST /cache/invalidate"
        }
    })


@app.route("/health")
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "orca-mcp"})


# ============ Client Data Endpoints ============

@app.route("/client/info")
@require_api_key
def get_client_info():
    """Get information about client configuration"""
    try:
        client_id = request.args.get("client_id")
        config = get_client_config(client_id)

        result = {
            "client_id": config.client_id,
            "client_name": config.client_config.get("name"),
            "bigquery_dataset": config.get_bigquery_dataset(),
            "database_registry": str(config.get_database_registry_path()),
            "license_tier": config.client_config.get("license_tier"),
            "active": config.client_config.get("active")
        }
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_client_info: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/client/portfolios")
@require_api_key
def get_client_portfolios():
    """Get list of portfolios for a client"""
    try:
        client_id = request.args.get("client_id")
        sql = "SELECT DISTINCT portfolio_id FROM transactions ORDER BY portfolio_id"
        df = query_bigquery(sql, client_id)
        return jsonify({"portfolios": df.to_dict(orient='records')})
    except Exception as e:
        logger.error(f"Error in get_client_portfolios: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/client/holdings/<portfolio_id>")
@require_api_key
def get_client_holdings(portfolio_id):
    """Get current holdings for a portfolio"""
    try:
        client_id = request.args.get("client_id")

        sql = f"""
        SELECT
            isin,
            ticker,
            description,
            country,
            SUM(par_amount) as par_amount,
            AVG(price) as avg_price,
            SUM(market_value) as total_market_value,
            COUNT(*) as num_transactions
        FROM transactions
        WHERE portfolio_id = '{portfolio_id}'
            AND transaction_type = 'BUY'
        GROUP BY isin, ticker, description, country
        HAVING par_amount > 0
        ORDER BY total_market_value DESC
        """

        df = query_bigquery(sql, client_id)
        return jsonify({"holdings": df.to_dict(orient='records')})
    except Exception as e:
        logger.error(f"Error in get_client_holdings: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/client/transactions/<portfolio_id>")
@require_api_key
def get_client_transactions(portfolio_id):
    """Get transactions for a portfolio"""
    try:
        client_id = request.args.get("client_id")
        limit = request.args.get("limit", 100, type=int)
        transaction_date = request.args.get("transaction_date")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        transaction_type = request.args.get("transaction_type")

        # Build WHERE clauses
        where_clauses = [f"portfolio_id = '{portfolio_id}'"]

        if transaction_date:
            where_clauses.append(f"transaction_date = '{transaction_date}'")
        if start_date:
            where_clauses.append(f"transaction_date >= '{start_date}'")
        if end_date:
            where_clauses.append(f"transaction_date <= '{end_date}'")
        if transaction_type:
            where_clauses.append(f"transaction_type = '{transaction_type}'")

        where_clause = " AND ".join(where_clauses)
        limit_clause = "" if limit == -1 else f"LIMIT {limit}"

        sql = f"""
        SELECT *
        FROM transactions
        WHERE {where_clause}
        ORDER BY transaction_date DESC, settlement_date DESC
        {limit_clause}
        """

        df = query_bigquery(sql, client_id)
        return jsonify({"transactions": df.to_dict(orient='records')})
    except Exception as e:
        logger.error(f"Error in get_client_transactions: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/client/query", methods=["POST"])
@require_api_key
def query_client_data():
    """Execute custom SQL query"""
    try:
        data = request.get_json()
        sql = data.get("sql")
        client_id = data.get("client_id")

        if not sql:
            return jsonify({"error": "SQL query required"}), 400

        df = query_bigquery(sql, client_id)
        return jsonify({"data": df.to_dict(orient='records')})
    except Exception as e:
        logger.error(f"Error in query_client_data: {e}")
        return jsonify({"error": str(e)}), 500


# ============ Staging Endpoints ============

@app.route("/staging/<portfolio_id>")
@require_api_key
def get_staging_holdings(portfolio_id):
    """Get staging transactions for a portfolio"""
    try:
        client_id = request.args.get("client_id")

        sql = f"""
        SELECT
            transaction_id,
            isin, ticker, description, country,
            par_amount, price, market_value,
            ytm, duration, spread,
            transaction_date, notes, created_at
        FROM transactions
        WHERE portfolio_id = '{portfolio_id}'
            AND status = 'staging'
            AND transaction_type = 'BUY'
        ORDER BY country, ticker
        """

        df = query_bigquery(sql, client_id)
        holdings = df.to_dict(orient='records')

        result = {
            "portfolio_id": portfolio_id,
            "status": "staging",
            "num_bonds": len(holdings),
            "total_market_value": sum(h.get('market_value', 0) for h in holdings),
            "holdings": holdings
        }
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_staging_holdings: {e}")
        return jsonify({"error": str(e)}), 500


# ============ Portfolio Endpoints ============

@app.route("/portfolio/<portfolio_id>/cash")
@require_api_key
def get_portfolio_cash(portfolio_id):
    """Get cash position and portfolio summary"""
    try:
        client_id = request.args.get("client_id")

        sql = f"""
        SELECT *
        FROM portfolio_summary
        WHERE portfolio_id = '{portfolio_id}'
        """

        df = query_bigquery(sql, client_id)

        if df.empty:
            return jsonify({
                "error": f"No summary found for portfolio '{portfolio_id}'. Run refresh_portfolio_summary first."
            }), 404

        row = df.iloc[0]
        result = {
            "portfolio_id": portfolio_id,
            "cash": {
                "settled_cash": float(row['settled_cash']),
                "total_cash": float(row['total_cash']),
                "cash_description": "Settled cash = current; Total cash = including staging transactions"
            },
            "portfolio_breakdown": {
                "starting_cash": float(row['starting_cash']),
                "settled_bonds_value": float(row['settled_bonds_value']),
                "staging_buy_value": float(row['staging_buy_value']),
                "staging_sell_value": float(row['staging_sell_value'])
            },
            "counts": {
                "num_settled_bonds": int(row['num_settled_bonds']),
                "num_staging_transactions": int(row['num_staging_transactions'])
            }
        }
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_portfolio_cash: {e}")
        return jsonify({"error": str(e)}), 500


# ============ IMF Data Endpoints ============

@app.route("/imf/data", methods=["POST"])
@require_api_key
def imf_data():
    """Fetch IMF economic data"""
    try:
        data = request.get_json()
        indicator = data.get("indicator")
        countries = data.get("countries")
        start_year = data.get("start_year")
        end_year = data.get("end_year")
        use_mcp = data.get("use_mcp", False)

        if not indicator or not countries:
            return jsonify({"error": "indicator and countries required"}), 400

        result = fetch_imf_data(
            indicator=indicator,
            countries=countries,
            start_year=start_year,
            end_year=end_year,
            use_mcp=use_mcp
        )
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in imf_data: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/imf/indicators")
@require_api_key
def imf_indicators():
    """List available IMF indicators"""
    try:
        result = get_available_indicators()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in imf_indicators: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/imf/country-groups")
@require_api_key
def imf_country_groups():
    """List available country groups"""
    try:
        result = get_available_country_groups()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in imf_country_groups: {e}")
        return jsonify({"error": str(e)}), 500


# ============ ETF Endpoints ============

@app.route("/etf")
@require_api_key
def etf_list():
    """List all available ETFs"""
    try:
        result = list_etf_allocations()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in etf_list: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/etf/<isin>")
@require_api_key
def etf_allocation(isin):
    """Get ETF country allocation"""
    try:
        result = get_etf_allocation(isin)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in etf_allocation: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/etf/country/<country>")
@require_api_key
def etf_country_exposure(country):
    """Find ETFs with exposure to a country"""
    try:
        result = get_etf_country_exposure(country)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in etf_country_exposure: {e}")
        return jsonify({"error": str(e)}), 500


# ============ Cache Endpoints ============

@app.route("/cache/stats")
@require_api_key
def cache_stats():
    """Get cache statistics"""
    try:
        result = get_cache_stats()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in cache_stats: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/cache/invalidate", methods=["POST"])
@require_api_key
def cache_invalidate():
    """Invalidate cache keys"""
    try:
        data = request.get_json()
        pattern = data.get("pattern")

        if not pattern:
            return jsonify({"error": "pattern required"}), 400

        result = invalidate_cache_pattern(pattern)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in cache_invalidate: {e}")
        return jsonify({"error": str(e)}), 500


# ============ Main Entry Point ============

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    logger.info(f"Starting Orca MCP API on port {port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
