#!/usr/bin/env python3
"""
Orca MCP - Gateway & Orchestrator for Portfolio Management

Gateway to all backend services for multi-client portfolio management.
Pure orchestration - bond analytics moved to James MCP.

Architecture:
- Routes requests by client_id
- Gateway to BigQuery for data access
- Gateway to portfolio_mcp for portfolio operations
- Gateway to auth-mcp for credentials
- Manages Redis caching layer
- Supports multi-client isolation

Services:
- Data Access: BigQuery queries, data upload/delete, caching
- Portfolio Operations: Staging transactions, cash management
- Client Management: Multi-client routing

Bond Analytics: Use James MCP for RVM scoring, country eligibility, etc.

Usage:
    # As MCP server
    python orca_mcp/server.py

    # As Python module
    from orca_mcp import query_client_data, get_client_portfolio
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Any, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from mcp.server import Server
from mcp.types import Tool, TextContent
import mcp.server.stdio

from orca_mcp.client_config import get_client_config
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
from orca_mcp.tools.video_gateway import (
    video_search,
    video_list,
    video_synthesize,
    video_get_transcript,
    video_keyword_search
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orca-mcp")

# Create server instance
server = Server("orca-mcp")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available Orca MCP tools"""
    return [
        Tool(
            name="get_client_info",
            description="Get information about the current client configuration",
            inputSchema={
                "type": "object",
                "properties": {
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional, uses env var if not provided)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="query_client_data",
            description="Query client's portfolio data from BigQuery",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query (use simple table names like 'transactions')"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="get_client_portfolios",
            description="Get list of portfolios for a client",
            inputSchema={
                "type": "object",
                "properties": {
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_client_transactions",
            description="Get transactions for a client's portfolio with optional filters",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio identifier (e.g., 'wnbf')"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    },
                    "transaction_date": {
                        "type": "string",
                        "description": "Filter by specific transaction date (YYYY-MM-DD format, optional)"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Filter transactions from this date onwards (YYYY-MM-DD format, optional)"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Filter transactions up to this date (YYYY-MM-DD format, optional)"
                    },
                    "transaction_type": {
                        "type": "string",
                        "description": "Filter by transaction type (e.g., 'BUY', 'SELL', 'INITIAL', optional)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of transactions (default: 100, use -1 for all)"
                    }
                },
                "required": ["portfolio_id"]
            }
        ),
        Tool(
            name="get_client_holdings",
            description="Get current holdings for a client's portfolio",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio identifier"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": ["portfolio_id"]
            }
        ),
        Tool(
            name="get_service_info",
            description="Get information about available services (auth-mcp, BigQuery, etc.)",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_name": {
                        "type": "string",
                        "description": "Service name (optional, returns all if not provided)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_staging_holdings",
            description="Get proposed staging transactions (status='staging') for a portfolio. These are planned/proposed trades that haven't been executed yet. Returns all bonds with status='staging'.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_staging_versions",
            description="List staging transaction groups by creation time. Shows when staging transactions were added and their current status.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of transaction groups to return (default: 10)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="compare_staging_vs_actual",
            description="Compare staging transactions (status='staging') vs actual portfolio (status='settled'). Shows what would change if staging transactions were executed: additions, removals, and cash impact.",
            inputSchema={
                "type": "object",
                "properties": {
                    "actual_portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (e.g., 'wnbf')"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": ["actual_portfolio_id"]
            }
        ),
        Tool(
            name="add_staging_buy",
            description="Calculate staging BUY transaction to add a target percentage allocation to a bond. Automatically calculates par amount with proper sizing (min $200k, $50k increments), gets current price + accrued interest, and creates staging BUY transaction.",
            inputSchema={
                "type": "object",
                "properties": {
                    "isin": {
                        "type": "string",
                        "description": "ISIN of the bond to add"
                    },
                    "target_pct": {
                        "type": "number",
                        "description": "Target allocation percentage (e.g., 3.0 for 3%)"
                    },
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    },
                    "min_size": {
                        "type": "number",
                        "description": "Minimum par amount (default: 200000)"
                    },
                    "increment": {
                        "type": "number",
                        "description": "Par amount increment (default: 50000)"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": ["isin", "target_pct"]
            }
        ),
        Tool(
            name="add_staging_sell",
            description="Calculate staging transaction to sell a bond position. If country is specified, automatically selects the bond with the lowest return_ytw (total return) in that country. Calculates par amount to sell with proper sizing (min $200k, $50k increments), gets current price + accrued interest, and creates staging SELL transaction.",
            inputSchema={
                "type": "object",
                "properties": {
                    "isin": {
                        "type": "string",
                        "description": "ISIN of the bond to sell (optional if country is specified)"
                    },
                    "country": {
                        "type": "string",
                        "description": "Country name to auto-select bond with lowest return_ytw (optional if isin is specified)"
                    },
                    "cash_to_raise": {
                        "type": "number",
                        "description": "Dollar amount of cash to raise (e.g., 150000 for $150k)"
                    },
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    },
                    "min_size": {
                        "type": "number",
                        "description": "Minimum par amount (default: 200000)"
                    },
                    "increment": {
                        "type": "number",
                        "description": "Par amount increment (default: 50000)"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": ["cash_to_raise"]
            }
        ),
        Tool(
            name="get_portfolio_cash",
            description="Get current cash position and portfolio summary from the portfolio_summary table. Returns both settled cash (current) and total cash (including staging transactions).",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="refresh_portfolio_summary",
            description="Recalculate and update the portfolio summary table with latest transaction data. Use this after creating/deleting staging transactions or promoting staging to executed.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="upload_table",
            description="Upload Parquet/CSV file to BigQuery table with automatic cache invalidation. Use this to push data from external sources (like rvm_app_v2) to BigQuery. Cache is automatically invalidated after upload.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Target table name (e.g., 'agg_analysis_data', 'transactions')"
                    },
                    "source_file": {
                        "type": "string",
                        "description": "Path to source file (Parquet or CSV)"
                    },
                    "format": {
                        "type": "string",
                        "enum": ["parquet", "csv"],
                        "description": "File format (default: parquet)"
                    },
                    "write_disposition": {
                        "type": "string",
                        "enum": ["WRITE_TRUNCATE", "WRITE_APPEND"],
                        "description": "WRITE_TRUNCATE (replace table) or WRITE_APPEND (add rows). Default: WRITE_TRUNCATE"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": ["table_name", "source_file"]
            }
        ),
        Tool(
            name="delete_records",
            description="Delete records from BigQuery table with automatic cache invalidation. Use for data cleanup operations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Target table name (e.g., 'transactions')"
                    },
                    "where_clause": {
                        "type": "string",
                        "description": "SQL WHERE clause (e.g., \"transaction_date < '2020-01-01'\")"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": ["table_name", "where_clause"]
            }
        ),
        Tool(
            name="invalidate_cache",
            description="Manually invalidate cache keys matching pattern. Use this to force refresh of cached data (e.g., after manual BigQuery updates). Patterns: 'universe:*' (all universe data), 'holdings:client_id:*' (client holdings), 'query:*' (all queries).",
            inputSchema={
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Redis pattern (e.g., 'universe:*', 'holdings:guinness:*')"
                    }
                },
                "required": ["pattern"]
            }
        ),
        Tool(
            name="get_cache_stats",
            description="Get Redis cache statistics including hit rate, memory usage, and total keys. Use this to monitor cache performance.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),

        # IMF Gateway - Generic access to ALL IMF data
        Tool(
            name="fetch_imf_data",
            description="GENERIC GATEWAY to fetch ANY IMF economic indicator for ANY country/countries. Supports country groups (G7, G20, BRICS, EU, ASEAN). Indicators: gdp_growth, government_debt, inflation, unemployment, fiscal_deficit, current_account, gdp_per_capita, or IMF codes (GGXWDG_NGDP, NGDP_RPCH, etc.). Examples: fetch_imf_data('debt', 'G7'), fetch_imf_data('inflation', ['USA', 'CHN']), fetch_imf_data('NGDP_RPCH', 'Germany', start_year=2020)",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {
                        "type": "string",
                        "description": "Indicator name (debt, gdp_growth, inflation, unemployment, fiscal_deficit, current_account) or IMF code (GGXWDG_NGDP, NGDP_RPCH, PCPIPCH, LUR, etc.)"
                    },
                    "countries": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}}
                        ],
                        "description": "Country name(s), ISO code(s), or group name (G7, G20, BRICS, EU, ASEAN). Examples: 'Japan', ['USA', 'CHN'], 'G7'"
                    },
                    "start_year": {
                        "type": "integer",
                        "description": "Optional start year (default: 2010)"
                    },
                    "end_year": {
                        "type": "integer",
                        "description": "Optional end year (default: latest with projections)"
                    },
                    "use_mcp": {
                        "type": "boolean",
                        "description": "If true, use IMF MCP with AI analysis (slower). If false, use direct API (faster). Default: false"
                    }
                },
                "required": ["indicator", "countries"]
            }
        ),
        Tool(
            name="get_available_indicators",
            description="List all available IMF indicators with codes, names, units, and aliases. Use this to discover what economic data you can fetch via fetch_imf_data.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="get_available_country_groups",
            description="List all available country groups (G7, G20, BRICS, EU, ASEAN) with member countries. Use these group names in fetch_imf_data to get data for multiple countries at once.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),

        # ETF Reference Data Tools
        Tool(
            name="get_etf_allocation",
            description="Get country allocation breakdown for a specific ETF by ISIN. Returns country weights based on underlying index. Supports major MSCI World, ACWI, SRI, and thematic ETFs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "isin": {
                        "type": "string",
                        "description": "ETF ISIN code (e.g., 'IE00B0M62Q58' for iShares MSCI World)"
                    }
                },
                "required": ["isin"]
            }
        ),
        Tool(
            name="list_etf_allocations",
            description="List all available ETFs with summary info including name, index, TER, and top country exposure. Use this to discover which ETFs have allocation data available.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="get_etf_country_exposure",
            description="Find all ETFs with exposure to a specific country. Returns list of ETFs sorted by weight in that country. Example: get_etf_country_exposure('Japan') returns ETFs with Japan exposure.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {
                        "type": "string",
                        "description": "Country name (e.g., 'Japan', 'China', 'United States')"
                    }
                },
                "required": ["country"]
            }
        ),
        # Video Intelligence Tools
        Tool(
            name="video_search",
            description="Search video transcripts for relevant content. Returns video segments with timestamps and relevance scores.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query text"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum number of results to return",
                        "default": 10
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="video_list",
            description="List all available videos in the library with metadata.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="video_synthesize",
            description="Generate an AI-synthesized answer from video search results. Use after video_search to create a coherent response.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The original question"
                    },
                    "video_results": {
                        "type": "array",
                        "description": "Results from video_search",
                        "items": {"type": "object"}
                    },
                    "tone": {
                        "type": "string",
                        "description": "Response tone: professional, casual, educational",
                        "default": "professional"
                    }
                },
                "required": ["query", "video_results"]
            }
        ),
        Tool(
            name="video_get_transcript",
            description="Get the full transcript for a specific video by ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "video_id": {
                        "type": "string",
                        "description": "YouTube video ID"
                    }
                },
                "required": ["video_id"]
            }
        ),
        Tool(
            name="video_keyword_search",
            description="Fast keyword search across video transcripts. Simpler than semantic search.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Keyword or phrase to search"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum results",
                        "default": 10
                    }
                },
                "required": ["query"]
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls"""

    try:
        client_id = arguments.get("client_id")
        config = get_client_config(client_id)

        # Access check
        if not config.is_tool_allowed(name):
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": "ACCESS_DENIED",
                    "message": f"Tool '{name}' is not available for this client",
                    "client_id": config.client_id
                }, indent=2)
            )]

        if name == "get_client_info":
            config = get_client_config(client_id)

            result = {
                "client_id": config.client_id,
                "client_name": config.client_config.get("name"),
                "bigquery_dataset": config.get_bigquery_dataset(),
                "database_registry": str(config.get_database_registry_path()),
                "access_level": config.client_config.get("access_level", "full"),
                "active": config.client_config.get("active")
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "query_client_data":
            sql = arguments["sql"]

            df = query_bigquery(sql, client_id)
            result = df.to_dict(orient='records')

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_client_portfolios":
            sql = "SELECT DISTINCT portfolio_id FROM transactions ORDER BY portfolio_id"

            df = query_bigquery(sql, client_id)
            result = df.to_dict(orient='records')

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_client_transactions":
            portfolio_id = arguments["portfolio_id"]
            limit = arguments.get("limit", 100)
            transaction_date = arguments.get("transaction_date")
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            transaction_type = arguments.get("transaction_type")

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

            # Build LIMIT clause (-1 means no limit)
            limit_clause = "" if limit == -1 else f"LIMIT {limit}"

            sql = f"""
            SELECT *
            FROM transactions
            WHERE {where_clause}
            ORDER BY transaction_date DESC, settlement_date DESC
            {limit_clause}
            """

            df = query_bigquery(sql, client_id)
            result = df.to_dict(orient='records')

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "get_client_holdings":
            portfolio_id = arguments["portfolio_id"]

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
            result = df.to_dict(orient='records')

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_service_info":
            service_name = arguments.get("service_name")
            config = get_client_config(client_id)

            if service_name:
                service = config.get_service(service_name)
                result = {service_name: service}
            else:
                result = config.registry.get("services", {})

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_staging_holdings":
            # UNIFIED MODEL: Get staging transactions from transactions table with status='staging'
            portfolio_id = arguments.get("portfolio_id", "wnbf")  # Changed default to wnbf

            # Get staging holdings from unified transactions table
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

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "get_staging_versions":
            # UNIFIED MODEL: List staging transaction groups by creation timestamp
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            limit = arguments.get("limit", 10)

            sql = f"""
            SELECT
                CAST(created_at AS STRING) as batch_timestamp,
                COUNT(*) as num_transactions,
                SUM(market_value) as total_value,
                STRING_AGG(DISTINCT ticker, ', ' ORDER BY ticker) as tickers,
                MAX(notes) as notes
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND status = 'staging'
            GROUP BY created_at
            ORDER BY created_at DESC
            LIMIT {limit}
            """

            df = query_bigquery(sql, client_id)
            batches = df.to_dict(orient='records')

            result = {
                "portfolio_id": portfolio_id,
                "num_batches": len(batches),
                "status_filter": "staging",
                "batches": batches
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "compare_staging_vs_actual":
            # UNIFIED MODEL: Compare staging (status='staging') vs actual (status='settled')
            portfolio_id = arguments["actual_portfolio_id"]  # Now uses same portfolio

            # Get staging holdings (status='staging')
            staging_sql = f"""
            SELECT isin, ticker, description, country, par_amount, market_value
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND status = 'staging'
                AND transaction_type = 'BUY'
            """
            staging_df = query_bigquery(staging_sql, client_id)

            # Get actual holdings (status='settled')
            actual_sql = f"""
            WITH holdings_agg AS (
                SELECT
                    isin, ticker, description, country,
                    SUM(par_amount) as par_amount,
                    SUM(market_value) as market_value
                FROM transactions
                WHERE portfolio_id = '{portfolio_id}'
                    AND status = 'settled'
                    AND transaction_type = 'BUY'
                GROUP BY isin, ticker, description, country
            )
            SELECT * FROM holdings_agg
            WHERE par_amount > 0
            """
            actual_df = query_bigquery(actual_sql, client_id)

            # Get cash position
            cash_sql = f"""
            SELECT SUM(market_value) as cash
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND status = 'settled'
                AND ticker = 'CASH'
            """
            cash_df = query_bigquery(cash_sql, client_id)
            cash_position = float(cash_df.iloc[0]['cash']) if not cash_df.empty else 0.0

            # Find differences
            staging_isins = set(staging_df['isin'])
            actual_isins = set(actual_df['isin'])

            additions = staging_isins - actual_isins
            removals = actual_isins - staging_isins
            common = staging_isins & actual_isins

            result = {
                "comparison": {
                    "portfolio": portfolio_id,
                    "actual_status": "settled",
                    "staging_status": "staging"
                },
                "summary": {
                    "actual_bonds": len(actual_isins),
                    "staging_bonds": len(staging_isins),
                    "additions": len(additions),
                    "removals": len(removals),
                    "common": len(common)
                },
                "additions": staging_df[staging_df['isin'].isin(additions)].to_dict(orient='records'),
                "removals": actual_df[actual_df['isin'].isin(removals)].to_dict(orient='records'),
                "staging_total_mv": float(staging_df['market_value'].sum()) if not staging_df.empty else 0.0,
                "actual_total_mv": float(actual_df['market_value'].sum()) if not actual_df.empty else 0.0,
                "cash_position": cash_position
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "add_staging_allocation":
            isin = arguments["isin"]
            target_pct = arguments["target_pct"]
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            min_size = arguments.get("min_size", 200000)
            increment = arguments.get("increment", 50000)

            # Get portfolio total value (starting cash)
            portfolio_sql = f"""
            SELECT market_value as portfolio_value
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND ticker = 'CASH'
                AND transaction_type = 'INITIAL'
            """
            portfolio_df = query_bigquery(portfolio_sql, client_id)
            if portfolio_df.empty:
                return [TextContent(
                    type="text",
                    text=json.dumps({"error": "Cannot find portfolio starting value"}, indent=2)
                )]
            portfolio_value = float(portfolio_df.iloc[0]['portfolio_value'])

            # Get current bond price and data from agg_analysis_data
            bond_sql = f"""
            WITH latest AS (
                SELECT isin, MAX(bpdate) as max_date
                FROM agg_analysis_data
                WHERE isin = '{isin}'
                GROUP BY isin
            )
            SELECT a.isin, a.ticker, a.description, a.country,
                   a.price, a.accrued_interest, a.ytw, a.oad, a.notches
            FROM agg_analysis_data a
            JOIN latest l ON a.isin = l.isin AND a.bpdate = l.max_date
            """
            bond_df = query_bigquery(bond_sql, client_id)
            if bond_df.empty:
                return [TextContent(
                    type="text",
                    text=json.dumps({"error": f"Cannot find bond {isin} in agg_analysis_data"}, indent=2)
                )]

            bond = bond_df.iloc[0]
            clean_price = float(bond['price'])
            accrued = float(bond['accrued_interest']) if bond['accrued_interest'] else 0.0
            dirty_price = clean_price + accrued

            # Calculate target dollar amount
            target_dollars = portfolio_value * (target_pct / 100.0)

            # Calculate par needed (dirty price basis)
            par_needed = (target_dollars / dirty_price) * 100

            # Round to proper sizing (min + increments)
            import math
            if par_needed < min_size:
                par_amount = min_size
            else:
                # Round up to next increment
                par_amount = math.ceil((par_needed - min_size) / increment) * increment + min_size

            # Calculate actual transaction cost
            market_value = (dirty_price * par_amount) / 100
            actual_pct = (market_value / portfolio_value) * 100

            # Get next transaction_id
            next_id_sql = """
            SELECT COALESCE(MAX(transaction_id), 0) + 1 as next_id
            FROM transactions
            """
            next_id_df = query_bigquery(next_id_sql, client_id)
            next_id = int(next_id_df.iloc[0]['next_id'])

            # Create staging transaction
            from google.cloud import bigquery
            from orca_mcp.tools.data_access import setup_bigquery_credentials
            setup_bigquery_credentials()
            bq_client = bigquery.Client(project="future-footing-414610")

            insert_sql = f"""
            INSERT INTO `future-footing-414610.portfolio_data.transactions`
            (transaction_id, portfolio_id, transaction_date, settlement_date,
             transaction_type, isin, ticker, description, country,
             par_amount, price, accrued_interest, dirty_price, market_value,
             ytm, duration, status, notes, created_at)
            VALUES
            ({next_id}, '{portfolio_id}', FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()),
             FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), 'BUY', '{isin}',
             '{bond["ticker"]}', '{bond["description"]}', '{bond["country"]}',
             {par_amount}, {clean_price}, {accrued}, {dirty_price}, {market_value},
             {float(bond['ytw']) if bond['ytw'] else 0}, {float(bond['oad']) if bond['oad'] else 0},
             'staging', 'Added via add_staging_allocation: target {target_pct}%',
             FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP()))
            """

            query_job = bq_client.query(insert_sql)
            query_job.result()

            result = {
                "success": True,
                "transaction_id": next_id,
                "bond": {
                    "isin": isin,
                    "ticker": bond["ticker"],
                    "description": bond["description"],
                    "country": bond["country"]
                },
                "pricing": {
                    "clean_price": clean_price,
                    "accrued_interest": accrued,
                    "dirty_price": dirty_price
                },
                "sizing": {
                    "target_pct": target_pct,
                    "target_dollars": target_dollars,
                    "par_amount": par_amount,
                    "market_value": market_value,
                    "actual_pct": round(actual_pct, 2),
                    "min_size": min_size,
                    "increment": increment
                },
                "portfolio_value": portfolio_value
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "add_staging_sell":
            isin = arguments.get("isin")
            country = arguments.get("country")
            cash_to_raise = arguments["cash_to_raise"]
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            min_size = arguments.get("min_size", 200000)
            increment = arguments.get("increment", 50000)

            # If country specified but not isin, find the bond with lowest return_ytw in that country
            if country and not isin:
                select_sql = f"""
                WITH holdings AS (
                    SELECT DISTINCT isin, ticker, description, country, par_amount, market_value
                    FROM transactions
                    WHERE portfolio_id = '{portfolio_id}'
                        AND status = 'settled'
                        AND transaction_type = 'BUY'
                        AND country = '{country}'
                ),
                latest_prices AS (
                    SELECT isin, MAX(bpdate) as max_date
                    FROM agg_analysis_data
                    WHERE isin IN (SELECT isin FROM holdings)
                    GROUP BY isin
                )
                SELECT
                    h.isin, h.ticker, h.description, h.country,
                    h.par_amount, h.market_value,
                    a.return_ytw,
                    a.oad as duration
                FROM holdings h
                JOIN latest_prices l ON h.isin = l.isin
                JOIN agg_analysis_data a ON h.isin = a.isin AND a.bpdate = l.max_date
                ORDER BY a.return_ytw ASC
                LIMIT 1
                """
                select_df = query_bigquery(select_sql, client_id)
                if select_df.empty:
                    return [TextContent(
                        type="text",
                        text=json.dumps({"error": f"No holdings found for country '{country}'"}, indent=2)
                    )]
                isin = select_df.iloc[0]['isin']
                selected_bond_info = {
                    "ticker": select_df.iloc[0]['ticker'],
                    "description": select_df.iloc[0]['description'],
                    "return_ytw": float(select_df.iloc[0]['return_ytw']),
                    "reason": f"Lowest return_ytw ({float(select_df.iloc[0]['return_ytw']):.2f}%) in {country}"
                }
            elif not isin:
                return [TextContent(
                    type="text",
                    text=json.dumps({"error": "Must specify either 'isin' or 'country'"}, indent=2)
                )]
            else:
                selected_bond_info = None

            # Check current holdings
            holdings_sql = f"""
            SELECT SUM(par_amount) as current_par
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND isin = '{isin}'
                AND status = 'settled'
                AND transaction_type = 'BUY'
            """
            holdings_df = query_bigquery(holdings_sql, client_id)
            if holdings_df.empty or holdings_df.iloc[0]['current_par'] is None:
                return [TextContent(
                    type="text",
                    text=json.dumps({"error": f"No holdings found for {isin} in portfolio"}, indent=2)
                )]
            current_par = float(holdings_df.iloc[0]['current_par'])

            # Get current bond price and data from agg_analysis_data
            bond_sql = f"""
            WITH latest AS (
                SELECT isin, MAX(bpdate) as max_date
                FROM agg_analysis_data
                WHERE isin = '{isin}'
                GROUP BY isin
            )
            SELECT a.isin, a.ticker, a.description, a.country,
                   a.price, a.accrued_interest, a.ytw, a.oad
            FROM agg_analysis_data a
            JOIN latest l ON a.isin = l.isin AND a.bpdate = l.max_date
            """
            bond_df = query_bigquery(bond_sql, client_id)
            if bond_df.empty:
                return [TextContent(
                    type="text",
                    text=json.dumps({"error": f"Cannot find bond {isin} in agg_analysis_data"}, indent=2)
                )]

            bond = bond_df.iloc[0]
            clean_price = float(bond['price'])
            accrued = float(bond['accrued_interest']) if bond['accrued_interest'] else 0.0
            dirty_price = clean_price + accrued

            # Calculate par needed to raise target cash
            par_needed = (cash_to_raise / dirty_price) * 100

            # Round to proper sizing (min + increments)
            import math
            if par_needed < min_size:
                par_amount = min_size
            else:
                # Round up to next increment
                par_amount = math.ceil((par_needed - min_size) / increment) * increment + min_size

            # Check if we have enough to sell
            if par_amount > current_par:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": f"Insufficient holdings. Have ${current_par:,.0f}, need ${par_amount:,.0f}",
                        "current_par": current_par,
                        "requested_par": par_amount
                    }, indent=2)
                )]

            # Calculate actual cash raised
            market_value = (dirty_price * par_amount) / 100

            # Get next transaction_id
            next_id_sql = """
            SELECT COALESCE(MAX(transaction_id), 0) + 1 as next_id
            FROM transactions
            """
            next_id_df = query_bigquery(next_id_sql, client_id)
            next_id = int(next_id_df.iloc[0]['next_id'])

            # Create staging SELL transaction
            from google.cloud import bigquery
            from orca_mcp.tools.data_access import setup_bigquery_credentials
            setup_bigquery_credentials()
            bq_client = bigquery.Client(project="future-footing-414610")

            insert_sql = f"""
            INSERT INTO `future-footing-414610.portfolio_data.transactions`
            (transaction_id, portfolio_id, transaction_date, settlement_date,
             transaction_type, isin, ticker, description, country,
             par_amount, price, accrued_interest, dirty_price, market_value,
             ytm, duration, status, notes, created_at)
            VALUES
            ({next_id}, '{portfolio_id}', FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()),
             FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), 'SELL', '{isin}',
             '{bond["ticker"]}', '{bond["description"]}', '{bond["country"]}',
             {par_amount}, {clean_price}, {accrued}, {dirty_price}, {market_value},
             {float(bond['ytw']) if bond['ytw'] else 0}, {float(bond['oad']) if bond['oad'] else 0},
             'staging', 'Added via add_staging_sell: raise ${cash_to_raise:,.0f}',
             FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP()))
            """

            query_job = bq_client.query(insert_sql)
            query_job.result()

            result = {
                "success": True,
                "transaction_id": next_id,
                "transaction_type": "SELL",
                "bond": {
                    "isin": isin,
                    "ticker": bond["ticker"],
                    "description": bond["description"],
                    "country": bond["country"],
                    "current_holdings": current_par
                },
                "pricing": {
                    "clean_price": clean_price,
                    "accrued_interest": accrued,
                    "dirty_price": dirty_price,
                    "ytw": float(bond['ytw']) if bond['ytw'] else None
                },
                "sizing": {
                    "cash_target": cash_to_raise,
                    "par_amount": par_amount,
                    "cash_raised": market_value,
                    "min_size": min_size,
                    "increment": increment
                }
            }

            # Add selection info if bond was auto-selected by country
            if selected_bond_info:
                result["selection"] = selected_bond_info

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "get_portfolio_cash":
            portfolio_id = arguments.get("portfolio_id", "wnbf")

            # Query portfolio_summary table
            summary_sql = f"""
            SELECT *
            FROM portfolio_summary
            WHERE portfolio_id = '{portfolio_id}'
            """
            summary_df = query_bigquery(summary_sql, client_id)

            if summary_df.empty:
                return [TextContent(
                    type="text",
                    text=json.dumps({"error": f"No summary found for portfolio '{portfolio_id}'. Run refresh_portfolio_summary first."}, indent=2)
                )]

            row = summary_df.iloc[0]
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
                },
                "metadata": {
                    "last_transaction_id": int(row['last_transaction_id']),
                    "updated_at": str(row['updated_at'])
                }
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "refresh_portfolio_summary":
            portfolio_id = arguments.get("portfolio_id", "wnbf")

            # Calculate summary values
            # Get starting cash
            starting_cash_sql = f"""
            SELECT SUM(market_value) as starting_cash
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND transaction_type = 'INITIAL'
                AND ticker = 'CASH'
            """
            starting_cash_df = query_bigquery(starting_cash_sql, client_id)
            starting_cash = float(starting_cash_df.iloc[0]['starting_cash']) if not starting_cash_df.empty else 0.0

            # Get settled bonds
            settled_sql = f"""
            SELECT
                COALESCE(SUM(market_value), 0) as settled_bonds_value,
                COUNT(*) as num_settled_bonds
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND status = 'settled'
                AND transaction_type = 'BUY'
            """
            settled_df = query_bigquery(settled_sql, client_id)
            settled_bonds_value = float(settled_df.iloc[0]['settled_bonds_value'])
            num_settled_bonds = int(settled_df.iloc[0]['num_settled_bonds'])

            # Get staging BUY
            staging_buy_sql = f"""
            SELECT COALESCE(SUM(market_value), 0) as staging_buy_value
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND status = 'staging'
                AND transaction_type = 'BUY'
            """
            staging_buy_df = query_bigquery(staging_buy_sql, client_id)
            staging_buy_value = float(staging_buy_df.iloc[0]['staging_buy_value'])

            # Get staging SELL
            staging_sell_sql = f"""
            SELECT COALESCE(SUM(market_value), 0) as staging_sell_value
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND status = 'staging'
                AND transaction_type = 'SELL'
            """
            staging_sell_df = query_bigquery(staging_sell_sql, client_id)
            staging_sell_value = float(staging_sell_df.iloc[0]['staging_sell_value'])

            # Get counts
            staging_count_sql = f"""
            SELECT COUNT(*) as num_staging
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
                AND status = 'staging'
            """
            staging_count_df = query_bigquery(staging_count_sql, client_id)
            num_staging = int(staging_count_df.iloc[0]['num_staging'])

            last_txn_sql = f"""
            SELECT MAX(transaction_id) as last_id
            FROM transactions
            WHERE portfolio_id = '{portfolio_id}'
            """
            last_txn_df = query_bigquery(last_txn_sql, client_id)
            last_transaction_id = int(last_txn_df.iloc[0]['last_id']) if last_txn_df.iloc[0]['last_id'] else 0

            # Calculate cash
            settled_cash = starting_cash - settled_bonds_value
            total_cash = settled_cash - staging_buy_value + staging_sell_value

            # Update portfolio_summary table
            from google.cloud import bigquery
            from orca_mcp.tools.data_access import setup_bigquery_credentials
            setup_bigquery_credentials()
            bq_client = bigquery.Client(project="future-footing-414610")

            # Delete existing
            delete_sql = f"""
            DELETE FROM `future-footing-414610.portfolio_data.portfolio_summary`
            WHERE portfolio_id = '{portfolio_id}'
            """
            query_job = bq_client.query(delete_sql)
            query_job.result()

            # Insert new
            insert_sql = f"""
            INSERT INTO `future-footing-414610.portfolio_data.portfolio_summary`
            (portfolio_id, starting_cash, settled_bonds_value, staging_buy_value, staging_sell_value,
             settled_cash, total_cash, num_settled_bonds, num_staging_transactions,
             last_transaction_id, updated_at)
            VALUES
            ('{portfolio_id}', {starting_cash}, {settled_bonds_value}, {staging_buy_value},
             {staging_sell_value}, {settled_cash}, {total_cash}, {num_settled_bonds},
             {num_staging}, {last_transaction_id}, CURRENT_TIMESTAMP())
            """
            query_job = bq_client.query(insert_sql)
            query_job.result()

            result = {
                "success": True,
                "portfolio_id": portfolio_id,
                "cash": {
                    "settled_cash": settled_cash,
                    "total_cash": total_cash
                },
                "summary": {
                    "starting_cash": starting_cash,
                    "settled_bonds_value": settled_bonds_value,
                    "staging_buy_value": staging_buy_value,
                    "staging_sell_value": staging_sell_value,
                    "num_settled_bonds": num_settled_bonds,
                    "num_staging_transactions": num_staging
                }
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "upload_table":
            table_name = arguments["table_name"]
            source_file = arguments["source_file"]
            format = arguments.get("format", "parquet")
            write_disposition = arguments.get("write_disposition", "WRITE_TRUNCATE")

            result = upload_table(
                table_name=table_name,
                source_file=source_file,
                client_id=client_id,
                format=format,
                write_disposition=write_disposition,
                invalidate_cache=True
            )

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "delete_records":
            table_name = arguments["table_name"]
            where_clause = arguments["where_clause"]

            result = delete_records(
                table_name=table_name,
                where_clause=where_clause,
                client_id=client_id,
                invalidate_cache=True
            )

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "invalidate_cache":
            pattern = arguments["pattern"]

            result = invalidate_cache_pattern(pattern)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_cache_stats":
            result = get_cache_stats()

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        # IMF Gateway - Generic access to ALL IMF data
        elif name == "fetch_imf_data":
            indicator = arguments["indicator"]
            countries = arguments["countries"]
            start_year = arguments.get("start_year")
            end_year = arguments.get("end_year")
            use_mcp = arguments.get("use_mcp", False)

            result = fetch_imf_data(
                indicator=indicator,
                countries=countries,
                start_year=start_year,
                end_year=end_year,
                use_mcp=use_mcp
            )

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_available_indicators":
            result = get_available_indicators()

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_available_country_groups":
            result = get_available_country_groups()

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        # ETF Reference Data
        elif name == "get_etf_allocation":
            isin = arguments["isin"]
            result = get_etf_allocation(isin)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "list_etf_allocations":
            result = list_etf_allocations()

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_etf_country_exposure":
            country = arguments["country"]
            result = get_etf_country_exposure(country)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        # Video Intelligence Tools
        elif name == "video_search":
            query = arguments["query"]
            max_results = arguments.get("max_results", 10)
            result = await video_search(query, max_results)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "video_list":
            result = await video_list()

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "video_synthesize":
            query = arguments["query"]
            video_results = arguments["video_results"]
            tone = arguments.get("tone", "professional")
            result = await video_synthesize(query, video_results, tone)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "video_get_transcript":
            video_id = arguments["video_id"]
            result = await video_get_transcript(video_id)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "video_keyword_search":
            query = arguments["query"]
            max_results = arguments.get("max_results", 10)
            result = await video_keyword_search(query, max_results)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        else:
            return [TextContent(
                type="text",
                text=f"Unknown tool: {name}"
            )]

    except Exception as e:
        logger.error(f"Error in {name}: {e}", exc_info=True)
        return [TextContent(
            type="text",
            text=f"Error: {str(e)}"
        )]


async def main():
    """Run the Orca MCP server"""
    client_id = os.getenv("CLIENT_ID", "guinness")
    logger.info(f"Starting Orca MCP Server for client: {client_id}")

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
