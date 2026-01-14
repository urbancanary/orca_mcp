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
from orca_mcp.tools.compliance import (
    check_compliance,
    check_compliance_impact,
    compliance_to_dict
)
from orca_mcp.tools.external_mcps import (
    # NFA
    get_nfa_rating,
    get_nfa_batch,
    search_nfa_by_rating,
    # Rating
    get_credit_rating,
    get_credit_ratings_batch,
    # Country Mapping
    standardize_country,
    get_country_info,
    # FRED
    get_fred_series,
    search_fred_series,
    get_treasury_rates,
    # Sovereign Classification
    classify_issuer,
    classify_issuers_batch,
    filter_by_issuer_type,
    get_issuer_summary,
    # IMF (external MCP with AI)
    get_imf_indicator,
    compare_imf_countries,
    # World Bank
    get_worldbank_indicator,
    search_worldbank_indicators,
    get_worldbank_country_profile,
    # Reasoning
    call_reasoning,
    analyze_data,
    list_reasoning_skills,
)
from orca_mcp.tools.query_router import route_query, detect_complexity, ORCA_QUERY_TOOL_DESCRIPTION
from orca_mcp.tools.cloudflare_d1 import get_watchlist, get_watchlist_complete
from orca_mcp.tools.sovereign_reports import (
    list_available_countries as sovereign_list_countries,
    get_sovereign_report,
    get_sovereign_section,
    search_sovereign_reports,
    get_sovereign_comparison,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orca-mcp")

# Create server instance
server = Server("orca-mcp")

# Tools hidden from Claude but still available via orca_query router
# These tools are routed internally - Claude only sees orca_query
HIDDEN_TOOLS = {
    # NFA/Credit Rating tools
    "get_nfa_rating",
    "get_nfa_batch",
    "get_credit_rating",
    "get_credit_ratings_batch",
    # IMF tools
    "fetch_imf_data",
    "get_imf_indicator_external",
    "compare_imf_countries",
    # World Bank tools
    "get_worldbank_indicator",
    "search_worldbank_indicators",
    "get_worldbank_country_profile",
    # FRED tools
    "get_fred_series",
    "search_fred_series",
    "get_treasury_rates",
    # Video tools
    "video_search",
    "video_list",
    "video_synthesize",
    "video_get_transcript",
    "video_keyword_search",
    # Portfolio/Bond tools
    "get_client_holdings",
    "get_client_transactions",
    "get_watchlist",
    "search_bonds_rvm",
    "classify_issuer",
    "classify_issuers_batch",
    "filter_by_issuer_type",
    "get_issuer_summary",
    # Compliance tools
    "get_compliance_status",
    "check_trade_compliance_impact",
    "suggest_rebalancing",
}

# Tools only visible to specific clients (specialist projects, would confuse others)
# Format: tool_name -> set of client_ids who can see it
CLIENT_ONLY_TOOLS = {
    # ETF tools - Guinness specialist project
    "get_etf_allocation": {"guinness"},
    "list_etf_allocations": {"guinness"},
    "get_etf_country_exposure": {"guinness"},
}

def _get_current_client_id() -> str:
    """Get current client ID from environment."""
    return os.environ.get("ORCA_CLIENT_ID", "guinness")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available Orca MCP tools (filters out hidden and client-restricted tools)"""
    all_tools = [
        # ========== ROUTER TOOL (Primary Entry Point) ==========
        Tool(
            name="orca_query",
            description=ORCA_QUERY_TOOL_DESCRIPTION,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query describing what data you need"
                    }
                },
                "required": ["query"]
            }
        ),
        # ========== LEGACY TOOLS (Will be hidden after router validation) ==========
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
            name="get_watchlist",
            description="Get the bond watchlist - candidate bonds for purchase. Returns ISINs with full analytics (YTW, OAD, OAS, ratings, country, etc.) from D1 database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "full_details": {
                        "type": "boolean",
                        "description": "If true, include full bond analytics. If false, just ISINs and basic info (default: true)"
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
        ),

        # Compliance Tools
        Tool(
            name="get_compliance_status",
            description="Get comprehensive UCITS compliance status with rich metrics. Returns: overall pass/fail, rule-by-rule breakdown with current vs limit values, headroom analysis (how much room before breaching limits), concentration metrics (top issuers, top countries), and key stats. Much more informative than a simple pass/fail check.",
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
            name="check_trade_compliance_impact",
            description="Pre-trade compliance check. Simulates adding a trade and shows how it would impact compliance. Returns before/after comparison showing which rules would be affected, whether it would breach any limits, and the projected new values for each metric.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    },
                    "ticker": {
                        "type": "string",
                        "description": "Ticker of the bond to trade"
                    },
                    "country": {
                        "type": "string",
                        "description": "Country of the bond"
                    },
                    "action": {
                        "type": "string",
                        "enum": ["buy", "sell"],
                        "description": "Trade action"
                    },
                    "market_value": {
                        "type": "number",
                        "description": "Market value of the trade (positive number)"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": ["ticker", "country", "action", "market_value"]
            }
        ),
        Tool(
            name="search_bonds_rvm",
            description="Search the RVM (Relative Value Model) universe for bonds. Use this INSTEAD of web search when looking for bond investment opportunities. Returns bonds with yield, spread, duration, expected return from the analytics database. Can filter by country, issuer type (sovereign, quasi-sovereign, corporate), rating, and sort by expected return. Includes both sovereign bonds AND quasi-sovereigns (state-owned enterprises like Codelco, Pemex, Petrobras).",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {
                        "type": "string",
                        "description": "Country to filter by (e.g., 'Chile', 'Mexico', 'Saudi Arabia'). Case insensitive."
                    },
                    "ticker": {
                        "type": "string",
                        "description": "Ticker pattern to search (e.g., 'CHILE' for sovereigns, 'CDEL' for Codelco, 'PEMEX' for Pemex)"
                    },
                    "issuer_type": {
                        "type": "string",
                        "enum": ["sovereign", "quasi-sovereign", "corporate", "all"],
                        "description": "Filter by issuer type. 'sovereign' = government bonds, 'quasi-sovereign' = state-owned enterprises (Pemex, Codelco, etc), 'all' = everything"
                    },
                    "min_expected_return": {
                        "type": "number",
                        "description": "Minimum expected return (%) from RVM model"
                    },
                    "max_duration": {
                        "type": "number",
                        "description": "Maximum duration in years"
                    },
                    "min_rating": {
                        "type": "string",
                        "description": "Minimum S&P credit rating (e.g., 'BBB-', 'A', 'AA'). Filters out junk bonds."
                    },
                    "min_nfa_rating": {
                        "type": "integer",
                        "description": "Minimum NFA star rating (1-7). 3+ recommended for investment grade countries."
                    },
                    "sort_by": {
                        "type": "string",
                        "enum": ["expected_return", "yield", "spread", "duration"],
                        "description": "How to sort results (default: expected_return descending)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results (default: 10)"
                    },
                    "exclude_portfolio": {
                        "type": "boolean",
                        "description": "Exclude bonds already in portfolio (default: true)"
                    },
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID to exclude holdings from (default: 'wnbf')"
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
            name="suggest_rebalancing",
            description="Analyze portfolio and suggest rebalancing trades to improve compliance, diversification, or optimize expected returns. Returns prioritized list of suggested sells (overweight positions, low return bonds) and buys (underweight countries, high return opportunities). Considers compliance headroom and available cash.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    },
                    "focus": {
                        "type": "string",
                        "enum": ["compliance", "diversification", "returns", "all"],
                        "description": "What to optimize for: 'compliance' (fix breaches), 'diversification' (reduce concentration), 'returns' (sell low/buy high expected return), 'all' (balanced). Default: 'all'"
                    },
                    "max_suggestions": {
                        "type": "integer",
                        "description": "Maximum number of trade suggestions (default: 5)"
                    },
                    "client_id": {
                        "type": "string",
                        "description": "Client identifier (optional)"
                    }
                },
                "required": []
            }
        ),

        # ============================================================================
        # EXTERNAL MCP TOOLS - Gateway to Cloudflare Worker MCPs
        # ============================================================================
        # ========== RATING TOOLS (kept visible until router API key fixed) ==========
        # NFA MCP Tools
        Tool(
            name="get_nfa_rating",
            description="Get NFA (Net Foreign Assets) star rating for a country (1-7 scale).",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name"},
                    "year": {"type": "integer", "description": "Specific year (optional)"},
                    "history": {"type": "boolean", "description": "Return full history"}
                },
                "required": ["country"]
            }
        ),
        Tool(
            name="get_nfa_batch",
            description="Get NFA ratings for multiple countries.",
            inputSchema={
                "type": "object",
                "properties": {
                    "countries": {"type": "array", "items": {"type": "string"}},
                    "year": {"type": "integer"}
                },
                "required": ["countries"]
            }
        ),
        Tool(
            name="get_credit_rating",
            description="Get sovereign credit rating (S&P, Moody's, Fitch) for a country.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name"}
                },
                "required": ["country"]
            }
        ),
        Tool(
            name="get_credit_ratings_batch",
            description="Get credit ratings for multiple countries.",
            inputSchema={
                "type": "object",
                "properties": {
                    "countries": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["countries"]
            }
        ),

        # Country Mapping MCP Tools
        Tool(
            name="standardize_country",
            description="Standardize a country name to canonical form. Handles variations like 'UAE' vs 'United Arab Emirates', 'Korea' vs 'South Korea'. Essential for data consistency.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {
                        "type": "string",
                        "description": "Country name in any format"
                    }
                },
                "required": ["country"]
            }
        ),
        Tool(
            name="get_country_info",
            description="Get comprehensive country information including ISO codes, region, income level, and all known aliases.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {
                        "type": "string",
                        "description": "Country name"
                    }
                },
                "required": ["country"]
            }
        ),

        # FRED MCP Tools
        Tool(
            name="get_fred_series",
            description="Get Federal Reserve Economic Data (FRED) series. Common series: DGS10 (10Y Treasury), DGS2 (2Y Treasury), CPIAUCSL (CPI), UNRATE (unemployment), FEDFUNDS (Fed Funds rate).",
            inputSchema={
                "type": "object",
                "properties": {
                    "series_id": {
                        "type": "string",
                        "description": "FRED series ID (e.g., 'DGS10', 'CPIAUCSL', 'UNRATE')"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date (YYYY-MM-DD, optional)"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date (YYYY-MM-DD, optional)"
                    },
                    "analyze": {
                        "type": "boolean",
                        "description": "If true, include AI analysis of the data"
                    }
                },
                "required": ["series_id"]
            }
        ),
        Tool(
            name="search_fred_series",
            description="Search for FRED data series by keyword. Returns matching series with IDs and descriptions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search term (e.g., 'treasury', 'inflation', 'unemployment')"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_treasury_rates",
            description="Get current US Treasury rates across the entire yield curve (1M to 30Y). Essential for fixed income analysis and spread calculations.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),

        # Sovereign Classification MCP Tools
        Tool(
            name="classify_issuer",
            description="Classify a bond issuer by ISIN as sovereign, quasi-sovereign, or corporate. Quasi-sovereigns include state-owned enterprises like Pemex, Codelco, Petrobras.",
            inputSchema={
                "type": "object",
                "properties": {
                    "isin": {
                        "type": "string",
                        "description": "Bond ISIN"
                    }
                },
                "required": ["isin"]
            }
        ),
        Tool(
            name="classify_issuers_batch",
            description="Classify multiple bond issuers by ISIN. Efficient for portfolio analysis.",
            inputSchema={
                "type": "object",
                "properties": {
                    "isins": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of ISINs"
                    }
                },
                "required": ["isins"]
            }
        ),
        Tool(
            name="filter_by_issuer_type",
            description="Get all issuers of a specific type. Useful for building filtered watchlists.",
            inputSchema={
                "type": "object",
                "properties": {
                    "issuer_type": {
                        "type": "string",
                        "enum": ["sovereign", "quasi-sovereign", "corporate"],
                        "description": "Type of issuer to filter"
                    }
                },
                "required": ["issuer_type"]
            }
        ),
        Tool(
            name="get_issuer_summary",
            description="Get AI-generated summary for an issuer including business description, credit factors, and investment considerations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "issuer": {
                        "type": "string",
                        "description": "Issuer name or ticker (e.g., 'Pemex', 'Codelco', 'Colombia')"
                    }
                },
                "required": ["issuer"]
            }
        ),

        # IMF MCP Tools (with AI analysis)
        Tool(
            name="get_imf_indicator_external",
            description="Get IMF economic indicator data with optional AI analysis. This uses the IMF MCP which includes Haiku analysis. Common indicators: NGDP_RPCH (GDP growth), PCPIPCH (inflation), LUR (unemployment), GGXWDG_NGDP (government debt/GDP).",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {
                        "type": "string",
                        "description": "IMF indicator code"
                    },
                    "country": {
                        "type": "string",
                        "description": "Country name or ISO code"
                    },
                    "start_year": {
                        "type": "integer",
                        "description": "Start year (optional)"
                    },
                    "end_year": {
                        "type": "integer",
                        "description": "End year (optional)"
                    },
                    "analyze": {
                        "type": "boolean",
                        "description": "If true, include AI analysis via Haiku"
                    }
                },
                "required": ["indicator", "country"]
            }
        ),
        Tool(
            name="compare_imf_countries",
            description="Compare IMF indicator across multiple countries. Useful for relative value analysis.",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {
                        "type": "string",
                        "description": "IMF indicator code"
                    },
                    "countries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of country names"
                    },
                    "year": {
                        "type": "integer",
                        "description": "Specific year (optional, default: latest)"
                    }
                },
                "required": ["indicator", "countries"]
            }
        ),

        # World Bank MCP Tools
        Tool(
            name="get_worldbank_indicator",
            description="Get World Bank development indicator data. Common indicators: NY.GDP.PCAP.CD (GDP per capita), SP.POP.TOTL (population), SE.ADT.LITR.ZS (literacy rate).",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {
                        "type": "string",
                        "description": "World Bank indicator code"
                    },
                    "country": {
                        "type": "string",
                        "description": "Country name or ISO code"
                    },
                    "start_year": {
                        "type": "integer",
                        "description": "Start year (optional)"
                    },
                    "end_year": {
                        "type": "integer",
                        "description": "End year (optional)"
                    }
                },
                "required": ["indicator", "country"]
            }
        ),
        Tool(
            name="search_worldbank_indicators",
            description="Search for World Bank indicators by keyword. Returns matching indicators with codes and descriptions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search term (e.g., 'gdp', 'population', 'education')"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_worldbank_country_profile",
            description="Get comprehensive country development profile from World Bank including key economic and social indicators.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {
                        "type": "string",
                        "description": "Country name or ISO code"
                    }
                },
                "required": ["country"]
            }
        ),
        # ========== SOVEREIGN CREDIT REPORTS ==========
        Tool(
            name="sovereign_list_countries",
            description="List all available sovereign credit reports. Returns country names with full credit analysis available.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="sovereign_get_report",
            description="Get the full sovereign credit report for a country. Returns comprehensive credit analysis including ratings, economic indicators, fiscal policy, political assessment, and outlook.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {
                        "type": "string",
                        "description": "Country name (e.g., 'Brazil', 'Kazakhstan', 'Turkey')"
                    }
                },
                "required": ["country"]
            }
        ),
        Tool(
            name="sovereign_get_section",
            description="Get a specific section from a sovereign credit report. Sections: summary, ratings, economic, fiscal, external, political, banking, outlook, strengths, vulnerabilities.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {
                        "type": "string",
                        "description": "Country name"
                    },
                    "section": {
                        "type": "string",
                        "description": "Section name: summary, ratings, economic, fiscal, external, political, banking, outlook, strengths, vulnerabilities"
                    }
                },
                "required": ["country", "section"]
            }
        ),
        Tool(
            name="sovereign_search",
            description="Search across all sovereign credit reports for a term or phrase. Returns matching excerpts from each country.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search term or phrase"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum results per country (default 5)"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="sovereign_compare",
            description="Compare key credit metrics across multiple countries. Returns ratings, outlook, strengths, and risks for each.",
            inputSchema={
                "type": "object",
                "properties": {
                    "countries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of country names to compare"
                    }
                },
                "required": ["countries"]
            }
        ),
        # ========== REASONING TOOLS ==========
        Tool(
            name="analyze",
            description="Analyze data using AI reasoning. Provide portfolio data, query results, or other data and get intelligent analysis with reasoning trace.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data": {
                        "type": "object",
                        "description": "The data to analyze (holdings, transactions, query results, etc.)"
                    },
                    "objective": {
                        "type": "string",
                        "description": "What kind of analysis to perform (e.g., 'identify risks', 'suggest optimizations', 'explain trends')"
                    },
                    "require_compliance": {
                        "type": "boolean",
                        "description": "If true, any suggestions will be checked for compliance",
                        "default": False
                    }
                },
                "required": ["data", "objective"]
            }
        ),
        Tool(
            name="reason",
            description="Get AI-powered reasoning on a natural language query about portfolios, markets, or investments. Routes to specialized skills for trading, compliance, and more.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language question or request"
                    },
                    "portfolio_context": {
                        "type": "object",
                        "description": "Optional portfolio data for context"
                    },
                    "require_compliance": {
                        "type": "boolean",
                        "description": "If true, suggestions pass compliance checks",
                        "default": False
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="list_reasoning_skills",
            description="List available reasoning skills (trading, compliance, charting, etc.)",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="complex_query",
            description="""Handle complex multi-step queries that require data fetching, filtering, and analysis.

Use this for queries like:
- "Credit summary for our top 2 holdings"
- "Analyze risk exposure across all EM positions"
- "Compare yields of our highest rated bonds"
- "Which holdings have the worst credit outlook?"

This tool decomposes complex requests into sub-queries, fetches the required data, and synthesizes a comprehensive response.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The complex natural language query"
                    },
                    "portfolio_id": {
                        "type": "string",
                        "description": "Optional portfolio to focus on"
                    }
                },
                "required": ["query"]
            }
        )
    ]

    # Filter out hidden tools and client-restricted tools
    client_id = _get_current_client_id()

    def is_tool_visible(tool_name: str) -> bool:
        # Hidden tools are never visible (routed via orca_query)
        if tool_name in HIDDEN_TOOLS:
            return False
        # Client-only tools visible only to specified clients
        if tool_name in CLIENT_ONLY_TOOLS:
            return client_id in CLIENT_ONLY_TOOLS[tool_name]
        return True

    return [tool for tool in all_tools if is_tool_visible(tool.name)]


# ============================================================================
# COMPLEX QUERY HANDLER - Multi-step reasoning orchestration
# ============================================================================

async def handle_complex_query(query: str, portfolio_id: str = None, client_id: str = None) -> Dict[str, Any]:
    """
    Handle complex multi-step queries using reasoning MCP for decomposition and synthesis.

    Flow:
    1. Analyze query complexity
    2. Call reasoning MCP to decompose into sub-queries
    3. Execute sub-queries (data fetching)
    4. Call reasoning MCP to synthesize results
    5. Return combined analysis

    Args:
        query: The complex natural language query
        portfolio_id: Optional portfolio to focus on
        client_id: Optional client ID for data access

    Returns:
        {
            "query": original query,
            "complexity": complexity analysis,
            "steps": list of executed steps,
            "data": fetched data,
            "analysis": synthesized analysis,
            "summary": human-readable summary
        }
    """
    import asyncio

    result = {
        "query": query,
        "steps": [],
        "data": {},
        "analysis": None,
        "summary": None
    }

    try:
        # Step 1: Analyze complexity
        complexity = detect_complexity(query)
        result["complexity"] = complexity
        result["steps"].append({
            "step": 1,
            "action": "complexity_analysis",
            "result": complexity
        })

        # Step 2: Decompose query using reasoning MCP
        decomposition_prompt = f"""Decompose this financial query into executable sub-queries.

Query: "{query}"
{f'Portfolio: {portfolio_id}' if portfolio_id else ''}

Available data sources:
- get_client_holdings: Get portfolio holdings (returns ticker, country, weight, value, rating)
- get_credit_rating: Get S&P/Moody's credit rating for a country
- get_nfa_rating: Get NFA star rating (1-7) for a country
- get_portfolio_cash: Get cash positions
- get_compliance_status: Check UCITS compliance

Return a JSON object with:
{{
    "intent": "what the user wants to know",
    "sub_queries": [
        {{"tool": "tool_name", "args": {{}}, "purpose": "why this is needed"}}
    ],
    "synthesis_approach": "how to combine the results"
}}"""

        decomposition = call_reasoning(decomposition_prompt, require_compliance=False)
        result["steps"].append({
            "step": 2,
            "action": "decomposition",
            "result": decomposition
        })

        # Step 3: Execute sub-queries
        # For now, we'll execute common patterns directly
        # In production, we'd parse the decomposition and call tools dynamically

        # Common pattern: Get holdings first
        from orca_mcp.tools.data_access import get_client_holdings, get_portfolio_cash

        holdings_data = get_client_holdings(client_id=client_id, portfolio_id=portfolio_id)
        result["data"]["holdings"] = holdings_data
        result["steps"].append({
            "step": 3,
            "action": "fetch_holdings",
            "result": f"Fetched {len(holdings_data.get('holdings', []))} holdings"
        })

        # If query mentions "top N", filter to top N by weight
        import re
        top_match = re.search(r'\b(top|largest|biggest)\s+(\d+)', query.lower())
        if top_match:
            n = int(top_match.group(2))
            holdings = holdings_data.get("holdings", [])
            # Sort by weight descending and take top N
            sorted_holdings = sorted(holdings, key=lambda x: float(x.get("weight", 0)), reverse=True)
            top_holdings = sorted_holdings[:n]
            result["data"]["top_holdings"] = top_holdings
            result["steps"].append({
                "step": 4,
                "action": "filter_top_n",
                "result": f"Filtered to top {n} holdings"
            })

            # Get credit ratings for top holdings
            countries = list(set(h.get("country") for h in top_holdings if h.get("country")))
            from orca_mcp.tools.external_mcps import get_credit_rating, get_nfa_rating

            credit_data = {}
            for country in countries:
                try:
                    credit_data[country] = {
                        "credit_rating": get_credit_rating(country),
                        "nfa_rating": get_nfa_rating(country)
                    }
                except Exception as e:
                    credit_data[country] = {"error": str(e)}

            result["data"]["credit_ratings"] = credit_data
            result["steps"].append({
                "step": 5,
                "action": "fetch_credit_ratings",
                "result": f"Fetched ratings for {len(countries)} countries"
            })

        # Step 4: Synthesize results using reasoning MCP
        synthesis_prompt = f"""Analyze this portfolio data and provide a clear summary.

Original query: "{query}"

Data collected:
{json.dumps(result["data"], indent=2, default=str)}

Provide:
1. A concise answer to the user's query
2. Key insights from the data
3. Any risks or concerns to note

Format your response as a clear, readable summary."""

        synthesis = call_reasoning(synthesis_prompt, require_compliance=False)
        result["analysis"] = synthesis
        result["steps"].append({
            "step": 6,
            "action": "synthesis",
            "result": "Generated analysis"
        })

        # Extract summary from reasoning response
        if synthesis.get("response"):
            result["summary"] = synthesis["response"]
        elif synthesis.get("error"):
            result["summary"] = f"Analysis unavailable: {synthesis['error']}"
        else:
            # Generate a basic summary from the data
            if "top_holdings" in result["data"]:
                holdings = result["data"]["top_holdings"]
                summary_parts = []
                for h in holdings:
                    ticker = h.get("ticker", "Unknown")
                    country = h.get("country", "Unknown")
                    weight = h.get("weight", 0)
                    credit_info = result["data"].get("credit_ratings", {}).get(country, {})
                    rating = credit_info.get("credit_rating", {}).get("rating", "N/A")
                    summary_parts.append(f"- {ticker} ({country}): {weight:.1f}% weight, {rating} rating")
                result["summary"] = "Top holdings:\n" + "\n".join(summary_parts)
            else:
                result["summary"] = "Query processed successfully. See data for details."

        return result

    except Exception as e:
        logger.error(f"Complex query error: {e}", exc_info=True)
        result["error"] = str(e)
        result["summary"] = f"Error processing query: {e}"
        return result


@server.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls"""

    try:
        client_id = arguments.get("client_id")
        config = get_client_config(client_id)

        # Access check (skip for orca_query as it routes internally)
        if name != "orca_query" and not config.is_tool_allowed(name):
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": "ACCESS_DENIED",
                    "message": f"Tool '{name}' is not available for this client",
                    "client_id": config.client_id
                }, indent=2)
            )]

        # ========== ROUTER HANDLER ==========
        if name == "orca_query":
            query = arguments.get("query", "")
            if not query:
                return [TextContent(
                    type="text",
                    text=json.dumps({"error": "Query is required"}, indent=2)
                )]

            # Route the query using Haiku
            routing_result = await route_query(query)

            # Check if this is a complex query that needs multi-step reasoning
            complexity = routing_result.get("complexity", {})
            if complexity.get("is_complex") and complexity.get("confidence", 0) >= 0.7:
                logger.info(f"Complex query detected: {complexity.get('patterns_matched')} - routing to complex_query handler")
                result = await handle_complex_query(
                    query=query,
                    portfolio_id=arguments.get("portfolio_id"),
                    client_id=client_id
                )
                return [TextContent(
                    type="text",
                    text=json.dumps(result, indent=2, default=str)
                )]

            # Check for clarification needed
            if routing_result.get("tool") is None:
                if "clarification" in routing_result:
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "status": "clarification_needed",
                            "message": routing_result["clarification"]
                        }, indent=2)
                    )]
                elif "error" in routing_result:
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "error": "Routing failed",
                            "details": routing_result.get("error")
                        }, indent=2)
                    )]

            # Execute the routed tool
            routed_tool = routing_result["tool"]
            routed_args = routing_result.get("args", {})

            logger.info(f"Router executing: {routed_tool} with args: {routed_args}")

            # Recursively call this handler with the routed tool
            # Add client_id to args if not present
            if "client_id" not in routed_args and client_id:
                routed_args["client_id"] = client_id

            return await call_tool(routed_tool, routed_args)

        # ========== LEGACY TOOL HANDLERS ==========
        elif name == "get_client_info":
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

        elif name == "get_watchlist":
            full_details = arguments.get("full_details", True)

            if full_details:
                # Use /prices/latest which has consolidated watchlist + analytics
                import urllib.request
                pricing_url = os.environ.get('GA10_PRICING_URL', 'https://ga10-pricing.urbancanary.workers.dev')
                url = f"{pricing_url}/prices/latest"

                try:
                    req = urllib.request.Request(url)
                    with urllib.request.urlopen(req, timeout=15) as response:
                        data = json.loads(response.read().decode())
                        prices = data.get('prices', [])

                        result = {
                            "watchlist": prices,
                            "count": len(prices),
                            "source": "Cloudflare D1 (consolidated)",
                            "columns": ["isin", "description", "cbonds_country", "price", "yield_to_maturity", "modified_duration", "spread", "maturity_date"]
                        }
                except Exception as e:
                    logger.error(f"Failed to fetch watchlist: {e}")
                    result = {"watchlist": [], "count": 0, "error": str(e)}
            else:
                df = get_watchlist(client_id)
                if df.empty:
                    result = {"watchlist": [], "count": 0, "message": "No bonds in watchlist"}
                else:
                    result = {
                        "watchlist": df.to_dict(orient='records'),
                        "count": len(df),
                        "source": "Cloudflare D1"
                    }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
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

        # Compliance Tools
        elif name == "get_compliance_status":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            import pandas as pd

            # Get holdings from D1 via existing method
            holdings_sql = f"""
            WITH net_holdings AS (
                SELECT
                    isin, ticker, description, country,
                    SUM(CASE WHEN transaction_type = 'BUY' THEN par_amount ELSE 0 END) -
                    SUM(CASE WHEN transaction_type = 'SELL' THEN par_amount ELSE 0 END) as par_amount,
                    SUM(CASE WHEN transaction_type = 'BUY' THEN market_value ELSE 0 END) -
                    SUM(CASE WHEN transaction_type = 'SELL' THEN market_value ELSE 0 END) as market_value
                FROM transactions
                WHERE portfolio_id = '{portfolio_id}'
                    AND status = 'settled'
                    AND isin != 'CASH'
                GROUP BY isin, ticker, description, country
                HAVING par_amount > 0
            )
            SELECT * FROM net_holdings
            """
            holdings_df = query_bigquery(holdings_sql, client_id)

            # Get cash
            cash_sql = f"""
            SELECT
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND ticker = 'CASH' AND transaction_type = 'INITIAL') -
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND status = 'settled' AND transaction_type = 'BUY' AND isin != 'CASH') +
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND status = 'settled' AND transaction_type = 'SELL') as net_cash
            """
            cash_df = query_bigquery(cash_sql, client_id)
            net_cash = float(cash_df.iloc[0]['net_cash']) if not cash_df.empty else 0.0

            # Run compliance check
            compliance_result = check_compliance(holdings_df, net_cash)
            base_result = compliance_to_dict(compliance_result)

            # Add rich metrics for Claude
            metrics = compliance_result.metrics
            total_nav = metrics['total_nav']

            # Calculate headroom (how much room before breaching limits)
            headroom = {
                'max_issuer_headroom_pct': round(10.0 - metrics['max_position'], 2),
                'max_issuer_headroom_dollars': round((10.0 - metrics['max_position']) * total_nav / 100, 0),
                'max_country_headroom_pct': round(20.0 - metrics['max_country_pct'], 2),
                'max_country_headroom_dollars': round((20.0 - metrics['max_country_pct']) * total_nav / 100, 0),
                'cash_headroom_pct': round(5.0 - metrics['cash_pct'], 2) if metrics['cash_pct'] < 5 else 0,
            }

            # Top 5 issuers and countries
            issuer_weights = sorted(metrics['issuer_weights'].items(), key=lambda x: x[1], reverse=True)[:5]
            country_weights = sorted(metrics['country_breakdown'].items(), key=lambda x: x[1], reverse=True)[:5]

            # Build rich response
            result = {
                'summary': {
                    'is_compliant': compliance_result.is_compliant,
                    'status': 'PASS' if compliance_result.is_compliant else 'FAIL',
                    'hard_rules': f"{compliance_result.hard_pass}/{compliance_result.hard_total} pass",
                    'soft_rules': f"{compliance_result.soft_pass}/{compliance_result.soft_total} pass",
                },
                'key_metrics': {
                    'total_nav': f"${total_nav/1_000_000:.2f}M",
                    'num_holdings': metrics['num_holdings'],
                    'avg_position': f"{metrics['avg_position']:.1f}%",
                    'max_issuer': f"{metrics['max_position']:.1f}% ({metrics['max_position_ticker']})",
                    'max_country': f"{metrics['max_country_pct']:.1f}% ({metrics['max_country']})",
                    'cash': f"{metrics['cash_pct']:.1f}% (${net_cash/1000:.0f}k)",
                },
                'headroom': {
                    'max_issuer': f"{headroom['max_issuer_headroom_pct']:.1f}% (${headroom['max_issuer_headroom_dollars']/1000:.0f}k) before breach",
                    'max_country': f"{headroom['max_country_headroom_pct']:.1f}% (${headroom['max_country_headroom_dollars']/1000:.0f}k) before breach",
                },
                'concentration': {
                    'top_5_issuers': [{'ticker': t, 'weight': f"{w:.1f}%"} for t, w in issuer_weights],
                    'top_5_countries': [{'country': c, 'weight': f"{w:.1f}%"} for c, w in country_weights],
                    'issuers_over_5pct': metrics['num_issuers_over_5'],
                    'sum_issuers_over_5pct': f"{metrics['sum_over_5_pct']:.1f}%",
                },
                'rules': base_result['rules']
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "check_trade_compliance_impact":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            ticker = arguments["ticker"]
            country = arguments["country"]
            action = arguments["action"]
            market_value = arguments["market_value"]
            import pandas as pd

            # Get holdings from D1
            holdings_sql = f"""
            WITH net_holdings AS (
                SELECT
                    isin, ticker, description, country,
                    SUM(CASE WHEN transaction_type = 'BUY' THEN par_amount ELSE 0 END) -
                    SUM(CASE WHEN transaction_type = 'SELL' THEN par_amount ELSE 0 END) as par_amount,
                    SUM(CASE WHEN transaction_type = 'BUY' THEN market_value ELSE 0 END) -
                    SUM(CASE WHEN transaction_type = 'SELL' THEN market_value ELSE 0 END) as market_value
                FROM transactions
                WHERE portfolio_id = '{portfolio_id}'
                    AND status = 'settled'
                    AND isin != 'CASH'
                GROUP BY isin, ticker, description, country
                HAVING par_amount > 0
            )
            SELECT * FROM net_holdings
            """
            holdings_df = query_bigquery(holdings_sql, client_id)

            # Get cash
            cash_sql = f"""
            SELECT
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND ticker = 'CASH' AND transaction_type = 'INITIAL') -
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND status = 'settled' AND transaction_type = 'BUY' AND isin != 'CASH') +
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND status = 'settled' AND transaction_type = 'SELL') as net_cash
            """
            cash_df = query_bigquery(cash_sql, client_id)
            net_cash = float(cash_df.iloc[0]['net_cash']) if not cash_df.empty else 0.0

            # Run impact check
            proposed_trade = {
                'ticker': ticker,
                'country': country,
                'action': action,
                'market_value': market_value
            }
            impact_result = check_compliance_impact(holdings_df, net_cash, proposed_trade)

            # Add human-readable summary
            before = impact_result['before']
            after = impact_result['after']
            impact = impact_result['impact']

            summary = {
                'trade': f"{action.upper()} ${market_value/1000:.0f}k {ticker} ({country})",
                'will_breach': impact['would_breach'],
                'will_fix_breach': impact['would_fix'],
                'compliance_change': impact['compliance_change'],
                'before_status': 'PASS' if before['is_compliant'] else 'FAIL',
                'after_status': 'PASS' if after['is_compliant'] else 'FAIL',
            }

            # Find rules that change
            rule_changes = []
            for i, before_rule in enumerate(before['rules']):
                after_rule = after['rules'][i]
                if before_rule['status'] != after_rule['status']:
                    rule_changes.append({
                        'rule': before_rule['name'],
                        'before': f"{before_rule['current']} ({before_rule['status']})",
                        'after': f"{after_rule['current']} ({after_rule['status']})",
                    })

            result = {
                'summary': summary,
                'rule_changes': rule_changes if rule_changes else 'No rule status changes',
                'before_metrics': {
                    'max_issuer': f"{before['metrics']['max_position']:.1f}%",
                    'max_country': f"{before['metrics']['max_country_pct']:.1f}%",
                    'cash': f"{before['metrics']['cash_pct']:.1f}%",
                },
                'after_metrics': {
                    'max_issuer': f"{after['metrics']['max_position']:.1f}%",
                    'max_country': f"{after['metrics']['max_country_pct']:.1f}%",
                    'cash': f"{after['metrics']['cash_pct']:.1f}%",
                },
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "search_bonds_rvm":
            # Search RVM universe for bonds - use this instead of web search!
            country = arguments.get("country")
            ticker_pattern = arguments.get("ticker")
            issuer_type = arguments.get("issuer_type", "all")
            min_expected_return = arguments.get("min_expected_return")
            max_duration = arguments.get("max_duration")
            min_rating = arguments.get("min_rating")
            min_nfa_rating = arguments.get("min_nfa_rating")
            sort_by = arguments.get("sort_by", "expected_return")
            limit = arguments.get("limit", 10)
            exclude_portfolio = arguments.get("exclude_portfolio", True)
            portfolio_id = arguments.get("portfolio_id", "wnbf")

            # Map sort_by to SQL columns
            sort_map = {
                'expected_return': 'return_ytw',
                'yield': 'ytw',
                'spread': 'oas',
                'duration': 'oad'
            }
            sort_column = sort_map.get(sort_by, 'return_ytw')

            # Known quasi-sovereign tickers by country for issuer_type filtering
            quasi_sovereigns = {
                'PEMEX', 'CFE', 'NAFIN',  # Mexico
                'CDEL', 'ENAPCL', 'BMETR', 'BCHILE',  # Chile
                'PETBRA', 'BNDES',  # Brazil
                'ECOPET',  # Colombia
                'KSA', 'ARAMCO',  # Saudi Arabia (ARAMCO is quasi)
                'QATAEN', 'QNBK',  # Qatar
                'KUWSOV', 'KPC',  # Kuwait
                'ADSOVR',  # Abu Dhabi
                'PERULN', 'COFIDE',  # Peru
                'INDON', 'PLNIJ',  # Indonesia
                'MLAY', 'PETMK',  # Malaysia
            }

            # Build WHERE conditions
            conditions = []

            if country:
                # Use CAST to handle both STRING and INT64 country columns
                # Also handle case-insensitive matching
                conditions.append(f"LOWER(CAST(a.country AS STRING)) = LOWER('{country}')")

            if ticker_pattern:
                conditions.append(f"UPPER(a.ticker) LIKE UPPER('%{ticker_pattern}%')")

            if issuer_type == 'sovereign':
                # Sovereigns typically have country name as ticker or specific sovereign tickers
                quasi_list = "', '".join(quasi_sovereigns)
                conditions.append(f"UPPER(a.ticker) NOT IN ('{quasi_list}')")
            elif issuer_type == 'quasi-sovereign':
                quasi_list = "', '".join(quasi_sovereigns)
                conditions.append(f"UPPER(a.ticker) IN ('{quasi_list}')")

            if min_expected_return is not None:
                conditions.append(f"a.return_ytw >= {min_expected_return}")

            if max_duration is not None:
                conditions.append(f"a.oad <= {max_duration}")

            if min_rating:
                # Convert letter rating to numeric for comparison
                # Standard S&P scale: AAA=21, AA+=20, AA=19, ... BBB-=12, BB+=11, etc.
                rating_scale = {
                    'AAA': 21, 'AA+': 20, 'AA': 19, 'AA-': 18,
                    'A+': 17, 'A': 16, 'A-': 15,
                    'BBB+': 14, 'BBB': 13, 'BBB-': 12,
                    'BB+': 11, 'BB': 10, 'BB-': 9,
                    'B+': 8, 'B': 7, 'B-': 6,
                    'CCC+': 5, 'CCC': 4, 'CCC-': 3,
                    'CC': 2, 'C': 1, 'D': 0
                }
                min_rating_upper = min_rating.upper()
                if min_rating_upper in rating_scale:
                    min_numeric = rating_scale[min_rating_upper]
                    # Build rating comparison - check both S&P and Moody's, take best
                    rating_conditions = []
                    for r, n in rating_scale.items():
                        if n >= min_numeric:
                            rating_conditions.append(f"UPPER(a.rating_sp) = '{r}'")
                    if rating_conditions:
                        conditions.append(f"({' OR '.join(rating_conditions)})")

            # Build exclusion subquery if needed
            exclusion_sql = ""
            if exclude_portfolio:
                exclusion_sql = f"""
                AND a.isin NOT IN (
                    SELECT DISTINCT isin FROM transactions
                    WHERE portfolio_id = '{portfolio_id}'
                    AND status = 'settled'
                    AND isin != 'CASH'
                )
                """

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            sql = f"""
            WITH latest AS (
                SELECT isin, MAX(bpdate) as max_date
                FROM agg_analysis_data
                GROUP BY isin
            )
            SELECT
                a.isin,
                a.ticker,
                a.description,
                a.country,
                a.ytw as yield_pct,
                a.oas as spread_bp,
                a.oad as duration,
                a.return_ytw as expected_return,
                a.price,
                a.coupon,
                a.maturity,
                a.rating_sp,
                a.rating_moody,
                CASE
                    WHEN UPPER(a.ticker) IN ('{"', '".join(quasi_sovereigns)}') THEN 'quasi-sovereign'
                    ELSE 'sovereign'
                END as issuer_type
            FROM agg_analysis_data a
            JOIN latest l ON a.isin = l.isin AND a.bpdate = l.max_date
            WHERE {where_clause}
                AND a.return_ytw IS NOT NULL
                AND a.ytw IS NOT NULL
                {exclusion_sql}
            ORDER BY {sort_column} DESC
            LIMIT {limit}
            """

            df = query_bigquery(sql, client_id)

            if df.empty:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "bonds": [],
                        "count": 0,
                        "message": "No bonds found matching criteria. Try relaxing filters."
                    }, indent=2)
                )]

            # Post-filter by NFA rating if requested
            nfa_filter_applied = False
            if min_nfa_rating and not df.empty:
                # Get unique countries from results
                unique_countries = df['country'].dropna().unique().tolist()
                if unique_countries:
                    # Fetch NFA ratings for these countries
                    country_nfa = {}
                    for c in unique_countries:
                        try:
                            nfa_result = get_nfa_rating(str(c))
                            if 'nfa_star_rating' in nfa_result:
                                country_nfa[c] = nfa_result['nfa_star_rating']
                        except Exception as e:
                            logger.warning(f"Could not get NFA for {c}: {e}")

                    # Filter dataframe to only include countries meeting NFA threshold
                    if country_nfa:
                        df = df[df['country'].apply(
                            lambda x: country_nfa.get(x, 0) >= min_nfa_rating
                        )]
                        nfa_filter_applied = True

            if df.empty:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "bonds": [],
                        "count": 0,
                        "message": f"No bonds found meeting NFA rating >= {min_nfa_rating} stars. Try lowering the threshold."
                    }, indent=2)
                )]

            # Format results
            bonds = []
            for _, row in df.iterrows():
                bond = {
                    'isin': row['isin'],
                    'ticker': row['ticker'],
                    'description': row['description'],
                    'country': row['country'],
                    'issuer_type': row['issuer_type'],
                    'yield': f"{row['yield_pct']:.2f}%",
                    'spread': f"{row['spread_bp']:.0f}bp",
                    'duration': f"{row['duration']:.2f}y",
                    'expected_return': f"{row['expected_return']:.2f}%",
                    'price': f"{row['price']:.2f}",
                    'coupon': f"{row['coupon']:.2f}%" if row.get('coupon') else None,
                    'maturity': str(row['maturity']) if row.get('maturity') else None,
                    'rating': row.get('rating_sp') or row.get('rating_moody')
                }
                bonds.append(bond)

            result = {
                'bonds': bonds,
                'count': len(bonds),
                'filters_applied': {
                    'country': country,
                    'ticker': ticker_pattern,
                    'issuer_type': issuer_type,
                    'min_expected_return': min_expected_return,
                    'max_duration': max_duration,
                    'min_rating': min_rating,
                    'min_nfa_rating': min_nfa_rating,
                    'exclude_portfolio': exclude_portfolio,
                    'sort_by': sort_by
                },
                'tip': "For investment-grade sovereign bonds, use min_rating='BBB-' and min_nfa_rating=3"
            }

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, default=str)
            )]

        elif name == "suggest_rebalancing":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            focus = arguments.get("focus", "all")
            max_suggestions = arguments.get("max_suggestions", 5)
            import pandas as pd

            # Get holdings with analytics (expected_return, duration, yield, spread)
            holdings_sql = f"""
            WITH net_holdings AS (
                SELECT
                    t.isin, t.ticker, t.description, t.country,
                    SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.par_amount ELSE 0 END) -
                    SUM(CASE WHEN t.transaction_type = 'SELL' THEN t.par_amount ELSE 0 END) as par_amount,
                    SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.market_value ELSE 0 END) -
                    SUM(CASE WHEN t.transaction_type = 'SELL' THEN t.market_value ELSE 0 END) as market_value
                FROM transactions t
                WHERE t.portfolio_id = '{portfolio_id}'
                    AND t.status = 'settled'
                    AND t.isin != 'CASH'
                GROUP BY t.isin, t.ticker, t.description, t.country
                HAVING par_amount > 0
            ),
            latest_analytics AS (
                SELECT isin, MAX(bpdate) as max_date
                FROM agg_analysis_data
                GROUP BY isin
            )
            SELECT
                h.isin, h.ticker, h.description, h.country,
                h.par_amount, h.market_value,
                COALESCE(a.return_ytw, 0) as expected_return,
                COALESCE(a.ytw, 0) as yield,
                COALESCE(a.oad, 0) as duration,
                COALESCE(a.oas, 0) as spread
            FROM net_holdings h
            LEFT JOIN latest_analytics l ON h.isin = l.isin
            LEFT JOIN agg_analysis_data a ON h.isin = a.isin AND a.bpdate = l.max_date
            """
            holdings_df = query_bigquery(holdings_sql, client_id)

            # Get cash position
            cash_sql = f"""
            SELECT
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND ticker = 'CASH' AND transaction_type = 'INITIAL') -
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND status = 'settled' AND transaction_type = 'BUY' AND isin != 'CASH') +
                (SELECT COALESCE(SUM(market_value), 0) FROM transactions
                 WHERE portfolio_id = '{portfolio_id}' AND status = 'settled' AND transaction_type = 'SELL') as net_cash
            """
            cash_df = query_bigquery(cash_sql, client_id)
            net_cash = float(cash_df.iloc[0]['net_cash']) if not cash_df.empty else 0.0

            # Calculate portfolio metrics
            total_nav = holdings_df['market_value'].sum() + net_cash
            holdings_df['weight_pct'] = holdings_df['market_value'] / total_nav * 100

            # Group by country and issuer
            country_weights = holdings_df.groupby('country')['weight_pct'].sum().to_dict()
            issuer_weights = holdings_df.groupby('ticker')['weight_pct'].sum().to_dict()

            # Run compliance check
            compliance_result = check_compliance(holdings_df, net_cash)
            metrics = compliance_result.metrics

            suggestions = {
                'sell_candidates': [],
                'buy_candidates': [],
                'summary': {}
            }

            # === SELL SUGGESTIONS ===

            # 1. Compliance: Countries over 20% limit
            overweight_countries = {k: v for k, v in country_weights.items() if v > 20}
            for country, weight in sorted(overweight_countries.items(), key=lambda x: x[1], reverse=True):
                excess = weight - 20
                country_bonds = holdings_df[holdings_df['country'] == country].sort_values('expected_return')
                if not country_bonds.empty:
                    worst_bond = country_bonds.iloc[0]
                    suggestions['sell_candidates'].append({
                        'reason': f"Country overweight ({weight:.1f}% > 20% limit)",
                        'ticker': worst_bond['ticker'],
                        'country': country,
                        'current_weight': f"{worst_bond['weight_pct']:.1f}%",
                        'expected_return': f"{worst_bond['expected_return']:.2f}%",
                        'suggested_action': f"Sell to reduce {country} by {excess:.1f}%",
                        'priority': 'HIGH' if weight > 22 else 'MEDIUM'
                    })

            # 2. Issuers over 10% (hard limit breach)
            overweight_issuers = {k: v for k, v in issuer_weights.items() if v > 10}
            for issuer, weight in sorted(overweight_issuers.items(), key=lambda x: x[1], reverse=True):
                excess = weight - 10
                issuer_bonds = holdings_df[holdings_df['ticker'] == issuer].sort_values('expected_return')
                if not issuer_bonds.empty:
                    worst_bond = issuer_bonds.iloc[0]
                    suggestions['sell_candidates'].append({
                        'reason': f"Issuer overweight ({weight:.1f}% > 10% limit) - BREACH",
                        'ticker': worst_bond['ticker'],
                        'country': worst_bond['country'],
                        'current_weight': f"{worst_bond['weight_pct']:.1f}%",
                        'expected_return': f"{worst_bond['expected_return']:.2f}%",
                        'suggested_action': f"MUST SELL to reduce {issuer} by {excess:.1f}%",
                        'priority': 'CRITICAL'
                    })

            # 3. Low expected return bonds (bottom 20% by return)
            if focus in ['returns', 'all'] and len(holdings_df) > 5:
                low_return_threshold = holdings_df['expected_return'].quantile(0.2)
                low_return_bonds = holdings_df[holdings_df['expected_return'] <= low_return_threshold].sort_values('expected_return')
                for _, bond in low_return_bonds.head(3).iterrows():
                    # Skip if already suggested
                    if any(s['ticker'] == bond['ticker'] for s in suggestions['sell_candidates']):
                        continue
                    suggestions['sell_candidates'].append({
                        'reason': f"Low expected return (bottom quintile)",
                        'ticker': bond['ticker'],
                        'country': bond['country'],
                        'current_weight': f"{bond['weight_pct']:.1f}%",
                        'expected_return': f"{bond['expected_return']:.2f}%",
                        'suggested_action': f"Consider selling - low return vs peers",
                        'priority': 'LOW'
                    })

            # === BUY SUGGESTIONS ===

            # 1. Underweight countries (room to add)
            avg_country_weight = 100 / len(country_weights) if country_weights else 0
            underweight_countries = {k: v for k, v in country_weights.items() if v < avg_country_weight * 0.5 and v > 0}

            # Get high-return bonds from watchlist for underweight countries
            if focus in ['diversification', 'all'] and underweight_countries:
                watchlist_sql = f"""
                WITH latest AS (
                    SELECT isin, MAX(bpdate) as max_date
                    FROM agg_analysis_data
                    GROUP BY isin
                )
                SELECT a.isin, a.ticker, a.description, a.country,
                       a.return_ytw as expected_return, a.ytw as yield, a.oad as duration
                FROM agg_analysis_data a
                JOIN latest l ON a.isin = l.isin AND a.bpdate = l.max_date
                WHERE a.country IN ({','.join([f"'{c}'" for c in underweight_countries.keys()])})
                    AND a.return_ytw > 0
                    AND a.isin NOT IN (SELECT DISTINCT isin FROM transactions WHERE portfolio_id = '{portfolio_id}' AND status = 'settled')
                ORDER BY a.return_ytw DESC
                LIMIT 10
                """
                try:
                    watchlist_df = query_bigquery(watchlist_sql, client_id)
                    for _, bond in watchlist_df.head(3).iterrows():
                        current_country_weight = country_weights.get(bond['country'], 0)
                        headroom = 20 - current_country_weight
                        suggestions['buy_candidates'].append({
                            'reason': f"Underweight country ({bond['country']}: {current_country_weight:.1f}%)",
                            'ticker': bond['ticker'],
                            'isin': bond['isin'],
                            'country': bond['country'],
                            'expected_return': f"{bond['expected_return']:.2f}%",
                            'yield': f"{bond['yield']:.2f}%",
                            'suggested_action': f"Can add up to {headroom:.1f}% to {bond['country']}",
                            'priority': 'MEDIUM'
                        })
                except:
                    pass  # Watchlist query failed, skip buy suggestions

            # 2. High return opportunities from watchlist (if cash available)
            if focus in ['returns', 'all'] and net_cash > 200000:
                high_return_sql = f"""
                WITH latest AS (
                    SELECT isin, MAX(bpdate) as max_date
                    FROM agg_analysis_data
                    GROUP BY isin
                )
                SELECT a.isin, a.ticker, a.description, a.country,
                       a.return_ytw as expected_return, a.ytw as yield, a.oad as duration
                FROM agg_analysis_data a
                JOIN latest l ON a.isin = l.isin AND a.bpdate = l.max_date
                WHERE a.return_ytw > 5.0
                    AND a.isin NOT IN (SELECT DISTINCT isin FROM transactions WHERE portfolio_id = '{portfolio_id}' AND status = 'settled')
                ORDER BY a.return_ytw DESC
                LIMIT 5
                """
                try:
                    high_return_df = query_bigquery(high_return_sql, client_id)
                    for _, bond in high_return_df.head(2).iterrows():
                        current_country_weight = country_weights.get(bond['country'], 0)
                        if current_country_weight < 18:  # Room to add
                            # Skip if already suggested
                            if any(b.get('isin') == bond['isin'] for b in suggestions['buy_candidates']):
                                continue
                            suggestions['buy_candidates'].append({
                                'reason': f"High expected return opportunity",
                                'ticker': bond['ticker'],
                                'isin': bond['isin'],
                                'country': bond['country'],
                                'expected_return': f"{bond['expected_return']:.2f}%",
                                'yield': f"{bond['yield']:.2f}%",
                                'suggested_action': f"Buy with available cash (${net_cash/1000:.0f}k available)",
                                'priority': 'MEDIUM'
                            })
                except:
                    pass

            # Build summary
            suggestions['summary'] = {
                'portfolio_status': 'COMPLIANT' if compliance_result.is_compliant else 'NON-COMPLIANT',
                'available_cash': f"${net_cash/1000:.0f}k",
                'total_nav': f"${total_nav/1_000_000:.2f}M",
                'num_holdings': len(holdings_df),
                'focus_area': focus,
                'sell_suggestions': len(suggestions['sell_candidates']),
                'buy_suggestions': len(suggestions['buy_candidates']),
                'top_priority': next((s['priority'] for s in suggestions['sell_candidates'] if s['priority'] == 'CRITICAL'),
                                    next((s['priority'] for s in suggestions['sell_candidates'] if s['priority'] == 'HIGH'), 'NONE'))
            }

            # Limit suggestions
            suggestions['sell_candidates'] = suggestions['sell_candidates'][:max_suggestions]
            suggestions['buy_candidates'] = suggestions['buy_candidates'][:max_suggestions]

            return [TextContent(
                type="text",
                text=json.dumps(suggestions, indent=2, default=str)
            )]

        # ============================================================================
        # EXTERNAL MCP TOOL HANDLERS
        # ============================================================================

        # NFA MCP
        elif name == "get_nfa_rating":
            country = arguments["country"]
            year = arguments.get("year")
            history = arguments.get("history", False)
            result = get_nfa_rating(country, year, history)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_nfa_batch":
            countries = arguments["countries"]
            year = arguments.get("year")
            result = get_nfa_batch(countries, year)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "search_nfa_by_rating":
            rating = arguments.get("rating")
            min_rating = arguments.get("min_rating")
            max_rating = arguments.get("max_rating")
            year = arguments.get("year")
            result = search_nfa_by_rating(rating, min_rating, max_rating, year)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # Rating MCP
        elif name == "get_credit_rating":
            country = arguments["country"]
            result = get_credit_rating(country)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_credit_ratings_batch":
            countries = arguments["countries"]
            result = get_credit_ratings_batch(countries)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # Country Mapping MCP
        elif name == "standardize_country":
            country = arguments["country"]
            result = standardize_country(country)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_country_info":
            country = arguments["country"]
            result = get_country_info(country)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # FRED MCP
        elif name == "get_fred_series":
            series_id = arguments["series_id"]
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            analyze = arguments.get("analyze", False)
            result = get_fred_series(series_id, start_date, end_date, analyze)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "search_fred_series":
            query = arguments["query"]
            result = search_fred_series(query)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_treasury_rates":
            result = get_treasury_rates()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # Sovereign Classification MCP
        elif name == "classify_issuer":
            isin = arguments["isin"]
            result = classify_issuer(isin)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "classify_issuers_batch":
            isins = arguments["isins"]
            result = classify_issuers_batch(isins)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "filter_by_issuer_type":
            issuer_type = arguments["issuer_type"]
            result = filter_by_issuer_type(issuer_type)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_issuer_summary":
            issuer = arguments["issuer"]
            result = get_issuer_summary(issuer)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # IMF MCP (external with AI)
        elif name == "get_imf_indicator_external":
            indicator = arguments["indicator"]
            country = arguments["country"]
            start_year = arguments.get("start_year")
            end_year = arguments.get("end_year")
            analyze = arguments.get("analyze", False)
            result = get_imf_indicator(indicator, country, start_year, end_year, analyze)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "compare_imf_countries":
            indicator = arguments["indicator"]
            countries = arguments["countries"]
            year = arguments.get("year")
            result = compare_imf_countries(indicator, countries, year)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # World Bank MCP
        elif name == "get_worldbank_indicator":
            indicator = arguments["indicator"]
            country = arguments["country"]
            start_year = arguments.get("start_year")
            end_year = arguments.get("end_year")
            result = get_worldbank_indicator(indicator, country, start_year, end_year)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "search_worldbank_indicators":
            query = arguments["query"]
            result = search_worldbank_indicators(query)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_worldbank_country_profile":
            country = arguments["country"]
            result = get_worldbank_country_profile(country)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ========== SOVEREIGN CREDIT REPORTS ==========
        elif name == "sovereign_list_countries":
            result = sovereign_list_countries()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "sovereign_get_report":
            country = arguments["country"]
            result = get_sovereign_report(country)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "sovereign_get_section":
            country = arguments["country"]
            section = arguments["section"]
            result = get_sovereign_section(country, section)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "sovereign_search":
            query = arguments["query"]
            max_results = arguments.get("max_results", 5)
            result = search_sovereign_reports(query, max_results)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "sovereign_compare":
            countries = arguments["countries"]
            result = get_sovereign_comparison(countries)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ========== REASONING TOOLS ==========
        elif name == "analyze":
            data = arguments["data"]
            objective = arguments["objective"]
            require_compliance = arguments.get("require_compliance", False)
            result = analyze_data(data, objective, require_compliance)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "reason":
            query = arguments["query"]
            portfolio_context = arguments.get("portfolio_context")
            require_compliance = arguments.get("require_compliance", False)
            result = call_reasoning(query, portfolio_context, require_compliance)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "list_reasoning_skills":
            result = list_reasoning_skills()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "complex_query":
            result = await handle_complex_query(
                query=arguments["query"],
                portfolio_id=arguments.get("portfolio_id"),
                client_id=client_id
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

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
