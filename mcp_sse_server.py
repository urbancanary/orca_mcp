#!/usr/bin/env python3
"""
Orca MCP - SSE Server for Claude Desktop Remote Connection

This server provides MCP (Model Context Protocol) access via SSE transport,
allowing Claude Desktop to connect remotely using the "Add custom connector" feature.

ARCHITECTURE (v3.0):
    Claude Desktop sees only ONE tool: orca_query
    Internally, we route to 37+ tools using FallbackLLMClient (Gemini/Haiku)
    Tools are enabled gradually via ENABLED_TOOLS registry

Usage:
    # Run standalone
    python mcp_sse_server.py

    # Or via uvicorn
    uvicorn mcp_sse_server:app --host 0.0.0.0 --port 8000

Claude Desktop Configuration:
    Name: Orca Portfolio
    URL: https://orca-mcp-production.up.railway.app/sse
"""

import os
import sys
import json
import logging
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any

# =============================================================================
# ENABLED TOOLS REGISTRY
# Add tools here one at a time as we validate them via orca_query routing
# =============================================================================
ENABLED_TOOLS = {
    # Phase 1: Core portfolio tools
    "get_watchlist",           # Bond watchlist with filters
    "get_client_holdings",     # Portfolio holdings
    "get_portfolio_summary",   # Portfolio stats
    "get_compliance_status",   # UCITS compliance

    # Phase 2: Rating/country tools
    "get_nfa_rating",          # NFA star ratings
    "get_credit_rating",       # S&P/Moody's ratings

    # Phase 3: FRED / Treasury data
    "get_treasury_rates",      # US yield curve (1M to 30Y)
    "get_fred_series",         # FRED time series (CPI, GDP, unemployment, etc.)
    "search_fred_series",      # Search FRED by keyword

    # Phase 4: IMF data
    "get_imf_indicator",       # IMF indicators (GDP growth, inflation, etc.)
    "compare_imf_countries",   # Compare countries on IMF indicator

    # Phase 5: World Bank data
    "get_worldbank_indicator",       # World Bank development indicators
    "search_worldbank_indicators",   # Search World Bank indicators
    "get_worldbank_country_profile", # Country development profile

    # Phase 6: Display-Ready endpoints (for thin frontends)
    "get_holdings_display",          # Holdings with ALL display columns + _fmt values
    "get_portfolio_dashboard",       # Single call for Portfolio/Summary page
    "calculate_trade_settlement",    # Pre-trade settlement calculations
    "get_transactions_display",      # Transaction history with formatting
    "check_trade_compliance",        # Enhanced compliance with impact analysis
    "get_cashflows_display",         # Projected coupons and maturities

    # Phase 7: Additional portfolio tools
    "get_client_transactions",       # Transaction history
    "get_portfolio_cash",            # Cash positions
    "search_bonds_rvm",              # Search bond universe by country/rating/return
    "check_trade_compliance_impact", # Pre-trade compliance check

    # Phase 8: Screening tools
    "search_nfa_by_rating",          # Find countries by NFA star rating
}

# Add current directory to path for imports
SCRIPT_DIR = Path(__file__).parent.resolve()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# D1 API URL for fast edge queries
D1_API_URL = "https://portfolio-optimizer-mcp.urbancanary.workers.dev"

# Set up logging early
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orca-mcp-sse")


def get_holdings_from_d1(portfolio_id: str = 'wnbf', staging_id: int = 1) -> list:
    """
    Get portfolio holdings from Cloudflare D1 (fast edge database)
    """
    url = f"{D1_API_URL}/api/holdings?portfolio_id={portfolio_id}&staging_id={staging_id}"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            holdings = data.get('holdings', [])
            logger.info(f"Fetched {len(holdings)} holdings from D1 (staging_id={staging_id})")
            return holdings
    except Exception as e:
        logger.error(f"Failed to fetch holdings from D1: {e}")
        return []


def get_holdings_summary_from_d1(portfolio_id: str = 'wnbf', staging_id: int = 1) -> dict:
    """
    Get portfolio summary stats from Cloudflare D1
    """
    url = f"{D1_API_URL}/api/holdings/summary?portfolio_id={portfolio_id}&staging_id={staging_id}"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            logger.info(f"Fetched portfolio summary from D1 (staging_id={staging_id})")
            return data
    except Exception as e:
        logger.error(f"Failed to fetch holdings summary from D1: {e}")
        return {}


from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.responses import JSONResponse, Response
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent

# Import tool implementations
try:
    from tools.data_access import query_bigquery
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
    from tools.video_gateway import (
        video_search,
        video_list,
        video_synthesize,
        video_get_transcript,
        video_keyword_search
    )
    from tools.compliance import (
        check_compliance,
        check_compliance_impact,
        compliance_to_dict
    )
    from tools.display_endpoints import (
        get_holdings_display,
        get_portfolio_dashboard,
        calculate_trade_settlement,
        get_transactions_display,
        check_trade_compliance,
        get_cashflows_display,
    )
    from tools.external_mcps import (
        get_nfa_rating,
        get_nfa_batch,
        search_nfa_by_rating,
        get_credit_rating,
        get_credit_ratings_batch,
        standardize_country,
        get_country_info,
        get_fred_series,
        search_fred_series,
        get_treasury_rates,
        classify_issuer,
        classify_issuers_batch,
        filter_by_issuer_type,
        get_issuer_summary,
        get_imf_indicator,
        compare_imf_countries,
        get_worldbank_indicator,
        search_worldbank_indicators,
        get_worldbank_country_profile,
    )
    from client_config import get_client_config
except ImportError:
    from orca_mcp.tools.data_access import query_bigquery
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
    from orca_mcp.tools.display_endpoints import (
        get_holdings_display,
        get_portfolio_dashboard,
        calculate_trade_settlement,
        get_transactions_display,
        check_trade_compliance,
        get_cashflows_display,
    )
    from orca_mcp.tools.external_mcps import (
        get_nfa_rating,
        get_nfa_batch,
        search_nfa_by_rating,
        get_credit_rating,
        get_credit_ratings_batch,
        standardize_country,
        get_country_info,
        get_fred_series,
        search_fred_series,
        get_treasury_rates,
        classify_issuer,
        classify_issuers_batch,
        filter_by_issuer_type,
        get_issuer_summary,
        get_imf_indicator,
        compare_imf_countries,
        get_worldbank_indicator,
        search_worldbank_indicators,
        get_worldbank_country_profile,
    )
    from orca_mcp.client_config import get_client_config

# Import query router for natural language interface
try:
    from tools.query_router import route_query, ORCA_QUERY_TOOL_DESCRIPTION
except ImportError:
    from orca_mcp.tools.query_router import route_query, ORCA_QUERY_TOOL_DESCRIPTION

# Create MCP server instance
mcp_server = Server("orca-mcp")


@mcp_server.list_tools()
async def list_tools() -> list[Tool]:
    """
    List available Orca MCP tools.

    v3.0: Only exposes orca_query - all other tools are routed internally.
    This reduces Claude's context from ~11K tokens to ~500 tokens.
    """
    return [
        Tool(
            name="orca_query",
            description=ORCA_QUERY_TOOL_DESCRIPTION,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query describing what data you need"
                    },
                    "context": {
                        "type": "string",
                        "description": "Optional conversation context for better routing"
                    }
                },
                "required": ["query"]
            }
        ),
    ]


# =============================================================================
# INTERNAL TOOL DEFINITIONS (hidden from Claude, used by router)
# =============================================================================
INTERNAL_TOOLS = [
        # ============================================================================
        # PORTFOLIO CORE TOOLS
        # ============================================================================
        Tool(
            name="get_client_holdings",
            description="Get current portfolio holdings from D1 edge database. Returns full bond details including price, yield, duration, spread, rating.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio identifier (e.g., 'wnbf')"},
                    "staging_id": {"type": "integer", "description": "1=Live portfolio, 2=Staging portfolio (default: 1)"}
                },
                "required": ["portfolio_id"]
            }
        ),
        Tool(
            name="get_portfolio_summary",
            description="Get portfolio summary statistics including weighted duration, spread, yield, and country breakdown",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio identifier (e.g., 'wnbf')"},
                    "staging_id": {"type": "integer", "description": "1=Live portfolio, 2=Staging portfolio (default: 1)"}
                },
                "required": ["portfolio_id"]
            }
        ),
        Tool(
            name="get_client_transactions",
            description="Get transactions for a portfolio with optional date filters",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio identifier"},
                    "start_date": {"type": "string", "description": "Filter from date (YYYY-MM-DD)"},
                    "end_date": {"type": "string", "description": "Filter to date (YYYY-MM-DD)"},
                    "limit": {"type": "integer", "description": "Max records (default 100)"}
                },
                "required": ["portfolio_id"]
            }
        ),
        Tool(
            name="get_portfolio_cash",
            description="Get current cash position and portfolio summary",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"}
                },
                "required": []
            }
        ),

        # ============================================================================
        # COMPLIANCE TOOLS
        # ============================================================================
        Tool(
            name="get_compliance_status",
            description="Get comprehensive UCITS compliance status with rich metrics. Returns overall pass/fail, rule-by-rule breakdown, headroom analysis, and concentration metrics.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"}
                },
                "required": []
            }
        ),
        Tool(
            name="check_trade_compliance_impact",
            description="Pre-trade compliance check. Simulates adding a trade and shows how it would impact compliance.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "ticker": {"type": "string", "description": "Ticker of the bond to trade"},
                    "country": {"type": "string", "description": "Country of the bond"},
                    "action": {"type": "string", "enum": ["buy", "sell"], "description": "Trade action"},
                    "market_value": {"type": "number", "description": "Market value of the trade"}
                },
                "required": ["ticker", "country", "action", "market_value"]
            }
        ),

        # ============================================================================
        # BOND SEARCH & ANALYTICS
        # ============================================================================
        Tool(
            name="search_bonds_rvm",
            description="Search the RVM universe for bonds. Returns bonds with yield, spread, duration, expected return. Can filter by country, issuer type, rating.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country to filter by"},
                    "ticker": {"type": "string", "description": "Ticker pattern to search"},
                    "issuer_type": {"type": "string", "enum": ["sovereign", "quasi-sovereign", "corporate", "all"], "description": "Filter by issuer type"},
                    "min_expected_return": {"type": "number", "description": "Minimum expected return (%)"},
                    "max_duration": {"type": "number", "description": "Maximum duration in years"},
                    "sort_by": {"type": "string", "enum": ["expected_return", "yield", "spread", "duration"], "description": "Sort results by"},
                    "limit": {"type": "integer", "description": "Max results (default: 10)"},
                    "exclude_portfolio": {"type": "boolean", "description": "Exclude bonds in portfolio (default: true)"},
                    "portfolio_id": {"type": "string", "description": "Portfolio ID for exclusion"}
                },
                "required": []
            }
        ),
        Tool(
            name="suggest_rebalancing",
            description="Analyze portfolio and suggest rebalancing trades to improve compliance, diversification, or optimize expected returns.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "focus": {"type": "string", "enum": ["compliance", "diversification", "returns", "all"], "description": "What to optimize for"},
                    "max_suggestions": {"type": "integer", "description": "Max trade suggestions (default: 5)"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_watchlist",
            description="Get the bond watchlist - candidate bonds for purchase. Returns ISINs with full analytics (YTW, OAD, OAS, expected return, ratings, country). Use this to find undervalued bonds NOT already in the portfolio.",
            inputSchema={
                "type": "object",
                "properties": {
                    "full_details": {"type": "boolean", "description": "Include full analytics (default: true)"},
                    "min_rating": {"type": "string", "description": "Minimum S&P rating (e.g., 'BBB-')"},
                    "min_nfa_rating": {"type": "integer", "description": "Minimum NFA star rating (1-7, 3+ recommended)"},
                    "sort_by": {"type": "string", "enum": ["expected_return", "yield", "spread", "duration"], "description": "Sort by field (default: expected_return)"},
                    "limit": {"type": "integer", "description": "Max results"}
                },
                "required": []
            }
        ),

        # ============================================================================
        # IMF GATEWAY
        # ============================================================================
        Tool(
            name="fetch_imf_data",
            description="Get IMF economic data (debt, GDP, inflation) for countries or groups (G7, G20, BRICS)",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {"type": "string", "description": "Indicator: debt, gdp_growth, inflation, unemployment, fiscal_deficit, current_account"},
                    "countries": {"type": "string", "description": "Country name, ISO code, or group (G7, G20, BRICS)"},
                    "start_year": {"type": "integer", "description": "Start year (default: 2010)"},
                    "end_year": {"type": "integer", "description": "End year (optional)"}
                },
                "required": ["indicator", "countries"]
            }
        ),
        Tool(
            name="get_available_indicators",
            description="List available IMF economic indicators with codes and descriptions",
            inputSchema={"type": "object", "properties": {}, "required": []}
        ),
        Tool(
            name="get_available_country_groups",
            description="List available country groups (G7, G20, BRICS, EU, ASEAN) with member countries",
            inputSchema={"type": "object", "properties": {}, "required": []}
        ),

        # ============================================================================
        # ETF REFERENCE
        # ============================================================================
        Tool(
            name="get_etf_allocation",
            description="Get country allocation breakdown for a specific ETF by ISIN",
            inputSchema={
                "type": "object",
                "properties": {
                    "isin": {"type": "string", "description": "ETF ISIN code"}
                },
                "required": ["isin"]
            }
        ),
        Tool(
            name="list_etf_allocations",
            description="List all available ETFs with summary info",
            inputSchema={"type": "object", "properties": {}, "required": []}
        ),
        Tool(
            name="get_etf_country_exposure",
            description="Find all ETFs with exposure to a specific country",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name"}
                },
                "required": ["country"]
            }
        ),

        # ============================================================================
        # VIDEO INTELLIGENCE
        # ============================================================================
        Tool(
            name="video_search",
            description="Search video transcripts for relevant content",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query text"},
                    "max_results": {"type": "integer", "description": "Max results (default: 10)"}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="video_list",
            description="List all available videos in the library",
            inputSchema={"type": "object", "properties": {}, "required": []}
        ),
        Tool(
            name="video_synthesize",
            description="Generate AI-synthesized answer from video search results",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The original question"},
                    "video_results": {"type": "array", "description": "Results from video_search"},
                    "tone": {"type": "string", "description": "Response tone: professional, casual, educational"}
                },
                "required": ["query", "video_results"]
            }
        ),
        Tool(
            name="video_get_transcript",
            description="Get the full transcript for a specific video by ID",
            inputSchema={
                "type": "object",
                "properties": {
                    "video_id": {"type": "string", "description": "YouTube video ID"}
                },
                "required": ["video_id"]
            }
        ),
        Tool(
            name="video_keyword_search",
            description="Fast keyword search across video transcripts",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Keyword or phrase"},
                    "max_results": {"type": "integer", "description": "Max results (default: 10)"}
                },
                "required": ["query"]
            }
        ),

        # ============================================================================
        # EXTERNAL MCP TOOLS - NFA
        # ============================================================================
        Tool(
            name="get_nfa_rating",
            description="Get NFA (Net Foreign Assets) star rating for a country. Ratings: 1★ (extreme deficit) to 7★ (extremely strong). Critical for sovereign creditworthiness.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name (e.g., 'Colombia', 'Brazil')"},
                    "year": {"type": "integer", "description": "Specific year (optional)"},
                    "history": {"type": "boolean", "description": "Return full time series (1970-2023)"}
                },
                "required": ["country"]
            }
        ),
        Tool(
            name="get_nfa_batch",
            description="Get NFA ratings for multiple countries at once",
            inputSchema={
                "type": "object",
                "properties": {
                    "countries": {"type": "array", "items": {"type": "string"}, "description": "List of country names"},
                    "year": {"type": "integer", "description": "Specific year (optional)"}
                },
                "required": ["countries"]
            }
        ),
        Tool(
            name="search_nfa_by_rating",
            description="Find countries by NFA star rating",
            inputSchema={
                "type": "object",
                "properties": {
                    "rating": {"type": "integer", "description": "Exact rating (1-7)"},
                    "min_rating": {"type": "integer", "description": "Minimum rating"},
                    "max_rating": {"type": "integer", "description": "Maximum rating"},
                    "year": {"type": "integer", "description": "Specific year"}
                },
                "required": []
            }
        ),

        # ============================================================================
        # EXTERNAL MCP TOOLS - CREDIT RATINGS
        # ============================================================================
        Tool(
            name="get_credit_rating",
            description="Get sovereign credit rating for a country (S&P, Moody's, Fitch)",
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
            description="Get credit ratings for multiple countries",
            inputSchema={
                "type": "object",
                "properties": {
                    "countries": {"type": "array", "items": {"type": "string"}, "description": "List of country names"}
                },
                "required": ["countries"]
            }
        ),

        # ============================================================================
        # EXTERNAL MCP TOOLS - COUNTRY MAPPING
        # ============================================================================
        Tool(
            name="standardize_country",
            description="Standardize country name to canonical form. Handles variations like 'UAE' vs 'United Arab Emirates'",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name in any format"}
                },
                "required": ["country"]
            }
        ),
        Tool(
            name="get_country_info",
            description="Get comprehensive country information including ISO codes, region, aliases",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name"}
                },
                "required": ["country"]
            }
        ),

        # ============================================================================
        # EXTERNAL MCP TOOLS - FRED
        # ============================================================================
        Tool(
            name="get_fred_series",
            description="Get FRED economic data. Common: DGS10 (10Y Treasury), CPIAUCSL (CPI), UNRATE (unemployment), FEDFUNDS",
            inputSchema={
                "type": "object",
                "properties": {
                    "series_id": {"type": "string", "description": "FRED series ID"},
                    "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                    "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"},
                    "analyze": {"type": "boolean", "description": "Include AI analysis"}
                },
                "required": ["series_id"]
            }
        ),
        Tool(
            name="search_fred_series",
            description="Search for FRED data series by keyword",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search term (e.g., 'treasury', 'inflation')"}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_treasury_rates",
            description="Get current US Treasury rates across the yield curve (1M to 30Y)",
            inputSchema={"type": "object", "properties": {}, "required": []}
        ),

        # ============================================================================
        # EXTERNAL MCP TOOLS - SOVEREIGN CLASSIFICATION
        # ============================================================================
        Tool(
            name="classify_issuer",
            description="Classify bond issuer by ISIN as sovereign, quasi-sovereign, or corporate",
            inputSchema={
                "type": "object",
                "properties": {
                    "isin": {"type": "string", "description": "Bond ISIN"}
                },
                "required": ["isin"]
            }
        ),
        Tool(
            name="classify_issuers_batch",
            description="Classify multiple bond issuers by ISIN",
            inputSchema={
                "type": "object",
                "properties": {
                    "isins": {"type": "array", "items": {"type": "string"}, "description": "List of ISINs"}
                },
                "required": ["isins"]
            }
        ),
        Tool(
            name="filter_by_issuer_type",
            description="Get all issuers of a specific type",
            inputSchema={
                "type": "object",
                "properties": {
                    "issuer_type": {"type": "string", "enum": ["sovereign", "quasi-sovereign", "corporate"], "description": "Type to filter"}
                },
                "required": ["issuer_type"]
            }
        ),
        Tool(
            name="get_issuer_summary",
            description="Get AI-generated summary for an issuer",
            inputSchema={
                "type": "object",
                "properties": {
                    "issuer": {"type": "string", "description": "Issuer name or ticker"}
                },
                "required": ["issuer"]
            }
        ),

        # ============================================================================
        # EXTERNAL MCP TOOLS - IMF (with AI)
        # ============================================================================
        Tool(
            name="get_imf_indicator_external",
            description="Get IMF indicator data with optional AI analysis via external MCP",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {"type": "string", "description": "IMF indicator code"},
                    "country": {"type": "string", "description": "Country name or ISO code"},
                    "start_year": {"type": "integer", "description": "Start year"},
                    "end_year": {"type": "integer", "description": "End year"},
                    "analyze": {"type": "boolean", "description": "Include AI analysis"}
                },
                "required": ["indicator", "country"]
            }
        ),
        Tool(
            name="compare_imf_countries",
            description="Compare IMF indicator across multiple countries",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {"type": "string", "description": "IMF indicator code"},
                    "countries": {"type": "array", "items": {"type": "string"}, "description": "List of countries"},
                    "year": {"type": "integer", "description": "Specific year (optional)"}
                },
                "required": ["indicator", "countries"]
            }
        ),

        # ============================================================================
        # EXTERNAL MCP TOOLS - WORLD BANK
        # ============================================================================
        Tool(
            name="get_worldbank_indicator",
            description="Get World Bank indicator data. Common: NY.GDP.PCAP.CD (GDP per capita), SP.POP.TOTL (population)",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {"type": "string", "description": "World Bank indicator code"},
                    "country": {"type": "string", "description": "Country name or ISO code"},
                    "start_year": {"type": "integer", "description": "Start year"},
                    "end_year": {"type": "integer", "description": "End year"}
                },
                "required": ["indicator", "country"]
            }
        ),
        Tool(
            name="search_worldbank_indicators",
            description="Search for World Bank indicators by keyword",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search term (e.g., 'gdp', 'population')"}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_worldbank_country_profile",
            description="Get comprehensive country development profile from World Bank",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name or ISO code"}
                },
                "required": ["country"]
            }
        ),
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls - routes through orca_query or directly to internal tools."""
    try:
        client_id = arguments.get("client_id", "guinness")

        # ============================================================================
        # ORCA_QUERY - Natural language router (the only tool Claude sees)
        # ============================================================================
        if name == "orca_query":
            query = arguments.get("query", "")
            context = arguments.get("context", "")

            if not query:
                return [TextContent(type="text", text=json.dumps({
                    "error": "No query provided",
                    "hint": "Please describe what data you need in natural language"
                }))]

            # Route the query using FallbackLLMClient
            routing_result = await route_query(query, context)

            # Check if clarification needed
            if routing_result.get("clarification"):
                return [TextContent(type="text", text=json.dumps({
                    "clarification_needed": routing_result["clarification"],
                    "query": query
                }))]

            # Check if routing failed
            if routing_result.get("error"):
                return [TextContent(type="text", text=json.dumps({
                    "error": routing_result["error"],
                    "query": query
                }))]

            # Get the routed tool and args
            routed_tool = routing_result.get("tool")
            routed_args = routing_result.get("args", {})
            confidence = routing_result.get("confidence", 0)
            model_used = routing_result.get("model_used", "unknown")

            # Check if tool is enabled
            if routed_tool not in ENABLED_TOOLS:
                return [TextContent(type="text", text=json.dumps({
                    "error": f"Tool '{routed_tool}' is not yet enabled",
                    "enabled_tools": list(ENABLED_TOOLS),
                    "hint": "This capability is coming soon. Try a different query.",
                    "routing": {"tool": routed_tool, "confidence": confidence, "model": model_used}
                }))]

            # Log the routing decision
            logger.info(f"orca_query: '{query}' -> {routed_tool} (confidence: {confidence}, model: {model_used})")

            # Call the internal tool recursively
            result = await call_tool(routed_tool, routed_args)

            # Wrap result with routing metadata
            if result and len(result) > 0:
                try:
                    inner_result = json.loads(result[0].text)
                    wrapped = {
                        "data": inner_result,
                        "routing": {
                            "tool": routed_tool,
                            "args": routed_args,
                            "confidence": confidence,
                            "model": model_used
                        }
                    }
                    return [TextContent(type="text", text=json.dumps(wrapped, indent=2, default=str))]
                except json.JSONDecodeError:
                    return result  # Return as-is if not JSON

            return result

        # ============================================================================
        # PORTFOLIO CORE (internal - routed via orca_query)
        # Default portfolio_id to 'wnbf' for natural language queries
        # ============================================================================
        elif name == "get_client_holdings":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            staging_id = arguments.get("staging_id", 1)  # Default to live
            holdings = get_holdings_from_d1(portfolio_id, staging_id)
            return [TextContent(type="text", text=json.dumps(holdings, indent=2, default=str))]

        elif name == "get_portfolio_summary":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            staging_id = arguments.get("staging_id", 1)
            summary = get_holdings_summary_from_d1(portfolio_id, staging_id)
            return [TextContent(type="text", text=json.dumps(summary, indent=2, default=str))]

        elif name == "get_client_transactions":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            limit = arguments.get("limit", 100)
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")

            where_clauses = [f"portfolio_id = '{portfolio_id}'"]
            if start_date:
                where_clauses.append(f"transaction_date >= '{start_date}'")
            if end_date:
                where_clauses.append(f"transaction_date <= '{end_date}'")

            sql = f"""
            SELECT * FROM transactions
            WHERE {' AND '.join(where_clauses)}
            ORDER BY transaction_date DESC
            LIMIT {limit}
            """
            df = query_bigquery(sql, client_id)
            # Replace NaN with None for JSON compatibility
            records = json.loads(df.to_json(orient='records', date_format='iso'))
            return [TextContent(type="text", text=json.dumps(records, indent=2, default=str))]

        elif name == "get_portfolio_cash":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            sql = f"SELECT * FROM portfolio_summary WHERE portfolio_id = '{portfolio_id}'"
            df = query_bigquery(sql, client_id)
            if df.empty:
                return [TextContent(type="text", text=json.dumps({"error": "No summary found"}))]
            return [TextContent(type="text", text=json.dumps(df.to_dict(orient='records')[0], indent=2, default=str))]

        # ============================================================================
        # COMPLIANCE
        # ============================================================================
        elif name == "get_compliance_status":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            import pandas as pd

            # Get holdings
            holdings = get_holdings_from_d1(portfolio_id, staging_id=1)
            if not holdings:
                return [TextContent(type="text", text=json.dumps({"error": "No holdings found"}))]

            holdings_df = pd.DataFrame(holdings)

            # Get cash from D1
            summary = get_holdings_summary_from_d1(portfolio_id, staging_id=1)
            net_cash = summary.get('cash', 0)

            # Run compliance check
            compliance_result = check_compliance(holdings_df, net_cash)
            result = compliance_to_dict(compliance_result)
            result['summary'] = {
                'is_compliant': compliance_result.is_compliant,
                'hard_rules': f"{compliance_result.hard_pass}/{compliance_result.hard_total}",
                'soft_rules': f"{compliance_result.soft_pass}/{compliance_result.soft_total}"
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "check_trade_compliance_impact":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            import pandas as pd

            holdings = get_holdings_from_d1(portfolio_id, staging_id=1)
            holdings_df = pd.DataFrame(holdings) if holdings else pd.DataFrame()
            summary = get_holdings_summary_from_d1(portfolio_id, staging_id=1)
            net_cash = summary.get('cash', 0)

            proposed_trade = {
                'ticker': arguments["ticker"],
                'country': arguments["country"],
                'action': arguments["action"],
                'market_value': arguments["market_value"]
            }
            result = check_compliance_impact(holdings_df, net_cash, proposed_trade)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        # ============================================================================
        # BOND SEARCH
        # ============================================================================
        elif name == "search_bonds_rvm":
            country = arguments.get("country")
            ticker = arguments.get("ticker")
            issuer_type = arguments.get("issuer_type", "all")
            min_expected_return = arguments.get("min_expected_return")
            max_duration = arguments.get("max_duration")
            sort_by = arguments.get("sort_by", "expected_return")
            limit = arguments.get("limit", 10)
            exclude_portfolio = arguments.get("exclude_portfolio", True)
            portfolio_id = arguments.get("portfolio_id", "wnbf")

            sort_map = {'expected_return': 'return_ytw', 'yield': 'ytw', 'spread': 'oas', 'duration': 'oad'}
            sort_column = sort_map.get(sort_by, 'return_ytw')

            conditions = []
            if country:
                conditions.append(f"LOWER(country) = LOWER('{country}')")
            if ticker:
                conditions.append(f"UPPER(ticker) LIKE UPPER('%{ticker}%')")
            if min_expected_return:
                conditions.append(f"return_ytw >= {min_expected_return}")
            if max_duration:
                conditions.append(f"oad <= {max_duration}")

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            sql = f"""
            WITH latest AS (
                SELECT isin, MAX(bpdate) as max_date
                FROM agg_analysis_data
                GROUP BY isin
            )
            SELECT a.isin, a.ticker, a.description, a.country,
                   a.ytw as yield_pct, a.oas as spread_bp, a.oad as duration,
                   a.return_ytw as expected_return, a.price
            FROM agg_analysis_data a
            JOIN latest l ON a.isin = l.isin AND a.bpdate = l.max_date
            WHERE {where_clause}
                AND a.return_ytw IS NOT NULL
            ORDER BY {sort_column} DESC
            LIMIT {limit}
            """
            df = query_bigquery(sql, client_id)
            result = {"bonds": df.to_dict(orient='records'), "count": len(df)}
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "suggest_rebalancing":
            # Simplified version - return placeholder
            result = {
                "summary": {"message": "Use local stdio server for full rebalancing suggestions"},
                "sell_candidates": [],
                "buy_candidates": []
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_watchlist":
            # Fetch watchlist from D1 via ga10-pricing API
            import os
            import urllib.request
            pricing_url = os.getenv('GA10_PRICING_URL', 'https://ga10-pricing.urbancanary.workers.dev')
            url = f"{pricing_url}/prices/latest"

            try:
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=15) as response:
                    data = json.loads(response.read().decode())
                    watchlist = data.get('prices', [])

                    # Apply filters if provided
                    min_rating = arguments.get("min_rating")
                    min_nfa_rating = arguments.get("min_nfa_rating")
                    sort_by = arguments.get("sort_by", "expected_return")
                    limit = arguments.get("limit")

                    # Rating filter
                    if min_rating:
                        rating_order = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
                                       'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-',
                                       'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC', 'C', 'D']
                        min_idx = rating_order.index(min_rating.upper()) if min_rating.upper() in rating_order else 99
                        watchlist = [b for b in watchlist
                                    if b.get('rating_sp') and
                                    rating_order.index(b['rating_sp']) <= min_idx
                                    if b['rating_sp'] in rating_order]

                    # NFA filter - need to fetch NFA ratings
                    # Note: get_nfa_rating is already imported at module level
                    if min_nfa_rating:
                        countries = list(set(b.get('country') or b.get('cbonds_country') for b in watchlist if b.get('country') or b.get('cbonds_country')))
                        country_nfa = {}
                        for c in countries:
                            try:
                                nfa = get_nfa_rating(c)
                                if 'nfa_star_rating' in nfa:
                                    country_nfa[c] = nfa['nfa_star_rating']
                            except:
                                pass
                        watchlist = [b for b in watchlist
                                    if country_nfa.get(b.get('country') or b.get('cbonds_country'), 0) >= min_nfa_rating]

                    # Sort
                    sort_map = {'expected_return': 'return_ytw', 'yield': 'ytw', 'spread': 'oas', 'duration': 'oad'}
                    sort_key = sort_map.get(sort_by, 'return_ytw')
                    watchlist = sorted(watchlist, key=lambda x: float(x.get(sort_key, 0) or 0), reverse=True)

                    # Limit
                    if limit:
                        watchlist = watchlist[:limit]

                    result = {
                        "watchlist": watchlist,
                        "count": len(watchlist),
                        "filters": {"min_rating": min_rating, "min_nfa_rating": min_nfa_rating, "sort_by": sort_by}
                    }
            except Exception as e:
                logger.error(f"Failed to fetch watchlist: {e}")
                result = {"watchlist": [], "count": 0, "error": str(e)}

            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        # ============================================================================
        # IMF GATEWAY
        # ============================================================================
        elif name == "fetch_imf_data":
            result = fetch_imf_data(
                indicator=arguments["indicator"],
                countries=arguments["countries"],
                start_year=arguments.get("start_year"),
                end_year=arguments.get("end_year"),
                use_mcp=False
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_available_indicators":
            result = get_available_indicators()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_available_country_groups":
            result = get_available_country_groups()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # ETF REFERENCE
        # ============================================================================
        elif name == "get_etf_allocation":
            result = get_etf_allocation(arguments["isin"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "list_etf_allocations":
            result = list_etf_allocations()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_etf_country_exposure":
            result = get_etf_country_exposure(arguments["country"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # VIDEO INTELLIGENCE
        # ============================================================================
        elif name == "video_search":
            result = await video_search(arguments["query"], arguments.get("max_results", 10))
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "video_list":
            result = await video_list()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "video_synthesize":
            result = await video_synthesize(
                arguments["query"],
                arguments["video_results"],
                arguments.get("tone", "professional")
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "video_get_transcript":
            result = await video_get_transcript(arguments["video_id"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "video_keyword_search":
            result = await video_keyword_search(arguments["query"], arguments.get("max_results", 10))
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - NFA
        # ============================================================================
        elif name == "get_nfa_rating":
            country = arguments.get("country")
            if not country:
                return [TextContent(type="text", text=json.dumps({"error": "country is required"}))]
            result = get_nfa_rating(
                country,
                arguments.get("year"),
                arguments.get("history", False)
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_nfa_batch":
            result = get_nfa_batch(arguments["countries"], arguments.get("year"))
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "search_nfa_by_rating":
            result = search_nfa_by_rating(
                arguments.get("rating"),
                arguments.get("min_rating"),
                arguments.get("max_rating"),
                arguments.get("year")
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - RATINGS
        # ============================================================================
        elif name == "get_credit_rating":
            result = get_credit_rating(arguments["country"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_credit_ratings_batch":
            result = get_credit_ratings_batch(arguments["countries"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - COUNTRY MAPPING
        # ============================================================================
        elif name == "standardize_country":
            result = standardize_country(arguments["country"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_country_info":
            result = get_country_info(arguments["country"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - FRED
        # ============================================================================
        elif name == "get_fred_series":
            result = get_fred_series(
                arguments["series_id"],
                arguments.get("start_date"),
                arguments.get("end_date"),
                arguments.get("analyze", False)
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "search_fred_series":
            result = search_fred_series(arguments["query"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_treasury_rates":
            result = get_treasury_rates()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - SOVEREIGN CLASSIFICATION
        # ============================================================================
        elif name == "classify_issuer":
            result = classify_issuer(arguments["isin"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "classify_issuers_batch":
            result = classify_issuers_batch(arguments["isins"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "filter_by_issuer_type":
            result = filter_by_issuer_type(arguments["issuer_type"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_issuer_summary":
            result = get_issuer_summary(arguments["issuer"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # IMF DATA (using internal gateway)
        # ============================================================================
        elif name == "get_imf_indicator" or name == "get_imf_indicator_external":
            # Convert country name to ISO code
            country_input = arguments["country"]
            country_info = standardize_country(country_input)
            iso_code = country_info.get("imf_code") or country_info.get("iso_code", country_input)

            # Map common indicator names to IMF codes
            indicator = arguments["indicator"]
            indicator_map = {
                "gdp_growth": "NGDP_RPCH",
                "inflation": "PCPIPCH",
                "debt": "GGXWDG_NGDP",
                "current_account": "BCA_NGDPD",
            }
            indicator = indicator_map.get(indicator.lower(), indicator)

            result = fetch_imf_data(
                indicator=indicator,
                countries=iso_code,
                start_year=arguments.get("start_year"),
                end_year=arguments.get("end_year"),
                use_mcp=False
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "compare_imf_countries":
            # Convert country names to ISO codes
            countries_list = arguments["countries"]
            if isinstance(countries_list, str):
                countries_list = [c.strip() for c in countries_list.split(",")]

            iso_codes = []
            for c in countries_list:
                info = standardize_country(c)
                iso_codes.append(info.get("imf_code") or info.get("iso_code", c))

            # Map common indicator names to IMF codes
            indicator = arguments["indicator"]
            indicator_map = {
                "gdp_growth": "NGDP_RPCH",
                "inflation": "PCPIPCH",
                "debt": "GGXWDG_NGDP",
                "current_account": "BCA_NGDPD",
            }
            indicator = indicator_map.get(indicator.lower(), indicator)

            result = fetch_imf_data(
                indicator=indicator,
                countries=",".join(iso_codes),
                start_year=arguments.get("year"),
                end_year=arguments.get("year"),
                use_mcp=False
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - WORLD BANK
        # ============================================================================
        elif name == "get_worldbank_indicator":
            result = get_worldbank_indicator(
                arguments["indicator"],
                arguments["country"],
                arguments.get("start_year"),
                arguments.get("end_year")
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "search_worldbank_indicators":
            result = search_worldbank_indicators(arguments["query"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_worldbank_country_profile":
            result = get_worldbank_country_profile(arguments["country"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    except Exception as e:
        logger.error(f"Error in {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=f"Error: {str(e)}")]


# Create SSE transport and Starlette app
sse = SseServerTransport("/messages/")


async def handle_sse(request):
    """Handle SSE connection from Claude Desktop"""
    async with sse.connect_sse(
        request.scope,
        request.receive,
        request._send
    ) as streams:
        await mcp_server.run(
            streams[0],
            streams[1],
            mcp_server.create_initialization_options()
        )
    return Response()


async def handle_call(request):
    """
    HTTP POST endpoint to call tools directly.
    Allows Athena Streamlit to call Orca MCP tools without SSE.

    POST /call
    {
        "tool": "get_client_holdings",
        "args": {"portfolio_id": "wnbf", "staging_id": 1}
    }
    """
    try:
        body = await request.json()
        tool_name = body.get("tool")
        args = body.get("args", {})

        if not tool_name:
            return JSONResponse({"error": "Missing 'tool' parameter"}, status_code=400)

        # Call the tool
        result = await call_tool(tool_name, args)

        # Extract text from TextContent
        if result and len(result) > 0:
            text = result[0].text
            try:
                return JSONResponse(json.loads(text))
            except json.JSONDecodeError:
                return JSONResponse({"result": text})

        return JSONResponse({"error": "No result"}, status_code=500)

    except Exception as e:
        logger.error(f"Error in /call: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


async def health_check(request):
    """Health check endpoint"""
    return JSONResponse({
        "status": "healthy",
        "server": "orca-mcp-sse",
        "version": "3.0.0",
        "architecture": "Single orca_query router with internal tool routing",
        "transport": "sse",
        "claude_desktop_url": "/sse",
        "http_call_url": "/call",
        "data_source": "Cloudflare D1 (edge) + External MCPs",
        "exposed_tools": 1,
        "exposed_tool": "orca_query",
        "internal_tools": len(INTERNAL_TOOLS),
        "enabled_tools": list(ENABLED_TOOLS),
        "enabled_count": len(ENABLED_TOOLS),
        "routing": "FallbackLLMClient (Gemini Flash -> OpenAI Mini -> Haiku)",
        "token_savings": "~11K -> ~500 tokens (95% reduction)"
    })


# Create Starlette app
app = Starlette(
    debug=True,
    routes=[
        Route("/", health_check),
        Route("/health", health_check),
        Route("/call", handle_call, methods=["POST"]),
        Route("/sse", handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ]
)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    logger.info(f"Starting Orca MCP SSE Server v3.0 on port {port}")
    logger.info(f"Architecture: Single orca_query tool with internal routing")
    logger.info(f"Enabled tools: {list(ENABLED_TOOLS)}")
    logger.info(f"Claude Desktop URL: http://localhost:{port}/sse")
    uvicorn.run(app, host="0.0.0.0", port=port)
