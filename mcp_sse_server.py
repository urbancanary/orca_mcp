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

import asyncio
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
    "get_fred_series",         # FRED latest value + analysis
    "get_fred_timeseries",     # FRED historical data for charting
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
    "get_dashboard_complete",        # Unified endpoint: dashboard + holdings in one call
    "calculate_trade_settlement",    # Pre-trade settlement calculations
    "get_transactions_display",      # Transaction history with formatting
    "check_trade_compliance",        # Enhanced compliance with impact analysis
    "get_cashflows_display",         # Projected coupons and maturities
    "get_pnl_display",               # P&L reconciliation with validation
    "get_compliance_display",        # Compliance dashboard with rules + charts
    "get_ratings_display",           # Rating distribution by source
    "get_issuer_exposure",           # 5/10/40 issuer concentration
    "get_cash_event_horizon",        # Historical + future cash timeline

    # Phase 7: Additional portfolio tools
    "get_client_transactions",       # Transaction history
    "get_portfolio_cash",            # Cash positions
    "search_bonds_rvm",              # Search bond universe by country/rating/return
    "check_trade_compliance_impact", # Pre-trade compliance check

    # Phase 8: Screening tools
    "search_nfa_by_rating",          # Find countries by NFA star rating

    # Phase 9: Supabase MCP (parallel async)
    "get_portfolio_with_ratings",    # Holdings + NFA + credit ratings in parallel
    "get_supabase_holdings",         # Holdings from Supabase
    "get_supabase_dashboard",        # Dashboard from Supabase

    # Phase 10: Sovereign Credit Reports
    "get_sovereign_report",          # Full sovereign credit report
    "get_sovereign_section",         # Specific section from report (ratings, outlook, etc.)
    "list_sovereign_countries",      # List all available reports
    "search_sovereign_reports",      # Search across reports

    # Phase 12: Report Q&A (LLM-powered with Gemini 2.5 Flash + context caching)
    "query_sovereign_report",        # Ask questions about a country's report
    "compare_sovereign_reports",     # Compare multiple countries' reports

    # Phase 11: Report Pipeline Tracking (Sov-Quasi)
    "get_priority_countries",        # Countries needing research by priority
    "check_country_priority",        # Check specific country's priority/status
    "get_pending_reports",           # Summary of reports needing work

    # Phase 13: Microsoft 365 (per-user OAuth)
    "search_m365_emails",            # Search Outlook emails
    "get_m365_calendar",             # Calendar events in date range
    "search_m365_files",             # OneDrive file search
    "search_m365_sharepoint",        # SharePoint sites/docs search
    "search_m365_teams",             # Teams message search
    "get_m365_status",               # Check M365 connection status

    # Phase 14: SEC EDGAR Filing Analysis
    "edgar_search_company",          # Find company by ticker/name/CIK
    "edgar_filing_section",          # Extract filing section (risk factors, MD&A, etc.)
    "edgar_financials",              # XBRL financial statements
    "edgar_search_filings",          # Full-text search across EDGAR
}

# Add current directory to path for imports
SCRIPT_DIR = Path(__file__).parent.resolve()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# D1 API URL for fast edge queries (Worker URL, never exposed to frontend)
D1_API_URL = os.getenv("ORCA_URL", "https://portfolio-optimizer-mcp.urbancanary.workers.dev")

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


from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent


# Pydantic models for request validation
class CallToolRequest(BaseModel):
    """Request model for /call endpoint"""
    tool: str = Field(..., description="Name of the tool to call")
    args: Dict[str, Any] = Field(default_factory=dict, description="Tool arguments")

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
        get_dashboard_complete,
        calculate_trade_settlement,
        get_transactions_display,
        check_trade_compliance,
        get_cashflows_display,
        get_pnl_display,
        get_compliance_display,
        get_ratings_display,
        get_issuer_exposure,
        get_cash_event_horizon,
        # Async versions
        get_portfolio_dashboard_async,
        get_holdings_display_async,
        get_dashboard_complete_async,
        get_ratings_display_async,
    )
    from tools.external_mcps import (
        get_nfa_rating,
        get_nfa_rating_async,
        get_nfa_batch,
        get_nfa_batch_async,
        search_nfa_by_rating,
        get_credit_rating,
        get_credit_rating_async,
        get_credit_ratings_batch,
        get_credit_ratings_batch_async,
        get_country_ratings_async,
        standardize_country,
        get_country_info,
        get_fred_series,
        get_fred_timeseries,
        search_fred_series,
        get_treasury_rates,
        classify_issuer,
        classify_issuers_batch,
        filter_by_issuer_type,
        get_issuer_summary,
        get_imf_indicator,
        compare_imf_countries,
        compare_imf_countries_async,
        get_worldbank_indicator,
        search_worldbank_indicators,
        get_worldbank_country_profile,
        get_worldbank_country_profile_async,
        # Supabase MCP
        get_supabase_holdings,
        get_supabase_holdings_async,
        get_supabase_transactions,
        get_supabase_portfolio_summary,
        get_supabase_portfolio_summary_async,
        get_supabase_dashboard,
        get_supabase_dashboard_async,
        get_supabase_watchlist,
        add_to_supabase_watchlist,
        remove_from_supabase_watchlist,
        get_portfolio_with_ratings_async,
        # Sov-Quasi Report Tracking
        get_priority_countries,
        check_country_priority,
        get_pending_sov_quasi_reports,
        # M365 MCP
        search_m365_emails_async,
        get_m365_calendar_async,
        search_m365_files_async,
        search_m365_sharepoint_async,
        search_m365_teams_async,
        get_m365_status_async,
    )
    from tools.sovereign_reports import (
        get_sovereign_report,
        get_sovereign_section,
        list_available_countries as list_sovereign_countries,
        search_sovereign_reports,
        query_sovereign_report,
        compare_sovereign_reports,
    )
    from tools.edgar_gateway import (
        edgar_search_company,
        edgar_filing_section,
        edgar_financials,
        edgar_search_filings,
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
        get_dashboard_complete,
        calculate_trade_settlement,
        get_transactions_display,
        check_trade_compliance,
        get_cashflows_display,
        get_pnl_display,
        get_compliance_display,
        get_ratings_display,
        get_issuer_exposure,
        get_cash_event_horizon,
        # Async versions
        get_portfolio_dashboard_async,
        get_holdings_display_async,
        get_dashboard_complete_async,
        get_ratings_display_async,
    )
    from orca_mcp.tools.external_mcps import (
        get_nfa_rating,
        get_nfa_rating_async,
        get_nfa_batch,
        get_nfa_batch_async,
        search_nfa_by_rating,
        get_credit_rating,
        get_credit_rating_async,
        get_credit_ratings_batch,
        get_credit_ratings_batch_async,
        get_country_ratings_async,
        standardize_country,
        get_country_info,
        get_fred_series,
        get_fred_timeseries,
        search_fred_series,
        get_treasury_rates,
        classify_issuer,
        classify_issuers_batch,
        filter_by_issuer_type,
        get_issuer_summary,
        get_imf_indicator,
        compare_imf_countries,
        compare_imf_countries_async,
        get_worldbank_indicator,
        search_worldbank_indicators,
        get_worldbank_country_profile,
        get_worldbank_country_profile_async,
        # Supabase MCP
        get_supabase_holdings,
        get_supabase_holdings_async,
        get_supabase_transactions,
        get_supabase_portfolio_summary,
        get_supabase_portfolio_summary_async,
        get_supabase_dashboard,
        get_supabase_dashboard_async,
        get_supabase_watchlist,
        add_to_supabase_watchlist,
        remove_from_supabase_watchlist,
        get_portfolio_with_ratings_async,
        # Sov-Quasi Report Tracking
        get_priority_countries,
        check_country_priority,
        get_pending_sov_quasi_reports,
    )
    from orca_mcp.tools.sovereign_reports import (
        get_sovereign_report,
        get_sovereign_section,
        list_available_countries as list_sovereign_countries,
        search_sovereign_reports,
        query_sovereign_report,
        compare_sovereign_reports,
    )
    from orca_mcp.tools.edgar_gateway import (
        edgar_search_company,
        edgar_filing_section,
        edgar_financials,
        edgar_search_filings,
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
            description="Get FRED economic data (latest value + AI analysis). Common: DGS10 (10Y Treasury), CPIAUCSL (CPI), UNRATE (unemployment), FEDFUNDS",
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
            name="get_fred_timeseries",
            description="Get FRED historical time series data for charting. Returns array of {date, value} observations. Use for trends, charts, analysis over time.",
            inputSchema={
                "type": "object",
                "properties": {
                    "series_id": {"type": "string", "description": "FRED series ID (e.g., DGS10, CPIAUCSL, UNRATE)"},
                    "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD), default: 2019-01-01"}
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

        # ============================================================================
        # DISPLAY-READY ENDPOINTS (for thin frontends)
        # ============================================================================
        Tool(
            name="get_holdings_display",
            description="Get holdings with ALL display columns and formatted values (_fmt). Returns face_value, market_value, P&L, yield, duration, spread - all pre-formatted for direct display. Use this for Holdings page.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "include_staging": {"type": "boolean", "description": "Include staging holdings (default: false)"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_portfolio_dashboard",
            description="Single call for Portfolio/Summary page. Returns summary stats (total value, cash, duration, yield), allocation breakdowns (by country, rating, sector), and compliance summary. All values include _fmt versions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_dashboard_complete",
            description="Unified endpoint for complete dashboard. Returns summary, allocation, compliance_summary, totals, and holdings array - all pre-formatted. Replaces separate calls to get_portfolio_dashboard + get_holdings_display.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "include_staging": {"type": "boolean", "description": "Include staging transactions (default: false)"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="calculate_trade_settlement",
            description="Pre-trade settlement calculation. Returns principal, accrued interest, net settlement amount with formatted values. Uses 30/360 day count convention.",
            inputSchema={
                "type": "object",
                "properties": {
                    "isin": {"type": "string", "description": "Bond ISIN"},
                    "face_value": {"type": "number", "description": "Face/par value of the trade"},
                    "price": {"type": "number", "description": "Clean price as percentage of par"},
                    "settle_date": {"type": "string", "description": "Settlement date (YYYY-MM-DD)"},
                    "side": {"type": "string", "enum": ["BUY", "SELL"], "description": "Trade side"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": ["isin", "face_value", "price", "settle_date"]
            }
        ),
        Tool(
            name="get_transactions_display",
            description="Transaction history with display-ready formatting. Filter by type (BUY/SELL/COUPON), status, date range. Returns formatted values and summary.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "transaction_type": {"type": "string", "enum": ["ALL", "BUY", "SELL", "COUPON"], "description": "Filter by type"},
                    "status": {"type": "string", "enum": ["ALL", "settled", "pending", "staging"], "description": "Filter by status"},
                    "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                    "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"},
                    "limit": {"type": "integer", "description": "Max transactions (default: 100)"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="check_trade_compliance",
            description="Enhanced pre-trade compliance check. Shows before/after compliance, impact analysis, warnings, errors, and can_proceed flag. Use this before executing trades.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "ticker": {"type": "string", "description": "Bond ticker"},
                    "country": {"type": "string", "description": "Bond country"},
                    "action": {"type": "string", "enum": ["buy", "sell"], "description": "Trade action"},
                    "market_value": {"type": "number", "description": "Trade market value"}
                },
                "required": ["ticker", "country", "action", "market_value"]
            }
        ),
        Tool(
            name="get_cashflows_display",
            description="Projected cashflows for Cashflows page. Returns upcoming coupons and maturities with summary, individual flows, and monthly breakdown. All amounts pre-formatted.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "months_ahead": {"type": "integer", "description": "How many months ahead (default: 12)"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_compliance_display",
            description="Display-ready compliance dashboard. Returns is_compliant status, rules with pass/fail, country concentration chart data, violations, and metrics. Use for Compliance page.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_pnl_display",
            description="P&L reconciliation with validation. Returns opening/closing NAV, breakdown (realized, unrealized, coupons), by-holding P&L, and reconciliation check. Supports MTD, YTD, Since Inception periods.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "period": {"type": "string", "enum": ["MTD", "YTD", "Since Inception", "Custom"], "description": "Period for P&L calculation"},
                    "start_date": {"type": "string", "description": "Start date for Custom period (YYYY-MM-DD)"},
                    "end_date": {"type": "string", "description": "End date for Custom period (YYYY-MM-DD)"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_ratings_display",
            description="Rating distribution for Ratings page. Returns distribution by rating with IG/HY split, summary stats (ig_pct, hy_pct, avg_rating). Supports S&P, Moody's, and stub ratings.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "rating_source": {"type": "string", "enum": ["sp", "sp_stub", "moodys"], "description": "Rating source (default: sp_stub)"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_issuer_exposure",
            description="Issuer-level concentration for 5/10/40 rule compliance. Returns issuers with their bonds, total exposure %, over_5/over_10 flags, and rule pass/fail summary.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_cash_event_horizon",
            description="Historical + future cash timeline. Returns current balance, historical transactions with running balance, future cashflows (coupons/maturities) with projected balance.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"},
                    "future_days": {"type": "integer", "description": "How many days ahead for future cashflows (default: 90)"},
                    "client_id": {"type": "string", "description": "Client ID"}
                },
                "required": []
            }
        ),

        # ============================================================================
        # SUPABASE MCP - PARALLEL ASYNC TOOLS
        # ============================================================================
        Tool(
            name="get_portfolio_with_ratings",
            description="Get portfolio holdings with NFA and credit ratings for all countries - ALL IN PARALLEL. ~10x faster than sequential calls. Returns holdings, country ratings (NFA stars + S&P/Moody's).",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID (default: 'wnbf')"}
                },
                "required": []
            }
        ),
        Tool(
            name="get_supabase_holdings",
            description="Get portfolio holdings from Supabase. Alternative to D1 for Supabase-enabled portfolios.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID"}
                },
                "required": ["portfolio_id"]
            }
        ),
        Tool(
            name="get_supabase_dashboard",
            description="Get full dashboard (summary + allocations) from Supabase.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "string", "description": "Portfolio ID"}
                },
                "required": ["portfolio_id"]
            }
        ),

        # ============================================================================
        # SOVEREIGN CREDIT REPORTS
        # ============================================================================
        Tool(
            name="get_sovereign_report",
            description="Get a full sovereign credit report for a country. Returns comprehensive analysis including ratings, economic outlook, political assessment, and credit risks.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name (e.g., 'Brazil', 'Indonesia', 'Hungary')"}
                },
                "required": ["country"]
            }
        ),
        Tool(
            name="get_sovereign_section",
            description="Get a specific section from a sovereign credit report. Sections: summary, ratings, economic, fiscal, external, political, banking, outlook, strengths, vulnerabilities.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name"},
                    "section": {"type": "string", "description": "Section name", "enum": ["summary", "ratings", "economic", "fiscal", "external", "political", "banking", "outlook", "strengths", "vulnerabilities"]}
                },
                "required": ["country", "section"]
            }
        ),
        Tool(
            name="list_sovereign_countries",
            description="List all available sovereign credit reports. Returns country names and report availability.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="search_sovereign_reports",
            description="Search across all sovereign credit reports for a keyword or phrase. Finds matches with context.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search term or phrase"},
                    "max_results": {"type": "integer", "description": "Max results per country (default 5)"}
                },
                "required": ["query"]
            }
        ),

        # ============================================================================
        # REPORT Q&A (LLM-powered with Gemini 2.5 Flash + context caching)
        # ============================================================================
        Tool(
            name="query_sovereign_report",
            description="Ask any question about a country's sovereign credit report. Uses Gemini 2.5 Flash with context caching for cost-effective analysis of full reports. Best for: summarizing sections, explaining ratings, identifying risks, answering specific questions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name (e.g., 'Brazil', 'Turkey', 'Kazakhstan')"},
                    "question": {"type": "string", "description": "Question to ask about the report (e.g., 'summarize the external position', 'what are the main credit risks?')"}
                },
                "required": ["country", "question"]
            }
        ),
        Tool(
            name="compare_sovereign_reports",
            description="Compare 2-5 countries' sovereign credit reports using LLM analysis. Sends full reports to Gemini for comprehensive comparison.",
            inputSchema={
                "type": "object",
                "properties": {
                    "countries": {"type": "array", "items": {"type": "string"}, "description": "List of 2-5 country names to compare"},
                    "question": {"type": "string", "description": "Comparison question (e.g., 'compare fiscal positions', 'which has stronger reserves?')"}
                },
                "required": ["countries", "question"]
            }
        ),

        # ============================================================================
        # REPORT PIPELINE TRACKING (Sov-Quasi)
        # ============================================================================
        Tool(
            name="get_priority_countries",
            description="Get countries that need research reports, organized by priority (high/medium/low/minimal). Priority is based on portfolio exposure - number of bond positions we hold.",
            inputSchema={
                "type": "object",
                "properties": {
                    "priority": {"type": "string", "description": "Filter by priority level", "enum": ["high", "medium", "low", "minimal"]}
                },
                "required": []
            }
        ),
        Tool(
            name="check_country_priority",
            description="Check if a specific country needs a research report and what its priority is. Returns status, priority, and portfolio positions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name to check (e.g., 'Philippines', 'Egypt')"}
                },
                "required": ["country"]
            }
        ),
        Tool(
            name="get_pending_reports",
            description="Get summary of sovereign reports needing work - both those needing research and those with raw reports ready to process.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),

        # ============================================================================
        # MICROSOFT 365 (per-user OAuth - email, calendar, files, sharepoint, teams)
        # ============================================================================
        Tool(
            name="search_m365_emails",
            description="Search the user's Outlook emails. Requires M365 connection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_code": {"type": "string", "description": "User identifier for M365 auth"},
                    "query": {"type": "string", "description": "Search query (e.g., 'quarterly report', 'from:john')"},
                    "top": {"type": "integer", "description": "Max results (default 10)"}
                },
                "required": ["user_code", "query"]
            }
        ),
        Tool(
            name="get_m365_calendar",
            description="Get the user's calendar events for a date range. Requires M365 connection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_code": {"type": "string", "description": "User identifier for M365 auth"},
                    "start_date": {"type": "string", "description": "Start date (ISO 8601, e.g., '2026-02-12T00:00:00Z')"},
                    "end_date": {"type": "string", "description": "End date (ISO 8601, e.g., '2026-02-13T23:59:59Z')"}
                },
                "required": ["user_code", "start_date", "end_date"]
            }
        ),
        Tool(
            name="search_m365_files",
            description="Search the user's OneDrive files. Requires M365 connection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_code": {"type": "string", "description": "User identifier for M365 auth"},
                    "query": {"type": "string", "description": "File search query"}
                },
                "required": ["user_code", "query"]
            }
        ),
        Tool(
            name="search_m365_sharepoint",
            description="Search SharePoint sites and documents. Requires M365 connection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_code": {"type": "string", "description": "User identifier for M365 auth"},
                    "query": {"type": "string", "description": "Search query"}
                },
                "required": ["user_code", "query"]
            }
        ),
        Tool(
            name="search_m365_teams",
            description="Search Teams messages and channels. Requires M365 connection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_code": {"type": "string", "description": "User identifier for M365 auth"},
                    "query": {"type": "string", "description": "Search query"}
                },
                "required": ["user_code", "query"]
            }
        ),
        Tool(
            name="get_m365_status",
            description="Check if a user is connected to Microsoft 365. Returns connection status and email.",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_code": {"type": "string", "description": "User identifier to check"}
                },
                "required": ["user_code"]
            }
        ),

        # ============================================================================
        # SEC EDGAR FILING ANALYSIS
        # ============================================================================
        Tool(
            name="edgar_search_company",
            description="Search SEC EDGAR for a company by ticker symbol, company name, or CIK number. Returns company profile and recent filings list.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Ticker (AAPL), company name (Apple Inc), or CIK (320193)"}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="edgar_filing_section",
            description="Extract a specific section from a company's SEC filing (10-K or 10-Q). Sections: risk_factors, mda, business, financial_statements, legal_proceedings, market_risk, controls.",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "Company ticker symbol (e.g., AAPL, JPM, AMZN)"},
                    "section": {"type": "string", "description": "Section to extract", "enum": ["risk_factors", "mda", "business", "financial_statements", "legal_proceedings", "market_risk", "controls"]},
                    "form_type": {"type": "string", "description": "Filing type (default: 10-K)", "enum": ["10-K", "10-Q"]},
                    "filing_date": {"type": "string", "description": "Specific filing date (YYYY-MM-DD). If omitted, uses most recent."}
                },
                "required": ["ticker", "section"]
            }
        ),
        Tool(
            name="edgar_financials",
            description="Get XBRL financial statement data from SEC filings. Returns formatted income statement, balance sheet, or cash flow statement.",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "Company ticker symbol (e.g., AAPL, JPM, AMZN)"},
                    "statement": {"type": "string", "description": "Statement type", "enum": ["income", "balance", "cashflow", "all"]},
                    "periods": {"type": "integer", "description": "Number of periods (default: 4)"},
                    "annual": {"type": "boolean", "description": "True for annual (10-K), False for quarterly (10-Q)"}
                },
                "required": ["ticker"]
            }
        ),
        Tool(
            name="edgar_search_filings",
            description="Full-text search across all SEC EDGAR filings. Find filings mentioning specific topics, companies, or terms.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search terms. Supports boolean (AI AND revenue), exact phrases (\"artificial intelligence\")."},
                    "form_type": {"type": "string", "description": "Filter by form type (10-K, 10-Q, 8-K, etc.)"},
                    "ticker": {"type": "string", "description": "Filter by company ticker"},
                    "date_from": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                    "date_to": {"type": "string", "description": "End date (YYYY-MM-DD)"},
                    "max_results": {"type": "integer", "description": "Max results (default: 10, max: 20)"}
                },
                "required": ["query"]
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
            from tools.cloudflare_d1 import get_holdings_async as d1_holdings_async
            holdings_df = await d1_holdings_async(portfolio_id, staging_id)
            holdings = holdings_df.to_dict(orient='records') if not holdings_df.empty else []
            return [TextContent(type="text", text=json.dumps(holdings, indent=2, default=str))]

        elif name == "get_portfolio_summary":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            staging_id = arguments.get("staging_id", 1)
            from tools.cloudflare_d1 import get_holdings_summary_async as d1_summary_async
            summary = await d1_summary_async(portfolio_id, staging_id)
            return [TextContent(type="text", text=json.dumps(summary, indent=2, default=str))]

        elif name == "get_client_transactions":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            limit = arguments.get("limit", 100)
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")

            from tools.cloudflare_d1 import get_transactions_async as d1_transactions_async
            df = await d1_transactions_async(portfolio_id)

            # Apply date filters on DataFrame
            if not df.empty and 'transaction_date' in df.columns:
                if start_date:
                    df = df[df['transaction_date'] >= start_date]
                if end_date:
                    df = df[df['transaction_date'] <= end_date]

            # Apply limit
            if limit != -1:
                df = df.head(limit)

            records = json.loads(df.to_json(orient='records', date_format='iso')) if not df.empty else []
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

            # Get holdings and summary in parallel
            from tools.cloudflare_d1 import get_holdings_async as d1_holdings_async, get_holdings_summary_async as d1_summary_async
            holdings_list, summary = await asyncio.gather(
                d1_holdings_async(portfolio_id, staging_id=1),
                d1_summary_async(portfolio_id, staging_id=1),
            )

            if holdings_list.empty:
                return [TextContent(type="text", text=json.dumps({"error": "No holdings found"}))]

            holdings_df = holdings_list
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

            # Get holdings and summary in parallel
            from tools.cloudflare_d1 import get_holdings_async as d1_holdings_async, get_holdings_summary_async as d1_summary_async
            holdings_df, summary = await asyncio.gather(
                d1_holdings_async(portfolio_id, staging_id=1),
                d1_summary_async(portfolio_id, staging_id=1),
            )
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

                    # NFA filter - fetch all ratings concurrently using async
                    if min_nfa_rating:
                        countries = list(set(b.get('country') or b.get('cbonds_country') for b in watchlist if b.get('country') or b.get('cbonds_country')))

                        # Use async batch call for ~10x speedup (parallel instead of sequential)
                        nfa_results = await get_nfa_batch_async(countries)

                        country_nfa = {}
                        for c, nfa in nfa_results.items():
                            if isinstance(nfa, dict) and 'nfa_star_rating' in nfa:
                                country_nfa[c] = nfa['nfa_star_rating']
                            elif isinstance(nfa, dict) and 'error' in nfa:
                                logger.warning(f"NFA lookup error for {c}: {nfa.get('error')}")

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
            result = await get_nfa_rating_async(
                country,
                arguments.get("year"),
                arguments.get("history", False)
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_nfa_batch":
            result = await get_nfa_batch_async(arguments["countries"], arguments.get("year"))
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "search_nfa_by_rating":
            result = await asyncio.to_thread(
                search_nfa_by_rating,
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
            result = await get_credit_rating_async(arguments["country"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_credit_ratings_batch":
            result = await get_credit_ratings_batch_async(arguments["countries"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - COUNTRY MAPPING
        # ============================================================================
        elif name == "standardize_country":
            result = await asyncio.to_thread(standardize_country, arguments["country"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_country_info":
            result = await asyncio.to_thread(get_country_info, arguments["country"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - FRED
        # ============================================================================
        elif name == "get_fred_series":
            result = await asyncio.to_thread(
                get_fred_series,
                arguments["series_id"],
                arguments.get("start_date"),
                arguments.get("end_date"),
                arguments.get("analyze", False)
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_fred_timeseries":
            result = await asyncio.to_thread(
                get_fred_timeseries,
                arguments["series_id"],
                arguments.get("start_date", "2019-01-01")
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "search_fred_series":
            result = await asyncio.to_thread(search_fred_series, arguments["query"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_treasury_rates":
            result = await asyncio.to_thread(get_treasury_rates)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        # ============================================================================
        # EXTERNAL MCP - SOVEREIGN CLASSIFICATION
        # ============================================================================
        elif name == "classify_issuer":
            result = await asyncio.to_thread(classify_issuer, arguments["isin"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "classify_issuers_batch":
            result = await asyncio.to_thread(classify_issuers_batch, arguments["isins"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "filter_by_issuer_type":
            result = await asyncio.to_thread(filter_by_issuer_type, arguments["issuer_type"])
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_issuer_summary":
            result = await asyncio.to_thread(get_issuer_summary, arguments["issuer"])
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

        # ============================================================================
        # DISPLAY-READY ENDPOINTS (for thin frontends)
        # ============================================================================
        elif name == "get_holdings_display":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            include_staging = arguments.get("include_staging", False)
            result = await get_holdings_display_async(portfolio_id, include_staging, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_portfolio_dashboard":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            result = await get_portfolio_dashboard_async(portfolio_id, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_dashboard_complete":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            include_staging = arguments.get("include_staging", False)
            result = await get_dashboard_complete_async(portfolio_id, include_staging, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "calculate_trade_settlement":
            result = calculate_trade_settlement(
                isin=arguments["isin"],
                face_value=arguments["face_value"],
                price=arguments["price"],
                settle_date=arguments["settle_date"],
                side=arguments.get("side", "BUY"),
                client_id=client_id
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_transactions_display":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            result = get_transactions_display(
                portfolio_id=portfolio_id,
                transaction_type=arguments.get("transaction_type", "ALL"),
                status=arguments.get("status", "ALL"),
                start_date=arguments.get("start_date"),
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
                client_id=client_id
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "check_trade_compliance":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            result = check_trade_compliance(
                portfolio_id=portfolio_id,
                ticker=arguments["ticker"],
                country=arguments["country"],
                action=arguments["action"],
                market_value=arguments["market_value"],
                client_id=client_id
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_cashflows_display":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            months_ahead = arguments.get("months_ahead", 12)
            result = get_cashflows_display(portfolio_id, months_ahead, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_compliance_display":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            result = get_compliance_display(portfolio_id, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_pnl_display":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            period = arguments.get("period", "Since Inception")
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            result = get_pnl_display(portfolio_id, period, start_date, end_date, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_ratings_display":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            rating_source = arguments.get("rating_source", "sp_stub")
            result = await get_ratings_display_async(portfolio_id, rating_source, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_issuer_exposure":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            result = get_issuer_exposure(portfolio_id, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_cash_event_horizon":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            future_days = arguments.get("future_days", 90)
            result = get_cash_event_horizon(portfolio_id, future_days, client_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        # ============================================================================
        # SUPABASE MCP - PARALLEL ASYNC
        # ============================================================================
        elif name == "get_portfolio_with_ratings":
            # Parallel async: holdings + all country ratings at once
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            result = await get_portfolio_with_ratings_async(portfolio_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_supabase_holdings":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            result = await get_supabase_holdings_async(portfolio_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_supabase_dashboard":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            result = await get_supabase_dashboard_async(portfolio_id)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        # ============================================================================
        # SOVEREIGN CREDIT REPORTS
        # ============================================================================
        elif name == "get_sovereign_report":
            country = arguments.get("country")
            result = get_sovereign_report(country)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_sovereign_section":
            country = arguments.get("country")
            section = arguments.get("section")
            result = get_sovereign_section(country, section)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "list_sovereign_countries":
            result = list_sovereign_countries()
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "search_sovereign_reports":
            query = arguments.get("query")
            max_results = arguments.get("max_results", 5)
            result = search_sovereign_reports(query, max_results)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        # ============================================================================
        # REPORT Q&A (LLM-powered with Gemini 2.5 Flash + context caching)
        # ============================================================================
        elif name == "query_sovereign_report":
            country = arguments.get("country")
            question = arguments.get("question")
            result = query_sovereign_report(country, question)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "compare_sovereign_reports":
            countries = arguments.get("countries", [])
            question = arguments.get("question")
            result = compare_sovereign_reports(countries, question)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        # ============================================================================
        # REPORT PIPELINE TRACKING (Sov-Quasi)
        # ============================================================================
        elif name == "get_priority_countries":
            priority = arguments.get("priority")
            result = get_priority_countries(priority)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "check_country_priority":
            country = arguments.get("country")
            result = check_country_priority(country)
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_pending_reports":
            result = get_pending_sov_quasi_reports()
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        # ============================================================================
        # MICROSOFT 365
        # ============================================================================
        elif name == "search_m365_emails":
            result = await search_m365_emails_async(
                arguments["user_code"], arguments["query"], arguments.get("top", 10)
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_m365_calendar":
            result = await get_m365_calendar_async(
                arguments["user_code"], arguments["start_date"], arguments["end_date"]
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "search_m365_files":
            result = await search_m365_files_async(
                arguments["user_code"], arguments["query"]
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "search_m365_sharepoint":
            result = await search_m365_sharepoint_async(
                arguments["user_code"], arguments["query"]
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "search_m365_teams":
            result = await search_m365_teams_async(
                arguments["user_code"], arguments["query"]
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "get_m365_status":
            result = await get_m365_status_async(arguments["user_code"])
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        # ============================================================================
        # SEC EDGAR FILING ANALYSIS
        # ============================================================================
        elif name == "edgar_search_company":
            query = arguments.get("query")
            if not query:
                return [TextContent(type="text", text="ERROR: query is required")]
            result = await asyncio.to_thread(edgar_search_company, query)
            return [TextContent(type="text", text=result)]

        elif name == "edgar_filing_section":
            ticker = arguments.get("ticker")
            section = arguments.get("section")
            if not ticker or not section:
                return [TextContent(type="text", text="ERROR: ticker and section are required")]
            form_type = arguments.get("form_type", "10-K")
            filing_date = arguments.get("filing_date")
            result = await asyncio.to_thread(edgar_filing_section, ticker, section, form_type, filing_date)
            return [TextContent(type="text", text=result)]

        elif name == "edgar_financials":
            ticker = arguments.get("ticker")
            if not ticker:
                return [TextContent(type="text", text="ERROR: ticker is required")]
            statement = arguments.get("statement", "income")
            periods = arguments.get("periods", 4)
            annual = arguments.get("annual", True)
            result = await asyncio.to_thread(edgar_financials, ticker, statement, periods, annual)
            return [TextContent(type="text", text=result)]

        elif name == "edgar_search_filings":
            query = arguments.get("query")
            if not query:
                return [TextContent(type="text", text="ERROR: query is required")]
            result = await asyncio.to_thread(
                edgar_search_filings,
                query,
                arguments.get("form_type"),
                arguments.get("ticker"),
                arguments.get("date_from"),
                arguments.get("date_to"),
                arguments.get("max_results", 10),
            )
            return [TextContent(type="text", text=result)]

        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    except Exception as e:
        logger.error(f"Error in {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=f"Error: {str(e)}")]


# Create FastAPI app with auto-documentation
app = FastAPI(
    title="Orca MCP",
    description="Portfolio Data Gateway - Natural language interface to financial data",
    version="3.2.1",
    docs_url="/docs",      # Swagger UI at /docs
    redoc_url="/redoc",    # ReDoc at /redoc
)

# Enable CORS for browser access (Orion UI)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for local development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create SSE transport for MCP
sse = SseServerTransport("/messages/")


@app.get("/", tags=["Health"])
@app.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint.
    Returns server status and configuration info.
    """
    return {
        "status": "healthy",
        "server": "orca-mcp-fastapi",
        "version": "3.2.0",
        "architecture": "Single orca_query router with internal tool routing",
        "transport": "sse",
        "claude_desktop_url": "/sse",
        "http_call_url": "/call",
        "docs_url": "/docs",
        "data_source": "Cloudflare D1 (edge) + External MCPs",
        "exposed_tools": 1,
        "exposed_tool": "orca_query",
        "internal_tools": len(INTERNAL_TOOLS),
        "enabled_tools": list(ENABLED_TOOLS),
        "enabled_count": len(ENABLED_TOOLS),
        "routing": "FallbackLLMClient (Gemini Flash -> OpenAI Mini -> Haiku)",
        "token_savings": "~11K -> ~500 tokens (95% reduction)",
        "async_speedup": "7-18x with httpx concurrent calls"
    }


@app.post("/call", tags=["Tools"])
async def handle_call(request: CallToolRequest):
    """
    Call an Orca MCP tool directly via HTTP.

    This endpoint allows programmatic access to all internal tools without
    going through the MCP SSE protocol. Useful for Athena, scripts, and testing.

    Example:
    ```json
    {
        "tool": "get_client_holdings",
        "args": {"portfolio_id": "wnbf", "staging_id": 1}
    }
    ```
    """
    try:
        # Call the tool
        result = await call_tool(request.tool, request.args)

        # Extract text from TextContent
        if result and len(result) > 0:
            text = result[0].text
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"result": text}

        return JSONResponse({"error": "No result"}, status_code=500)

    except Exception as e:
        logger.error(f"Error in /call: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/tools", tags=["Tools"])
async def list_available_tools():
    """
    List all enabled tools available via the /call endpoint.

    Returns tool names and their descriptions for programmatic discovery.
    """
    tools_info = []
    for tool in INTERNAL_TOOLS:
        if tool.name in ENABLED_TOOLS:
            tools_info.append({
                "name": tool.name,
                "description": tool.description,
                "schema": tool.inputSchema
            })
    return {
        "enabled_count": len(ENABLED_TOOLS),
        "tools": tools_info
    }


@app.api_route("/sse", methods=["GET"], tags=["MCP"])
async def handle_sse(request: Request):
    """
    SSE endpoint for Claude Desktop MCP connections.

    Connect Claude Desktop using: https://your-server/sse
    """
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


# =============================================================================
# PROXY PASS-THROUGH: Forward /proxy/* and /api/* to Worker
# Keeps Worker URL hidden from frontend — all traffic routes through Orca
# =============================================================================

def _proxy_headers(request: Request) -> dict:
    """Build headers for proxied requests to Worker."""
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Orca-MCP/3.2",
        "Accept": "application/json",
    }
    client_id = request.headers.get("X-Client-ID")
    if client_id:
        headers["X-Client-ID"] = client_id
    return headers


@app.api_route("/proxy/{service}/{path:path}", methods=["GET", "POST", "PUT", "DELETE"], tags=["Proxy"])
async def proxy_to_worker(service: str, path: str, request: Request):
    """Forward proxy requests to Cloudflare Worker."""
    target_url = f"{D1_API_URL}/proxy/{service}/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"
    try:
        body = await request.body() if request.method in ("POST", "PUT") else None
        req = urllib.request.Request(
            target_url,
            data=body,
            headers=_proxy_headers(request),
            method=request.method
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_body = resp.read()
            return Response(
                content=resp_body,
                status_code=resp.status,
                media_type=resp.headers.get("Content-Type", "application/json")
            )
    except urllib.error.HTTPError as e:
        return Response(content=e.read(), status_code=e.code, media_type="application/json")
    except Exception as e:
        logger.error(f"Proxy error /{service}/{path}: {e}")
        return JSONResponse({"error": str(e)}, status_code=502)


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE"], tags=["Proxy"])
async def proxy_api_to_worker(path: str, request: Request):
    """Forward /api/* requests to Cloudflare Worker (cashflows, auth, etc.)."""
    target_url = f"{D1_API_URL}/api/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"
    try:
        body = await request.body() if request.method in ("POST", "PUT") else None
        req = urllib.request.Request(
            target_url,
            data=body,
            headers=_proxy_headers(request),
            method=request.method
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_body = resp.read()
            return Response(
                content=resp_body,
                status_code=resp.status,
                media_type=resp.headers.get("Content-Type", "application/json")
            )
    except urllib.error.HTTPError as e:
        return Response(content=e.read(), status_code=e.code, media_type="application/json")
    except Exception as e:
        logger.error(f"Proxy API error /api/{path}: {e}")
        return JSONResponse({"error": str(e)}, status_code=502)


# Mount SSE message handler for MCP protocol
app.mount("/messages/", app=sse.handle_post_message)

# Mount Orion UI static files at /ui to avoid route conflicts
ORION_UI_PATH = Path(__file__).parent.parent / "orion_v2"
if ORION_UI_PATH.exists():
    app.mount("/ui", StaticFiles(directory=str(ORION_UI_PATH), html=True), name="orion-ui")
    logger.info(f"Orion UI mounted at /ui from: {ORION_UI_PATH}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    logger.info(f"Starting Orca MCP FastAPI Server v3.2.0 on port {port}")
    logger.info(f"Architecture: Single orca_query tool with internal routing")
    logger.info(f"Enabled tools: {len(ENABLED_TOOLS)} tools")
    logger.info(f"Claude Desktop URL: http://localhost:{port}/sse")
    logger.info(f"API Docs: http://localhost:{port}/docs")
    logger.info(f"Tool discovery: http://localhost:{port}/tools")
    uvicorn.run(app, host="0.0.0.0", port=port)
