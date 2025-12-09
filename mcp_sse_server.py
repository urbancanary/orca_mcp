#!/usr/bin/env python3
"""
Orca MCP - SSE Server for Claude Desktop Remote Connection

This server provides MCP (Model Context Protocol) access via SSE transport,
allowing Claude Desktop to connect remotely using the "Add custom connector" feature.

Usage:
    # Run standalone
    python mcp_sse_server.py

    # Or via uvicorn
    uvicorn mcp_sse_server:app --host 0.0.0.0 --port 8000

Claude Desktop Configuration:
    Name: Orca Portfolio
    URL: https://your-railway-url.up.railway.app/sse
"""

import os
import sys
import json
import logging
import urllib.request
import urllib.error
from pathlib import Path

# Add current directory to path for imports
SCRIPT_DIR = Path(__file__).parent.resolve()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# D1 API URL for fast edge queries
D1_API_URL = "https://portfolio-optimizer-mcp.urbancanary.workers.dev"


def get_holdings_from_d1(portfolio_id: str = 'wnbf', staging_id: int = 1) -> list:
    """
    Get portfolio holdings from Cloudflare D1 (fast edge database)

    D1-First Architecture: User queries go to D1 for fast responses.
    Data is synced from BigQuery via background job.
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
    from client_config import get_client_config
except ImportError:
    from orca_mcp.tools.data_access import query_bigquery
    from orca_mcp.tools.imf_gateway import (
        fetch_imf_data,
        get_available_indicators,
        get_available_country_groups
    )
    from orca_mcp.client_config import get_client_config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orca-mcp-sse")

# Create MCP server instance
mcp_server = Server("orca-mcp")


@mcp_server.list_tools()
async def list_tools() -> list[Tool]:
    """List available Orca MCP tools"""
    return [
        Tool(
            name="get_client_holdings",
            description="Get current portfolio holdings from D1 edge database. Returns full bond details including price, yield, duration, spread, rating.",
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio identifier (e.g., 'wnbf')"
                    },
                    "staging_id": {
                        "type": "integer",
                        "description": "1=Live portfolio, 2=Staging portfolio (default: 2)"
                    }
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
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio identifier (e.g., 'wnbf')"
                    },
                    "staging_id": {
                        "type": "integer",
                        "description": "1=Live portfolio, 2=Staging portfolio (default: 2)"
                    }
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
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio identifier"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Filter from date (YYYY-MM-DD)"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Filter to date (YYYY-MM-DD)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max records (default 100)"
                    }
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
                    "portfolio_id": {
                        "type": "string",
                        "description": "Portfolio ID (default: 'wnbf')"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="query_client_data",
            description="Run custom SQL query on portfolio data",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query (use simple table names)"
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="fetch_imf_data",
            description="Get IMF economic data (debt, GDP, inflation) for countries or groups (G7, G20, BRICS)",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator": {
                        "type": "string",
                        "description": "Indicator: debt, gdp_growth, inflation, unemployment"
                    },
                    "countries": {
                        "type": "string",
                        "description": "Country name, ISO code, or group (G7, G20, BRICS)"
                    },
                    "start_year": {
                        "type": "integer",
                        "description": "Start year (default: 2010)"
                    }
                },
                "required": ["indicator", "countries"]
            }
        ),
        Tool(
            name="get_imf_indicators",
            description="List available IMF economic indicators",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        )
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls"""
    try:
        client_id = arguments.get("client_id", "guinness")

        if name == "get_client_holdings":
            portfolio_id = arguments["portfolio_id"]
            staging_id = arguments.get("staging_id", 2)  # Default to staging (synced data)
            # Use D1 for fast edge queries
            holdings = get_holdings_from_d1(portfolio_id, staging_id)
            return [TextContent(type="text", text=json.dumps(holdings, indent=2, default=str))]

        elif name == "get_portfolio_summary":
            portfolio_id = arguments["portfolio_id"]
            staging_id = arguments.get("staging_id", 2)  # Default to staging (synced data)
            # Use D1 for fast edge queries
            summary = get_holdings_summary_from_d1(portfolio_id, staging_id)
            return [TextContent(type="text", text=json.dumps(summary, indent=2, default=str))]

        elif name == "get_client_transactions":
            portfolio_id = arguments["portfolio_id"]
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
            return [TextContent(type="text", text=json.dumps(df.to_dict(orient='records'), indent=2, default=str))]

        elif name == "get_portfolio_cash":
            portfolio_id = arguments.get("portfolio_id", "wnbf")
            sql = f"SELECT * FROM portfolio_summary WHERE portfolio_id = '{portfolio_id}'"
            df = query_bigquery(sql, client_id)
            if df.empty:
                return [TextContent(type="text", text=json.dumps({"error": "No summary found"}))]
            return [TextContent(type="text", text=json.dumps(df.to_dict(orient='records')[0], indent=2, default=str))]

        elif name == "query_client_data":
            sql = arguments["sql"]
            df = query_bigquery(sql, client_id)
            return [TextContent(type="text", text=json.dumps(df.to_dict(orient='records'), indent=2, default=str))]

        elif name == "fetch_imf_data":
            result = fetch_imf_data(
                indicator=arguments["indicator"],
                countries=arguments["countries"],
                start_year=arguments.get("start_year"),
                end_year=arguments.get("end_year"),
                use_mcp=False
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_imf_indicators":
            result = get_available_indicators()
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
    # Return empty response to avoid NoneType error when client disconnects
    return Response()


async def health_check(request):
    """Health check endpoint"""
    return JSONResponse({
        "status": "healthy",
        "server": "orca-mcp-sse",
        "transport": "sse",
        "claude_desktop_url": "/sse",
        "data_source": "Cloudflare D1 (edge)",
        "tools": [
            "get_client_holdings",
            "get_portfolio_summary",
            "get_client_transactions",
            "get_portfolio_cash",
            "query_client_data",
            "fetch_imf_data",
            "get_imf_indicators"
        ]
    })


# Create Starlette app
app = Starlette(
    debug=True,
    routes=[
        Route("/", health_check),
        Route("/health", health_check),
        Route("/sse", handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ]
)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    logger.info(f"Starting Orca MCP SSE Server on port {port}")
    logger.info(f"Claude Desktop URL: http://localhost:{port}/sse")
    uvicorn.run(app, host="0.0.0.0", port=port)
