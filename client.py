"""
Orca MCP Client Helper

Simple interface for calling Orca MCP from Streamlit or other applications.
Hides MCP complexity and provides clean async/sync APIs.
"""

import os
import json
import asyncio
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class OrcaClient:
    """
    Client for calling Orca MCP orchestrator

    Usage:
        # Async context manager
        async with OrcaClient() as orca:
            holdings = await orca.get_holdings("wnbf")

        # Sync wrapper
        orca = OrcaClient()
        holdings = orca.get_holdings_sync("wnbf")
    """

    def __init__(
        self,
        client_id: Optional[str] = None,
        server_path: Optional[str] = None,
        server_url: Optional[str] = None
    ):
        """
        Initialize Orca client

        Args:
            client_id: Client identifier (defaults to env var CLIENT_ID)
            server_path: Path to orca_mcp/server.py (for local stdio)
            server_url: URL for remote Orca service (for HTTP calls)
        """
        self.client_id = client_id or os.getenv("CLIENT_ID", "guinness")
        self.server_path = server_path or self._find_server_path()
        self.server_url = server_url or os.getenv("ORCA_MCP_URL")

        self._session = None
        self._read = None
        self._write = None

    def _find_server_path(self) -> str:
        """Find orca_mcp/server.py relative to this file"""
        from pathlib import Path
        return str(Path(__file__).parent / "server.py")

    @asynccontextmanager
    async def _get_session(self):
        """Get MCP session (stdio or HTTP)"""
        if self.server_url:
            # TODO: Implement HTTP client for remote Orca
            raise NotImplementedError("HTTP client not yet implemented")

        # Use stdio client
        server_params = StdioServerParameters(
            command="python3",
            args=[self.server_path],
            env={"CLIENT_ID": self.client_id}
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        pass

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """
        Call an Orca MCP tool

        Args:
            tool_name: Name of the tool to call
            arguments: Tool arguments

        Returns:
            Parsed JSON response
        """
        async with self._get_session() as session:
            result = await session.call_tool(tool_name, arguments=arguments)

            if result.content:
                text = result.content[0].text
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return text

            return None

    # Convenience methods for common operations

    async def get_client_info(self) -> Dict:
        """Get client configuration info"""
        return await self.call_tool("get_client_info", {})

    async def query_data(self, sql: str) -> List[Dict]:
        """Query client data from BigQuery"""
        return await self.call_tool("query_client_data", {"sql": sql})

    async def get_portfolios(self) -> List[Dict]:
        """Get list of client portfolios"""
        return await self.call_tool("get_client_portfolios", {})

    async def get_transactions(self, portfolio_id: str, limit: int = 100) -> List[Dict]:
        """Get portfolio transactions"""
        return await self.call_tool("get_client_transactions", {
            "portfolio_id": portfolio_id,
            "limit": limit
        })

    async def get_holdings(self, portfolio_id: str) -> List[Dict]:
        """Get current portfolio holdings"""
        return await self.call_tool("get_client_holdings", {
            "portfolio_id": portfolio_id
        })

    async def calculate_rvm(
        self,
        isins: List[str],
        prices: Optional[List[float]] = None,
        durations: Optional[List[float]] = None,
        spreads: Optional[List[float]] = None,
        ytms: Optional[List[float]] = None
    ) -> Dict:
        """Calculate RVM analytics for bonds"""
        args = {"isins": isins}
        if prices:
            args["prices"] = prices
        if durations:
            args["durations"] = durations
        if spreads:
            args["spreads"] = spreads
        if ytms:
            args["ytms"] = ytms

        return await self.call_tool("calculate_rvm_analytics", args)

    # Synchronous wrappers for Streamlit compatibility

    def _run_async(self, coro):
        """Run async coroutine in sync context"""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(coro)

    def get_client_info_sync(self) -> Dict:
        """Sync version of get_client_info"""
        return self._run_async(self.get_client_info())

    def query_data_sync(self, sql: str) -> List[Dict]:
        """Sync version of query_data"""
        return self._run_async(self.query_data(sql))

    def get_portfolios_sync(self) -> List[Dict]:
        """Sync version of get_portfolios"""
        return self._run_async(self.get_portfolios())

    def get_transactions_sync(self, portfolio_id: str, limit: int = 100) -> List[Dict]:
        """Sync version of get_transactions"""
        return self._run_async(self.get_transactions(portfolio_id, limit))

    def get_holdings_sync(self, portfolio_id: str) -> List[Dict]:
        """Sync version of get_holdings"""
        return self._run_async(self.get_holdings(portfolio_id))

    def calculate_rvm_sync(
        self,
        isins: List[str],
        prices: Optional[List[float]] = None,
        durations: Optional[List[float]] = None,
        spreads: Optional[List[float]] = None,
        ytms: Optional[List[float]] = None
    ) -> Dict:
        """Sync version of calculate_rvm"""
        return self._run_async(self.calculate_rvm(isins, prices, durations, spreads, ytms))


# Singleton for Streamlit
_orca_instance = None

def get_orca_client(client_id: Optional[str] = None) -> OrcaClient:
    """
    Get singleton Orca client instance

    Usage in Streamlit:
        from orca_mcp.client import get_orca_client

        orca = get_orca_client()
        holdings = orca.get_holdings_sync("wnbf")
    """
    global _orca_instance

    if _orca_instance is None or (client_id and client_id != _orca_instance.client_id):
        _orca_instance = OrcaClient(client_id=client_id)

    return _orca_instance
