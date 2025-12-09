# Orca MCP Client Examples

## Overview

This guide shows how to retrieve transaction data by connecting to the **Orca MCP server** (the proper way).

**DO NOT** access BigQuery directly - always go through Orca MCP.

---

## Method 1: Claude Desktop (Easiest)

If you have Orca MCP configured in Claude Desktop, simply ask Claude in natural language:

### Examples

```
"Get all transactions for the wnbf portfolio"

"Show me BUY transactions from November 2024"

"Get transactions for wnbf on 2024-11-15"

"Show the last 10 transactions"
```

Claude will automatically call the `get_client_transactions` MCP tool with the appropriate parameters.

---

## Method 2: MCP Python Client

Use the MCP Python SDK to connect to Orca MCP server:

### Setup

```bash
pip install mcp
```

### Python Client Example

```python
#!/usr/bin/env python3
"""
Connect to Orca MCP and retrieve transactions
"""

import asyncio
import json
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def get_transactions():
    """Connect to Orca MCP and get transactions"""

    # Path to Orca MCP server
    server_params = StdioServerParameters(
        command="python",
        args=["-m", "orca_mcp.server"],
        env={"CLIENT_ID": "guinness"}
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:

            # Initialize the connection
            await session.initialize()

            # Example 1: Get all transactions
            print("\n=== Example 1: All Transactions ===")
            result = await session.call_tool(
                "get_client_transactions",
                arguments={
                    "portfolio_id": "wnbf",
                    "limit": -1
                }
            )

            data = json.loads(result.content[0].text)
            print(f"Retrieved {len(data)} transactions")
            print(json.dumps(data[:2], indent=2))  # Show first 2

            # Example 2: Specific day
            print("\n=== Example 2: Specific Day ===")
            result = await session.call_tool(
                "get_client_transactions",
                arguments={
                    "portfolio_id": "wnbf",
                    "transaction_date": "2024-11-15"
                }
            )

            data = json.loads(result.content[0].text)
            print(f"Retrieved {len(data)} transactions on 2024-11-15")

            # Example 3: BUY transactions
            print("\n=== Example 3: BUY Transactions ===")
            result = await session.call_tool(
                "get_client_transactions",
                arguments={
                    "portfolio_id": "wnbf",
                    "transaction_type": "BUY",
                    "limit": -1
                }
            )

            data = json.loads(result.content[0].text)
            print(f"Retrieved {len(data)} BUY transactions")

            # Example 4: Date range
            print("\n=== Example 4: Date Range ===")
            result = await session.call_tool(
                "get_client_transactions",
                arguments={
                    "portfolio_id": "wnbf",
                    "start_date": "2024-11-01",
                    "end_date": "2024-11-30"
                }
            )

            data = json.loads(result.content[0].text)
            print(f"Retrieved {len(data)} transactions in November")

            # Example 5: Last 10 transactions
            print("\n=== Example 5: Last 10 Transactions ===")
            result = await session.call_tool(
                "get_client_transactions",
                arguments={
                    "portfolio_id": "wnbf",
                    "limit": 10
                }
            )

            data = json.loads(result.content[0].text)
            print(f"Retrieved {len(data)} recent transactions")
            for txn in data:
                print(f"  {txn['transaction_date']} | {txn['transaction_type']:7} | {txn['ticker']:6} | ${txn['market_value']:,.0f}")


if __name__ == "__main__":
    asyncio.run(get_transactions())
```

### Save and Run

```bash
# Save as mcp_client_example.py
python mcp_client_example.py
```

---

## Method 3: MCP via Claude Desktop Configuration

### Configure Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "orca-mcp": {
      "command": "python",
      "args": ["-m", "orca_mcp.server"],
      "env": {
        "CLIENT_ID": "guinness"
      }
    }
  }
}
```

### Then Ask Claude

Once configured, simply ask Claude:

- "Get all transactions for wnbf"
- "Show BUY transactions from the last 30 days"
- "Get transactions for November 15, 2024"

---

## Available Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `portfolio_id` | string | **Yes** | Portfolio ID | `"wnbf"` |
| `client_id` | string | No | Client ID (defaults to env) | `"guinness"` |
| `transaction_date` | string | No | Specific date (YYYY-MM-DD) | `"2024-11-15"` |
| `start_date` | string | No | Range start date | `"2024-11-01"` |
| `end_date` | string | No | Range end date | `"2024-11-30"` |
| `transaction_type` | string | No | Type filter (BUY, SELL, INITIAL) | `"BUY"` |
| `limit` | integer | No | Max results (default 100, -1 = all) | `10` or `-1` |

---

## Common Use Cases

### 1. All Transactions

```python
result = await session.call_tool(
    "get_client_transactions",
    arguments={
        "portfolio_id": "wnbf",
        "limit": -1
    }
)
```

### 2. Transactions for Specific Day

```python
result = await session.call_tool(
    "get_client_transactions",
    arguments={
        "portfolio_id": "wnbf",
        "transaction_date": "2024-11-15"
    }
)
```

### 3. All BUY Transactions

```python
result = await session.call_tool(
    "get_client_transactions",
    arguments={
        "portfolio_id": "wnbf",
        "transaction_type": "BUY",
        "limit": -1
    }
)
```

### 4. Transactions in Date Range

```python
result = await session.call_tool(
    "get_client_transactions",
    arguments={
        "portfolio_id": "wnbf",
        "start_date": "2024-11-01",
        "end_date": "2024-11-30"
    }
)
```

### 5. Last 10 Transactions

```python
result = await session.call_tool(
    "get_client_transactions",
    arguments={
        "portfolio_id": "wnbf",
        "limit": 10
    }
)
```

---

## Response Format

All requests return JSON array of transaction objects:

```json
[
  {
    "transaction_id": 123,
    "portfolio_id": "wnbf",
    "transaction_date": "2024-11-15",
    "settlement_date": "2024-11-17",
    "transaction_type": "BUY",
    "isin": "XS2546781985",
    "ticker": "ADGB",
    "description": "Abu Dhabi 4.125% 2027",
    "country": "United Arab Emirates",
    "par_amount": 1000000.0,
    "price": 102.5,
    "accrued_interest": 1.234,
    "dirty_price": 103.734,
    "market_value": 1037340.0,
    "ytm": 3.45,
    "duration": 2.5,
    "spread": 125.5,
    "current_price": 102.8,
    "current_market_value": 1028000.0,
    "unrealized_pnl": -9340.0,
    "unrealized_pnl_pct": -0.9
  }
]
```

---

## Why Use Orca MCP?

✅ **Abstraction** - Don't worry about BigQuery credentials or SQL
✅ **Security** - Credentials managed centrally
✅ **Validation** - Input validation and error handling
✅ **Consistency** - Same interface across all clients
✅ **Flexibility** - Easy filtering without writing SQL

❌ **DO NOT** access BigQuery directly
❌ **DO NOT** bypass Orca MCP
❌ **DO NOT** hardcode credentials

---

## Testing Your Connection

Test that you can connect to Orca MCP:

```python
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def test_connection():
    server_params = StdioServerParameters(
        command="python",
        args=["-m", "orca_mcp.server"],
        env={"CLIENT_ID": "guinness"}
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # List available tools
            tools = await session.list_tools()
            print("Available Orca MCP tools:")
            for tool in tools:
                print(f"  - {tool.name}: {tool.description}")

asyncio.run(test_connection())
```

---

## For Tech Page

Add this section to the Tech page showing colleagues how to properly connect via MCP instead of direct database access.
