# Orca MCP Transaction API Guide

## How to Get Transactions via MCP

The Orca MCP server provides a `get_client_transactions` tool that allows you to retrieve transaction data with flexible filtering options.

## Tool Name
`get_client_transactions`

## Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `portfolio_id` | string | **Yes** | Portfolio identifier | `"wnbf"` |
| `client_id` | string | No | Client identifier (defaults to env var) | `"guinness"` |
| `transaction_date` | string | No | Get transactions from specific date | `"2024-11-15"` |
| `start_date` | string | No | Get transactions from this date onwards | `"2024-11-01"` |
| `end_date` | string | No | Get transactions up to this date | `"2024-11-30"` |
| `transaction_type` | string | No | Filter by type (BUY, SELL, INITIAL) | `"BUY"` |
| `limit` | integer | No | Maximum number of transactions (default: 100, use -1 for all) | `100` or `-1` |

## Usage Examples

### 1. Get All Transactions (No Limit)
```json
{
  "portfolio_id": "wnbf",
  "limit": -1
}
```

### 2. Get Transactions for a Specific Day
```json
{
  "portfolio_id": "wnbf",
  "transaction_date": "2024-11-15"
}
```

### 3. Get All BUY Transactions
```json
{
  "portfolio_id": "wnbf",
  "transaction_type": "BUY",
  "limit": -1
}
```

### 4. Get Transactions for a Date Range
```json
{
  "portfolio_id": "wnbf",
  "start_date": "2024-11-01",
  "end_date": "2024-11-30",
  "limit": -1
}
```

### 5. Get Last 10 SELL Transactions
```json
{
  "portfolio_id": "wnbf",
  "transaction_type": "SELL",
  "limit": 10
}
```

### 6. Get BUY Transactions Since Specific Date
```json
{
  "portfolio_id": "wnbf",
  "transaction_type": "BUY",
  "start_date": "2024-10-01",
  "limit": -1
}
```

## Response Format

The tool returns JSON array of transaction objects with all columns from the `transactions` table:

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
    "par_amount": 1000000,
    "price": 102.5,
    "accrued_interest": 1.234,
    "dirty_price": 103.734,
    "market_value": 1037340.00,
    "ytm": 3.45,
    "duration": 2.5,
    "spread": 125.5,
    "current_price": 102.8,
    "current_market_value": 1028000.00,
    "unrealized_pnl": -9340.00,
    "unrealized_pnl_pct": -0.90,
    ...
  },
  ...
]
```

## Using in Claude Desktop

If you have Orca MCP configured in Claude Desktop, you can simply ask:

- "Get all transactions for wnbf portfolio"
- "Show me BUY transactions from November 2024"
- "Get transactions for wnbf on 2024-11-15"
- "Show all SELL transactions in the last month"

Claude will automatically call the `get_client_transactions` tool with the appropriate filters.

## Using via Python

```python
from orca_mcp import query_bigquery

# Or use the MCP client directly if you have it set up
# The tool handles the query internally
```

## Common Use Cases

1. **Daily Transaction Report**: Filter by `transaction_date` to get all trades from a specific day
2. **Monthly Analysis**: Use `start_date` and `end_date` to get a month's worth of transactions
3. **Full Portfolio Export**: Set `limit: -1` to get all transactions without pagination
4. **Buy/Sell Analysis**: Filter by `transaction_type` to analyze purchase or sale activity
5. **Recent Activity**: Keep default `limit: 100` to get the most recent 100 transactions

## Notes

- All dates use `YYYY-MM-DD` format
- Transactions are sorted by `transaction_date DESC, settlement_date DESC` (newest first)
- Use `limit: -1` to retrieve all matching transactions without pagination
- The `transaction_date` parameter takes precedence over `start_date`/`end_date` if both are provided
- Default limit is 100 transactions if not specified

## Contact

For questions or issues, contact the Orca team or check the main documentation.
