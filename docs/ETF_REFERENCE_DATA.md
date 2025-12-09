# ETF Reference Data

Country allocation data for common UCITS ETFs, based on MSCI index weights.

## Overview

This module provides country allocation breakdowns for 10 popular ETFs:
- MSCI World trackers (iShares, Xtrackers, HSBC, UBS)
- MSCI ACWI trackers
- SRI/ESG variants
- Thematic ETFs (Clean Energy, Automation & Robotics, Min Volatility)

**Data Source**: MSCI Index factsheets, justETF, iShares (Nov 2025)

**Note**: Country allocations are based on underlying index weights. Actual ETF weights may vary slightly due to tracking differences.

## Available ETFs

| ISIN | Name | Index | TER | Top Country |
|------|------|-------|-----|-------------|
| IE00B6R52259 | iShares MSCI ACWI UCITS ETF | MSCI ACWI | 0.20% | USA 63.5% |
| IE00B0M62Q58 | iShares MSCI World UCITS ETF | MSCI World | 0.50% | USA 71.9% |
| LU0274208692 | Xtrackers MSCI World Swap | MSCI World | 0.45% | USA 71.9% |
| IE00BYZK4552 | iShares Automation & Robotics | iSTOXX Robotics | 0.40% | USA 52.1% |
| IE00B8FHGS14 | iShares Min Volatility | MSCI World MinVol | 0.30% | USA 61.5% |
| IE00B4X9L533 | HSBC MSCI World | MSCI World | 0.15% | USA 71.9% |
| IE00B1XNHC34 | iShares Global Clean Energy | S&P Clean Energy | 0.65% | USA 40.5% |
| IE00BDR55927 | UBS MSCI ACWI SRI | MSCI ACWI SRI | 0.33% | USA 63.5% |
| LU0629459743 | UBS MSCI World SRI | MSCI World SRI | 0.22% | USA 71.9% |
| IE00BYX2JD69 | iShares MSCI World SRI | MSCI World SRI | 0.20% | USA 71.9% |

---

## Access via HTTP (CBonds MCP Worker)

The CBonds MCP Cloudflare Worker exposes ETF allocation endpoints.

**Base URL**: `https://cbonds-mcp.urbancanary.workers.dev`

### List All ETFs

```bash
curl -s "https://cbonds-mcp.urbancanary.workers.dev/cbonds/etf/allocations" | jq .
```

### Get Allocation for Specific ETF

```bash
curl -s -X POST "https://cbonds-mcp.urbancanary.workers.dev/cbonds/etf/allocation" \
  -H "Content-Type: application/json" \
  -d '{"isin":"IE00B0M62Q58"}' | jq .
```

---

## Access via Python

### Direct Import (from orca_mcp directory)

```python
# Run from /Users/andyseaman/Notebooks/mcp_central/orca_mcp/
from tools.etf_reference import (
    get_etf_allocation,
    list_etf_allocations,
    get_etf_country_exposure
)

# Get allocation for specific ETF
result = get_etf_allocation("IE00B0M62Q58")
print(f"{result['name']}")
for alloc in result['allocation'][:5]:
    print(f"  {alloc['country']}: {alloc['weight_pct']}%")

# List all ETFs
etfs = list_etf_allocations()
for etf in etfs['etfs']:
    print(f"{etf['isin']}: {etf['name']} - {etf['top_country']} {etf['top_country_weight']}%")

# Find ETFs with Japan exposure
japan = get_etf_country_exposure("Japan")
for etf in japan['etfs']:
    print(f"{etf['name']}: {etf['country_weight_pct']}% Japan")
```

### Via HTTP Requests

```python
import requests

BASE_URL = "https://cbonds-mcp.urbancanary.workers.dev"

# List all ETFs
response = requests.get(f"{BASE_URL}/cbonds/etf/allocations")
data = response.json()
print(f"Found {data['count']} ETFs")

# Get specific ETF allocation
response = requests.post(
    f"{BASE_URL}/cbonds/etf/allocation",
    json={"isin": "IE00B0M62Q58"}
)
data = response.json()
print(f"\n{data['data']['name']}")
for alloc in data['data']['allocation'][:5]:
    print(f"  {alloc['country']}: {alloc['weight_pct']}%")
```

---

## MCP Tool Usage (Claude Desktop)

When using via Orca MCP in Claude Desktop, the following tools are available:

### get_etf_allocation
Get country allocation for a specific ETF.

```json
{
  "name": "get_etf_allocation",
  "arguments": {
    "isin": "IE00B0M62Q58"
  }
}
```

### list_etf_allocations
List all available ETFs with summary info.

```json
{
  "name": "list_etf_allocations",
  "arguments": {}
}
```

### get_etf_country_exposure
Find ETFs with exposure to a specific country.

```json
{
  "name": "get_etf_country_exposure",
  "arguments": {
    "country": "Japan"
  }
}
```

---

## Response Examples

### get_etf_allocation Response

```json
{
  "success": true,
  "isin": "IE00B0M62Q58",
  "name": "iShares MSCI World UCITS ETF (Dist)",
  "index": "MSCI World",
  "ter_pct": 0.5,
  "aum_eur_m": 7489,
  "allocation": [
    {"country": "United States", "weight_pct": 71.86},
    {"country": "Japan", "weight_pct": 5.43},
    {"country": "United Kingdom", "weight_pct": 3.65},
    {"country": "France", "weight_pct": 2.89},
    {"country": "Canada", "weight_pct": 2.88}
  ],
  "source": "MSCI Index factsheets",
  "note": "Country allocations based on underlying index weights."
}
```

### get_etf_country_exposure Response

```json
{
  "success": true,
  "country": "Japan",
  "count": 10,
  "etfs": [
    {"isin": "IE00BYZK4552", "name": "iShares Automation & Robotics", "country_weight_pct": 19.3},
    {"isin": "IE00B8FHGS14", "name": "iShares Min Volatility", "country_weight_pct": 11.2},
    {"isin": "IE00B0M62Q58", "name": "iShares MSCI World", "country_weight_pct": 5.43}
  ]
}
```
