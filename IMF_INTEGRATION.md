# IMF & World Bank Data Integration

**Status:** ✅ Integrated into Orca MCP (Gateway)

## Overview

Orca MCP now acts as a gateway to IMF and World Bank macroeconomic data through the external IMF MCP and World Bank MCP services.

## New Tools Added (8)

| Tool | Purpose | Example |
|------|---------|---------|
| `get_government_debt_to_gdp` | Fetch debt-to-GDP ratio | `get_government_debt_to_gdp("Japan")` |
| `get_g7_debt_to_gdp` | Get debt for ALL G7 countries | `get_g7_debt_to_gdp()` |
| `get_gdp_growth` | GDP growth rate | `get_gdp_growth("United States")` |
| `get_inflation_rate` | Inflation rate | `get_inflation_rate("Germany")` |
| `get_unemployment_rate` | Unemployment rate | `get_unemployment_rate("France")` |
| `get_fiscal_deficit` | Fiscal deficit as % GDP | `get_fiscal_deficit("Italy")` |
| `get_current_account_balance` | Current account balance | `get_current_account_balance("Canada")` |
| `get_multiple_indicators` | Fetch multiple indicators at once | `get_multiple_indicators("Brazil", ["debt", "gdp", "inflation"])` |

## Architecture

```
Orca MCP (Gateway)
    ↓
    ├─→ country-mapping-mcp (country name → ISO code)
    │
    └─→ imf-mcp (IMF DataMapper API)
         └─→ IMF DataMapper (actual data)
```

**Flow:**
1. User calls `orca.get_government_debt_to_gdp("Japan")`
2. Orca converts "Japan" → "JPN" via country-mapping-mcp
3. Orca calls imf-mcp with "JPN"
4. IMF MCP fetches data from IMF DataMapper
5. IMF MCP adds AI analysis (Claude Haiku)
6. Orca returns comprehensive result

## Usage

### Via MCP (Claude Desktop)

```
User: "Get the latest government debt-to-GDP ratio for Japan"
Claude: *calls orca.get_government_debt_to_gdp("Japan")*

User: "Show me G7 debt comparison"
Claude: *calls orca.get_g7_debt_to_gdp()*
```

### Via Python

```python
from orca_mcp.tools.macroeconomic_data import get_government_debt_to_gdp, get_g7_debt_to_gdp

# Single country
japan_debt = get_government_debt_to_gdp("Japan")
print(f"Japan debt-to-GDP: {japan_debt['latest_value']}%")

# All G7 countries
g7_data = get_g7_debt_to_gdp()
print(f"Highest: {g7_data['summary']['highest']}")
print(f"Lowest: {g7_data['summary']['lowest']}")
```

### Multiple Indicators

```python
from orca_mcp.tools.macroeconomic_data import get_multiple_indicators

# Get comprehensive economic overview
data = get_multiple_indicators(
    "Brazil",
    ["debt", "gdp", "inflation", "unemployment"]
)

print(data['indicators']['debt']['latest_value'])
print(data['indicators']['gdp']['latest_value'])
```

## Authentication

The IMF MCP requires authentication via bearer token.

**Setup:**

```bash
# Set the auth token
export MCP_AUTH_TOKEN="your-token-here"

# Or add to .env
echo "MCP_AUTH_TOKEN=your-token-here" >> .env
```

**For Claude Desktop:**

```json
{
  "mcpServers": {
    "orca": {
      "command": "python",
      "args": ["-m", "orca_mcp.server"],
      "env": {
        "CLIENT_ID": "guinness",
        "MCP_AUTH_TOKEN": "your-token-here"
      }
    }
  }
}
```

## Service Registry

The following services have been added to `service_registry.json`:

### imf_mcp
- **Endpoint:** `https://imf-mcp.urbancanary.workers.dev`
- **Data Source:** IMF DataMapper
- **Coverage:** 150+ countries
- **Format:** ISO 3-letter codes (USA, JPN, DEU, etc.)

### worldbank_mcp
- **Endpoint:** `https://worldbank-mcp.urbancanary.workers.dev`
- **Data Source:** World Bank Open Data API
- **Coverage:** 200+ countries

### country-mapping-mcp
- **Endpoint:** `https://country-mapping-mcp.urbancanary.workers.dev`
- **Purpose:** Convert country names to ISO/IMF/World Bank codes
- **Coverage:** 376 countries, 1,102 input variants

## Data Flow Example

```
User Request: "What's the debt-to-GDP for Japan?"
    ↓
1. Orca receives: get_government_debt_to_gdp("Japan")
    ↓
2. Country mapping: "Japan" → "JPN"
   GET https://country-mapping-mcp.urbancanary.workers.dev/map/Japan?api=iso
   Response: {"iso_code_3": "JPN"}
    ↓
3. IMF data fetch: "JPN" → debt data
   POST https://imf-mcp.urbancanary.workers.dev/mcp/tools/call
   Body: {"name": "imf_debt", "arguments": {"country": "JPN"}}
    ↓
4. IMF MCP calls IMF DataMapper API
   GET https://www.imf.org/external/datamapper/api/...
    ↓
5. IMF MCP adds AI analysis (Claude Haiku)
    ↓
6. Orca returns comprehensive result:
   {
     "country": "Japan",
     "country_code": "JPN",
     "indicator": "Government Debt to GDP",
     "latest_value": 230.5,
     "latest_year": 2025,
     "trend": "declining",
     "data": [...historical data...],
     "analysis": "Japan has the highest debt-to-GDP ratio among G7 countries...",
     "source": "IMF DataMapper"
   }
```

## Country Name Support

The system supports flexible country names:

**Works:**
- ✅ "United States" → USA
- ✅ "USA" → USA
- ✅ "US" → USA
- ✅ "Japan" → JPN
- ✅ "Germany" → DEU
- ✅ "Brasil" → BRA (Portuguese spelling)
- ✅ "Brasil" → BRA (Spanish spelling)

The country-mapping-mcp handles 1,102+ country name variants.

## G7 Countries Mapping

| Country | ISO Code |
|---------|----------|
| United States | USA |
| Japan | JPN |
| Germany | DEU |
| United Kingdom | GBR |
| France | FRA |
| Italy | ITA |
| Canada | CAN |

## Error Handling

### Country Not Found
```python
result = get_government_debt_to_gdp("Atlantis")
# Returns:
{
  "error": "Could not map country 'Atlantis' to ISO code",
  "suggestion": "Try using ISO 3-letter code (e.g., 'USA', 'DEU', 'JPN')"
}
```

### IMF Data Unavailable
```python
result = get_government_debt_to_gdp("XXX")
# Returns:
{
  "error": "IMF MCP returned status 404",
  "details": "Country code 'XXX' not found in IMF database"
}
```

### Authentication Required
```python
# If MCP_AUTH_TOKEN not set
result = get_government_debt_to_gdp("JPN")
# Returns:
{
  "error": "IMF MCP returned status 403",
  "note": "Set MCP_AUTH_TOKEN environment variable"
}
```

## Testing

### Unit Test

```python
from orca_mcp.tools.macroeconomic_data import get_government_debt_to_gdp

def test_imf_integration():
    result = get_government_debt_to_gdp("JPN")

    assert "country_code" in result
    assert result["country_code"] == "JPN"
    assert "source" in result
    assert result["source"] == "IMF DataMapper"
```

### Integration Test

```bash
# Test via Orca MCP server
python orca_mcp/server.py &

# Call the tool
curl -X POST http://localhost:8000/mcp/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name":"get_government_debt_to_gdp","arguments":{"country":"Japan"}}'
```

## Files Modified/Created

**New:**
- `orca_mcp/tools/macroeconomic_data.py` - IMF/World Bank gateway tools

**Modified:**
- `orca_mcp/server.py` - Added 8 IMF tools
- `orca_mcp/service_registry.json` - Added imf_mcp, worldbank_mcp, country-mapping-mcp

## Total Orca Tools

**Before:** 24 tools
**After:** 32 tools (+8 macroeconomic data tools)

## Benefits

1. **Direct access** - No need for web search fallback
2. **Structured data** - JSON format, easy to process
3. **AI analysis** - Claude Haiku provides insights
4. **Historical data** - Time series, not just latest
5. **Country flexibility** - Accepts names or ISO codes
6. **Batch support** - Get G7 data in one call
7. **Multi-indicator** - Fetch multiple metrics at once

## Comparison: Before vs After

### Before (Claude Desktop without IMF integration)

```
User: "Get G7 debt-to-GDP data"
Claude: *Orca doesn't have this* → WebSearch fallback
Result: Inconsistent, requires parsing, no structure
```

### After (With IMF integration)

```
User: "Get G7 debt-to-GDP data"
Claude: *calls orca.get_g7_debt_to_gdp()*
Result: Structured JSON with historical data + AI analysis
```

## Future Enhancements

1. **Cache IMF data** - Add Redis caching (5-minute TTL)
2. **World Bank tools** - Add World Bank indicators
3. **Historical queries** - Support date ranges
4. **Batch countries** - Fetch multiple countries at once
5. **Compare countries** - Built-in comparison tool

## Summary

✅ **Orca can now fetch IMF data!**

- 8 new tools for macroeconomic data
- Gateway to IMF MCP and World Bank MCP
- Automatic country name mapping
- Supports 150+ countries
- AI-powered analysis included
- Structured JSON responses

**Your original question:** "Can I fetch G7 debt data via Orca?"
**Answer:** YES! Use `orca.get_g7_debt_to_gdp()` 🎉
