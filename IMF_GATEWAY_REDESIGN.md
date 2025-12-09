# IMF Gateway Redesign - Generic Access to ALL IMF Data

**Problem:** Original implementation was too narrow (8 specific tools)
**Solution:** Generic gateway to fetch ANY IMF indicator for ANY country/countries

---

## What Changed

### ❌ BEFORE: Narrow, Specific Tools (WRONG)

```python
get_government_debt_to_gdp("Japan")      # Just debt
get_g7_debt_to_gdp()                     # Just G7 debt
get_gdp_growth("USA")                    # Just GDP growth
get_inflation_rate("Germany")            # Just inflation
# ... 8 separate tools
```

**Problems:**
- Limited to predefined use cases
- Can't combine indicators
- Can't fetch custom country groups
- Need new tool for each indicator

### ✅ AFTER: Generic Gateway (RIGHT)

```python
# Single generic tool for EVERYTHING
fetch_imf_data("debt", "G7")
fetch_imf_data("inflation", ["USA", "CHN", "JPN"])
fetch_imf_data("NGDP_RPCH", "BRICS", start_year=2020)
fetch_imf_data("unemployment", "Germany")
```

**Benefits:**
- Fetch ANY IMF indicator
- For ANY country or country group
- Flexible parameters (year ranges, analysis mode)
- Extensible to new indicators automatically

---

## New Architecture

### 3 Generic Tools (Down from 8 Narrow Tools)

| Tool | Purpose | Examples |
|------|---------|----------|
| `fetch_imf_data` | Fetch ANY indicator for ANY countries | `fetch_imf_data("debt", "G7")` |
| `get_available_indicators` | List all indicators | Returns all 7 IMF indicators with codes |
| `get_available_country_groups` | List all country groups | Returns G7, G20, BRICS, EU, ASEAN |

---

## fetch_imf_data - The Universal Gateway

### Signature

```python
fetch_imf_data(
    indicator: str,              # "debt" or "GGXWDG_NGDP"
    countries: str | List[str],  # "G7", ["USA", "JPN"], or "Japan"
    start_year: int = 2010,      # Optional
    end_year: int = 2030,        # Optional (includes projections)
    use_mcp: bool = False        # True = AI analysis, False = fast
)
```

### Supported Indicators

| User-Friendly Name | IMF Code | Unit |
|--------------------|----------|------|
| `gdp_growth` | NGDP_RPCH | Annual % change |
| `government_debt` | GGXWDG_NGDP | % of GDP |
| `inflation` | PCPIPCH | Annual % change |
| `unemployment` | LUR | Percent |
| `fiscal_deficit` | GGXCNL_NGDP | % of GDP |
| `current_account` | BCA | Billions USD |
| `gdp_per_capita` | NGDPPC | USD per capita |

**Both work:**
- `fetch_imf_data("debt", "Japan")` ✅
- `fetch_imf_data("GGXWDG_NGDP", "Japan")` ✅

### Supported Country Groups

| Group | Members | Count |
|-------|---------|-------|
| `G7` | USA, JPN, DEU, GBR, FRA, ITA, CAN | 7 |
| `G20` | G7 + CHN, BRA, IND, RUS, AUS, KOR, MEX, IDN, TUR, SAU, ARG, ZAF | 19 |
| `BRICS` | BRA, RUS, IND, CHN, ZAF | 5 |
| `EU` | DEU, FRA, ITA, ESP, NLD, BEL, AUT, PRT, GRC, FIN | 10 |
| `ASEAN` | IDN, THA, MYS, SGP, PHL, VNM, MMR, KHM, LAO, BRN | 10 |

---

## Usage Examples

### 1. G7 Debt Comparison (Your Original Request)

```python
# Before (narrow tool)
get_g7_debt_to_gdp()

# After (generic gateway)
fetch_imf_data("debt", "G7")

# Returns:
{
  "indicator": "GGXWDG_NGDP",
  "indicator_name": "Government Debt to GDP",
  "unit": "Percent of GDP",
  "countries": {
    "USA": {"latest_value": 125.0, "latest_year": "2025", "time_series": {...}},
    "JPN": {"latest_value": 230.5, "latest_year": "2025", "time_series": {...}},
    "DEU": {"latest_value": 65.0, "latest_year": "2025", "time_series": {...}},
    ...
  },
  "source": "IMF DataMapper API",
  "method": "direct"
}
```

### 2. Compare China vs USA Inflation

```python
fetch_imf_data("inflation", ["CHN", "USA"], start_year=2020)

# Returns inflation data for both countries since 2020
```

### 3. BRICS GDP Growth Trends

```python
fetch_imf_data("gdp_growth", "BRICS")

# Expands to: BRA, RUS, IND, CHN, ZAF
# Returns GDP growth for all 5 countries
```

### 4. Custom Country List

```python
fetch_imf_data("unemployment", ["Germany", "France", "Italy"])

# Accepts country names, converts to ISO codes automatically
```

### 5. Use AI Analysis (via IMF MCP)

```python
fetch_imf_data("debt", "Japan", use_mcp=True)

# Slower, but includes Claude Haiku analysis of the data
```

### 6. Historical Data with Year Range

```python
fetch_imf_data("fiscal_deficit", "USA", start_year=2015, end_year=2023)

# Returns only data for 2015-2023 period
```

---

## Two Fetch Methods

### Method 1: Direct API (Default, Fast)

```python
fetch_imf_data("debt", "G7", use_mcp=False)
```

- Fetches directly from IMF DataMapper API
- Fast (< 1 second)
- Structured JSON data
- Time series included
- No AI analysis

### Method 2: Via IMF MCP (Optional, Slower)

```python
fetch_imf_data("debt", "Japan", use_mcp=True)
```

- Routes through IMF MCP (same endpoint as @isla uses)
- Slower (2-5 seconds)
- Includes Claude Haiku AI analysis
- More context and insights
- Same data, plus interpretation

---

## Integration with @isla

**Coordination with existing IMF agent:**

| Feature | @isla (Conversational Agent) | Orca fetch_imf_data (Tool) |
|---------|------------------------------|----------------------------|
| Purpose | Interactive analysis | Programmatic data fetch |
| Interface | Natural language | Structured parameters |
| Analysis | Full Claude analysis | Optional (use_mcp=true) |
| Response | Conversational | Structured JSON |
| Use Case | User queries | App integration, automation |

**@isla uses the same IMF MCP endpoint:**
- Endpoint: `https://imf-mcp.urbancanary.workers.dev`
- Indicators: Same 7 core indicators
- Country codes: Same ISO 3-letter format

**Complementary, not competing:**
- @isla for conversational exploration
- Orca for structured data access

---

## Discovery Tools

### List Available Indicators

```python
get_available_indicators()

# Returns:
{
  "indicators": [
    {
      "code": "GGXWDG_NGDP",
      "name": "Government Debt to GDP",
      "unit": "Percent of GDP",
      "aliases": ["debt", "government_debt"]
    },
    ...
  ],
  "total": 7
}
```

### List Country Groups

```python
get_available_country_groups()

# Returns:
{
  "groups": {
    "G7": {"members": ["USA", "JPN", ...], "count": 7},
    "G20": {"members": [...], "count": 19},
    ...
  },
  "total": 5
}
```

---

## Claude Desktop Usage

After restarting Claude Desktop:

```
User: "Get G7 government debt-to-GDP data"

Claude: *sees fetch_imf_data tool*
        *calls: fetch_imf_data("government_debt", "G7")*
        *returns structured data for all 7 countries*

User: "Compare inflation in BRICS countries since 2020"

Claude: *calls: fetch_imf_data("inflation", "BRICS", start_year=2020)*
        *returns inflation trends for Brazil, Russia, India, China, South Africa*
```

**Key difference from before:**
- ❌ Before: "I don't see a tool for that, let me use web search"
- ✅ After: "I'll use fetch_imf_data with those parameters"

---

## Error Handling

### Unknown Indicator

```python
fetch_imf_data("xyz", "USA")

# Returns:
{
  "error": "Unknown indicator: xyz",
  "available_indicators": ["gdp_growth", "government_debt", ...]
}
```

### Country Not Found

```python
fetch_imf_data("debt", "Atlantis")

# Returns:
{
  "error": "Could not map any countries to ISO codes",
  "input_countries": "Atlantis"
}
```

### IMF API Error

```python
fetch_imf_data("debt", "XXX")

# Returns:
{
  "error": "IMF DataMapper API returned status 404",
  "indicator": "GGXWDG_NGDP",
  "countries": ["XXX"]
}
```

---

## File Changes

### New File

- `orca_mcp/tools/imf_gateway.py` - Generic IMF gateway (400+ lines)

### Modified

- `orca_mcp/server.py` - Replaced 8 narrow tools with 3 generic tools
- `orca_mcp/service_registry.json` - Already had IMF MCP registered

### Deleted

- `orca_mcp/tools/macroeconomic_data.py` - Narrow tools (replaced)

---

## Tool Count

**Before:** 32 tools (24 portfolio + 8 narrow IMF)
**After:** 27 tools (24 portfolio + 3 generic IMF)

**Better because:**
- ✅ Fewer tools to maintain
- ✅ More flexible capabilities
- ✅ Easier to discover (get_available_indicators)
- ✅ Extensible to new indicators automatically

---

## Testing

### Test All Indicators

```python
from orca_mcp.tools.imf_gateway import fetch_imf_data

indicators = ["debt", "gdp_growth", "inflation", "unemployment"]

for indicator in indicators:
    result = fetch_imf_data(indicator, "USA")
    print(f"{indicator}: {result['countries']['USA']['latest_value']}")
```

### Test All Country Groups

```python
groups = ["G7", "G20", "BRICS", "EU", "ASEAN"]

for group in groups:
    result = fetch_imf_data("debt", group)
    print(f"{group}: {len(result['countries'])} countries")
```

---

## Benefits of Generic Approach

### 1. Future-Proof

New IMF indicator? Just add to mappings, all tools automatically support it.

```python
# Add to imf_gateway.py
INDICATOR_MAPPINGS["poverty_rate"] = "POVERTY"

# Immediately works:
fetch_imf_data("poverty_rate", "G20")  # ✅
```

### 2. Composable

```python
# Get multiple indicators for same country
debt = fetch_imf_data("debt", "Japan")
gdp = fetch_imf_data("gdp_growth", "Japan")
inflation = fetch_imf_data("inflation", "Japan")

# Or use loops
indicators = ["debt", "gdp_growth", "inflation"]
data = {ind: fetch_imf_data(ind, "Japan") for ind in indicators}
```

### 3. Flexible

```python
# Year ranges
fetch_imf_data("debt", "USA", start_year=2000, end_year=2010)

# Custom country lists
fetch_imf_data("debt", ["Brazil", "Mexico", "Argentina"])

# Mix groups and countries
fetch_imf_data("debt", ["G7", "CHN", "IND"])  # G7 + China + India
```

### 4. Discoverable

```python
# Don't remember indicator names?
get_available_indicators()

# Don't remember group members?
get_available_country_groups()
```

---

## Summary

✅ **Redesigned IMF integration to be a generic gateway**

**From:** 8 narrow, specific tools
**To:** 1 universal tool + 2 discovery tools

**Capabilities:**
- ✅ Fetch ANY IMF indicator
- ✅ For ANY country or country group (G7, G20, BRICS, EU, ASEAN)
- ✅ With flexible parameters (year ranges, analysis mode)
- ✅ Compatible with @isla's IMF MCP endpoint
- ✅ Discoverable (list indicators and groups)

**Now you can:**
- Get G7 debt: `fetch_imf_data("debt", "G7")`
- Compare inflation: `fetch_imf_data("inflation", ["USA", "CHN"])`
- BRICS GDP: `fetch_imf_data("gdp_growth", "BRICS")`
- Historical data: `fetch_imf_data("debt", "Japan", start_year=2015)`

**Restart Claude Desktop to see the new generic tool!** 🚀
