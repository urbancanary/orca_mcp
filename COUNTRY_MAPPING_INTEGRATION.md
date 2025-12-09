# Orca MCP - Country Mapping Integration

**Date:** 2025-01-16
**Status:** ✅ COMPLETE

---

## Overview

Orca MCP now integrates with the **country-mapping-mcp** service for universal country name standardization. This provides a single source of truth for country names across all services (IMF, NFA, World Bank, etc.).

---

## Architecture

### Two Complementary Services

**1. rvm-country-mcp** (ISIN → Country)
- **Purpose:** Bond-specific lookups
- **Input:** ISIN codes (e.g., "XS2546781985")
- **Output:** Country names (e.g., "UAE")
- **Use case:** Enriching bond data with country information

**2. country-mapping-mcp** (Universal Country Standardization)
- **Purpose:** General country name standardization
- **Input:** Any country variant (e.g., "USA", "United States", "Brasil")
- **Output:** Standard name + all API codes (IMF, NFA, World Bank, etc.)
- **Use case:** Standardizing country inputs before calling external APIs

---

## Updated Files

### 1. service_registry.json

Added comprehensive entry for `country_mapping_mcp`:

```json
{
  "country_mapping_mcp": {
    "nickname": "country_standardization",
    "full_name": "Country Name Standardization Service",
    "type": "api_service",
    "endpoint": "https://country-mapping-mcp.urbancanary.workers.dev",
    "capabilities": [
      "standardize_country_names",
      "get_api_specific_code",
      "reverse_lookup",
      "batch_mapping"
    ],
    "coverage": {
      "total_countries": 376,
      "input_variants": 1102,
      "imf_codes": 199,
      "nfa_names": 198
    }
  }
}
```

### 2. tools/country_standardization.py

Added 5 new functions for universal country standardization:

**Core Functions:**
- `standardize_country_name(country_input, api=None)` - Main standardization function
- `get_api_country_code(country_input, api)` - Get API-specific code
- `batch_standardize_countries(country_inputs, api=None)` - Batch processing
- `reverse_lookup_country(iso_code)` - ISO code → country info
- `standardize_country_list(items, country_field='country')` - Convenience wrapper

**Existing Functions (unchanged):**
- `standardize_countries_from_isins(isins)` - ISIN → country (uses rvm-country-mcp)
- `get_country_for_isin(isin)` - Single ISIN lookup
- `enrich_bonds_with_countries(bonds)` - Bond enrichment
- `get_available_countries()` - List countries from rvm-country-mcp

---

## Usage Examples

### 1. Standardize a Country Name

```python
from tools.country_standardization import standardize_country_name

# Get full mapping
result = standardize_country_name("United States")
# {
#   "found": True,
#   "input": "United States",
#   "standard": "US",
#   "imf_code": "USA",
#   "nfa_name": "US",
#   "worldbank_code": "USA",
#   "iso_code": "USA",
#   "ifs_code": 111,
#   "confidence": "exact"
# }

# Get API-specific code
result = standardize_country_name("Brazil", api="imf")
# {
#   "input": "Brazil",
#   "standard": "Brazil",
#   "api": "imf",
#   "code": "BRA"
# }
```

### 2. Get API-Specific Code

```python
from tools.country_standardization import get_api_country_code

# Get IMF code
imf_code = get_api_country_code("United States", "imf")
# → "USA"

# Get NFA name
nfa_name = get_api_country_code("Brazil", "nfa")
# → "Brazil"

# Get World Bank code
wb_code = get_api_country_code("UK", "worldbank")
# → "GBR"
```

### 3. Batch Standardization

```python
from tools.country_standardization import batch_standardize_countries

countries = ["USA", "Brazil", "UK", "China"]
results = batch_standardize_countries(countries)
# [
#   {"input": "USA", "standard": "US", "imf_code": "USA", ...},
#   {"input": "Brazil", "standard": "Brazil", "imf_code": "BRA", ...},
#   {"input": "UK", "standard": "UK", "imf_code": "GBR", ...},
#   {"input": "China", "standard": "China", "imf_code": "CHN", ...}
# ]

# With API-specific codes
results = batch_standardize_countries(countries, api="imf")
```

### 4. Reverse Lookup (ISO → Country)

```python
from tools.country_standardization import reverse_lookup_country

result = reverse_lookup_country("BRA")
# {
#   "found": True,
#   "iso_code": "BRA",
#   "standard": "Brazil",
#   "imf_code": "BRA",
#   "worldbank_code": "BRA",
#   "nfa_name": "Brazil"
# }
```

### 5. Standardize Data Lists

```python
from tools.country_standardization import standardize_country_list

# Standardize country names in portfolio data
portfolio_data = [
    {"country": "USA", "value": 100},
    {"country": "United Kingdom", "value": 200}
]

standardized = standardize_country_list(portfolio_data)
# [
#   {"country": "US", "value": 100},
#   {"country": "UK", "value": 200}
# ]

# Works with custom field names
bonds = [
    {"issuer_country": "Brasil", "amount": 1000}
]
standardized = standardize_country_list(bonds, country_field="issuer_country")
```

---

## Integration Patterns

### Pattern 1: Before Calling External APIs

```python
from tools.country_standardization import get_api_country_code

def get_imf_data(country_input, indicator):
    # Standardize country first
    imf_code = get_api_country_code(country_input, "imf")

    if not imf_code:
        raise ValueError(f"Country '{country_input}' not found")

    # Call IMF API with correct code
    return imf_api.fetch(imf_code, indicator)
```

### Pattern 2: Enriching Portfolio Data

```python
from tools.country_standardization import standardize_country_list

def analyze_portfolio(portfolio):
    # Standardize all country names
    portfolio = standardize_country_list(portfolio, country_field="country")

    # Now all country names are consistent for analysis
    return calculate_country_exposure(portfolio)
```

### Pattern 3: Multi-API Workflow

```python
from tools.country_standardization import standardize_country_name

def get_sovereign_data(country_input):
    # Get all API codes at once
    mapping = standardize_country_name(country_input)

    # Call multiple APIs with correct codes
    imf_data = imf_api.fetch(mapping["imf_code"], "NGDP_RPCH")
    nfa_data = nfa_api.fetch(mapping["nfa_name"])
    wb_data = worldbank_api.fetch(mapping["worldbank_code"], "NY.GDP.MKTP.CD")

    return {
        "imf": imf_data,
        "nfa": nfa_data,
        "worldbank": wb_data
    }
```

---

## Testing

### Run Integration Tests

```bash
cd /Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp
python3 test_country_integration.py
```

### Test Results (2025-01-16)

✅ All tests passed:
- Basic country name standardization
- API-specific code retrieval
- Batch standardization
- Reverse lookup
- List standardization
- Country name variants

---

## API Coverage

The country-mapping-mcp service provides:

- **376 unique countries**
- **1,102 input variants**
- **199 IMF codes** (ISO 3166-1 alpha-3)
- **242 World Bank codes** (ISO 3166-1 alpha-3)
- **198 NFA names** (90.8% coverage from EWN database)
- **246 ISO codes**
- **218 IFS codes** (External Wealth of Nations database)

---

## Error Handling

All functions include graceful error handling:

```python
result = standardize_country_name("InvalidCountry")
# {
#   "found": False,
#   "input": "InvalidCountry",
#   "error": "..."
# }

code = get_api_country_code("InvalidCountry", "imf")
# → None
```

---

## Defense in Depth Architecture

The country standardization system uses a three-tier defense:

1. **Country Mapping MCP** (Primary) - Shared service for all standardization
2. **Orca MCP** (Convenience) - Python wrapper functions
3. **Individual MCPs** (Fallback) - Local mappings in IMF/NFA MCPs (future)

---

## Next Steps

### Immediate
- ✅ Service deployed to Cloudflare
- ✅ Orca MCP integrated
- ✅ Tests passing

### Future Enhancements
- [ ] Add fallback mappings to IMF MCP
- [ ] Add fallback mappings to NFA MCP
- [ ] Create NPM package: `@sovereign-credit/country-mapping`
- [ ] Add fuzzy matching for typos
- [ ] Extend variant coverage (Portuguese, local names)

---

## Documentation

- **Deployment Summary:** `/Users/andyseaman/Notebooks/mcp_central/country-mapping-mcp/DEPLOYMENT_SUMMARY.md`
- **Implementation Guide:** `/Users/andyseaman/Notebooks/sovereign-credit-system/IMPLEMENTATION_COMPLETE.md`
- **Service Registry:** `/Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/service_registry.json`
- **Test Script:** `/Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/test_country_integration.py`

---

## Support

For issues or questions:
- Check service health: `curl https://country-mapping-mcp.urbancanary.workers.dev/health`
- View logs: `wrangler tail country-mapping-mcp`
- Test mapping: `curl "https://country-mapping-mcp.urbancanary.workers.dev/map/USA"`

---

**Last Updated:** 2025-01-16
**Integration Status:** Production Ready ✅
