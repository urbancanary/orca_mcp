# Country Eligibility System

**Date:** 2025-01-16
**Status:** ✅ OPERATIONAL

---

## Overview

Iterative, AI-guided country eligibility system for bond portfolio construction. Combines automatic NFA-based rules with manual overrides for specific risk factors.

---

## Rules

### Base Rule: NFA Rating Tiers

```
1-2 star NFA rating → INELIGIBLE (high external debt)
3+ star NFA rating  → ELIGIBLE (acceptable risk)
```

### Manual Overrides

Specific risk factors can override the base NFA rule:

**Override to INELIGIBLE:**
- Twin deficits (fiscal + current account)
- Political instability
- Capital controls
- Debt restructuring risk

**Override to ELIGIBLE:**
- IMF program with strong reforms
- Recent investment grade upgrade
- Strategic portfolio diversification needs

---

## Current Overrides

### Romania
- **NFA Rating:** 3-star (would be eligible)
- **Override:** INELIGIBLE ⚠️
- **Reason:** Twin deficits (fiscal + current account)
- **Guidance:** "Strong advise against adding Romania bonds despite 3-star NFA rating. Twin deficits create unsustainable fiscal position."

---

## Statistics

**Total Countries:** 198 with NFA data

**Automatic Eligibility (NFA-based):**
- Eligible (3+ stars): 121 countries (61.1%)
- Ineligible (1-2 stars): 77 countries (38.9%)

**By NFA Rating:**
- 7-star: 19 countries (9.6%) - Preferred
- 6-star: 9 countries (4.5%) - Highly recommended
- 5-star: 14 countries (7.1%) - Good
- 4-star: 41 countries (20.7%) - Acceptable
- 3-star: 38 countries (19.2%) - Monitor closely
- 2-star: 44 countries (22.2%) - Ineligible
- 1-star: 33 countries (16.7%) - Strongly ineligible

---

## MCP Endpoints (via Orca)

### 1. Check Country Eligibility

```python
from tools.country_eligibility import check_country_eligibility

result = check_country_eligibility("Romania")
# {
#   "country": "Romania",
#   "eligible": False,
#   "nfa_rating": 3,
#   "reason": "Manual override: Twin deficits...",
#   "guidance": "⚠️ Strong advise against...",
#   "override": True
# }
```

### 2. List Eligible Countries

```python
from tools.country_eligibility import list_eligible_countries

# Get all eligible countries
eligible = list_eligible_countries()

# Get only 5+ star countries (preferred)
preferred = list_eligible_countries(min_rating=5)
```

### 3. Filter Bonds by Eligibility

```python
from tools.country_eligibility import filter_bonds_by_eligibility

bonds = [
    {"isin": "RO123", "country": "Romania", "yield": 7.0},
    {"isin": "DE456", "country": "Germany", "yield": 3.5}
]

result = filter_bonds_by_eligibility(bonds)
# {
#   "eligible": [{"isin": "DE456", ...}],
#   "ineligible": [{"isin": "RO123", ...}],
#   "warnings": ["Romania bond RO123: 3-star rating but overridden to ineligible"]
# }
```

### 4. Manual Override

```python
from tools.country_eligibility import set_country_eligibility

# Override a country
set_country_eligibility(
    country="Romania",
    eligible=False,
    reason="Twin deficits - fiscal and current account unsustainable",
    override=True
)
```

---

## Conversational Workflow

### Scenario 1: User Asks to Add Ineligible Country

```
👤 User: "Add Romania bond with 3% allocation"

🤖 Claude:
   ⚠️ STRONG ADVISE AGAINST adding Romania bonds.

   Details:
   - NFA Rating: 3-star (moderate)
   - Issue: Twin deficits (fiscal + current account)
   - Risk: Unsustainable fiscal position

   Even though Romania has a 3-star NFA rating, the twin deficits
   override this and make it unsuitable for investment.

   Would you like to explore alternatives in similar yield range?
```

### Scenario 2: User Asks for Eligible Countries

```
👤 User: "Find me cheap bonds from eligible countries"

🤖 Claude:
   Searching eligible countries (3+ star NFA rating)...

   Found 12 bonds with yields > 6%:

   ✅ Brazil (3-star): 6.5% yield
   ✅ Mexico (4-star): 6.2% yield
   ✅ Indonesia (3-star): 6.8% yield

   All are eligible for portfolio inclusion.
   Would you like details on any of these?
```

### Scenario 3: Borderline Case (3-star)

```
👤 User: "What about Brazil bonds?"

🤖 Claude:
   ⚡ Brazil is ELIGIBLE but requires close monitoring.

   Details:
   - NFA Rating: 3-star (moderate risk)
   - NFA/GDP: -27.5%
   - Status: Borderline eligible

   Recommendation: Acceptable for portfolio inclusion, but:
   - Limit allocation to 5-7%
   - Monitor fiscal developments
   - Consider setting tighter stop-loss

   Proceed with Brazil bond addition?
```

---

## Portfolio Construction Workflow

The eligibility system integrates into the iterative portfolio building process:

### Step 1: Find Cheap Bonds (Eligible Countries Only)
```
→ Filter universe by eligibility
→ Exclude 1-2 star countries automatically
→ Flag manual overrides (like Romania)
```

### Step 2: Add to Watchlist
```
→ Pre-check eligibility before adding
→ Warn if borderline (3-star)
→ Block if ineligible (with explanation)
```

### Step 3: Run Price Update
```
→ Update prices for eligible bonds only
→ Flag if previously eligible country degraded
```

### Step 4: Portfolio Optimization
```
→ Suggest bonds that improve characteristics
→ Prioritize higher-rated countries (4+ stars)
→ Limit exposure to 3-star countries
```

### Step 5: Scenario Analysis (Future)
```
→ Test portfolio under stress scenarios
→ Model impact of country downgrades
→ Assess diversification benefits
```

---

## Data Sources

1. **NFA MCP** (`https://nfa-mcp.urbancanary.workers.dev`)
   - 199 countries with Net Foreign Assets data
   - 7-tier rating system (1-7 stars)
   - Updated: 2023 data

2. **Country Mapping MCP** (`https://country-mapping-mcp.urbancanary.workers.dev`)
   - Country name standardization
   - 376 countries, 1,102 variants
   - Maps to IMF, NFA, World Bank codes

3. **Manual Overrides** (`data/country_eligibility.json`)
   - Stored locally in Orca MCP
   - Editable via `set_country_eligibility()` function
   - Tracks override reason and date

---

## Adding New Overrides

### Via Python

```python
from tools.country_eligibility import set_country_eligibility

# Mark country as ineligible
set_country_eligibility(
    country="Turkey",
    eligible=False,
    reason="Currency volatility, high inflation, capital controls",
    override=True
)

# Or make eligible despite low NFA rating
set_country_eligibility(
    country="Kenya",
    eligible=True,
    reason="IMF program showing strong progress, improving fundamentals",
    override=True
)
```

### Via Direct JSON Edit

Edit `orca_mcp/data/country_eligibility.json`:

```json
{
  "countries": {
    "Romania": {
      "eligible": false,
      "status": "reject",
      "nfa_rating": 3,
      "reason": "Manual override: Twin deficits...",
      "override": true,
      "override_date": "2025-01-16"
    }
  }
}
```

---

## Integration Points

### Orca MCP
- Country standardization → Country eligibility → Portfolio tools
- Provides eligibility checks to other MCPs

### Streamlit Portfolio Builder
- Filter bond universe by eligibility
- Display eligibility warnings in UI
- Allow manual override review/approval

### GA10 Optimizer (Future)
- Exclude ineligible countries from optimization
- Apply tighter constraints on 3-star countries

---

## Files

```
orca_mcp/
├── data/
│   ├── country_eligibility.json         # Eligibility database
│   └── build_country_eligibility.py     # Build from NFA data
│
├── tools/
│   └── country_eligibility.py           # Core eligibility functions
│
├── test_eligibility.py                  # Test suite
├── set_romania_ineligible.py            # Example override script
└── COUNTRY_ELIGIBILITY_SYSTEM.md        # This file
```

---

## Future Enhancements

### Phase 1: Risk Factor Tracking
- Track specific risk factors (twin deficits, inflation, etc.)
- Auto-suggest overrides based on macro data
- Historical override audit trail

### Phase 2: Dynamic Updates
- Auto-refresh NFA ratings monthly
- Alert when country downgrades
- Suggest portfolio rebalancing

### Phase 3: ML-Based Risk Scoring
- Combine NFA + fiscal + political risk
- Generate composite eligibility score
- Predictive early warning system

---

## Example: Twin Deficits Override

**Romania Case Study:**

```
NFA Rating: 3-star (moderate risk)
NFA/GDP: -43.5%

BUT:

Fiscal Deficit: -5.7% of GDP (2024)
Current Account Deficit: -6.2% of GDP (2024)
= TWIN DEFICITS

Impact:
- Increasing external financing needs
- Currency depreciation pressure
- Sovereign debt sustainability concerns
- Rising borrowing costs

Conclusion:
Despite acceptable NFA rating, twin deficits create unsustainable
trajectory → Manual override to INELIGIBLE
```

---

**Last Updated:** 2025-01-16
**Maintainer:** Guinness Global Investors
**Status:** Production Ready ✅
