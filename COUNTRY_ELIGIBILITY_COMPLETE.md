# Country Eligibility System - Implementation Complete ✅

**Date:** 2025-01-16
**Status:** Ready for Claude Desktop

---

## 🎯 What We Built

An **AI-powered country eligibility system** that gives Claude the intelligence to guide bond portfolio construction with automatic warnings for ineligible countries.

### Key Features

✅ **Automatic NFA-based eligibility** (1-2 stars = ineligible)
✅ **Manual overrides** for specific risk factors (twin deficits, etc.)
✅ **Real-time eligibility checking** via MCP tools
✅ **Conversational portfolio building** with Claude
✅ **Iterative, guided workflow** for finding cheap bonds

---

## 📊 System Coverage

**198 countries with NFA data:**
- **121 eligible** (61.1%) - 3+ star NFA rating
- **77 ineligible** (38.9%) - 1-2 star NFA rating
- **1 manual override** - Romania (3-star but twin deficits)

**Rating Distribution:**
- 7-star: 19 countries (preferred)
- 6-star: 9 countries (highly recommended)
- 5-star: 14 countries (good)
- 4-star: 41 countries (acceptable)
- 3-star: 38 countries (monitor closely)
- 2-star: 44 countries (ineligible)
- 1-star: 33 countries (strongly ineligible)

---

## 🔧 Architecture

```
Claude Desktop
    ↓
Orca MCP (MCP Server)
    ↓
Country Eligibility Tools
    ↓
    ├─ Country Eligibility Database (198 countries)
    ├─ NFA MCP (199 countries with ratings)
    └─ Country Mapping MCP (1,102 name variants)
```

---

## 📁 Files Created

### Core System
```
orca_mcp/
├── data/
│   ├── country_eligibility.json         # 198 countries, 68KB
│   └── build_country_eligibility.py     # NFA → eligibility builder
│
├── tools/
│   ├── country_eligibility.py           # Core functions (380 lines)
│   └── country_standardization.py       # Country name mapping (380 lines)
│
└── server.py                            # MCP server (UPDATED with 3 new tools)
```

### Testing & Documentation
```
orca_mcp/
├── test_eligibility.py                  # Test suite ✅ all pass
├── set_romania_ineligible.py            # Example override script
│
├── COUNTRY_ELIGIBILITY_SYSTEM.md        # Technical documentation
├── CLAUDE_DESKTOP_SETUP.md              # Configuration guide
└── COUNTRY_ELIGIBILITY_COMPLETE.md      # This file
```

---

## 🚀 MCP Tools Added to Orca

### 1. `check_country_eligibility`

**Purpose:** Check if a country is eligible for bond investment

**Returns:**
```json
{
  "country": "Romania",
  "eligible": false,
  "nfa_rating": 3,
  "nfa_percentage": -43.5,
  "reason": "Manual override: Twin deficits...",
  "guidance": "⚠️ Strong advise against...",
  "risk_level": "high",
  "override": true
}
```

### 2. `list_eligible_countries`

**Purpose:** List all eligible countries (optionally by minimum rating)

**Returns:**
```json
{
  "total_eligible": 42,
  "min_rating_filter": 5,
  "countries": [
    {
      "country": "Norway",
      "nfa_rating": 7,
      "risk_level": "very_low",
      "nfa_percentage": 185.2
    },
    ...
  ]
}
```

### 3. `filter_bonds_by_eligibility`

**Purpose:** Filter watchlist by country eligibility

**Returns:**
```json
{
  "eligible": [...],
  "ineligible": [...],
  "warnings": [
    "Romania bond RO123: 3-star rating, twin deficits override"
  ],
  "summary": {
    "total": 10,
    "eligible_count": 7,
    "ineligible_count": 3
  }
}
```

---

## 💬 How Claude Uses This

### Example 1: User Asks to Add Ineligible Country

```
👤 User: "Add Romania bond with 3% allocation"

🤖 Claude:
   [Automatically calls check_country_eligibility("Romania")]

   ⚠️ I must STRONGLY ADVISE AGAINST adding Romania bonds.

   Romania has:
   • NFA Rating: 3-star (moderate, normally eligible)
   • BUT: Twin deficits (fiscal -5.7% + current account -6.2% of GDP)
   • This overrides the NFA rating

   Impact:
   - Increasing external financing needs
   - Currency depreciation pressure
   - Rising borrowing costs
   - Debt sustainability concerns

   Would you like me to suggest alternative bonds in a similar
   yield range from eligible countries?
```

### Example 2: User Asks for Cheap Bonds

```
👤 User: "Find me cheap bonds yielding > 6%"

🤖 Claude:
   [Calls list_eligible_countries()]

   I'll search eligible countries only (121 countries with 3+ star NFA rating).

   Found 12 bonds yielding > 6%:

   ✅ Brazil 6.5% (3-star - monitor closely)
   ✅ Mexico 6.2% (4-star - acceptable)
   ✅ Indonesia 6.8% (3-star - monitor closely)
   ✅ South Africa 7.1% (4-star - acceptable)

   All are eligible. Would you like me to add these to the watchlist?
```

### Example 3: Borderline Case (3-star)

```
👤 User: "What about Brazil bonds?"

🤖 Claude:
   [Calls check_country_eligibility("Brazil")]

   ⚡ Brazil is ELIGIBLE but requires close monitoring.

   Details:
   - NFA Rating: 3-star (moderate risk)
   - NFA/GDP: -27.5%
   - Status: Borderline eligible

   Recommendation:
   • Acceptable for portfolio inclusion
   • Limit allocation to 5-7%
   • Monitor fiscal developments
   • Consider tighter stop-loss

   Proceed with adding Brazil bond?
```

---

## 🔄 Iterative Portfolio Workflow

The eligibility system enables this guided workflow:

### Step 1: Find Cheap Bonds (Eligible Only)
```
User: "Find cheap bonds"
→ Claude filters by eligibility automatically
→ Returns only 3+ star countries
→ Flags any borderline cases (3-star)
```

### Step 2: Add to Watchlist (With Pre-Check)
```
User: "Add these to watchlist"
→ Claude checks each country's eligibility
→ Warns if ineligible
→ Asks for confirmation on 3-star countries
```

### Step 3: Run Price Update
```
User: "Update prices"
→ Only updates eligible bonds
→ Flags if country was downgraded
```

### Step 4: Identify Improvements
```
User: "Which bonds improve the portfolio?"
→ Analyzes eligible bonds only
→ Suggests 1-2 best candidates
→ Shows impact on portfolio characteristics
```

### Step 5: Scenario Analysis (Future)
```
User: "Test under stress scenarios"
→ Models portfolio performance
→ Tests impact of country downgrades
→ Validates diversification
```

---

## 📝 Configuration for Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "orca-mcp": {
      "command": "python3",
      "args": [
        "/Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/server.py"
      ],
      "env": {
        "CLIENT_ID": "guinness"
      }
    }
  }
}
```

Then restart Claude Desktop.

---

## ✏️ Adding New Overrides

### Example: Turkey (if needed)

```python
from tools.country_eligibility import set_country_eligibility

set_country_eligibility(
    country="Turkey",
    eligible=False,
    reason="Currency volatility (50%+ depreciation), high inflation (60%+), capital controls",
    override=True
)
```

### Current Overrides

**Romania:**
- NFA Rating: 3-star
- Override Reason: Twin deficits (fiscal + current account)
- Status: INELIGIBLE

---

## 🧪 Testing

All tests passing ✅:

```bash
python3 test_eligibility.py
```

Results:
- ✅ Romania correctly rejected (twin deficits override)
- ✅ Germany correctly approved (6-star)
- ✅ 42 countries with 5+ stars listed
- ✅ 78 ineligible countries (77 auto + 1 override)
- ✅ Bond filtering works correctly
- ✅ Conversational scenario flows naturally

---

## 🎯 Success Criteria

All met ✅:

- [x] NFA-based eligibility rules (1-2 stars = ineligible)
- [x] Manual override capability for specific risk factors
- [x] MCP tools exposed to Claude Desktop
- [x] Real-time eligibility checking
- [x] Conversational guidance for ineligible countries
- [x] Support for borderline cases (3-star countries)
- [x] Integration with country name standardization
- [x] Comprehensive testing
- [x] Documentation complete

---

## 🔮 Future Enhancements

### Phase 1: Additional Risk Factors
- Auto-track twin deficits
- Inflation rate monitoring
- Political stability scores
- Capital control warnings

### Phase 2: Dynamic Updates
- Monthly NFA rating refresh
- Alert when country downgrades
- Suggest portfolio rebalancing

### Phase 3: ML-Based Scoring
- Combine multiple risk factors
- Generate composite eligibility score
- Predictive early warning system

---

## 📖 Documentation References

- **Setup Guide:** `CLAUDE_DESKTOP_SETUP.md`
- **Technical Docs:** `COUNTRY_ELIGIBILITY_SYSTEM.md`
- **Integration:** `COUNTRY_MAPPING_INTEGRATION.md`
- **Test Script:** `test_eligibility.py`

---

## ✅ Ready to Use!

The country eligibility system is now **fully operational** and ready for Claude Desktop.

**What Claude can now do:**
1. ✅ Automatically warn against ineligible countries
2. ✅ Guide users toward eligible alternatives
3. ✅ Provide nuanced advice on borderline cases
4. ✅ Filter watchlists by eligibility
5. ✅ Support iterative portfolio construction

**How to activate:**
1. Add Orca MCP to Claude Desktop config
2. Restart Claude Desktop
3. Ask Claude: "Check if Romania is eligible for bond investment"

Claude will automatically use the eligibility intelligence to guide portfolio construction!

---

**Implementation Date:** 2025-01-16
**Status:** Production Ready ✅
**Next Step:** Add to Claude Desktop and start using!
