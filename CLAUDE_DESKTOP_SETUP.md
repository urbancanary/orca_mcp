# Claude Desktop Setup - Country Eligibility Intelligence

**Last Updated:** 2025-01-16

---

## Overview

Configure Claude Desktop to access the **Orca MCP** with country eligibility intelligence. This gives Claude the ability to:

- ✅ Check if countries are eligible for bond investment
- ⚠️ Warn against ineligible countries (1-2 star NFA or manual overrides like Romania)
- 📊 List eligible countries by rating
- 🔍 Filter bond watchlists by eligibility

---

## Configuration

### 1. Find Claude Desktop Config

**macOS:**
```bash
~/Library/Application Support/Claude/claude_desktop_config.json
```

**Windows:**
```
%APPDATA%\Claude\claude_desktop_config.json
```

### 2. Add Orca MCP Configuration

Edit `claude_desktop_config.json`:

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

**Important:** Update the path to match your installation directory.

### 3. Restart Claude Desktop

- Quit Claude Desktop completely
- Relaunch the app
- New tools should appear in the tools panel

---

## Available Tools

### 1. `check_country_eligibility`

**Purpose:** Check if a country is eligible for bond investment

**When Claude Uses This:**
- Before adding any bond to portfolio
- When user asks about specific countries
- To provide investment guidance

**Example Usage:**
```
👤 User: "Should I add Romania bonds?"

🤖 Claude calls: check_country_eligibility(country="Romania")

🤖 Response:
   ⚠️ STRONG ADVISE AGAINST adding Romania bonds.

   Details:
   - NFA Rating: 3-star (moderate)
   - Issue: Twin deficits (fiscal + current account)
   - Status: Manually overridden to INELIGIBLE

   Even though Romania has a 3-star NFA rating, the twin
   deficits create unsustainable fiscal position.
```

### 2. `list_eligible_countries`

**Purpose:** List all countries eligible for investment

**When Claude Uses This:**
- "Which countries can I invest in?"
- "Show me 5+ star countries"
- Finding cheap bonds from eligible countries

**Example Usage:**
```
👤 User: "Find me cheap bonds from highly-rated countries"

🤖 Claude calls: list_eligible_countries(min_rating=5)

🤖 Response:
   Found 42 countries with 5+ star NFA ratings:

   7-star (Preferred):
   - Norway, Germany, Switzerland...

   6-star (Highly Recommended):
   - Denmark, Netherlands, Singapore...

   5-star (Good):
   - China, Malaysia, Poland...
```

### 3. `filter_bonds_by_eligibility`

**Purpose:** Filter a watchlist by country eligibility

**When Claude Uses This:**
- Reviewing watchlist before portfolio construction
- Checking if proposed bonds are eligible
- Generating warnings for ineligible bonds

**Example Usage:**
```
👤 User: "Here's my watchlist, which bonds can I add?"

🤖 Claude calls: filter_bonds_by_eligibility(bonds=[...])

🤖 Response:
   Reviewed 10 bonds:

   ✅ Eligible (7 bonds):
   - Brazil 6.5% (3-star)
   - Germany 3.5% (6-star)
   - Mexico 6.2% (4-star)
   ...

   🚫 Ineligible (3 bonds):
   - Romania 7.0% (3-star, twin deficits override)
   - Argentina 12% (1-star, high debt)
   - Lebanon 15% (1-star, default risk)
```

---

## How Claude Uses This Intelligence

### Proactive Warnings

When you ask to add a bond, Claude will **automatically check eligibility** and warn you:

```
👤 User: "Add Romania 7% bond with 5% allocation"

🤖 Claude:
   ⚠️ Before adding this bond, I need to check country eligibility.

   [Calls check_country_eligibility("Romania")]

   I must STRONGLY ADVISE AGAINST adding Romania bonds:

   • Romania has twin deficits (fiscal + current account)
   • Even with a 3-star NFA rating, this creates unsustainable
     fiscal position
   • Risk of currency depreciation and rising borrowing costs

   Would you like me to suggest alternative bonds in a similar
   yield range from eligible countries?
```

### Guided Portfolio Construction

Claude will guide you through the iterative workflow:

```
👤 User: "Help me find cheap bonds to add to the portfolio"

🤖 Claude:
   I'll search for bonds from eligible countries only.

   [Calls list_eligible_countries()]

   Searching 121 eligible countries for bonds yielding > 6%...

   Found 12 candidates:

   ✅ Brazil 6.5% (3-star - monitor closely)
   ✅ Mexico 6.2% (4-star - acceptable)
   ✅ Indonesia 6.8% (3-star - monitor closely)

   All are eligible. Would you like me to add these to the
   watchlist for price updates?
```

### Borderline Cases (3-star countries)

For 3-star countries (borderline eligible), Claude will provide nuanced guidance:

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
   • Monitor fiscal developments closely
   • Consider setting tighter stop-loss

   Proceed with Brazil bond addition?
```

---

## Behind the Scenes

### Data Sources

1. **NFA MCP** - 199 countries with Net Foreign Assets data
2. **Country Mapping MCP** - Name standardization
3. **Manual Overrides** - Expert judgment (twin deficits, etc.)

### Eligibility Rules

**Base Rule (Automatic):**
```
1-2 star NFA rating → INELIGIBLE (high external debt)
3+ star NFA rating  → ELIGIBLE (acceptable risk)
```

**Manual Overrides:**
```
Romania: 3-star BUT twin deficits → INELIGIBLE
Turkey: 3-star BUT currency risk → INELIGIBLE (if set)
```

### Example: Romania

```json
{
  "country": "Romania",
  "eligible": false,
  "nfa_rating": 3,
  "nfa_percentage": -43.5,
  "reason": "Manual override: Twin deficits (fiscal + current account)",
  "override": true,
  "guidance": "⚠️ Strong advise against..."
}
```

---

## Adding New Overrides

### Via Python (Recommended)

```python
from orca_mcp.tools.country_eligibility import set_country_eligibility

# Mark country as ineligible
set_country_eligibility(
    country="Turkey",
    eligible=False,
    reason="Currency volatility, high inflation, capital controls",
    override=True
)
```

### Via Direct JSON Edit

Edit `orca_mcp/data/country_eligibility.json`:

```json
{
  "countries": {
    "Turkey": {
      "eligible": false,
      "status": "reject",
      "nfa_rating": 3,
      "reason": "Manual override: Currency volatility...",
      "override": true,
      "override_date": "2025-01-16"
    }
  }
}
```

Then restart Claude Desktop to reload the data.

---

## Troubleshooting

### Tools Not Appearing

1. **Check config path:**
   ```bash
   cat ~/Library/Application\ Support/Claude/claude_desktop_config.json
   ```

2. **Verify server path is correct:**
   ```bash
   ls /Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/server.py
   ```

3. **Test server manually:**
   ```bash
   python3 /Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/server.py
   ```

4. **Check Claude Desktop logs:**
   ```bash
   tail -f ~/Library/Logs/Claude/mcp*.log
   ```

### Import Errors

If you see import errors, ensure dependencies are installed:

```bash
cd /Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp
pip3 install -r requirements.txt
```

### Data Not Loading

Verify eligibility data exists:

```bash
ls -lh /Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/data/country_eligibility.json
```

If missing, rebuild:

```bash
python3 /Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/data/build_country_eligibility.py
```

---

## Testing

### Quick Test in Claude Desktop

Once configured, try:

```
"Check if Romania is eligible for bond investment"
```

Claude should automatically call `check_country_eligibility` and warn you about Romania.

### Advanced Test

```
"List all 5+ star countries"
```

Claude should call `list_eligible_countries(min_rating=5)` and show highly-rated countries.

---

## What's Next?

With this configuration, Claude now has:

✅ **Real-time country eligibility checking**
✅ **Automatic warnings for ineligible countries**
✅ **Guidance on borderline cases (3-star countries)**
✅ **Ability to filter watchlists by eligibility**

This enables **conversational portfolio construction** where Claude guides you through:

1. Finding cheap bonds (eligible countries only)
2. Adding to watchlist (with eligibility pre-check)
3. Running price updates
4. Identifying improvement opportunities
5. Building optimal portfolios

---

**Configuration File:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Server Location:** `/Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/server.py`
**Data File:** `orca_mcp/data/country_eligibility.json`

**Ready to use!** 🚀
