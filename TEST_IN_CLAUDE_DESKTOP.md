# Test Country Eligibility in Claude Desktop

**Status:** Config added ✅
**Next step:** Restart Claude Desktop and test

---

## ✅ Configuration Complete

I've added Orca MCP to your Claude Desktop config:

**File:** `~/Library/Application Support/Claude/claude_desktop_config.json`

**Added:**
```json
"orca-mcp": {
  "command": "python3",
  "args": [
    "/Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp/server.py"
  ],
  "env": {
    "CLIENT_ID": "guinness"
  }
}
```

**Backup created:** `claude_desktop_config.json.backup_[timestamp]`

---

## 🔄 Next Steps

### 1. Restart Claude Desktop

**Fully quit and restart:**
- Cmd+Q to quit Claude Desktop completely
- Relaunch Claude Desktop from Applications

**Wait for tools to load:**
- This may take 10-20 seconds
- You should see new tools appear in the tools panel

---

## 🧪 Tests to Run

### Test 1: Check Romania (Should Warn)

**Type in Claude Desktop:**
```
Check if Romania is eligible for bond investment
```

**Expected Response:**
```
⚠️ Romania is NOT eligible for bond portfolio investment.

Details:
- NFA Rating: 3-star (moderate)
- Issue: Twin deficits (fiscal + current account)
- Status: Manually overridden to INELIGIBLE

Even though Romania has a 3-star NFA rating, the twin deficits
create unsustainable fiscal position.
```

---

### Test 2: Check Germany (Should Approve)

**Type:**
```
Check if Germany is eligible for bond investment
```

**Expected Response:**
```
✅ Germany is HIGHLY RECOMMENDED for portfolio inclusion.

Details:
- NFA Rating: 6-star (strong)
- NFA/GDP: 66.3%
- Risk Level: Very Low
- Status: Preferred for investment
```

---

### Test 3: List Eligible Countries

**Type:**
```
List countries with 5+ star NFA ratings
```

**Expected Response:**
```
Found 42 countries with 5+ star ratings:

7-star (Preferred):
- Norway, Andorra, Kuwait, Hong Kong, Libya...

6-star (Highly Recommended):
- Germany, Denmark, Switzerland, Netherlands...

5-star (Good):
- China, Poland, Malaysia, Philippines...
```

---

### Test 4: Conversational Scenario

**Type:**
```
I want to add a Romania bond with 7% yield to my portfolio
```

**Expected Response:**
```
Before adding this bond, let me check Romania's eligibility...

⚠️ I must STRONGLY ADVISE AGAINST adding Romania bonds.

Romania is currently ineligible due to twin deficits
(fiscal + current account), which override its 3-star
NFA rating.

Would you like me to suggest alternative bonds in a
similar yield range from eligible countries?
```

---

## 🔍 Checking If It's Working

### Look for these signs:

**1. Tools Panel:**
- You should see new Orca MCP tools
- Including: `check_country_eligibility`, `list_eligible_countries`

**2. Tool Usage:**
- When you ask about countries, Claude should automatically call the tools
- You'll see tool results in the conversation

**3. Intelligent Responses:**
- Claude should warn about Romania
- Claude should recommend Germany
- Claude should explain the twin deficits issue

---

## ⚠️ Troubleshooting

### If tools don't appear:

**1. Check Claude Desktop logs:**
```bash
tail -f ~/Library/Logs/Claude/mcp*.log
```

**2. Verify config syntax:**
```bash
python3 -m json.tool ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

**3. Test server manually:**
```bash
cd /Users/andyseaman/Notebooks/mcp_central/portfolio_optimizer_mcp/orca_mcp
python3 server.py
```

If you see errors about missing dependencies, install them:
```bash
pip3 install mcp google-cloud-bigquery requests pandas
```

---

## 📊 Available Tools

Once loaded, Claude will have access to:

1. **check_country_eligibility** - Check single country
2. **list_eligible_countries** - List eligible by rating
3. **filter_bonds_by_eligibility** - Filter watchlists
4. **get_client_info** - Client configuration
5. **query_client_data** - BigQuery access
6. **calculate_rvm_analytics** - RVM calculations

---

## 🎯 What This Enables

With Orca MCP active, you can now:

✅ Ask Claude to check any country's eligibility
✅ Get automatic warnings for ineligible countries
✅ Filter bond watchlists by eligibility
✅ Build portfolios conversationally with country intelligence
✅ Get explanations for why countries are ineligible

---

## 📝 Example Workflows

### Finding Cheap Bonds

```
You: "Find me cheap bonds yielding over 6%"

Claude: [Automatically checks eligible countries]
        "I'll search eligible countries only (121 countries
        with 3+ star NFA rating)..."
```

### Portfolio Construction

```
You: "Add these bonds to the watchlist"

Claude: [Checks each bond's country eligibility]
        "✅ Brazil bond added (3-star, monitor closely)
         🚫 Romania bond rejected (twin deficits)
         ✅ Mexico bond added (4-star, acceptable)"
```

### Getting Alternatives

```
You: "Why can't I add Romania?"

Claude: "Romania has twin deficits (fiscal -5.7%, current
        account -6.2% of GDP). Alternative countries with
        similar yields: Brazil 6.5%, Mexico 6.2%, Indonesia 6.8%"
```

---

## ✅ Success Checklist

After restarting Claude Desktop, verify:

- [ ] Orca MCP appears in tools panel
- [ ] Romania check returns warning
- [ ] Germany check returns approval
- [ ] List eligible countries works
- [ ] Claude uses tools automatically when discussing bonds
- [ ] Explanations include twin deficits reasoning

---

**Ready to test!** Restart Claude Desktop and try the tests above.
