# Orca MCP Router Migration Plan

## Overview

Migrate from 51 exposed tools (~11K tokens) to a single `orca_query` router tool (~500 tokens) using Haiku for intelligent routing.

---

## How Claude Desktop Discovers Tools

### Current Flow (51 tools)
```
1. Claude Desktop connects to Orca MCP
2. Calls list_tools() via MCP protocol
3. Receives 51 Tool objects, each with:
   - name: "get_treasury_rates"
   - description: "Get current US Treasury rates..."
   - inputSchema: {type: object, properties: {...}}
4. Claude sees all 51 tools in its context (~11K tokens)
5. Claude decides which tool to call based on user query
```

### New Flow (1 router tool)
```
1. Claude Desktop connects to Orca MCP
2. Calls list_tools() via MCP protocol
3. Receives 1 Tool object:
   - name: "orca_query"
   - description: "Query Orca for financial data. Available capabilities:
     - Treasury rates & yield curves
     - FRED economic data (GDP, inflation, unemployment)
     - Credit ratings (NFA, S&P, Moody's)
     - IMF indicators by country
     - World Bank development data
     - Client portfolios & holdings
     - ETF allocations & exposures
     - Compliance checks
     - Video search & transcripts
     Just describe what you need in plain English."
   - inputSchema: {query: string}
4. Claude sees 1 tool in its context (~500 tokens)
5. Claude calls orca_query("get the 10Y treasury rate")
6. Orca internally routes to get_treasury_rates() via Haiku
7. Result returned to Claude
```

### Key Insight
Claude Desktop only knows what we tell it in `list_tools()`. The description must convey ALL capabilities clearly so Claude knows when to use the tool.

---

## Router Tool Design

### Exposed Tool Definition
```python
Tool(
    name="orca_query",
    description="""Query Orca for financial and portfolio data.

CAPABILITIES:
- Treasury rates: Current yield curve (1M to 30Y)
- FRED data: US economic indicators (GDP, CPI, unemployment, Fed funds)
- Credit ratings: Sovereign ratings from NFA, S&P, Moody's, Fitch
- IMF data: GDP growth, inflation, current account by country
- World Bank: Development indicators (poverty, life expectancy, education)
- Client data: Portfolios, holdings, transactions, cash positions
- ETF analysis: Allocations, country exposures
- Compliance: UCITS 5/10/40 checks, trade impact analysis
- Bonds: RVM search, issuer classification
- Video: Search transcripts, get summaries

EXAMPLES:
- "What's the current 10Y treasury rate?"
- "Get Colombia's NFA rating"
- "Show me US inflation data from FRED"
- "What's Brazil's GDP growth forecast from IMF?"
- "Check compliance status for GA10"

Just describe what you need in natural language.""",
    inputSchema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Natural language query describing what data you need"
            }
        },
        "required": ["query"]
    }
)
```

---

## Haiku Router Implementation

### Router Prompt
```python
ROUTER_PROMPT = """You are a tool router for the Orca financial data system.

Given a user query, determine which internal tool to call and extract the required arguments.

AVAILABLE TOOLS:
1. get_treasury_rates() - No args. Returns full yield curve.
2. get_fred_series(series_id, start_date?) - FRED data. Common: DGS10, CPIAUCSL, UNRATE, GDP
3. search_fred_series(query) - Search FRED by keyword
4. get_nfa_rating(country, year?, history?) - NFA star rating (1-7)
5. get_nfa_batch(countries, year?) - Multiple NFA ratings
6. get_credit_rating(country) - S&P/Moody's/Fitch rating
7. get_credit_ratings_batch(countries) - Multiple credit ratings
8. get_imf_indicator(indicator, country, start_year?, end_year?) - IMF data
9. compare_imf_countries(indicator, countries, year?) - Compare countries
10. get_worldbank_indicator(indicator, country, start_year?, end_year?)
11. get_worldbank_country_profile(country) - Key development stats
12. get_client_info(client_id?) - Client configuration
13. get_client_holdings(client_id?, portfolio_id?) - Current holdings
14. get_client_portfolios(client_id?) - List portfolios
15. get_portfolio_cash(client_id?, portfolio_id?) - Cash positions
16. get_etf_allocation(etf_name) - ETF holdings breakdown
17. get_compliance_status(client_id?, portfolio_id?) - UCITS compliance
18. check_trade_compliance_impact(portfolio_id, isin, trade_type, units)
19. search_bonds_rvm(query, filters?) - Search RVM bond database
20. classify_issuer(isin) - Sovereign/quasi/corporate classification
21. standardize_country(country) - Normalize country names
22. video_search(query) - Search video transcripts
23. video_get_transcript(video_id) - Full transcript

Respond with JSON only:
{
  "tool": "tool_name",
  "args": {"arg1": "value1", ...},
  "confidence": 0.95,
  "reasoning": "brief explanation"
}

If query is ambiguous or needs clarification, respond:
{
  "tool": null,
  "clarification_needed": "What specifically would you like..."
}

USER QUERY: {query}
"""
```

### Router Code
```python
async def route_query(query: str) -> dict:
    """Use Haiku to route query to appropriate tool."""
    import anthropic

    client = anthropic.Anthropic()

    response = client.messages.create(
        model="claude-3-5-haiku-latest",
        max_tokens=200,
        messages=[{
            "role": "user",
            "content": ROUTER_PROMPT.format(query=query)
        }]
    )

    result = json.loads(response.content[0].text)
    logger.info(f"Router: {query} -> {result['tool']} (confidence: {result.get('confidence', 'N/A')})")

    return result
```

---

## Test Plan

### Phase 1: Baseline - Document Current Behavior

For each tool, capture expected inputs and outputs:

| Tool | Test Query | Expected Args | Expected Result Pattern |
|------|------------|---------------|------------------------|
| get_treasury_rates | "treasury rates" | {} | {rates: {10Y: 4.17, ...}} |
| get_fred_series | "10Y treasury from FRED" | {series_id: "DGS10"} | {latest_value: 4.17, ...} |
| get_nfa_rating | "Colombia NFA" | {country: "Colombia"} | {nfa_star_rating: 5, ...} |
| get_credit_rating | "Brazil credit rating" | {country: "Brazil"} | {rating: "BB", ...} |
| ... | ... | ... | ... |

### Phase 2: Router Accuracy Testing

```python
# test_router_accuracy.py

TEST_CASES = [
    # Treasury
    {"query": "What's the 10Y treasury rate?", "expected_tool": "get_treasury_rates"},
    {"query": "Show me the yield curve", "expected_tool": "get_treasury_rates"},
    {"query": "Current treasury rates", "expected_tool": "get_treasury_rates"},

    # FRED
    {"query": "US inflation rate", "expected_tool": "get_fred_series", "expected_args": {"series_id": "CPIAUCSL"}},
    {"query": "Get DGS10 from FRED", "expected_tool": "get_fred_series", "expected_args": {"series_id": "DGS10"}},
    {"query": "Search FRED for unemployment", "expected_tool": "search_fred_series"},

    # NFA
    {"query": "Colombia NFA rating", "expected_tool": "get_nfa_rating", "expected_args": {"country": "Colombia"}},
    {"query": "NFA for Brazil and Mexico", "expected_tool": "get_nfa_batch"},

    # Credit ratings
    {"query": "What's Argentina's credit rating?", "expected_tool": "get_credit_rating"},

    # IMF
    {"query": "Brazil GDP growth from IMF", "expected_tool": "get_imf_indicator"},
    {"query": "Compare inflation: US, UK, Germany", "expected_tool": "compare_imf_countries"},

    # World Bank
    {"query": "India poverty rate", "expected_tool": "get_worldbank_indicator"},
    {"query": "Country profile for Nigeria", "expected_tool": "get_worldbank_country_profile"},

    # Client data
    {"query": "Show my holdings", "expected_tool": "get_client_holdings"},
    {"query": "What portfolios do I have?", "expected_tool": "get_client_portfolios"},
    {"query": "Cash position in GA10", "expected_tool": "get_portfolio_cash"},

    # Compliance
    {"query": "Check compliance for GA10", "expected_tool": "get_compliance_status"},
    {"query": "What if I buy 1000 shares of XS1234567890?", "expected_tool": "check_trade_compliance_impact"},

    # Ambiguous - should ask for clarification
    {"query": "data", "expected_tool": None, "expects_clarification": True},
    {"query": "rating", "expected_tool": None, "expects_clarification": True},
]

def test_router_accuracy():
    correct = 0
    total = len(TEST_CASES)

    for case in TEST_CASES:
        result = route_query(case["query"])

        if case.get("expects_clarification"):
            passed = result["tool"] is None and "clarification_needed" in result
        else:
            passed = result["tool"] == case["expected_tool"]
            if "expected_args" in case:
                passed = passed and all(
                    result["args"].get(k) == v
                    for k, v in case["expected_args"].items()
                )

        status = "✓" if passed else "✗"
        print(f"{status} '{case['query']}' -> {result.get('tool')} (expected: {case.get('expected_tool')})")

        if passed:
            correct += 1

    print(f"\nAccuracy: {correct}/{total} ({100*correct/total:.1f}%)")
    return correct / total
```

### Phase 3: End-to-End Comparison

```python
# test_e2e_comparison.py

async def compare_direct_vs_routed(query: str, direct_tool: str, direct_args: dict):
    """Compare direct tool call vs routed call."""

    # Direct call
    direct_result = await call_tool(direct_tool, direct_args)

    # Routed call
    routed_result = await call_tool("orca_query", {"query": query})

    # Compare
    match = direct_result == routed_result

    return {
        "query": query,
        "direct_tool": direct_tool,
        "match": match,
        "direct_result": direct_result,
        "routed_result": routed_result
    }

# Test all tools
COMPARISON_TESTS = [
    ("current treasury rates", "get_treasury_rates", {}),
    ("Colombia NFA rating", "get_nfa_rating", {"country": "Colombia"}),
    ("US 10Y from FRED", "get_fred_series", {"series_id": "DGS10"}),
    # ... more tests
]
```

---

## Migration Checklist

### Pre-Migration
- [ ] Document all 51 current tools with test cases
- [ ] Set up router accuracy test suite
- [ ] Achieve >95% routing accuracy on test cases
- [ ] Set up logging for router decisions

### Phase 1: Parallel Deployment
- [ ] Add `orca_query` tool alongside existing tools
- [ ] Deploy to staging
- [ ] Run comparison tests
- [ ] Monitor routing accuracy in production logs

### Phase 2: Gradual Tool Hiding (per tool)
- [ ] Verify tool routes correctly (>99% accuracy)
- [ ] Remove tool from `list_tools()`
- [ ] Keep implementation in `call_tool()` for router
- [ ] Test routed access still works
- [ ] Monitor for issues

### Phase 3: Final Cleanup
- [ ] All tools hidden except `orca_query`
- [ ] Remove legacy tool definitions from `list_tools()`
- [ ] Update documentation
- [ ] Measure token savings

---

## Rollback Plan

If issues arise:
1. Re-add tool to `list_tools()` - immediate fix
2. Both paths continue to work (router + direct)
3. No code changes needed for rollback

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Router accuracy | >95% |
| Token reduction | >90% (~11K → ~500) |
| Response latency | <100ms added (Haiku call) |
| User experience | No degradation |
| Error rate | No increase |

---

## Open Questions

1. **Multi-tool queries**: "Get treasury rates AND Colombia NFA" - chain calls?
2. **Streaming**: Can router support streaming responses?
3. **Caching**: Cache Haiku routing decisions for common queries?
4. **Fallback**: What if Haiku API is down? Expose all tools as fallback?
