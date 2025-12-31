"""
Orca Query Router - Multi-LLM routing for natural language queries to tools.

This module provides intelligent routing from a single orca_query() entry point
to the appropriate internal tool, reducing token usage by ~95%.

Uses FallbackLLMClient with purpose="routing" to try cheapest models first:
  Gemini Flash -> OpenAI Mini -> Grok Mini -> Haiku
"""

import json
import logging
import sys
from typing import Any, Dict, Optional

logger = logging.getLogger("orca-mcp.router")

# Add mcp_central to path for imports
MCP_CENTRAL_PATH = "/Users/andyseaman/Notebooks/mcp_central"
if MCP_CENTRAL_PATH not in sys.path:
    sys.path.insert(0, MCP_CENTRAL_PATH)


def _get_fallback_client():
    """
    Get FallbackLLMClient configured for routing (cheapest models first).

    Returns:
        FallbackLLMClient instance or None if import fails
    """
    try:
        from minerva_mcp.src.fallback_client import FallbackLLMClient
        client = FallbackLLMClient(purpose="routing")
        client.initialize()
        return client
    except Exception as e:
        logger.warning(f"FallbackLLMClient not available: {e}")
        return None


def _route_with_haiku(query: str, context: str = "") -> Dict[str, Any]:
    """
    Fallback router using Anthropic Haiku directly.
    Used when FallbackLLMClient is not available (e.g., Railway deployment).
    """
    import os
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed")
        return {"tool": None, "error": "anthropic package not installed"}

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        # Try auth_mcp with self-validating token
        try:
            import urllib.request
            import hashlib
            import secrets as sec

            # Generate self-validating token (format: {random}-{sha256_checksum})
            random_part = sec.token_hex(8)
            checksum = hashlib.sha256(random_part.encode()).hexdigest()[:8]
            auth_token = f"{random_part}-{checksum}"

            auth_url = os.environ.get("AUTH_MCP_URL", "https://auth-mcp.urbancanary.workers.dev")
            req = urllib.request.Request(
                f"{auth_url}/api/key/ANTHROPIC_API_KEY",
                headers={
                    "Authorization": f"Bearer {auth_token}",
                    "X-Requester": "orca-mcp-router"
                }
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                api_key = data.get("value")  # Note: 'value' not 'key'
                if api_key:
                    logger.info("Got ANTHROPIC_API_KEY from auth_mcp")
        except Exception as e:
            logger.error(f"Failed to get API key from auth_mcp: {e}")

    if not api_key:
        return {"tool": None, "error": "No Anthropic API key available"}

    try:
        client = anthropic.Anthropic(api_key=api_key)

        context_text = context if context else "No prior context available."
        formatted_prompt = ROUTER_PROMPT.format(context=context_text, query=query)

        response = client.messages.create(
            model="claude-3-5-haiku-latest",
            max_tokens=300,
            messages=[{"role": "user", "content": formatted_prompt}]
        )

        result_text = response.content[0].text.strip()

        # Handle markdown code blocks
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1]
            if result_text.startswith("json"):
                result_text = result_text[4:]
            result_text = result_text.strip()

        result = json.loads(result_text)
        result["model_used"] = "anthropic/claude-3-5-haiku"

        logger.info(f"Haiku router: '{query}' -> {result.get('tool')} (confidence: {result.get('confidence', 'N/A')})")
        return result

    except json.JSONDecodeError as e:
        logger.error(f"Haiku router JSON parse error: {e}")
        return {"tool": None, "error": f"JSON parse error: {e}"}
    except Exception as e:
        logger.error(f"Haiku router error: {e}")
        return {"tool": None, "error": str(e)}

# Router prompt - enhanced with disambiguation rules and context support
# Designed to be >2048 tokens for guaranteed Gemini caching
ROUTER_PROMPT = """You are a tool router for the Orca financial data system.

Given a user query and optional conversation context, determine which internal tool to call and extract the required arguments.

## CONVERSATION CONTEXT
{context}

## DISAMBIGUATION RULES (use these when queries are ambiguous)

### Rating Queries
- "rating" or "star rating" alone → get_nfa_rating (NFA is our primary rating system, 1-7 stars)
- "credit rating" or "S&P/Moody's/Fitch" → get_credit_rating (traditional letter ratings)
- "ratings for multiple countries" → get_nfa_batch or get_credit_ratings_batch

### Economic Data Queries
- Country + economic indicator → get_imf_indicator (default for international data)
- "US" or "United States" + economic term → get_fred_series (FRED is authoritative for US)
- "inflation" without country specified → get_fred_series with CPIAUCSL (US inflation)
- "GDP growth" without country → get_imf_indicator (international comparison)
- "treasury" or "yield curve" → get_treasury_rates

### Portfolio Queries
- "holdings", "portfolio", "positions", "what do I own" → get_client_holdings
- "cash", "cash position", "available cash" → get_portfolio_cash
- "transactions", "trades", "history" → get_client_transactions
- "watchlist", "buy candidates", "opportunities" → get_watchlist
- "compliance", "UCITS", "5/10/40" → get_compliance_status

### Bond Queries
- "bonds", "search bonds", "find bonds" → search_bonds_rvm
- "bond from [country]" → search_bonds_rvm with country filter
- "classify", "issuer type", "sovereign or corporate" → classify_issuer

### Single Word Queries
- Just a country name (e.g., "Colombia", "Brazil") → get_nfa_rating (most common use case)
- Just "inflation" → get_fred_series with CPIAUCSL
- Just "GDP" → get_imf_indicator with NGDP_RPCH for context country or ask clarification

## CONFIDENCE SCORING
- 0.95-1.00: Clear, unambiguous match - proceed confidently
- 0.85-0.94: Strong match with minor ambiguity - proceed with the most likely interpretation
- 0.70-0.84: Moderate confidence - make best guess but note uncertainty
- Below 0.70: Too ambiguous - ask for clarification

## AVAILABLE TOOLS

## Treasury & FRED (US Economic Data)
1. get_treasury_rates() - No args. Returns full US Treasury yield curve (1M to 30Y). Use for: treasury rates, yield curve, government bonds
2. get_fred_series(series_id, start_date?) - Get FRED time series. Common series:
   - DGS10, DGS2, DGS30 = Treasury rates
   - CPIAUCSL = CPI/Inflation
   - UNRATE = Unemployment rate
   - GDP = Gross Domestic Product
   - FEDFUNDS = Fed Funds rate
3. search_fred_series(query) - Search FRED by keyword

## Credit Ratings
4. get_nfa_rating(country, year?, history?) - NFA star rating (1-7 scale). Args: country name
5. get_nfa_batch(countries, year?) - Multiple NFA ratings. Args: list of countries
6. search_nfa_by_rating(rating?, min_rating?, max_rating?, year?) - Find countries by NFA rating
7. get_credit_rating(country) - S&P/Moody's/Fitch sovereign rating
8. get_credit_ratings_batch(countries) - Multiple credit ratings

## IMF Data (International)
9. get_imf_indicator(indicator, country, start_year?, end_year?, analyze?) - IMF indicators:
   - NGDP_RPCH = Real GDP growth
   - PCPIPCH = Inflation rate
   - BCA_NGDPD = Current account % GDP
10. compare_imf_countries(indicator, countries, year?) - Compare countries on IMF indicator

## World Bank (Development Data)
11. get_worldbank_indicator(indicator, country, start_year?, end_year?) - World Bank data:
    - NY.GDP.PCAP.CD = GDP per capita
    - SP.POP.TOTL = Population
    - SI.POV.DDAY = Poverty rate
12. search_worldbank_indicators(query) - Search World Bank indicators
13. get_worldbank_country_profile(country) - Key development stats for country

## Client Portfolio Data
14. get_client_info(client_id?) - Client configuration
15. get_client_holdings(client_id?, portfolio_id?) - Current portfolio holdings
16. get_client_portfolios(client_id?) - List of portfolios
17. get_client_transactions(client_id?, portfolio_id?, start_date?, end_date?) - Transaction history
18. get_portfolio_cash(client_id?, portfolio_id?) - Cash positions
19. query_client_data(sql, client_id?) - Custom SQL query on client data
20. get_watchlist(full_details?, client_id?) - Bond watchlist (buy candidates) with analytics from D1

## ETF Analysis
20. get_etf_allocation(etf_name) - ETF holdings breakdown
21. list_etf_allocations() - Available ETFs
22. get_etf_country_exposure(etf_name) - Country weights in ETF

## Compliance
23. get_compliance_status(client_id?, portfolio_id?) - UCITS 5/10/40 compliance check
24. check_trade_compliance_impact(portfolio_id, isin, trade_type, units) - Pre-trade compliance check
25. suggest_rebalancing(portfolio_id, target_allocation) - Rebalancing suggestions

## Bonds & Classification
26. search_bonds_rvm(query, country?, rating?, maturity_min?, maturity_max?) - Search RVM bond database
27. classify_issuer(isin) - Classify as sovereign/quasi-sovereign/corporate
28. classify_issuers_batch(isins) - Batch classification
29. get_issuer_summary(issuer) - AI summary of issuer

## Staging (Trade Preparation)
30. get_staging_holdings(client_id?, portfolio_id?) - Staged transactions
31. add_staging_buy(portfolio_id, isin, units, price?) - Stage a buy order
32. add_staging_sell(portfolio_id, isin, units, price?) - Stage a sell order

## Video Search
33. video_search(query) - Search video transcripts
34. video_list() - List available videos
35. video_get_transcript(video_id) - Get full transcript
36. video_keyword_search(keyword, video_id?) - Find keyword mentions

## Utilities
37. standardize_country(country) - Normalize country name to standard form
38. get_country_info(country) - Country details (ISO codes, region, etc.)

RESPONSE FORMAT:
Respond with valid JSON only, no other text:
{{"tool": "tool_name", "args": {{"arg1": "value1"}}, "confidence": 0.95}}

If the query is ambiguous or you need more information:
{{"tool": null, "clarification": "Please specify which country you'd like the rating for."}}

## EXAMPLES

### Clear Queries (high confidence)
- "10Y treasury rate" -> {{"tool": "get_treasury_rates", "args": {{}}, "confidence": 0.99}}
- "Colombia NFA rating" -> {{"tool": "get_nfa_rating", "args": {{"country": "Colombia"}}, "confidence": 0.98}}
- "US inflation from FRED" -> {{"tool": "get_fred_series", "args": {{"series_id": "CPIAUCSL"}}, "confidence": 0.95}}
- "Brazil GDP growth IMF" -> {{"tool": "get_imf_indicator", "args": {{"indicator": "NGDP_RPCH", "country": "Brazil"}}, "confidence": 0.95}}
- "Compare GDP: US, China, Germany" -> {{"tool": "compare_imf_countries", "args": {{"indicator": "NGDP_RPCH", "countries": ["United States", "China", "Germany"]}}, "confidence": 0.90}}
- "my holdings" -> {{"tool": "get_client_holdings", "args": {{}}, "confidence": 0.95}}
- "show me the watchlist" -> {{"tool": "get_watchlist", "args": {{}}, "confidence": 0.95}}
- "check compliance" -> {{"tool": "get_compliance_status", "args": {{}}, "confidence": 0.95}}

### Disambiguation Examples (apply rules)
- "Colombia" -> {{"tool": "get_nfa_rating", "args": {{"country": "Colombia"}}, "confidence": 0.90}}
- "inflation" -> {{"tool": "get_fred_series", "args": {{"series_id": "CPIAUCSL"}}, "confidence": 0.88}}
- "Mexico rating" -> {{"tool": "get_nfa_rating", "args": {{"country": "Mexico"}}, "confidence": 0.95}}
- "Mexico credit rating" -> {{"tool": "get_credit_rating", "args": {{"country": "Mexico"}}, "confidence": 0.95}}
- "bonds from Brazil" -> {{"tool": "search_bonds_rvm", "args": {{"country": "Brazil"}}, "confidence": 0.92}}
- "cash position" -> {{"tool": "get_portfolio_cash", "args": {{}}, "confidence": 0.95}}
- "unemployment rate" -> {{"tool": "get_fred_series", "args": {{"series_id": "UNRATE"}}, "confidence": 0.90}}

### Queries Needing Clarification
- "rating for" -> {{"tool": null, "clarification": "Please specify which country you'd like the rating for."}}
- "compare" -> {{"tool": null, "clarification": "What would you like to compare? Please specify countries and an indicator (GDP, inflation, etc.)."}}
- "data" -> {{"tool": null, "clarification": "What data are you looking for? Please be more specific."}}

### Context-Aware Examples
If context mentions "discussing Brazil": "what's the GDP growth?" -> {{"tool": "get_imf_indicator", "args": {{"indicator": "NGDP_RPCH", "country": "Brazil"}}, "confidence": 0.88}}
If context mentions "portfolio GA10": "show holdings" -> {{"tool": "get_client_holdings", "args": {{"portfolio_id": "GA10"}}, "confidence": 0.92}}

USER QUERY: {query}
"""


async def route_query(query: str, context: str = "", model_override: str = None) -> Dict[str, Any]:
    """
    Use multi-LLM fallback to route a natural language query to the appropriate tool.

    Tries models in cost order (cheapest first):
      Gemini Flash -> OpenAI Mini -> Grok Mini -> Haiku

    Falls back to direct Haiku call if FallbackLLMClient is not available
    (e.g., on Railway where minerva_mcp is not installed).

    Args:
        query: Natural language query from user
        context: Optional conversation context from memory (recent messages, current topic, etc.)
        model_override: Optional model to use instead of fallback chain (for testing)

    Returns:
        Dict with 'tool', 'args', 'confidence', and 'model_used' or 'clarification' if ambiguous
    """
    result_text = ""

    # Format context for prompt (or use default)
    context_text = context if context else "No prior context available."

    try:
        # Get fallback client (tries cheapest models first)
        client = _get_fallback_client()

        if not client:
            # Fallback to direct Haiku call (Railway deployment)
            logger.info("Using Haiku fallback router (FallbackLLMClient not available)")
            return _route_with_haiku(query, context)

        # System prompt for routing
        system_prompt = "You are a tool router. Respond with valid JSON only, no other text."

        # Format prompt with context and query
        formatted_prompt = ROUTER_PROMPT.format(context=context_text, query=query)

        # Call with fallback across providers
        response = client.chat(
            messages=[{
                "role": "user",
                "content": formatted_prompt
            }],
            system=system_prompt,
            max_tokens=300
        )

        if not response.success:
            logger.error(f"All LLM providers failed: {response.error}")
            return {
                "tool": None,
                "error": f"All routing providers failed: {response.error}"
            }

        result_text = response.text.strip()
        model_used = f"{response.provider}/{response.model_used}"

        # Parse JSON response
        # Handle potential markdown code blocks
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1]
            if result_text.startswith("json"):
                result_text = result_text[4:]
            result_text = result_text.strip()

        result = json.loads(result_text)

        # Add model info to result
        result["model_used"] = model_used

        logger.info(f"Router [{model_used}]: '{query}' -> {result.get('tool')} (confidence: {result.get('confidence', 'N/A')})")

        return result

    except json.JSONDecodeError as e:
        logger.error(f"Router JSON parse error: {e}, response: {result_text}")
        return {
            "tool": None,
            "error": f"Failed to parse router response: {e}",
            "raw_response": result_text
        }
    except Exception as e:
        logger.error(f"Router error: {e}")
        return {
            "tool": None,
            "error": str(e)
        }


# Tool description for Claude Desktop discovery
ORCA_QUERY_TOOL_DESCRIPTION = """Query Orca for financial and portfolio data using natural language.

CAPABILITIES:
- Treasury rates: Current US yield curve (1M to 30Y)
- FRED data: US economic indicators (GDP, CPI, unemployment, Fed funds)
- Credit ratings: Sovereign ratings from NFA (1-7 stars), S&P, Moody's, Fitch
- IMF data: International GDP growth, inflation, current account by country
- World Bank: Development indicators (poverty, population, GDP per capita)
- Client data: Portfolios, holdings, transactions, cash positions
- Watchlist: Bond buy candidates with full analytics (YTW, OAD, ratings)
- ETF analysis: Holdings breakdown, country exposures
- Compliance: UCITS 5/10/40 checks, pre-trade impact analysis
- Bonds: RVM database search, issuer classification
- Video: Search transcripts, get summaries

EXAMPLES:
- "What's the current 10Y treasury rate?"
- "Get Colombia's NFA rating"
- "Show me US inflation data from FRED"
- "What's Brazil's GDP growth forecast from IMF?"
- "Compare inflation: US, UK, Germany"
- "Check compliance status for GA10"
- "Search for bonds from Mexico rated BBB or higher"
- "Show me the watchlist"

Just describe what you need in natural language."""
