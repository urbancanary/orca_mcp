# Orca MCP - Portfolio Data Gateway

## Architecture (v3.0)

### Key Design Decision: Two Access Patterns

Orca MCP serves two types of clients differently:

| Client Type | Interface | Access Pattern | Why |
|-------------|-----------|----------------|-----|
| **Claude Desktop** | MCP protocol (`list_tools`) | Single `orca_query` tool | Minimize context, prevent routing errors |
| **Code/Athena** | HTTP `/call` endpoint | Direct tool calls | Programmatic, knows exact tool needed |

```
Claude Desktop (LLM)              Athena / Code (Programmatic)
       │                                    │
       ▼                                    ▼
  MCP list_tools()                    POST /call
  Returns: [orca_query]               {"tool": "get_holdings", "args": {...}}
       │                                    │
       ▼                                    ▼
  orca_query("show watchlist")        call_tool("get_holdings", args)
       │                                    │
       ▼                                    ▼
  FallbackLLMClient routes            Direct handler execution
  to get_watchlist internally         (no routing needed)
```

### Why This Split?

**For Claude Desktop (LLM clients):**
- Seeing 37 tools = ~11K tokens of context overhead
- LLM might pick wrong tool (e.g., `get_nfa_rating` vs `get_credit_rating`)
- Natural language is more intuitive for LLM-to-LLM communication
- Single tool = 95% token reduction (~500 tokens)

**For Code/Athena (Programmatic clients):**
- Already knows exactly which tool to call
- No routing overhead needed
- Direct, fast, predictable
- Can call any internal tool via `/call` endpoint

### Internal Routing

When Claude Desktop uses `orca_query`, requests are routed via FallbackLLMClient:

```
orca_query("show me the watchlist")
     │
     ▼
FallbackLLMClient (purpose="routing")
Tries: Gemini Flash → OpenAI Mini → Haiku
     │
     ▼
{"tool": "get_watchlist", "args": {}, "confidence": 0.95}
     │
     ▼
call_tool("get_watchlist", {})  ← Same handler Athena uses!
```

**Benefits:**
- Reduces Claude context from ~11K tokens to ~500 tokens (95% reduction)
- Prevents Claude from picking wrong tools
- Allows gradual rollout of tools via ENABLED_TOOLS registry
- Programmatic access remains fast and direct

---

## ENABLED_TOOLS Registry

Tools are enabled one at a time as we validate routing accuracy. Edit `mcp_sse_server.py`:

```python
ENABLED_TOOLS = {
    # Phase 1: Core portfolio tools
    "get_watchlist",           # Bond watchlist with filters
    "get_client_holdings",     # Portfolio holdings
    "get_portfolio_summary",   # Portfolio stats
    "get_compliance_status",   # UCITS compliance

    # Phase 2: Rating/country tools
    "get_nfa_rating",          # NFA star ratings
    "get_credit_rating",       # S&P/Moody's ratings

    # Add more tools here as we validate them...
}
```

To enable a new tool:
1. Add tool name to `ENABLED_TOOLS` set
2. Test via Claude Desktop: "show me the watchlist" → should route to get_watchlist
3. Verify response is correct
4. Deploy to Railway

---

## Key Files

| File | Purpose |
|------|---------|
| `mcp_sse_server.py` | SSE server for Railway (v3.0 - single orca_query tool) |
| `server.py` | Local stdio server (has full tool set) |
| `tools/query_router.py` | FallbackLLMClient routing logic |
| `tools/external_mcps.py` | Wrappers for NFA, ratings, FRED, etc. |

---

## Testing Locally

```bash
cd /Users/andyseaman/Notebooks/mcp_central/orca_mcp
python mcp_sse_server.py
```

Then test via curl:
```bash
# Health check
curl http://localhost:8000/

# Test orca_query via /call endpoint
curl -X POST http://localhost:8000/call \
  -H "Content-Type: application/json" \
  -d '{"tool": "orca_query", "args": {"query": "show me the watchlist"}}'
```

---

## Deployment

Railway auto-deploys from main branch. After pushing:
1. Check Railway logs for startup
2. Verify health: `curl https://orca-mcp-production.up.railway.app/`
3. Test via Claude Desktop

---

## Adding New Tools

1. Implement tool in `tools/` directory
2. Add handler in `call_tool()` (internal section)
3. Add to `INTERNAL_TOOLS` list
4. Update `ROUTER_PROMPT` in `tools/query_router.py`
5. Test routing accuracy
6. Add to `ENABLED_TOOLS` when ready

---

*Last Updated: 2025-12-31*
