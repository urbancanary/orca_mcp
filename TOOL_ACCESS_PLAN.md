# Orca MCP Tool Access Plan

## Overview

This document outlines the access control strategy for Orca MCP tools across different user tiers:
- **Admin** (you/Andy): Full access to all tools including admin/debug tools
- **Guinness**: Client-specific portfolio tools + public data tools
- **Subscribers**: Public data tools only (macro/research data)

## Current State

Orca MCP now has **51 tools** organized into categories:

### Tool Categories

| Category | Count | Examples |
|----------|-------|----------|
| Portfolio Core | 12 | get_client_holdings, get_staging_holdings, add_staging_buy |
| Compliance | 3 | get_compliance_status, check_trade_compliance_impact |
| Data Management | 4 | upload_table, delete_records, invalidate_cache |
| IMF Gateway | 3 | fetch_imf_data, get_available_indicators |
| ETF Reference | 3 | get_etf_allocation, get_etf_country_exposure |
| Video Intelligence | 5 | video_search, video_synthesize |
| External MCPs | 18 | get_nfa_rating, get_treasury_rates, classify_issuer |

### Proposed Access Tiers

## Tier 1: Admin Only (Andy)

Tools that should **never** be exposed to clients:

| Tool | Reason |
|------|--------|
| `upload_table` | Direct BigQuery writes |
| `delete_records` | Destructive operations |
| `invalidate_cache` | Cache manipulation |
| `query_client_data` | Raw SQL access |

## Tier 2: Guinness Client

Client-specific portfolio tools + curated analytics:

| Category | Tools | Notes |
|----------|-------|-------|
| **Portfolio Read** | `get_client_holdings`, `get_client_transactions`, `get_portfolio_cash` | Read their own portfolio |
| **Staging** | `get_staging_holdings`, `add_staging_buy`, `add_staging_sell` | Propose trades |
| **Compliance** | `get_compliance_status`, `check_trade_compliance_impact` | Check UCITS compliance |
| **Analytics** | `search_bonds_rvm`, `suggest_rebalancing` | RVM-based recommendations |
| **External Data** | All NFA, Rating, IMF, WorldBank, FRED tools | Public macro data |
| **ETF Reference** | All ETF allocation tools | Public ETF data |
| **Video** | All video tools | Internal training videos |

**NOT included:**
- Raw data upload/delete
- Cache manipulation
- Direct SQL access

## Tier 3: Subscribers

Public data tools only - no portfolio access:

| Category | Tools |
|----------|-------|
| **NFA** | `get_nfa_rating`, `get_nfa_batch`, `search_nfa_by_rating` |
| **Ratings** | `get_credit_rating`, `get_credit_ratings_batch` |
| **Country** | `standardize_country`, `get_country_info` |
| **FRED** | `get_fred_series`, `search_fred_series`, `get_treasury_rates` |
| **Sovereign** | `classify_issuer`, `classify_issuers_batch`, `filter_by_issuer_type`, `get_issuer_summary` |
| **IMF** | `get_imf_indicator_external`, `compare_imf_countries` |
| **World Bank** | `get_worldbank_indicator`, `search_worldbank_indicators`, `get_worldbank_country_profile` |
| **IMF Internal** | `fetch_imf_data`, `get_available_indicators`, `get_available_country_groups` |
| **ETF** | `get_etf_allocation`, `list_etf_allocations`, `get_etf_country_exposure` |

**Total: 21 public tools**

## Implementation Plan

### Phase 1: Update service_registry.json

Add new access levels:

```json
{
  "access_levels": {
    "admin": {
      "name": "Admin Access",
      "allowed_tools": ["*"],
      "features": {"all": true}
    },
    "client": {
      "name": "Client Access",
      "allowed_tools": [
        "get_client_info",
        "get_client_portfolios",
        "get_client_transactions",
        "get_client_holdings",
        "get_staging_holdings",
        "get_staging_versions",
        "compare_staging_vs_actual",
        "add_staging_buy",
        "add_staging_sell",
        "get_portfolio_cash",
        "refresh_portfolio_summary",
        "get_compliance_status",
        "check_trade_compliance_impact",
        "search_bonds_rvm",
        "suggest_rebalancing",
        "get_nfa_rating",
        "get_nfa_batch",
        "search_nfa_by_rating",
        "get_credit_rating",
        "get_credit_ratings_batch",
        "standardize_country",
        "get_country_info",
        "get_fred_series",
        "search_fred_series",
        "get_treasury_rates",
        "classify_issuer",
        "classify_issuers_batch",
        "filter_by_issuer_type",
        "get_issuer_summary",
        "fetch_imf_data",
        "get_available_indicators",
        "get_available_country_groups",
        "get_imf_indicator_external",
        "compare_imf_countries",
        "get_worldbank_indicator",
        "search_worldbank_indicators",
        "get_worldbank_country_profile",
        "get_etf_allocation",
        "list_etf_allocations",
        "get_etf_country_exposure",
        "video_search",
        "video_list",
        "video_synthesize",
        "video_get_transcript",
        "video_keyword_search"
      ],
      "features": {
        "rvm_analytics": true,
        "portfolio_optimization": true
      }
    },
    "subscriber": {
      "name": "Subscriber Access",
      "allowed_tools": [
        "get_nfa_rating",
        "get_nfa_batch",
        "search_nfa_by_rating",
        "get_credit_rating",
        "get_credit_ratings_batch",
        "standardize_country",
        "get_country_info",
        "get_fred_series",
        "search_fred_series",
        "get_treasury_rates",
        "classify_issuer",
        "classify_issuers_batch",
        "filter_by_issuer_type",
        "get_issuer_summary",
        "fetch_imf_data",
        "get_available_indicators",
        "get_available_country_groups",
        "get_imf_indicator_external",
        "compare_imf_countries",
        "get_worldbank_indicator",
        "search_worldbank_indicators",
        "get_worldbank_country_profile",
        "get_etf_allocation",
        "list_etf_allocations",
        "get_etf_country_exposure"
      ],
      "features": {
        "rvm_analytics": false,
        "portfolio_optimization": false
      }
    }
  }
}
```

### Phase 2: Add Client Entries

```json
{
  "clients": {
    "andy": {
      "client_id": "andy",
      "name": "Admin (Andy)",
      "access_level": "admin",
      "active": true
    },
    "guinness": {
      "client_id": "guinness",
      "name": "Guinness Asset Management",
      "access_level": "client",
      "active": true
    },
    "subscriber_demo": {
      "client_id": "subscriber_demo",
      "name": "Demo Subscriber",
      "access_level": "subscriber",
      "active": true
    }
  }
}
```

### Phase 3: Deployment Options

1. **Claude Desktop**: Uses `CLIENT_ID` env var to determine access
2. **Cloudflare Worker**: Uses API key to look up client → access level
3. **Streamlit (Athena)**: Uses `?code=` URL param → maps to client_id

## Questions to Decide

1. **Video access for subscribers?** Currently excluded, could add as premium feature
2. **ETF data for subscribers?** Currently included, might want to gate
3. **AI analysis features?** (`analyze=True` on FRED/IMF) - include for subscribers?
4. **Rate limiting per tier?** Should subscribers have request limits?

## Next Steps

1. [ ] Review and confirm tier assignments
2. [ ] Update service_registry.json with new access levels
3. [ ] Add subscriber client entries
4. [ ] Test access control with each tier
5. [ ] Update Cloudflare worker to check client access level
6. [ ] Document public API endpoints for subscriber tier

---

*Created: 2024-12-22*
*Status: Draft for review*
