# Display-Ready MCP Endpoints Specification

## Goal

Enable **thin frontends** (Streamlit, Reflex, or any future UI) by having the MCP return **display-ready data**. The frontend should only need to:

1. Call the MCP
2. Render the response

No joins, no calculations, no formatting in the frontend.

---

## Formatting Conventions

**Currency-neutral formatting**: All `_fmt` fields for monetary values use number formatting WITHOUT currency symbols. The response includes a `currency` field so frontends can prefix if needed.

| Type | Raw | Formatted | Notes |
|------|-----|-----------|-------|
| Money | 1000000 | "1,000,000" | No currency symbol |
| Money (negative) | -75250 | "-75,250" | Minus sign preserved |
| Money (millions) | 14760000 | "14.76M" | Abbreviated for large values |
| Percentage | 6.16 | "6.16%" | Include % symbol |
| Duration | 13.27 | "13.27y" | Include 'y' suffix |
| Spread | 285 | "285 bp" | Include 'bp' suffix |
| Price | 82.50 | "82.50" | 2 decimal places |

Frontends can prepend currency symbol based on account settings (e.g., "$", "€", "£").

---

## New Endpoints Required

### 1. `get_portfolio_dashboard`

**Purpose:** Single call to populate the Portfolio/Summary page.

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "client_id": "guinness"  // optional
}
```

**Output:**
```json
{
  "currency": "USD",
  "summary": {
    "total_value": 14760000,
    "total_value_fmt": "14.76M",
    "bond_value": 4760000,
    "bond_value_fmt": "4.76M",
    "cash_balance": 10000000,
    "cash_balance_fmt": "10.00M",
    "cash_pct": 67.8,
    "cash_pct_fmt": "67.8%",
    "duration": 13.27,
    "duration_fmt": "13.27y",
    "yield": 6.16,
    "yield_fmt": "6.16%",
    "num_holdings": 8,
    "unrealized_pnl": -75250,
    "unrealized_pnl_fmt": "-75,250",
    "day_change": 12400,
    "day_change_fmt": "+12,400"
  },
  "allocation": {
    "by_country": [
      {"country": "Brazil", "pct": 12.3, "value": 1817000},
      {"country": "Saudi Arabia", "pct": 8.6, "value": 1270000},
      {"country": "Colombia", "pct": 9.6, "value": 1418000},
      {"country": "Mexico", "pct": 7.2, "value": 1064000},
      {"country": "Indonesia", "pct": 6.3, "value": 930000}
    ],
    "by_rating": [
      {"rating": "A", "pct": 8.6, "value": 1270000, "bucket": "IG"},
      {"rating": "BBB", "pct": 13.5, "value": 1995000, "bucket": "IG"},
      {"rating": "BB+", "pct": 9.6, "value": 1418000, "bucket": "HY"},
      {"rating": "BB-", "pct": 12.3, "value": 1817000, "bucket": "HY"},
      {"rating": "B", "pct": 4.3, "value": 635000, "bucket": "HY"}
    ],
    "by_sector": [
      {"sector": "Sovereign", "pct": 85.2, "value": 12584000},
      {"sector": "Quasi", "pct": 14.8, "value": 2186000}
    ]
  },
  "compliance_summary": {
    "is_compliant": true,
    "hard_rules_pass": 4,
    "hard_rules_total": 4,
    "soft_warnings": 1
  },
  "as_of": "2026-01-02T10:30:00Z"
}
```

**Implementation Notes:**
- Join `transactions` (holdings) with pricing data for current values
- Calculate weighted duration/yield
- Use existing `check_compliance()` for compliance summary
- All `_fmt` fields are pre-formatted strings for direct display

---

### 2. `get_holdings_display`

**Purpose:** Single call to populate the Holdings table with ALL display columns.

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "include_staging": false,  // optional, default false
  "client_id": "guinness"    // optional
}
```

**Output:**
```json
{
  "currency": "USD",
  "holdings": [
    {
      "isin": "US105756BV13",
      "ticker": "BRAZIL 4.75 2050",
      "description": "Brazil 4.75% 2050",
      "country": "Brazil",
      "rating": "BB-",
      "rating_bucket": "HY",
      "sector": "Sovereign",
      "coupon": 4.75,
      "maturity_date": "2050-01-14",

      "face_value": 1000000,
      "face_value_fmt": "1,000,000",
      "avg_cost": 85.0,
      "current_price": 82.5,
      "current_price_fmt": "82.50",

      "cost_basis": 850000,
      "market_value": 837000,
      "market_value_fmt": "837,000",

      "unrealized_pnl": -13000,
      "unrealized_pnl_fmt": "-13,000",
      "unrealized_pnl_pct": -1.53,
      "unrealized_pnl_pct_fmt": "-1.53%",

      "weight_pct": 8.5,
      "weight_pct_fmt": "8.5%",

      "yield_to_worst": 6.25,
      "yield_fmt": "6.25%",
      "duration": 12.5,
      "duration_fmt": "12.5y",
      "spread": 285,
      "spread_fmt": "285 bp",

      "last_coupon_date": "2025-07-14",
      "next_coupon_date": "2026-01-14",
      "accrued_interest": 22125,
      "accrued_interest_fmt": "22,125"
    }
    // ... more holdings
  ],
  "totals": {
    "face_value": 5450000,
    "market_value": 4759750,
    "unrealized_pnl": -75250,
    "avg_yield": 6.16,
    "avg_duration": 13.27,
    "avg_spread": 267
  },
  "count": 8,
  "as_of": "2026-01-02T10:30:00Z"
}
```

**Implementation Notes:**
- Start with aggregated holdings from `transactions`
- Join with pricing/analytics data (D1 `agg_analysis_data` or similar)
- Calculate P&L = market_value - cost_basis
- Calculate weights = market_value / total_market_value
- Include both raw values AND formatted strings

---

### 3. `get_transactions_display`

**Purpose:** Transaction history with display-ready formatting.

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "transaction_type": "ALL",  // or "BUY", "SELL", "COUPON"
  "status": "ALL",            // or "settled", "pending", "staging"
  "start_date": "2025-01-01", // optional
  "end_date": "2025-12-31",   // optional
  "limit": 100,
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "currency": "USD",
  "transactions": [
    {
      "id": 12345,
      "trade_date": "2025-12-02",
      "settle_date": "2025-12-04",
      "transaction_type": "BUY",
      "status": "settled",

      "isin": "US105756BV13",
      "ticker": "BRAZIL 2050",
      "description": "Brazil 4.75% 2050",
      "country": "Brazil",

      "face_value": 1000000,
      "face_value_fmt": "1,000,000",
      "price": 85.0,
      "price_fmt": "85.00",
      "accrued_interest": 12500,
      "settlement_amount": 862500,
      "settlement_amount_fmt": "862,500",

      "yield_at_trade": 5.95,
      "duration_at_trade": 13.2,
      "spread_at_trade": 265,

      "counterparty": "JPMORGAN @ NY",
      "notes": ""
    }
  ],
  "summary": {
    "total_transactions": 7,
    "buys": 6,
    "sells": 0,
    "coupons": 1,
    "total_settled_amount": 4036500,
    "total_settled_amount_fmt": "4.04M"
  },
  "count": 7
}
```

---

### 4. `calculate_trade_settlement`

**Purpose:** Pre-trade settlement calculation for the Trade Ticket page.

**Input:**
```json
{
  "isin": "US105756BV13",
  "face_value": 500000,
  "price": 82.50,
  "settle_date": "2026-01-04",
  "side": "BUY",
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "bond": {
    "isin": "US105756BV13",
    "ticker": "BRAZIL 4.75 2050",
    "description": "Brazil 4.75% 2050",
    "country": "Brazil",
    "rating": "BB-",
    "coupon": 4.75,
    "maturity_date": "2050-01-14",
    "last_coupon_date": "2025-07-14",
    "next_coupon_date": "2026-01-14"
  },
  "analytics": {
    "yield_to_worst": 6.25,
    "yield_fmt": "6.25%",
    "duration": 12.5,
    "duration_fmt": "12.5y",
    "spread": 285,
    "spread_fmt": "285 bp"
  },
  "settlement": {
    "face_value": 500000,
    "face_value_fmt": "500,000",
    "price": 82.50,
    "price_fmt": "82.50",

    "principal": 412500,
    "principal_fmt": "412,500.00",

    "days_accrued": 174,
    "accrued_interest": 11562.50,
    "accrued_interest_fmt": "11,562.50",
    "accrued_pct": 2.3125,
    "accrued_pct_fmt": "2.31%",

    "net_settlement": 424062.50,
    "net_settlement_fmt": "424,062.50",

    "side": "BUY",
    "direction": "PAY"  // "PAY" for buys, "RECEIVE" for sells
  },
  "currency": "USD",
  "settle_date": "2026-01-04",
  "calculation_method": "30/360"
}
```

**Implementation Notes:**
- Fetch bond details from D1/pricing
- Calculate accrued using standard 30/360 convention
- Include both raw values AND formatted strings
- Direction indicates cash flow direction

---

### 5. `check_trade_compliance` (enhanced)

**Purpose:** Pre-trade compliance check with display-ready output.

The existing `check_trade_compliance_impact` is good. Enhance to include:

```json
{
  "before": { /* existing compliance_to_dict output */ },
  "after": { /* existing compliance_to_dict output */ },
  "impact": {
    "compliance_change": "unchanged",  // or "improved", "worsened"
    "would_breach": false,
    "would_fix": false,
    "hard_rules_change": 0,
    "soft_rules_change": 0
  },
  "warnings": [
    "This trade would increase Brazil exposure to 15.2% (limit: 20%)"
  ],
  "errors": [],
  "can_proceed": true,
  "can_proceed_reason": "All hard rules pass"
}
```

---

### 6. `get_cashflows_display`

**Purpose:** Projected cashflows for the Cashflows page.

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "months_ahead": 12,
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "currency": "USD",
  "summary": {
    "total_12m": 159225,
    "total_12m_fmt": "159,225",
    "next_coupon_date": "2026-01-14",
    "next_coupon_amount": 23750,
    "avg_coupon_rate": 5.2,
    "maturities_5yr": 0
  },
  "cashflows": [
    {
      "date": "2026-01-14",
      "type": "COUPON",
      "ticker": "BRAZIL 2050",
      "isin": "US105756BV13",
      "amount": 23750,
      "amount_fmt": "23,750"
    },
    {
      "date": "2026-02-15",
      "type": "COUPON",
      "ticker": "COLOM 2051",
      "isin": "USP17625AC16",
      "amount": 12375,
      "amount_fmt": "12,375"
    }
    // ... more cashflows
  ],
  "by_month": [
    {"month": "2026-01", "amount": 23750},
    {"month": "2026-02", "amount": 12375}
    // ... grouped by month for charting
  ],
  "maturities": []  // bonds maturing in the period
}
```

---

---

### 7. `get_compliance_display`

**Purpose:** Display-ready compliance dashboard (wraps existing `check_compliance`).

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "is_compliant": true,
  "summary": {
    "hard_pass": 4,
    "hard_total": 4,
    "soft_pass": 3,
    "soft_total": 4
  },
  "rules": [
    {
      "type": "Hard",
      "name": "Single Issuer",
      "limit": "10%",
      "current": "8.5%",
      "status": "pass",
      "details": "Max: BRAZIL 8.5%"
    }
  ],
  "country_chart": [
    {"country": "Brazil", "pct": 12.3, "over_limit": false},
    {"country": "Mexico", "pct": 22.1, "over_limit": true}
  ],
  "violations": {
    "issuers_over_5": [...],
    "nfa_violations": [...]
  },
  "metrics": {
    "max_position": 8.5,
    "max_country_pct": 22.1,
    "cash_pct": 5.2
  }
}
```

---

### 8. `get_pnl_display`

**Purpose:** P&L reconciliation for any period.

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "period": "MTD",  // or "YTD", "Since Inception", "Custom"
  "start_date": "2025-12-01",  // for Custom
  "end_date": "2025-12-31",
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "currency": "USD",
  "period": {
    "start": "2025-12-01",
    "end": "2025-12-31",
    "label": "MTD"
  },
  "summary": {
    "opening_nav": 10000000,
    "closing_nav": 10125000,
    "total_return": 125000,
    "total_return_pct": 1.25,
    "total_return_fmt": "125,000"
  },
  "breakdown": {
    "realised_pnl": 15000,
    "unrealised_pnl": 85000,
    "coupon_income": 25000,
    "accrued_change": 0
  },
  "by_holding": [
    {
      "ticker": "BRAZIL 2050",
      "realised": 0,
      "unrealised": 12500,
      "coupons": 5000,
      "total": 17500
    }
  ]
}
```

---

### 9. `get_cash_event_horizon`

**Purpose:** Historical + future cash timeline.

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "future_days": 90,  // null for all
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "currency": "USD",
  "current_balance": 1250000,
  "current_balance_fmt": "1,250,000",
  "historical": [
    {
      "date": "2025-12-01",
      "type": "BUY",
      "description": "BRAZIL 2050",
      "debit": 500000,
      "credit": 0,
      "balance": 9500000
    }
  ],
  "future": [
    {
      "date": "2026-01-14",
      "type": "COUPON",
      "ticker": "BRAZIL 2050",
      "amount": 23750,
      "projected_balance": 1273750
    }
  ],
  "summary": {
    "total_future_income": 159225,
    "next_event_date": "2026-01-14",
    "next_event_amount": 23750
  }
}
```

---

### 10. `save_transaction` / `update_transaction`

**Purpose:** Transaction CRUD (currently direct Worker calls).

**Input (save):**
```json
{
  "portfolio_id": "wnbf",
  "transaction_type": "BUY",
  "isin": "US105756BV13",
  "face_value": 500000,
  "price": 82.50,
  "settlement_date": "2026-01-04",
  "status": "staging",
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "success": true,
  "transaction_id": 12345,
  "settlement": {
    "principal": 412500,
    "accrued": 11562.50,
    "net_amount": 424062.50
  }
}
```

---

### 11. `get_ratings_display`

**Purpose:** Rating distribution for ratings page.

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "rating_source": "sp_stub",  // or "sp", "moodys"
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "distribution": [
    {"rating": "A", "pct": 8.6, "value": 1270000, "bucket": "IG", "count": 1},
    {"rating": "BBB", "pct": 13.5, "value": 1995000, "bucket": "IG", "count": 2},
    {"rating": "BB", "pct": 21.9, "value": 3235000, "bucket": "HY", "count": 3}
  ],
  "summary": {
    "ig_pct": 22.1,
    "hy_pct": 77.9,
    "avg_rating": "BB+",
    "avg_notches": 11.2
  }
}
```

---

### 12. `get_issuer_exposure`

**Purpose:** Issuer-level aggregation for 5/10/40 compliance.

**Input:**
```json
{
  "portfolio_id": "wnbf",
  "client_id": "guinness"
}
```

**Output:**
```json
{
  "issuers": [
    {
      "ticker": "BRAZIL",
      "country": "Brazil",
      "total_pct": 8.5,
      "total_value": 1250000,
      "bonds": [
        {"isin": "US105756BV13", "pct": 5.2, "value": 765000},
        {"isin": "US105756BW12", "pct": 3.3, "value": 485000}
      ],
      "over_5": true,
      "over_10": false
    }
  ],
  "summary": {
    "issuers_over_5": 3,
    "total_over_5_pct": 28.5,
    "max_issuer_pct": 8.5,
    "rule_5_10_40_pass": true
  }
}
```

---

## Implementation Priority

1. **`get_holdings_display`** - Enables Holdings page, needed by most other pages
2. **`get_portfolio_dashboard`** - Enables Portfolio/Summary page
3. **`calculate_trade_settlement`** - Enables Trade Ticket page
4. **`get_transactions_display`** - Enhances Transactions page
5. **`get_cashflows_display`** - Enables Cashflows page
6. **`save_transaction`** - Replace direct Worker calls
7. **`get_compliance_display`** - Wrap existing check_compliance
8. **`get_pnl_display`** - P&L reconciliation
9. **`get_cash_event_horizon`** - Cash timeline
10. **`get_ratings_display`** - Rating breakdown
11. **`get_issuer_exposure`** - 5/10/40 analysis

---

## Data Sources

| Data | Source |
|------|--------|
| Holdings (aggregated) | BigQuery `transactions` table |
| Current prices | D1 `agg_analysis_data` or pricing API |
| Yield, duration, spread | D1 `agg_analysis_data` |
| Ratings | D1 or NFA MCP |
| Compliance rules | `tools/compliance.py` |
| Coupon dates | Bond reference data |

---

## Key Principles

1. **Every value has a `_fmt` version** - Pre-formatted string for display
2. **Single call per page** - No need for frontend to make multiple calls
3. **Include totals/summaries** - Don't make frontend aggregate
4. **Consistent structure** - Same field names across endpoints
5. **Include `as_of` timestamp** - Client knows data freshness

---

## Frontend Usage Example

```python
# Streamlit
data = mcp_call("get_holdings_display", {"portfolio_id": "wnbf"})
st.dataframe(pd.DataFrame(data["holdings"]))

# Reflex
holdings = mcp_call("get_holdings_display", {"portfolio_id": "wnbf"})
rx.foreach(holdings["holdings"], render_row)
```

Both frontends: **zero calculation, zero formatting**.
