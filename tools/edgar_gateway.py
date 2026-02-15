"""
Edgar Gateway - SEC EDGAR filing analysis via edgar_mcp.

Thin wrapper around edgar_mcp's EdgarClient for use by Orca's query router.
Follows the same pattern as sovereign_reports.py (direct Python import, not HTTP).

Exposes 4 tools:
  - edgar_search_company: Find company by ticker/name/CIK
  - edgar_filing_section: Extract section from 10-K/10-Q
  - edgar_financials: XBRL financial statements
  - edgar_search_filings: Full-text search across EDGAR
"""

import sys
import logging
from typing import Optional

logger = logging.getLogger("orca-mcp.edgar")

# Add edgar_mcp to path for imports
EDGAR_MCP_PATH = "/Users/andyseaman/Notebooks/mcp_central/edgar_mcp"
if EDGAR_MCP_PATH not in sys.path:
    sys.path.insert(0, EDGAR_MCP_PATH)

# SEC EDGAR requires User-Agent with name + email (identification, not a secret)
EDGAR_IDENTITY = "Urban Canary Research research@x-trillion.com"

# Lazy client - created on first call
_client = None


def _get_client():
    """Get or create EdgarClient singleton."""
    global _client
    if _client is None:
        from edgar_client import EdgarClient
        _client = EdgarClient(identity=EDGAR_IDENTITY)
        logger.info("Edgar client initialized")
    return _client


def edgar_search_company(query: str) -> str:
    """
    Search for a company by ticker, name, or CIK.
    Returns company profile and recent filings.
    """
    try:
        import edgar as edgar_lib
        client = _get_client()

        try:
            company = client.get_company(query)
        except Exception:
            results = edgar_lib.find(query)
            if results and len(results) > 0:
                return (
                    f"=== COMPANY SEARCH: '{query}' ===\n"
                    f"Multiple matches found:\n\n{results}\n\n"
                    f"Use a specific ticker to get company details."
                )
            return f"No companies found matching '{query}'."

        client.rate_limiter.acquire()
        filings = company.get_filings()

        lines = [
            f"=== COMPANY: {company.name} ===",
            f"CIK: {company.cik}",
            f"Ticker(s): {', '.join(company.tickers) if hasattr(company, 'tickers') and company.tickers else query.upper()}",
        ]

        if hasattr(company, 'sic_description') and company.sic_description:
            lines.append(f"SIC: {company.sic} - {company.sic_description}")

        sec_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={company.cik}"
        lines.append(f"SEC URL: {sec_url}")
        lines.append(f"\nRECENT FILINGS (last 15):")
        lines.append(str(filings.head(15)))

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"edgar_search_company({query}): {e}")
        return f"ERROR: Could not find company '{query}': {e}"


def edgar_filing_section(
    ticker: str,
    section: str,
    form_type: str = "10-K",
    filing_date: Optional[str] = None,
) -> str:
    """
    Extract a specific section from a 10-K or 10-Q filing.

    Sections: risk_factors, mda, business, financial_statements,
              legal_proceedings, market_risk, controls
    """
    try:
        client = _get_client()
        return client.get_filing_section(ticker, section, form_type, filing_date)
    except ValueError as e:
        return f"ERROR: {e}"
    except Exception as e:
        logger.error(f"edgar_filing_section({ticker}, {section}): {e}")
        return f"ERROR: Could not extract {section} from {form_type} for {ticker}: {e}"


def edgar_financials(
    ticker: str,
    statement: str = "income",
    periods: int = 4,
    annual: bool = True,
) -> str:
    """
    Get XBRL financial statement data.

    Statements: income, balance, cashflow, all
    """
    try:
        client = _get_client()
        return client.get_financials(ticker, statement, periods, annual)
    except ValueError as e:
        return f"ERROR: {e}"
    except Exception as e:
        logger.error(f"edgar_financials({ticker}, {statement}): {e}")
        return f"ERROR: Could not retrieve financials for {ticker}: {e}"


def edgar_search_filings(
    query: str,
    form_type: Optional[str] = None,
    ticker: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    max_results: int = 10,
) -> str:
    """
    Full-text search across SEC filings via EDGAR's EFTS engine.
    """
    try:
        client = _get_client()
        return client.search_efts(query, form_type, ticker, date_from, date_to, max_results)
    except Exception as e:
        logger.error(f"edgar_search_filings({query}): {e}")
        return f"ERROR: Search failed: {e}"
