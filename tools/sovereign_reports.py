"""
Sovereign Credit Reports - Access to country credit analysis

Reads HTML reports from sovereign-credit-system and provides:
- Full report text for AI analysis
- Section-specific extracts
- Cross-country search
- Available countries list

Reports location: /Users/andyseaman/Notebooks/sovereign-credit-system/credit_reports/moodys_style/
"""

import os
import re
import logging
import requests
from pathlib import Path
from typing import Dict, List, Optional, Any
from functools import lru_cache

logger = logging.getLogger(__name__)

# Default reports directory (local development)
REPORTS_DIR = Path(os.environ.get(
    "SOVEREIGN_REPORTS_DIR",
    "/Users/andyseaman/Notebooks/sovereign-credit-system/credit_reports/moodys_style"
))

# Lexa MCP URL for fetching reports when local files unavailable (Railway deployment)
LEXA_MCP_URL = os.environ.get("LEXA_MCP_URL", "https://lexa-mcp-production.up.railway.app")

# Check if we're running in Railway (no local reports)
IS_RAILWAY = not REPORTS_DIR.exists()
logger.info(f"Sovereign reports: REPORTS_DIR={REPORTS_DIR}, exists={REPORTS_DIR.exists()}, IS_RAILWAY={IS_RAILWAY}")

# Section patterns for HTML
HTML_SECTION_PATTERNS = {
    "summary": r"<h2[^>]*>.*?(?:Summary|Overview|Key Economic).*?</h2>(.*?)(?=<h2|$)",
    "ratings": r"<h2[^>]*>.*?(?:Rating|Credit Rating).*?</h2>(.*?)(?=<h2|$)",
    "economic": r"<h2[^>]*>.*?(?:Economic (?:Analysis|Indicators|Performance)).*?</h2>(.*?)(?=<h2|$)",
    "fiscal": r"<h2[^>]*>.*?(?:Fiscal|Debt).*?</h2>(.*?)(?=<h2|$)",
    "external": r"<h2[^>]*>.*?(?:External|NFA|Foreign).*?</h2>(.*?)(?=<h2|$)",
    "political": r"<h2[^>]*>.*?(?:Political|Institutional|Governance).*?</h2>(.*?)(?=<h2|$)",
    "banking": r"<h2[^>]*>.*?(?:Banking|Financial).*?</h2>(.*?)(?=<h2|$)",
    "outlook": r"<h2[^>]*>.*?(?:Outlook|Scenario).*?</h2>(.*?)(?=<h2|$)",
    "strengths": r"<h2[^>]*>.*?(?:Strength).*?</h2>(.*?)(?=<h2|$)",
    "vulnerabilities": r"<h2[^>]*>.*?(?:Vulnerabilit|Weakness|Risk).*?</h2>(.*?)(?=<h2|$)",
}

# Section patterns for Markdown (matches ## and ### headings)
# Note: Strengths and Vulnerabilities are h3 subsections, others are h2
MD_SECTION_PATTERNS = {
    "summary": r"##\s*(?:Summary|Overview|Key Economic)[^\n]*\n([\s\S]*?)\n##",
    "ratings": r"##[#]?\s*(?:Rating|Credit Rating)[^\n]*\n([\s\S]*?)\n##",
    "economic": r"##\s*Economic (?:Analysis|Indicators)\s*\n([\s\S]*?)\n##",
    "fiscal": r"###\s*(?:Fiscal|Government Finance)[^\n]*\n([\s\S]*?)\n##",
    "external": r"##\s*(?:External|Net Foreign Assets)[^\n]*\n([\s\S]*?)\n##",
    "political": r"##\s*Political[^\n]*\n([\s\S]*?)\n##",
    "banking": r"##[#]?\s*(?:Banking|Financial)[^\n]*\n([\s\S]*?)\n##",
    "outlook": r"##[#]?\s*(?:Outlook|Scenario)[^\n]*\n([\s\S]*?)\n##",
    # These are h3 subsections - require exactly ###
    "strengths": r"###\s*Credit Strengths?\s*\n([\s\S]*?)\n###",
    "vulnerabilities": r"###\s*Credit (?:Vulnerabilities?|Challenges?|Risks?)\s*\n([\s\S]*?)\n##",
}

# Keep SECTION_PATTERNS for backward compatibility
SECTION_PATTERNS = HTML_SECTION_PATTERNS


def _strip_html(html: str) -> str:
    """Remove HTML tags and clean up text"""
    # Remove script and style elements
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html)
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    # Decode HTML entities
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")
    text = text.replace('&nbsp;', ' ')
    return text.strip()


def _find_report_file(country: str) -> Optional[Path]:
    """Find the report file for a country (HTML or Markdown)"""
    if not REPORTS_DIR.exists():
        logger.error(f"Reports directory not found: {REPORTS_DIR}")
        return None

    # Try exact match first - prefer HTML, then MD
    patterns = [
        f"{country}.html",
        f"{country}_Moodys_With_Charts.html",
        f"{country}_Moodys_With_Charts.md",
        f"{country}_Moodys_Style.md",
    ]
    for pattern in patterns:
        path = REPORTS_DIR / pattern
        if path.exists():
            return path

    # Try case-insensitive match for HTML first
    country_lower = country.lower().replace(" ", "").replace("_", "")
    for file in REPORTS_DIR.glob("*.html"):
        if file.stem.lower() in ['index', 'test', 'preview', 'public_index']:
            continue
        file_name = file.stem.lower().replace(" ", "").replace("_", "")
        # Handle _Moodys_With_Charts suffix
        file_name = file_name.replace("moodys", "").replace("with", "").replace("charts", "").replace("style", "")
        if file_name == country_lower or file_name.startswith(country_lower):
            return file

    # Fall back to markdown files
    for file in REPORTS_DIR.glob("*_Moodys_*.md"):
        file_name = file.stem.lower().replace(" ", "").replace("_", "")
        file_name = file_name.replace("moodys", "").replace("with", "").replace("charts", "").replace("style", "")
        if file_name == country_lower or file_name.startswith(country_lower):
            return file

    return None


@lru_cache(maxsize=50)
def _load_report(country: str) -> Optional[tuple]:
    """Load and cache a report's content. Returns (content, format) tuple."""
    # Try local file first
    file_path = _find_report_file(country)
    if file_path:
        try:
            content = file_path.read_text(encoding='utf-8')
            file_format = "html" if file_path.suffix.lower() == ".html" else "md"
            return (content, file_format)
        except Exception as e:
            logger.error(f"Error reading report for {country}: {e}")

    # Fall back to Lexa MCP (Railway deployment)
    if IS_RAILWAY:
        report = _fetch_report_from_lexa(country)
        if report:
            return (report, "text")

    return None


def _fetch_report_from_lexa(country: str) -> Optional[str]:
    """
    Fetch a full report from Lexa MCP by asking for all sections.

    Used when running on Railway where local files aren't available.
    """
    try:
        # Normalize country name for Lexa (removes spaces)
        country_normalized = country.replace(" ", "")

        # Ask Lexa to provide the full report content
        response = requests.post(
            f"{LEXA_MCP_URL}/api/ask",
            json={
                "question": f"Provide the complete credit report content for {country}. Include all sections: executive summary, ratings, economic analysis, fiscal position, external position, political/institutional, banking sector, outlook, strengths, and vulnerabilities. Give me the full detailed content.",
                "country": country_normalized,
                "max_words": 5000  # Request full content
            },
            timeout=60
        )

        if response.status_code == 200:
            data = response.json()
            answer = data.get("answer", "")
            if answer and len(answer) > 500:  # Ensure we got substantial content
                logger.info(f"Fetched report for {country} from Lexa MCP ({len(answer)} chars)")
                return answer
            else:
                logger.warning(f"Lexa MCP returned insufficient content for {country}")
        else:
            logger.warning(f"Lexa MCP returned {response.status_code} for {country}")

    except Exception as e:
        logger.error(f"Error fetching report from Lexa MCP for {country}: {e}")

    return None


def _get_lexa_countries() -> List[str]:
    """Get list of available countries from Lexa MCP."""
    try:
        response = requests.get(f"{LEXA_MCP_URL}/api/countries", timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("countries", [])
    except Exception as e:
        logger.error(f"Error fetching countries from Lexa MCP: {e}")
    return []


def list_available_countries() -> Dict[str, Any]:
    """
    List all available sovereign credit reports.

    Returns:
        Dict with countries list and count
    """
    logger.info(f"list_available_countries called, IS_RAILWAY={IS_RAILWAY}")

    # Use Lexa MCP when running on Railway
    if IS_RAILWAY:
        logger.info(f"Using Lexa MCP fallback at {LEXA_MCP_URL}")
        countries = _get_lexa_countries()
        if countries:
            return {
                "countries": countries,
                "count": len(countries),
                "source": "lexa_mcp"
            }
        return {"error": "Could not fetch countries from Lexa MCP", "countries": [], "is_railway": True, "lexa_url": LEXA_MCP_URL}

    if not REPORTS_DIR.exists():
        return {"error": f"Reports directory not found: {REPORTS_DIR}", "countries": [], "is_railway": IS_RAILWAY}

    countries_set = set()

    # Check HTML files
    for file in REPORTS_DIR.glob("*.html"):
        if file.stem.lower() in ['index', 'test', 'preview', 'public_index']:
            continue
        if 'test' in file.stem.lower() or 'preview' in file.stem.lower():
            continue
        name = file.stem.replace("_Moodys_With_Charts", "").replace("_", " ")
        countries_set.add(name)

    # Check markdown files
    for file in REPORTS_DIR.glob("*_Moodys_*.md"):
        name = file.stem.replace("_Moodys_With_Charts", "").replace("_Moodys_Style", "").replace("_", " ")
        countries_set.add(name)

    countries = sorted(list(countries_set))

    return {
        "countries": countries,
        "count": len(countries),
        "reports_dir": str(REPORTS_DIR)
    }


def get_sovereign_report(country: str) -> Dict[str, Any]:
    """
    Get the full sovereign credit report for a country.

    Args:
        country: Country name (e.g., "Brazil", "Kazakhstan")

    Returns:
        Dict with report text and metadata
    """
    result = _load_report(country)
    if not result:
        available = list_available_countries()
        return {
            "error": f"Report not found for '{country}'",
            "available_countries": available.get("countries", [])
        }

    content, file_format = result

    if file_format == "html":
        text = _strip_html(content)
        # Extract title from HTML
        title_match = re.search(r'<title>(.*?)</title>', content, re.IGNORECASE)
        title = title_match.group(1) if title_match else f"{country} Sovereign Credit Report"
    else:
        # Markdown - use content as-is (already text)
        text = content
        # Extract title from first # heading
        title_match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
        title = title_match.group(1) if title_match else f"{country} Sovereign Credit Report"

    return {
        "country": country,
        "title": title,
        "content": text,
        "format": file_format,
        "char_count": len(text),
        "word_count": len(text.split())
    }


def get_sovereign_section(country: str, section: str) -> Dict[str, Any]:
    """
    Get a specific section from a sovereign credit report.

    Args:
        country: Country name
        section: Section name (summary, ratings, economic, fiscal, external,
                 political, banking, outlook, strengths, vulnerabilities)

    Returns:
        Dict with section content
    """
    result = _load_report(country)
    if not result:
        return {"error": f"Report not found for '{country}'"}

    content_raw, file_format = result

    section_lower = section.lower()
    if section_lower not in HTML_SECTION_PATTERNS:
        return {
            "error": f"Unknown section '{section}'",
            "available_sections": list(HTML_SECTION_PATTERNS.keys())
        }

    # Use appropriate patterns based on file format
    if file_format == "html":
        pattern = HTML_SECTION_PATTERNS[section_lower]
        match = re.search(pattern, content_raw, re.DOTALL | re.IGNORECASE)
        if match:
            section_content = _strip_html(match.group(1))
        else:
            section_content = None
    else:
        # Markdown format
        pattern = MD_SECTION_PATTERNS[section_lower]
        match = re.search(pattern, content_raw, re.DOTALL | re.IGNORECASE)
        if match:
            section_content = match.group(1).strip()
        else:
            section_content = None

    if not section_content:
        return {
            "country": country,
            "section": section,
            "content": f"Section '{section}' not found in report",
            "found": False
        }

    return {
        "country": country,
        "section": section,
        "content": section_content,
        "found": True,
        "word_count": len(section_content.split())
    }


def search_sovereign_reports(query: str, max_results: int = 5) -> Dict[str, Any]:
    """
    Search across all sovereign credit reports for a query.

    Args:
        query: Search term or phrase
        max_results: Maximum number of results per country

    Returns:
        Dict with search results grouped by country
    """
    available = list_available_countries()
    if "error" in available:
        return available

    results = []
    query_lower = query.lower()

    for country in available["countries"]:
        report = get_sovereign_report(country)
        if "error" in report:
            continue

        content = report["content"]
        content_lower = content.lower()

        # Find all matches
        matches = []
        start = 0
        while True:
            pos = content_lower.find(query_lower, start)
            if pos == -1:
                break

            # Extract context (200 chars before and after)
            context_start = max(0, pos - 200)
            context_end = min(len(content), pos + len(query) + 200)
            context = content[context_start:context_end]

            # Add ellipsis if truncated
            if context_start > 0:
                context = "..." + context
            if context_end < len(content):
                context = context + "..."

            matches.append({
                "position": pos,
                "context": context.strip()
            })

            start = pos + 1
            if len(matches) >= max_results:
                break

        if matches:
            results.append({
                "country": country,
                "match_count": len(matches),
                "matches": matches
            })

    # Sort by match count
    results.sort(key=lambda x: x["match_count"], reverse=True)

    return {
        "query": query,
        "countries_searched": len(available["countries"]),
        "countries_with_matches": len(results),
        "results": results
    }


def get_sovereign_comparison(countries: List[str]) -> Dict[str, Any]:
    """
    Get key metrics comparison across multiple countries.

    Args:
        countries: List of country names to compare

    Returns:
        Dict with comparison data
    """
    comparison = []

    for country in countries:
        report = get_sovereign_report(country)
        if "error" in report:
            comparison.append({
                "country": country,
                "error": report["error"]
            })
            continue

        # Extract key sections for comparison
        ratings = get_sovereign_section(country, "ratings")
        outlook = get_sovereign_section(country, "outlook")
        strengths = get_sovereign_section(country, "strengths")
        vulnerabilities = get_sovereign_section(country, "vulnerabilities")

        comparison.append({
            "country": country,
            "ratings_summary": ratings.get("content", "")[:500] if ratings.get("found") else "N/A",
            "outlook_summary": outlook.get("content", "")[:500] if outlook.get("found") else "N/A",
            "key_strengths": strengths.get("content", "")[:300] if strengths.get("found") else "N/A",
            "key_risks": vulnerabilities.get("content", "")[:300] if vulnerabilities.get("found") else "N/A",
        })

    return {
        "countries": countries,
        "comparison": comparison
    }


# ============================================================================
# LLM-Powered Report Analysis
# ============================================================================

def _get_report_analysis_client():
    """
    Get FallbackLLMClient configured for report analysis.

    Uses Gemini 2.5 Flash as primary (large context + caching benefits),
    with Claude Sonnet and GPT-4o-mini as fallbacks.
    """
    import sys
    MCP_CENTRAL_PATH = "/Users/andyseaman/Notebooks/mcp_central"
    if MCP_CENTRAL_PATH not in sys.path:
        sys.path.insert(0, MCP_CENTRAL_PATH)

    # Try auth_mcp first (local development)
    try:
        from auth_mcp import FallbackLLMClient
        logger.debug("Using FallbackLLMClient from auth_mcp")
        client = FallbackLLMClient(purpose="report_analysis", requester="orca-mcp-reports")
        client.initialize()
        return client
    except ImportError:
        pass

    # Fall back to local copy (Railway deployment)
    try:
        from orca_mcp.tools.fallback_client import FallbackLLMClient
        logger.debug("Using FallbackLLMClient from local tools/")
        client = FallbackLLMClient(purpose="report_analysis", requester="orca-mcp-reports")
        client.initialize()
        return client
    except ImportError:
        pass

    # Last resort - relative import
    try:
        from .fallback_client import FallbackLLMClient
        logger.debug("Using FallbackLLMClient from relative import")
        client = FallbackLLMClient(purpose="report_analysis", requester="orca-mcp-reports")
        client.initialize()
        return client
    except Exception as e:
        logger.error(f"FallbackLLMClient import failed: {e}")
        return None


def query_sovereign_report(country: str, question: str, max_tokens: int = 2048) -> Dict[str, Any]:
    """
    Query a sovereign credit report using LLM analysis.

    Sends the full report to Gemini 2.5 Flash (with context caching) for
    intelligent Q&A. Falls back to Claude Sonnet or GPT-4o-mini if needed.

    This approach is simpler and more effective than embeddings/RAG for
    document-sized reports (10-50K tokens):
    - Gemini's 1M context easily handles full reports
    - Context caching reduces cost ~80% for repeated queries on same report
    - No embedding maintenance or vector DB needed

    Args:
        country: Country name (e.g., "Brazil", "Kazakhstan")
        question: Natural language question about the report
        max_tokens: Maximum tokens in response (default 2048)

    Returns:
        Dict with answer, sources, model used, and metadata

    Examples:
        query_sovereign_report("Brazil", "summarize the external position")
        query_sovereign_report("Turkey", "what are the main credit risks?")
        query_sovereign_report("Kazakhstan", "explain the NFA rating rationale")
    """
    # 1. Fetch the full report
    report = get_sovereign_report(country)
    if "error" in report:
        return {
            "error": report["error"],
            "country": country,
            "question": question,
            "available_countries": report.get("available_countries", [])
        }

    # 2. Get LLM client
    client = _get_report_analysis_client()
    if not client:
        return {
            "error": "LLM client unavailable. Check FallbackLLMClient configuration.",
            "country": country,
            "question": question
        }

    # 3. Build prompt with full report
    system_prompt = """You are a sovereign credit analyst assistant. Your role is to answer
questions about sovereign credit reports with precision and professionalism.

Guidelines:
- Ground all answers in the provided report content
- Use specific numbers, percentages, and data points from the report
- If the report doesn't contain information to answer the question, say so clearly
- Write in professional financial prose suitable for institutional investors
- Be concise but thorough - include all relevant data points
- When citing data, indicate the source section (e.g., "The external position section shows...")
- Do not invent or hallucinate data not present in the report"""

    user_prompt = f"""Question: {question}

=== {country} Sovereign Credit Report ===

{report['content']}

=== End of Report ===

Please answer the question based on the report above. Be specific and cite relevant data."""

    # 4. Call LLM with fallback
    try:
        response = client.chat(
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
            max_tokens=max_tokens
        )

        if not response.success:
            return {
                "error": f"LLM call failed: {response.error}",
                "country": country,
                "question": question
            }

        return {
            "country": country,
            "question": question,
            "answer": response.text,
            "model_used": f"{response.provider}/{response.model_used}",
            "report_title": report.get("title", f"{country} Credit Report"),
            "report_words": report.get("word_count", 0),
            "report_chars": report.get("char_count", 0)
        }

    except Exception as e:
        logger.error(f"Error querying report for {country}: {e}")
        return {
            "error": str(e),
            "country": country,
            "question": question
        }


def compare_sovereign_reports(
    countries: List[str],
    question: str,
    max_tokens: int = 3000
) -> Dict[str, Any]:
    """
    Compare multiple sovereign credit reports using LLM analysis.

    Fetches reports for all specified countries and sends them to the LLM
    for comparative analysis. Best for 2-4 countries to stay within context limits.

    Args:
        countries: List of country names to compare (2-4 recommended)
        question: Comparison question (e.g., "compare external positions")
        max_tokens: Maximum tokens in response

    Returns:
        Dict with comparative analysis and metadata

    Examples:
        compare_sovereign_reports(["Brazil", "Mexico"], "compare fiscal positions")
        compare_sovereign_reports(["Turkey", "South Africa", "Egypt"], "which has strongest reserves?")
    """
    if len(countries) < 2:
        return {"error": "Need at least 2 countries to compare"}

    if len(countries) > 5:
        return {"error": "Maximum 5 countries for comparison (context limits)"}

    # 1. Fetch all reports
    reports = {}
    errors = []
    total_words = 0

    for country in countries:
        report = get_sovereign_report(country)
        if "error" in report:
            errors.append(f"{country}: {report['error']}")
        else:
            reports[country] = report
            total_words += report.get("word_count", 0)

    if len(reports) < 2:
        return {
            "error": "Could not fetch enough reports for comparison",
            "details": errors
        }

    # 2. Get LLM client
    client = _get_report_analysis_client()
    if not client:
        return {"error": "LLM client unavailable"}

    # 3. Build comparative prompt
    system_prompt = """You are a sovereign credit analyst assistant specializing in
comparative analysis. Your role is to compare sovereign credit profiles objectively.

Guidelines:
- Compare and contrast directly, highlighting key differences and similarities
- Use specific numbers from each report for fair comparison
- Rank or order countries where relevant to the question
- Be balanced and objective - acknowledge strengths and weaknesses of each
- Structure your response clearly (consider using country subheadings if helpful)
- Do not invent data - only use information from the provided reports"""

    # Build report sections
    report_sections = []
    for country, report in reports.items():
        report_sections.append(f"""
=== {country} ===
{report['content']}
""")

    all_reports = "\n".join(report_sections)

    user_prompt = f"""Comparison Question: {question}

Countries: {', '.join(reports.keys())}

{all_reports}

=== End of Reports ===

Provide a comparative analysis addressing the question. Be specific with data from each report."""

    # 4. Call LLM
    try:
        response = client.chat(
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
            max_tokens=max_tokens
        )

        if not response.success:
            return {"error": f"LLM call failed: {response.error}"}

        return {
            "countries": list(reports.keys()),
            "question": question,
            "analysis": response.text,
            "model_used": f"{response.provider}/{response.model_used}",
            "total_report_words": total_words,
            "reports_fetched": len(reports),
            "errors": errors if errors else None
        }

    except Exception as e:
        logger.error(f"Error comparing reports: {e}")
        return {"error": str(e)}
