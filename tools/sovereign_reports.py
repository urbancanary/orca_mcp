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
from pathlib import Path
from typing import Dict, List, Optional, Any
from functools import lru_cache

logger = logging.getLogger(__name__)

# Default reports directory
REPORTS_DIR = Path(os.environ.get(
    "SOVEREIGN_REPORTS_DIR",
    "/Users/andyseaman/Notebooks/sovereign-credit-system/credit_reports/moodys_style"
))

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
    file_path = _find_report_file(country)
    if not file_path:
        return None
    try:
        content = file_path.read_text(encoding='utf-8')
        file_format = "html" if file_path.suffix.lower() == ".html" else "md"
        return (content, file_format)
    except Exception as e:
        logger.error(f"Error reading report for {country}: {e}")
        return None


def list_available_countries() -> Dict[str, Any]:
    """
    List all available sovereign credit reports.

    Returns:
        Dict with countries list and count
    """
    if not REPORTS_DIR.exists():
        return {"error": f"Reports directory not found: {REPORTS_DIR}", "countries": []}

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
