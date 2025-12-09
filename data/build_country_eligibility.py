#!/usr/bin/env python3
"""
Build Country Eligibility Mapping from NFA Ratings

Rule: 1-2 star NFA ratings = ineligible
      3+ star ratings = eligible (with notes)
"""

import json
from pathlib import Path
from datetime import datetime

# Load NFA test results
NFA_RESULTS_PATH = Path("/Users/andyseaman/Notebooks/sovereign-credit-system/nfa_api_test_results.json")
OUTPUT_PATH = Path(__file__).parent / "country_eligibility.json"

def get_eligibility_reason(rating: str, nfa_percentage: float, country: str) -> dict:
    """Generate eligibility status and reason based on NFA rating"""

    rating_int = int(rating) if rating else 0

    # 1-2 stars = reject (ineligible)
    if rating_int <= 2:
        return {
            "eligible": False,
            "status": "reject",
            "reason": f"{rating}-star NFA rating. High external debt burden ({nfa_percentage:.1f}% NFA/GDP). Ineligible for investment.",
            "risk_level": "high"
        }

    # 3 stars = borderline (eligible with caution)
    elif rating_int == 3:
        return {
            "eligible": True,
            "status": "include",
            "reason": f"{rating}-star NFA rating. Moderate risk ({nfa_percentage:.1f}% NFA/GDP). Monitor closely.",
            "risk_level": "medium"
        }

    # 4-5 stars = good (eligible)
    elif rating_int in [4, 5]:
        return {
            "eligible": True,
            "status": "include",
            "reason": f"{rating}-star NFA rating. Acceptable risk profile ({nfa_percentage:.1f}% NFA/GDP).",
            "risk_level": "low"
        }

    # 6-7 stars = excellent (highly eligible)
    else:
        return {
            "eligible": True,
            "status": "include",
            "reason": f"{rating}-star NFA rating. Strong external position ({nfa_percentage:.1f}% NFA/GDP). Preferred for investment.",
            "risk_level": "very_low"
        }

def build_eligibility_mapping():
    """Build country eligibility mapping from NFA data"""

    print("Loading NFA test results...")
    with open(NFA_RESULTS_PATH) as f:
        nfa_results = json.load(f)

    print(f"Found {len(nfa_results)} countries with NFA data")

    eligibility_map = {}
    stats = {
        "total": 0,
        "eligible": 0,
        "ineligible": 0,
        "by_rating": {}
    }

    for country_data in nfa_results:
        if not country_data.get("success"):
            continue

        country = country_data["short_name"]
        rating = country_data.get("rating", "0")
        nfa_percentage = country_data.get("nfa_percentage", 0)

        # Get eligibility info
        eligibility_info = get_eligibility_reason(rating, nfa_percentage, country)

        # Build full entry
        eligibility_map[country] = {
            **eligibility_info,
            "nfa_rating": int(rating) if rating else None,
            "nfa_percentage": nfa_percentage,
            "last_updated": datetime.now().isoformat(),
            "source": "NFA MCP auto-generated"
        }

        # Update stats
        stats["total"] += 1
        if eligibility_info["eligible"]:
            stats["eligible"] += 1
        else:
            stats["ineligible"] += 1

        rating_key = f"{rating}-star"
        stats["by_rating"][rating_key] = stats["by_rating"].get(rating_key, 0) + 1

    # Print summary
    print("\n" + "="*60)
    print("Country Eligibility Summary")
    print("="*60)
    print(f"Total countries: {stats['total']}")
    print(f"Eligible (3+ stars): {stats['eligible']} ({stats['eligible']/stats['total']*100:.1f}%)")
    print(f"Ineligible (1-2 stars): {stats['ineligible']} ({stats['ineligible']/stats['total']*100:.1f}%)")
    print("\nBy Rating:")
    for rating, count in sorted(stats["by_rating"].items()):
        pct = count/stats['total']*100
        print(f"  {rating}: {count} ({pct:.1f}%)")

    # Show some examples
    print("\n" + "="*60)
    print("Example Countries:")
    print("="*60)

    # Ineligible examples
    print("\n🚫 INELIGIBLE (1-2 stars):")
    ineligible = [(c, d) for c, d in eligibility_map.items() if not d["eligible"]]
    for country, data in sorted(ineligible[:5]):
        print(f"  {country}: {data['reason']}")

    # Eligible examples
    print("\n✅ ELIGIBLE (3+ stars):")
    eligible = [(c, d) for c, d in eligibility_map.items() if d["eligible"]]
    for country, data in sorted(eligible[:5]):
        print(f"  {country}: {data['reason']}")

    # Save to JSON
    output_data = {
        "version": "1.0.0",
        "description": "Country eligibility for bond portfolio investment based on NFA ratings",
        "rule": "1-2 star NFA ratings = ineligible, 3+ stars = eligible",
        "generated_at": datetime.now().isoformat(),
        "source": "NFA MCP",
        "statistics": stats,
        "countries": eligibility_map
    }

    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output_data, f, indent=2)

    print(f"\n✅ Saved to: {OUTPUT_PATH}")
    print(f"File size: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")

    return eligibility_map, stats

if __name__ == "__main__":
    eligibility_map, stats = build_eligibility_mapping()
