#!/usr/bin/env python3
"""
Test Country Eligibility System

Tests the eligibility checking with realistic scenarios
"""

import sys
from pathlib import Path

# Add tools to path
sys.path.insert(0, str(Path(__file__).parent))

from tools.country_eligibility import (
    check_country_eligibility,
    list_eligible_countries,
    list_ineligible_countries,
    filter_bonds_by_eligibility,
    get_eligibility_stats
)

def test_romania_check():
    """Test Romania eligibility (should be rejected due to twin deficits override)"""
    print("\n=== Test 1: Romania Eligibility Check ===")
    print("Romania has 3-star NFA rating BUT twin deficits → manual override to INELIGIBLE")

    result = check_country_eligibility("Romania")

    print(f"\nCountry: {result['country']}")
    print(f"Eligible: {result['eligible']}")
    print(f"NFA Rating: {result['nfa_rating']}-star (good, but overridden)")
    print(f"Reason: {result['reason']}")
    print(f"Guidance: {result['guidance']}")

    assert result['eligible'] == False, "Romania should be ineligible (twin deficits override)"
    assert result['nfa_rating'] == 3, "Romania has 3-star rating (but still ineligible)"
    assert result.get('override') == True, "Should show manual override"
    print("\n✅ Test passed - Romania correctly rejected despite 3-star NFA rating")

def test_germany_check():
    """Test Germany eligibility (should be approved)"""
    print("\n=== Test 2: Germany Eligibility Check ===")

    result = check_country_eligibility("Germany")

    print(f"Country: {result['country']}")
    print(f"Eligible: {result['eligible']}")
    print(f"NFA Rating: {result['nfa_rating']}")
    print(f"Reason: {result['reason']}")
    print(f"Guidance: {result['guidance']}")

    assert result['eligible'] == True, "Germany should be eligible"
    assert result['nfa_rating'] >= 3, "Germany should have 3+ star rating"
    print("✅ Test passed")

def test_list_eligible():
    """Test listing eligible countries"""
    print("\n=== Test 3: List Eligible Countries (5+ stars) ===")

    eligible = list_eligible_countries(min_rating=5)

    print(f"Found {len(eligible)} countries with 5+ star ratings:")
    for country in eligible[:10]:
        print(f"  {country['country']}: {country['nfa_rating']}-star, {country['risk_level']}")

    assert len(eligible) > 0, "Should have some 5+ star countries"
    print("✅ Test passed")

def test_list_ineligible():
    """Test listing ineligible countries"""
    print("\n=== Test 4: List Ineligible Countries ===")

    ineligible = list_ineligible_countries()

    print(f"Found {len(ineligible)} ineligible countries:")

    # Separate into auto-ineligible and overridden
    auto_ineligible = [c for c in ineligible if c.get('nfa_rating', 0) <= 2]
    overridden = [c for c in ineligible if c.get('override') == True]

    print(f"  Auto-ineligible (1-2 stars): {len(auto_ineligible)}")
    print(f"  Manual overrides: {len(overridden)}")

    print(f"\nFirst 10 ineligible countries:")
    for country in ineligible[:10]:
        override_marker = " [OVERRIDE]" if country.get('override') else ""
        print(f"  {country['country']}: {country['nfa_rating']}-star{override_marker}")

    assert len(ineligible) > 0, "Should have ineligible countries"
    assert len(auto_ineligible) > 0, "Should have auto-ineligible (1-2 star) countries"
    print("\n✅ Test passed")

def test_bond_filtering():
    """Test filtering bonds by eligibility"""
    print("\n=== Test 5: Filter Bonds by Eligibility ===")

    # Mock bond watchlist
    bonds = [
        {"isin": "DE0001102440", "country": "Germany", "yield": 3.5},
        {"isin": "RO123456789", "country": "Romania", "yield": 7.0},
        {"isin": "US912828Z490", "country": "US", "yield": 4.2},
        {"isin": "BR123456789", "country": "Brazil", "yield": 6.5},
    ]

    result = filter_bonds_by_eligibility(bonds)

    print(f"\nTotal bonds: {result['summary']['total']}")
    print(f"Eligible: {result['summary']['eligible_count']}")
    print(f"Ineligible: {result['summary']['ineligible_count']}")

    print("\n✅ Eligible bonds:")
    for bond in result['eligible']:
        print(f"  {bond['isin']} ({bond['country']}) - {bond['yield']}%")

    print("\n🚫 Ineligible bonds:")
    for bond in result['ineligible']:
        print(f"  {bond['isin']} ({bond['country']}) - {bond['yield']}%")

    print("\n⚠️ Warnings:")
    for warning in result['warnings']:
        print(f"  {warning}")

    assert result['summary']['ineligible_count'] > 0, "Should have ineligible bonds"
    print("\n✅ Test passed")

def test_stats():
    """Test eligibility statistics"""
    print("\n=== Test 6: Eligibility Statistics ===")

    stats = get_eligibility_stats()

    print(f"Total countries: {stats['total']}")
    print(f"Eligible: {stats['eligible']} ({stats['eligible']/stats['total']*100:.1f}%)")
    print(f"Ineligible: {stats['ineligible']} ({stats['ineligible']/stats['total']*100:.1f}%)")

    print("\nBy Rating:")
    for rating, count in sorted(stats['by_rating'].items()):
        print(f"  {rating}: {count}")

    print("✅ Test passed")

def test_conversational_scenario():
    """Test conversational portfolio building scenario"""
    print("\n" + "="*60)
    print("=== Conversational Portfolio Building Scenario ===")
    print("="*60)

    print("\n👤 User: 'Add Romania bond with 3% allocation'")
    print("\n🤖 Claude checks eligibility...")

    result = check_country_eligibility("Romania")

    print(f"\n🤖 Assistant Response:")
    print(f"   {result['guidance']}")
    print(f"\n   Details:")
    print(f"   - NFA Rating: {result['nfa_rating']}-star")
    print(f"   - NFA/GDP: {result['nfa_percentage']:.1f}%")
    print(f"   - Reason: {result['reason']}")
    print(f"\n   Would you like to proceed anyway? (Not recommended)")

    print("\n" + "="*60)
    print("\n👤 User: 'How about Germany instead?'")
    print("\n🤖 Claude checks eligibility...")

    result = check_country_eligibility("Germany")

    print(f"\n🤖 Assistant Response:")
    print(f"   {result['guidance']}")
    print(f"\n   Details:")
    print(f"   - NFA Rating: {result['nfa_rating']}-star")
    print(f"   - NFA/GDP: {result['nfa_percentage']:.1f}%")
    print(f"   - Reason: {result['reason']}")
    print(f"\n   ✅ Proceeding to add Germany bond to portfolio...")

    print("\n✅ Scenario test passed")

def main():
    """Run all tests"""
    print("="*60)
    print("Testing Country Eligibility System")
    print("="*60)

    try:
        test_romania_check()
        test_germany_check()
        test_list_eligible()
        test_list_ineligible()
        test_bond_filtering()
        test_stats()
        test_conversational_scenario()

        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED")
        print("="*60)
        print("\nCountry eligibility system is ready!")
        print("\nKey Functions:")
        print("  - check_country_eligibility(country)")
        print("  - list_eligible_countries(min_rating)")
        print("  - list_ineligible_countries()")
        print("  - filter_bonds_by_eligibility(bonds)")
        print("  - get_eligibility_stats()")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
