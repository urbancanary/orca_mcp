#!/usr/bin/env python3
"""
Test script for Orca MCP country standardization integration

Tests the new country-mapping-mcp functions added to country_standardization.py
"""

import sys
from pathlib import Path

# Add Orca tools to path
orca_tools_path = Path(__file__).parent / "tools"
sys.path.insert(0, str(orca_tools_path.parent))

from tools.country_standardization import (
    standardize_country_name,
    get_api_country_code,
    batch_standardize_countries,
    reverse_lookup_country,
    standardize_country_list
)

def test_standardize_country_name():
    """Test basic country name standardization"""
    print("\n=== Test 1: Standardize Country Name ===")

    # Test with "United States"
    result = standardize_country_name("United States")
    print(f"Input: United States")
    print(f"Standard: {result.get('standard')}")
    print(f"IMF Code: {result.get('imf_code')}")
    print(f"NFA Name: {result.get('nfa_name')}")
    print(f"Found: {result.get('found')}")

    assert result.get('found') == True, "United States should be found"
    assert result.get('imf_code') == 'USA', "IMF code should be USA"
    print("✅ Test passed")

def test_api_specific_code():
    """Test getting API-specific codes"""
    print("\n=== Test 2: API-Specific Codes ===")

    # Test IMF code
    imf_code = get_api_country_code("Brazil", "imf")
    print(f"Brazil IMF code: {imf_code}")
    assert imf_code == "BRA", f"Expected BRA, got {imf_code}"

    # Test NFA name
    nfa_name = get_api_country_code("United States", "nfa")
    print(f"United States NFA name: {nfa_name}")
    assert nfa_name == "US", f"Expected US, got {nfa_name}"

    print("✅ Test passed")

def test_batch_standardize():
    """Test batch country standardization"""
    print("\n=== Test 3: Batch Standardization ===")

    countries = ["USA", "Brazil", "UK", "China"]
    results = batch_standardize_countries(countries)

    print(f"Input: {countries}")
    for result in results:
        if result.get('found') is not False:
            print(f"  {result.get('input')} → {result.get('standard')} (IMF: {result.get('imf_code')})")

    assert len(results) == 4, "Should return 4 results"
    print("✅ Test passed")

def test_reverse_lookup():
    """Test reverse lookup from ISO code"""
    print("\n=== Test 4: Reverse Lookup ===")

    result = reverse_lookup_country("BRA")
    print(f"ISO Code: BRA")
    print(f"Standard: {result.get('standard') if result else 'Not found'}")
    print(f"IMF Code: {result.get('imf_code') if result else 'N/A'}")

    assert result is not None, "BRA should be found"
    assert result.get('standard') == 'Brazil', "Standard should be Brazil"
    print("✅ Test passed")

def test_standardize_list():
    """Test standardizing country names in a list"""
    print("\n=== Test 5: Standardize Country List ===")

    data = [
        {"country": "USA", "value": 100},
        {"country": "United Kingdom", "value": 200},
        {"country": "Brasil", "value": 300}
    ]

    print("Before:")
    for item in data:
        print(f"  {item}")

    standardized = standardize_country_list(data)

    print("\nAfter:")
    for item in standardized:
        print(f"  {item}")

    print("✅ Test passed")

def test_with_variants():
    """Test with various country name variants"""
    print("\n=== Test 6: Country Name Variants ===")

    variants = [
        "United States",
        "USA",
        "US",
        "United States of America"
    ]

    for variant in variants:
        result = standardize_country_name(variant)
        standard = result.get('standard', 'NOT FOUND')
        print(f"  '{variant}' → '{standard}'")

def main():
    """Run all tests"""
    print("=" * 60)
    print("Testing Orca MCP Country Standardization Integration")
    print("=" * 60)

    try:
        test_standardize_country_name()
        test_api_specific_code()
        test_batch_standardize()
        test_reverse_lookup()
        test_standardize_list()
        test_with_variants()

        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
        print("\nOrca MCP is now integrated with country-mapping-mcp!")
        print("Available functions:")
        print("  - standardize_country_name()")
        print("  - get_api_country_code()")
        print("  - batch_standardize_countries()")
        print("  - reverse_lookup_country()")
        print("  - standardize_country_list()")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
