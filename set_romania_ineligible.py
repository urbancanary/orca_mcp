#!/usr/bin/env python3
"""
Manual Country Eligibility Override - Romania Example

Even though Romania has a 3-star NFA rating (technically eligible),
we want to mark it as ineligible due to current risk Y classification.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from tools.country_eligibility import set_country_eligibility, check_country_eligibility

print("="*60)
print("Manual Override: Romania Eligibility")
print("="*60)

# Check current status
print("\n1. Current Romania Status:")
current = check_country_eligibility("Romania")
print(f"   Eligible: {current['eligible']}")
print(f"   NFA Rating: {current['nfa_rating']}-star")
print(f"   Reason: {current['reason']}")

# Override to make ineligible
print("\n2. Overriding to INELIGIBLE...")
print("   Reason: Risk Y country - high political risk, fiscal concerns")

result = set_country_eligibility(
    country="Romania",
    eligible=False,
    reason="Risk Y country - high political risk, fiscal concerns. Even with 3-star NFA rating, current conditions make this unsuitable for investment.",
    override=True
)

print("\n3. Updated Romania Status:")
print(f"   Eligible: {result['eligible']}")
print(f"   Status: {result['status']}")
print(f"   NFA Rating: {result['nfa_rating']}-star (unchanged)")
print(f"   Reason: {result['reason']}")
print(f"   Override: {result['override']}")

# Verify the change
print("\n4. Verification:")
updated = check_country_eligibility("Romania")
print(f"   {updated['guidance']}")

print("\n" + "="*60)
print("✅ Romania is now marked as INELIGIBLE")
print("="*60)
print("\nClaude will now strongly advise against Romania bonds,")
print("regardless of the 3-star NFA rating.")
