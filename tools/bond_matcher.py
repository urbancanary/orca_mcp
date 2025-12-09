"""
Bond Matcher - Athena's Intelligence Layer for Bond Resolution

Server-side NLP and fuzzy matching for bond references in chat commands.
This module runs on Orca infrastructure (not delivered to clients).

Parses natural language bond references like:
- "3% colombia 61"
- "COLTES 3.25 2061"
- "sell half mexico"
- "US91086QBC23" (ISIN)

Works on both holdings (sells) and watchlist (buys).

Architecture:
- Client sends raw text query to Orca API
- Orca performs NLP parsing + fuzzy matching
- Returns structured match results to client
- Client displays options to user (dumb terminal pattern)
"""

import re
from typing import Optional, List, Dict, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class BondMatch:
    """A matched bond with confidence score"""
    isin: str
    ticker: str
    description: str
    country: str
    coupon: float
    maturity_year: int
    score: float  # 0-100
    match_reasons: List[str]  # Why this matched
    price: Optional[float] = None  # Clean price
    accrued: Optional[float] = None  # Accrued interest

    @property
    def dirty_price(self) -> float:
        """Dirty price = clean price + accrued interest"""
        clean = self.price if self.price is not None else 100.0
        acc = self.accrued if self.accrued is not None else 0.0
        return clean + acc

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict for API response"""
        return {
            'isin': self.isin,
            'ticker': self.ticker,
            'description': self.description,
            'country': self.country,
            'coupon': self.coupon,
            'maturity_year': self.maturity_year,
            'score': self.score,
            'match_reasons': self.match_reasons,
            'price': self.price,
            'accrued': self.accrued,
            'dirty_price': self.dirty_price
        }

    def __repr__(self):
        return f"{self.ticker} {self.coupon}% {self.maturity_year} ({self.country}) - Score: {self.score:.0f}"


@dataclass
class ParsedTradeIntent:
    """Parsed trade command"""
    action: str  # 'buy', 'sell', 'query'
    bond_query: str  # The bond reference part
    quantity_type: str  # 'par', 'percent', 'all', 'half'
    quantity_value: Optional[float]  # Amount if specified
    raw_input: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict for API response"""
        return {
            'action': self.action,
            'bond_query': self.bond_query,
            'quantity_type': self.quantity_type,
            'quantity_value': self.quantity_value,
            'raw_input': self.raw_input
        }


# Country name variations and aliases - EM expertise embedded in Orca
COUNTRY_ALIASES = {
    'colombia': 'Colombia',
    'colom': 'Colombia',
    'col': 'Colombia',
    'mexico': 'Mexico',
    'mex': 'Mexico',
    'brazil': 'Brazil',
    'braz': 'Brazil',
    'chile': 'Chile',
    'peru': 'Peru',
    'panama': 'Panama',
    'saudi': 'Saudi Arabia',
    'ksa': 'Saudi Arabia',
    'uae': 'United Arab Emirates',
    'emirates': 'United Arab Emirates',
    'dubai': 'United Arab Emirates',
    'qatar': 'Qatar',
    'oman': 'Oman',
    'bahrain': 'Bahrain',
    'israel': 'Israel',
    'turkey': 'Turkey',
    'turkiye': 'Turkey',
    'indonesia': 'Indonesia',
    'indo': 'Indonesia',
    'philippines': 'Philippines',
    'phils': 'Philippines',
    'kazakh': 'Kazakhstan',
    'kaz': 'Kazakhstan',
    'south africa': 'South Africa',
    'sa': 'South Africa',
    'nigeria': 'Nigeria',
    'egypt': 'Egypt',
    'morocco': 'Morocco',
    'ivory': 'Ivory Coast',
    'cote': 'Ivory Coast',
    'dominican': 'Dominican Republic',
    'dom rep': 'Dominican Republic',
    'uruguay': 'Uruguay',
    'paraguay': 'Paraguay',
    'argentina': 'Argentina',
    'argie': 'Argentina',
}


def extract_isin(text: str) -> Optional[str]:
    """Extract ISIN if present (12 char alphanumeric starting with 2 letters)"""
    # ISIN pattern: 2 letters + 10 alphanumeric
    pattern = r'\b([A-Z]{2}[A-Z0-9]{10})\b'
    match = re.search(pattern, text.upper())
    return match.group(1) if match else None


def extract_coupon(text: str) -> Optional[float]:
    """
    Extract coupon rate from text.
    Handles: "3%", "3.25%", "3 1/4", "3.25", "3 1/2%"
    """
    # Try percentage format first: "3.25%" or "3%"
    pct_match = re.search(r'(\d+(?:\.\d+)?)\s*%', text)
    if pct_match:
        return float(pct_match.group(1))

    # Try fraction format: "3 1/4" or "3 1/2"
    frac_match = re.search(r'(\d+)\s+(\d)/(\d)', text)
    if frac_match:
        whole = int(frac_match.group(1))
        num = int(frac_match.group(2))
        denom = int(frac_match.group(3))
        return whole + (num / denom)

    # Try standalone decimal that looks like a coupon (1-15 range)
    # Be careful not to match years or amounts
    dec_match = re.search(r'\b(\d+\.\d+)\b', text)
    if dec_match:
        val = float(dec_match.group(1))
        if 0.5 <= val <= 15:  # Reasonable coupon range
            return val

    return None


def extract_year(text: str) -> Optional[int]:
    """
    Extract maturity year from text.
    Handles: "2061", "61", "'61", "05/15/61"
    """
    current_year = datetime.now().year
    current_century = (current_year // 100) * 100

    # Try full 4-digit year first
    full_year = re.search(r'\b(20\d{2})\b', text)
    if full_year:
        return int(full_year.group(1))

    # Try 2-digit year with apostrophe: '61
    apos_year = re.search(r"'(\d{2})\b", text)
    if apos_year:
        yy = int(apos_year.group(1))
        return current_century + yy if yy >= 0 else current_century - 100 + yy

    # Try date format: MM/DD/YY or DD/MM/YY
    date_match = re.search(r'\d{1,2}/\d{1,2}/(\d{2})\b', text)
    if date_match:
        yy = int(date_match.group(1))
        return current_century + yy

    # Try standalone 2-digit at end of text (common: "colombia 61")
    # Must be after stripping other patterns
    end_year = re.search(r'\b(\d{2})\s*$', text.strip())
    if end_year:
        yy = int(end_year.group(1))
        # Assume 20xx for reasonable bond maturities
        if 24 <= yy <= 99:  # 2024-2099
            return 2000 + yy
        elif 0 <= yy <= 23:  # Could be 2100+ but unlikely
            return 2100 + yy

    return None


def extract_country(text: str) -> Optional[str]:
    """Extract country from text using aliases"""
    text_lower = text.lower()

    # Check each alias
    for alias, country in COUNTRY_ALIASES.items():
        # Use word boundary matching for short aliases
        if len(alias) <= 3:
            if re.search(rf'\b{alias}\b', text_lower):
                return country
        else:
            if alias in text_lower:
                return country

    return None


def extract_ticker_part(text: str) -> Optional[str]:
    """Extract ticker-like patterns (uppercase letters, maybe numbers)"""
    # Common EM bond ticker patterns
    ticker_match = re.search(r'\b([A-Z]{3,10}(?:\s+\d)?)\b', text.upper())
    if ticker_match:
        return ticker_match.group(1)
    return None


def parse_trade_intent(text: str) -> ParsedTradeIntent:
    """
    Parse a trade command into structured intent.

    Examples:
    - "buy 500k colombia 61" -> action=buy, qty_type=par, qty_value=500000
    - "sell half mexico" -> action=sell, qty_type=half
    - "sell all PEMEX 27" -> action=sell, qty_type=all
    - "buy 3% of portfolio in chile" -> action=buy, qty_type=percent, qty_value=3
    """
    text_lower = text.lower().strip()

    # Determine action
    action = 'query'  # default
    if text_lower.startswith('buy') or 'purchase' in text_lower:
        action = 'buy'
    elif text_lower.startswith('sell') or 'reduce' in text_lower:
        action = 'sell'

    # Extract quantity
    quantity_type = 'par'
    quantity_value = None

    # Check for "all"
    if re.search(r'\ball\b', text_lower):
        quantity_type = 'all'
    # Check for "half"
    elif re.search(r'\bhalf\b', text_lower):
        quantity_type = 'half'
    # Check for percentage of portfolio: "3% of portfolio", "3 percent"
    elif re.search(r'(\d+(?:\.\d+)?)\s*%?\s*(?:of\s+)?(?:portfolio|nav|aum)', text_lower):
        match = re.search(r'(\d+(?:\.\d+)?)\s*%?\s*(?:of\s+)?(?:portfolio|nav|aum)', text_lower)
        quantity_type = 'percent'
        quantity_value = float(match.group(1))
    # Check for par amount: "500k", "1m", "500000", "1,000,000"
    else:
        # Try K/M notation: 500k, 1m, 1.5m
        km_match = re.search(r'(\d+(?:\.\d+)?)\s*([km])\b', text_lower)
        if km_match:
            val = float(km_match.group(1))
            multiplier = 1000 if km_match.group(2) == 'k' else 1000000
            quantity_value = val * multiplier
            quantity_type = 'par'
        else:
            # Try plain number (with optional commas)
            num_match = re.search(r'\b(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\b', text)
            if num_match:
                val_str = num_match.group(1).replace(',', '')
                val = float(val_str)
                if val >= 1000:  # Assume par amount if >= 1000
                    quantity_value = val
                    quantity_type = 'par'

    # Extract bond reference (remove action and quantity parts)
    bond_query = text_lower
    # Remove action words
    bond_query = re.sub(r'^(buy|sell|purchase|reduce)\s+', '', bond_query)
    # Remove quantity patterns (K/M notation)
    bond_query = re.sub(r'\d+(?:\.\d+)?\s*[km]\b', '', bond_query)
    # Remove large standalone numbers (par amounts), but keep small numbers that could be coupons/years
    bond_query = re.sub(r'\b\d{4,}\b', '', bond_query)  # 4+ digit numbers only
    bond_query = re.sub(r'\d{1,3}(?:,\d{3})+(?:\.\d+)?', '', bond_query)  # Comma-separated numbers
    bond_query = re.sub(r'\ball\b|\bhalf\b', '', bond_query)
    bond_query = re.sub(r'%?\s*(?:of\s+)?(?:portfolio|nav|aum)', '', bond_query)
    bond_query = re.sub(r'\s+', ' ', bond_query).strip()

    return ParsedTradeIntent(
        action=action,
        bond_query=bond_query,
        quantity_type=quantity_type,
        quantity_value=quantity_value,
        raw_input=text
    )


class BondMatcher:
    """
    Matches fuzzy bond references against a list of bonds.

    Server-side Usage (Orca Worker):
        bonds = [{'isin': '...', 'ticker': '...', ...}, ...]
        matcher = BondMatcher(bonds)
        matches = matcher.match("3% colombia 61")

    Returns JSON-serializable results for API response.
    """

    # Score thresholds
    CONFIDENT_THRESHOLD = 70  # Single match above this = auto-select
    MINIMUM_THRESHOLD = 30    # Below this = no match

    def __init__(self, bonds: List[Dict[str, Any]]):
        """
        Initialize with a list of bond dictionaries.

        Expected keys:
        - isin
        - ticker
        - description
        - country
        - coupon (or extract from description)
        - maturity_date or maturity_year
        """
        self.bonds = bonds
        self._prepare_bonds()

    def _prepare_bonds(self):
        """Prepare bonds for matching"""
        if not self.bonds:
            return

        for bond in self.bonds:
            # Ensure coupon exists
            if 'coupon' not in bond or bond['coupon'] is None:
                if 'description' in bond and bond['description']:
                    bond['coupon'] = self._extract_coupon_from_desc(bond['description'])
                else:
                    bond['coupon'] = 0.0

            # Ensure maturity_year exists
            if 'maturity_year' not in bond or bond['maturity_year'] is None:
                if 'maturity_date' in bond and bond['maturity_date']:
                    # Extract year from date string
                    try:
                        date_str = str(bond['maturity_date'])
                        if len(date_str) >= 4:
                            bond['maturity_year'] = int(date_str[:4])
                        else:
                            bond['maturity_year'] = 0
                    except:
                        bond['maturity_year'] = 0
                elif 'description' in bond and bond['description']:
                    year = self._extract_year_from_desc(bond['description'])
                    bond['maturity_year'] = year if year else 0
                else:
                    bond['maturity_year'] = 0

            # Normalize country
            if 'country' not in bond or bond['country'] is None:
                bond['country'] = 'Unknown'

            # Ensure numeric types
            try:
                bond['coupon'] = float(bond['coupon']) if bond['coupon'] else 0.0
            except:
                bond['coupon'] = 0.0

            try:
                bond['maturity_year'] = int(bond['maturity_year']) if bond['maturity_year'] else 0
            except:
                bond['maturity_year'] = 0

    def _extract_coupon_from_desc(self, desc: str) -> float:
        """Extract coupon from bond description"""
        if not isinstance(desc, str):
            return 0.0
        coupon = extract_coupon(desc)
        return coupon if coupon else 0.0

    def _extract_year_from_desc(self, desc: str) -> int:
        """Extract maturity year from bond description"""
        if not isinstance(desc, str):
            return 0
        year = extract_year(desc)
        return year if year else 0

    def match(self, query: str, top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Match a bond reference query against the bonds.

        Args:
            query: Natural language bond reference
            top_n: Maximum matches to return

        Returns:
            List of match dictionaries (JSON-serializable)
        """
        query = query.strip()
        if not query:
            return []

        # Try exact ISIN match first
        isin = extract_isin(query)
        if isin:
            for bond in self.bonds:
                if bond.get('isin', '').upper() == isin:
                    match = self._bond_to_match(bond, 100, ['Exact ISIN match'])
                    return [match.to_dict()]

        # Extract query components
        query_coupon = extract_coupon(query)
        query_year = extract_year(query)
        query_country = extract_country(query)
        query_ticker = extract_ticker_part(query)

        # Score each bond
        matches = []
        for bond in self.bonds:
            score, reasons = self._score_bond(
                bond, query, query_coupon, query_year, query_country, query_ticker
            )
            if score >= self.MINIMUM_THRESHOLD:
                matches.append(self._bond_to_match(bond, score, reasons))

        # Sort by score descending
        matches.sort(key=lambda m: m.score, reverse=True)

        # Convert to dicts for JSON response
        return [m.to_dict() for m in matches[:top_n]]

    def _score_bond(
        self,
        bond: Dict[str, Any],
        query: str,
        query_coupon: Optional[float],
        query_year: Optional[int],
        query_country: Optional[str],
        query_ticker: Optional[str]
    ) -> Tuple[float, List[str]]:
        """Score a bond against query components"""
        score = 0
        reasons = []

        # Coupon match (30 points)
        if query_coupon is not None and bond.get('coupon', 0) > 0:
            coupon_diff = abs(bond['coupon'] - query_coupon)
            if coupon_diff < 0.01:  # Exact match
                score += 30
                reasons.append(f"Coupon: {bond['coupon']}%")
            elif coupon_diff < 0.25:  # Close match (rounding)
                score += 25
                reasons.append(f"Coupon ~{bond['coupon']}%")
            elif coupon_diff < 0.5:
                score += 15
                reasons.append(f"Coupon close: {bond['coupon']}%")

        # Year match (30 points)
        if query_year is not None and bond.get('maturity_year', 0) > 0:
            if bond['maturity_year'] == query_year:
                score += 30
                reasons.append(f"Maturity: {query_year}")
            elif abs(bond['maturity_year'] - query_year) == 1:
                score += 15  # Off by one year
                reasons.append(f"Maturity ~{bond['maturity_year']}")

        # Country match (25 points)
        if query_country is not None and bond.get('country'):
            if query_country.lower() == bond['country'].lower():
                score += 25
                reasons.append(f"Country: {bond['country']}")

        # Ticker match (15 points)
        if query_ticker is not None and bond.get('ticker'):
            ticker = str(bond['ticker']).upper()
            if query_ticker.upper() in ticker:
                score += 15
                reasons.append(f"Ticker: {bond['ticker']}")
            elif query_ticker.upper()[:4] in ticker:
                score += 10
                reasons.append(f"Ticker partial: {bond['ticker']}")

        # Description fuzzy match (bonus points)
        desc = str(bond.get('description', '')).lower()
        query_lower = query.lower()

        # Check if significant query words appear in description
        query_words = [w for w in query_lower.split() if len(w) > 2]
        matching_words = sum(1 for w in query_words if w in desc)
        if matching_words > 0:
            bonus = min(10, matching_words * 3)
            score += bonus
            if bonus > 0 and not reasons:
                reasons.append("Description match")

        return score, reasons

    def _bond_to_match(self, bond: Dict[str, Any], score: float, reasons: List[str]) -> BondMatch:
        """Convert bond dict to BondMatch"""
        # Extract price - try multiple keys
        price = None
        for key in ['price', 'clean_price']:
            if key in bond and bond[key] is not None:
                try:
                    price = float(bond[key])
                    break
                except:
                    pass

        # Extract accrued interest
        accrued = None
        for key in ['accrued_interest', 'accrued', 'ai']:
            if key in bond and bond[key] is not None:
                try:
                    accrued = float(bond[key])
                    break
                except:
                    pass

        return BondMatch(
            isin=str(bond.get('isin', '')),
            ticker=str(bond.get('ticker', '')),
            description=str(bond.get('description', '')),
            country=str(bond.get('country', 'Unknown')),
            coupon=float(bond.get('coupon', 0)),
            maturity_year=int(bond.get('maturity_year', 0)),
            score=score,
            match_reasons=reasons,
            price=price,
            accrued=accrued
        )

    def get_confident_match(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Get a single confident match, or None if ambiguous/no match.

        Returns:
            Match dict if exactly one match above CONFIDENT_THRESHOLD
            None if no match or multiple matches (ambiguous)
        """
        matches = self.match(query, top_n=3)

        if not matches:
            return None

        # Single high-confidence match
        if matches[0]['score'] >= self.CONFIDENT_THRESHOLD:
            # Check if there's a close second
            if len(matches) > 1 and matches[1]['score'] >= self.CONFIDENT_THRESHOLD - 10:
                return None  # Too close, ambiguous
            return matches[0]

        return None

    def format_matches_for_display(self, matches: List[Dict[str, Any]]) -> str:
        """Format matches for chat display"""
        if not matches:
            return "No matching bonds found."

        lines = ["Found these matches:\n"]
        for i, m in enumerate(matches, 1):
            reasons = ", ".join(m['match_reasons']) if m['match_reasons'] else "Partial match"
            lines.append(f"{i}. **{m['ticker']}** {m['coupon']}% {m['maturity_year']} ({m['country']})")
            lines.append(f"   Score: {m['score']:.0f} - {reasons}")

        return "\n".join(lines)


# Convenience function for quick matching
def match_bond(query: str, bonds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Quick bond matching without creating a persistent matcher"""
    matcher = BondMatcher(bonds)
    return matcher.match(query)


def match_bond_with_intent(
    query: str,
    bonds: List[Dict[str, Any]],
    top_n: int = 5
) -> Dict[str, Any]:
    """
    Full bond matching with parsed trade intent.

    This is the main API entry point for Orca Worker.

    Args:
        query: Natural language trade command (e.g., "buy 500k colombia 61")
        bonds: List of bond dictionaries to search
        top_n: Maximum matches to return

    Returns:
        {
            'intent': {...},  # Parsed trade intent
            'matches': [...],  # Matching bonds with scores
            'confident_match': {...} or None,  # Single confident match if unambiguous
        }
    """
    # Parse the trade intent
    intent = parse_trade_intent(query)

    # Match bonds
    matcher = BondMatcher(bonds)
    matches = matcher.match(intent.bond_query, top_n=top_n)

    # Check for confident match
    confident_match = matcher.get_confident_match(intent.bond_query)

    return {
        'intent': intent.to_dict(),
        'matches': matches,
        'confident_match': confident_match
    }
