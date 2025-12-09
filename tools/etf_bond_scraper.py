#!/usr/bin/env python3
"""
ETF Bond Scraper - Extracts bond holdings data from iShares ETFs

Scrapes bond-level data including:
- Static: ISIN, CUSIP, name, sector, country, coupon, maturity, issue_date, currency
- Pricing: clean_price, market_value, par_value, accrued, duration, ytm

Stores data in SQLite database for later integration with pricing systems.

Supports residential proxy to avoid IP blocking by iShares.
Set PROXY_URL environment variable or use auth_mcp to configure.
"""

import os
import requests
import json
import sqlite3
from datetime import datetime, date
from typing import Dict, List, Optional
from pathlib import Path


def get_proxy_url() -> Optional[str]:
    """
    Get residential proxy URL from environment or auth_mcp.

    Returns proxy URL like: http://user:pass@gate.smartproxy.com:10001
    """
    # First try environment variable
    proxy_url = os.getenv("PROXY_URL")
    if proxy_url:
        return proxy_url

    # Try auth_mcp if available
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "auth_mcp"))
        from auth_client import get_api_key
        proxy_url = get_api_key("PROXY_URL")
        if proxy_url:
            return proxy_url
    except Exception:
        pass

    return None


# Bond ETF definitions with iShares product IDs
BOND_ETFS = {
    "EMB": {
        "product_id": "239572",
        "slug": "ishares-jp-morgan-usd-emerging-markets-bond-etf",
        "name": "iShares J.P. Morgan USD Emerging Markets Bond ETF",
        "index": "JPM EMBI Global Core",
        "category": "EM Sovereign"
    },
    "CEMB": {
        "product_id": "239525",
        "slug": "ishares-emerging-markets-corporate-bond-etf",
        "name": "iShares J.P. Morgan EM Corporate Bond ETF",
        "index": "JPM CEMBI Broad Diversified Core",
        "category": "EM Corporate"
    },
    "LQD": {
        "product_id": "239566",
        "slug": "ishares-iboxx-investment-grade-corporate-bond-etf",
        "name": "iShares iBoxx $ Investment Grade Corporate Bond ETF",
        "index": "iBoxx USD Liquid IG",
        "category": "IG Corporate"
    },
    "HYG": {
        "product_id": "239565",
        "slug": "ishares-iboxx-high-yield-corporate-bond-etf",
        "name": "iShares iBoxx $ High Yield Corporate Bond ETF",
        "index": "iBoxx USD Liquid HY",
        "category": "HY Corporate"
    },
    "TLT": {
        "product_id": "239454",
        "slug": "ishares-20-year-treasury-bond-etf",
        "name": "iShares 20+ Year Treasury Bond ETF",
        "index": "ICE US Treasury 20+ Year",
        "category": "US Treasury"
    },
    "IEF": {
        "product_id": "239453",
        "slug": "ishares-7-10-year-treasury-bond-etf",
        "name": "iShares 7-10 Year Treasury Bond ETF",
        "index": "ICE US Treasury 7-10 Year",
        "category": "US Treasury"
    },
    "AGG": {
        "product_id": "239458",
        "slug": "ishares-core-us-aggregate-bond-etf",
        "name": "iShares Core U.S. Aggregate Bond ETF",
        "index": "Bloomberg US Aggregate",
        "category": "US Aggregate"
    },
    "IGOV": {
        "product_id": "239620",
        "slug": "ishares-international-treasury-bond-etf",
        "name": "iShares International Treasury Bond ETF",
        "index": "FTSE World Gov't ex-US",
        "category": "Intl Sovereign"
    },
    "MBB": {
        "product_id": "239465",
        "slug": "ishares-mbs-etf",
        "name": "iShares MBS ETF",
        "index": "Bloomberg MBS",
        "category": "MBS"
    },
}


def get_db_path() -> Path:
    """Get path to ETF bonds database."""
    return Path(__file__).parent.parent / "data" / "etf_bonds.db"


def init_database(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Initialize database with schema."""
    if db_path is None:
        db_path = get_db_path()

    # Ensure data directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Static bond reference data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bond_static (
            isin TEXT PRIMARY KEY,
            cusip TEXT,
            name TEXT,
            sector TEXT,
            country TEXT,
            coupon REAL,
            maturity_date TEXT,
            issue_date TEXT,
            currency TEXT,
            accrual_date TEXT,
            effective_date TEXT,
            first_seen_date TEXT,
            last_updated TEXT
        )
    """)

    # Add new columns if they don't exist (for existing databases)
    try:
        cursor.execute("ALTER TABLE bond_static ADD COLUMN accrual_date TEXT")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE bond_static ADD COLUMN effective_date TEXT")
    except:
        pass

    # Daily pricing snapshots
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bond_prices (
            isin TEXT,
            price_date TEXT,
            clean_price REAL,
            market_value REAL,
            par_value REAL,
            accrued_pct REAL,
            accrued_value REAL,
            duration REAL,
            ytm REAL,
            etf_source TEXT,
            weight_in_etf REAL,
            PRIMARY KEY (isin, price_date, etf_source)
        )
    """)

    # ETF scrape history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            etf_ticker TEXT,
            scrape_date TEXT,
            scrape_time TEXT,
            bonds_found INTEGER,
            bonds_updated INTEGER,
            status TEXT,
            error_message TEXT
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON bond_prices(price_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_isin ON bond_prices(isin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_static_country ON bond_static(country)")

    conn.commit()
    return conn


def fetch_etf_holdings(etf_ticker: str, use_proxy: bool = True) -> List[Dict]:
    """
    Fetch holdings from iShares API for a given ETF.

    Args:
        etf_ticker: ETF ticker symbol (e.g., 'EMB', 'CEMB')
        use_proxy: If True, use residential proxy if available

    Returns:
        List of holding dictionaries from iShares API
    """
    if etf_ticker not in BOND_ETFS:
        raise ValueError(f"Unknown ETF: {etf_ticker}. Available: {list(BOND_ETFS.keys())}")

    etf = BOND_ETFS[etf_ticker]
    product_id = etf["product_id"]
    slug = etf["slug"]

    urls_to_try = [
        f"https://www.ishares.com/us/products/{product_id}/{slug}/1467271812596.ajax?fileType=json&tab=all&dataType=fund",
        f"https://www.ishares.com/us/products/{product_id}/1467271812596.ajax?fileType=json&tab=all&dataType=fund",
    ]

    # Browser-like headers to avoid detection
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': f'https://www.ishares.com/us/products/{product_id}/{slug}'
    }

    # Configure proxy if available
    proxies = None
    if use_proxy:
        proxy_url = get_proxy_url()
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}

    response = None
    for url in urls_to_try:
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=30)
            if response.status_code == 200:
                break
        except Exception as e:
            continue

    if response is None or response.status_code != 200:
        raise Exception(f"Could not fetch data for {etf_ticker}")

    content = response.text
    if content.startswith('\ufeff'):
        content = content[1:]

    data = json.loads(content)
    holdings = data.get('aaData', [])

    return holdings


def parse_bond_holding(h: List, etf_ticker: str) -> Optional[Dict]:
    """Parse a single holding row from iShares API response."""
    if not isinstance(h, list) or len(h) < 19:
        return None

    try:
        # Extract ISIN
        isin = h[8]
        if not isin or isin == '-':
            return None

        # Skip non-fixed income
        asset_class = h[2]
        if asset_class != 'Fixed Income':
            return None

        # Helper to extract raw value from dict or direct value
        def get_raw(val, default=0):
            if isinstance(val, dict):
                return val.get('raw', default)
            return val if val is not None else default

        def get_display(val, default=''):
            if isinstance(val, dict):
                return val.get('display', default)
            return str(val) if val is not None else default

        # Extract fields
        market_value = get_raw(h[3], 0)
        weight_pct = get_raw(h[4], 0)
        par_value = get_raw(h[6], 0)
        clean_price = get_raw(h[10], 0)
        duration = get_raw(h[14], 0)
        ytm = get_raw(h[15], 0)
        coupon = get_raw(h[18], 0)

        # Parse maturity date - format as YYYY-MMM-DD (e.g., 2035-Jul-09)
        maturity_raw = get_raw(h[17], '')
        if maturity_raw and str(maturity_raw).isdigit():
            mat_str = str(int(maturity_raw))
            try:
                from datetime import datetime
                dt = datetime(int(mat_str[:4]), int(mat_str[4:6]), int(mat_str[6:]))
                maturity_date = dt.strftime('%Y-%b-%d')
            except (ValueError, IndexError):
                maturity_date = f"{mat_str[:4]}-{mat_str[4:6]}-{mat_str[6:]}"
        else:
            maturity_date = get_display(h[17], '')

        # Calculate accrued interest
        if par_value > 0 and clean_price > 0:
            clean_value = (clean_price / 100) * par_value
            accrued_value = market_value - clean_value
            accrued_pct = (accrued_value / par_value) * 100
        else:
            accrued_value = 0
            accrued_pct = 0

        # Get accrual date (when interest starts accruing) and effective date
        # These are at indices 25 and 26 in the iShares response
        accrual_date = h[25] if len(h) > 25 and h[25] and h[25] != '-' else None
        effective_date = h[26] if len(h) > 26 and h[26] and h[26] != '-' else None

        return {
            # Static data
            'isin': isin,
            'cusip': h[7] if h[7] != '-' else None,
            'name': h[0][:100] if h[0] else None,
            'sector': h[1],
            'country': h[11],
            'coupon': coupon,
            'maturity_date': maturity_date,
            'issue_date': accrual_date,  # Use accrual_date as issue_date fallback
            'accrual_date': accrual_date,
            'effective_date': effective_date,
            'currency': h[13],

            # Pricing data
            'clean_price': clean_price,
            'market_value': market_value,
            'par_value': par_value,
            'accrued_pct': round(accrued_pct, 6),
            'accrued_value': round(accrued_value, 2),
            'duration': duration,
            'ytm': ytm,
            'weight_in_etf': weight_pct,
            'etf_source': etf_ticker,
        }
    except Exception as e:
        return None


def scrape_etf(etf_ticker: str, conn: sqlite3.Connection, verbose: bool = False) -> Dict:
    """Scrape a single ETF and store data in database."""
    today = date.today().isoformat()
    now = datetime.now().strftime("%H:%M:%S")

    try:
        if verbose:
            print(f"Fetching {etf_ticker} holdings...")

        holdings = fetch_etf_holdings(etf_ticker)

        if verbose:
            print(f"  Raw holdings: {len(holdings)}")

        # Parse holdings
        bonds = []
        for h in holdings:
            bond = parse_bond_holding(h, etf_ticker)
            if bond:
                bonds.append(bond)

        if verbose:
            print(f"  Parsed bonds: {len(bonds)}")

        cursor = conn.cursor()
        updated_static = 0
        updated_prices = 0

        for bond in bonds:
            # Upsert static data
            cursor.execute("""
                INSERT INTO bond_static (isin, cusip, name, sector, country, coupon,
                                        maturity_date, issue_date, currency,
                                        accrual_date, effective_date,
                                        first_seen_date, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(isin) DO UPDATE SET
                    cusip = COALESCE(excluded.cusip, bond_static.cusip),
                    name = COALESCE(excluded.name, bond_static.name),
                    sector = COALESCE(excluded.sector, bond_static.sector),
                    country = COALESCE(excluded.country, bond_static.country),
                    coupon = COALESCE(excluded.coupon, bond_static.coupon),
                    maturity_date = COALESCE(excluded.maturity_date, bond_static.maturity_date),
                    issue_date = COALESCE(excluded.issue_date, bond_static.issue_date),
                    currency = COALESCE(excluded.currency, bond_static.currency),
                    accrual_date = COALESCE(excluded.accrual_date, bond_static.accrual_date),
                    effective_date = COALESCE(excluded.effective_date, bond_static.effective_date),
                    last_updated = excluded.last_updated
            """, (
                bond['isin'], bond['cusip'], bond['name'], bond['sector'],
                bond['country'], bond['coupon'], bond['maturity_date'],
                bond['issue_date'], bond['currency'],
                bond['accrual_date'], bond['effective_date'],
                today, today
            ))
            updated_static += cursor.rowcount

            # Insert price snapshot
            cursor.execute("""
                INSERT OR REPLACE INTO bond_prices
                (isin, price_date, clean_price, market_value, par_value,
                 accrued_pct, accrued_value, duration, ytm, etf_source, weight_in_etf)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bond['isin'], today, bond['clean_price'], bond['market_value'],
                bond['par_value'], bond['accrued_pct'], bond['accrued_value'],
                bond['duration'], bond['ytm'], bond['etf_source'], bond['weight_in_etf']
            ))
            updated_prices += 1

        # Log scrape
        cursor.execute("""
            INSERT INTO scrape_log (etf_ticker, scrape_date, scrape_time, bonds_found, bonds_updated, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (etf_ticker, today, now, len(bonds), updated_prices, 'success'))

        conn.commit()

        result = {
            'etf': etf_ticker,
            'date': today,
            'bonds_found': len(bonds),
            'prices_updated': updated_prices,
            'status': 'success'
        }

        if verbose:
            print(f"  ✓ Stored {updated_prices} price records")

        return result

    except Exception as e:
        # Log error
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO scrape_log (etf_ticker, scrape_date, scrape_time, bonds_found, bonds_updated, status, error_message)
            VALUES (?, ?, ?, 0, 0, 'error', ?)
        """, (etf_ticker, today, now, str(e)))
        conn.commit()

        if verbose:
            print(f"  ✗ Error: {e}")

        return {
            'etf': etf_ticker,
            'date': today,
            'bonds_found': 0,
            'prices_updated': 0,
            'status': 'error',
            'error': str(e)
        }


def scrape_all_etfs(etfs: Optional[List[str]] = None, verbose: bool = True) -> List[Dict]:
    """Scrape multiple ETFs and store data."""
    if etfs is None:
        etfs = list(BOND_ETFS.keys())

    conn = init_database()
    results = []

    # Check if proxy is configured
    proxy_url = get_proxy_url()
    if proxy_url:
        # Mask credentials in log
        masked = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url[:30]
        print(f"Using residential proxy: ...@{masked}")
    else:
        print("No proxy configured - using direct connection")

    print(f"Scraping {len(etfs)} bond ETFs...")
    print()

    for etf in etfs:
        result = scrape_etf(etf, conn, verbose=verbose)
        results.append(result)

    conn.close()

    # Summary
    total_bonds = sum(r['bonds_found'] for r in results)
    successful = sum(1 for r in results if r['status'] == 'success')

    print()
    print(f"Summary: {successful}/{len(etfs)} ETFs scraped, {total_bonds} total bonds")

    return results


def get_bond_static(isin: str, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict]:
    """Get static data for a single bond."""
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(get_db_path())
        close_conn = True

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bond_static WHERE isin = ?", (isin,))
    row = cursor.fetchone()

    if close_conn:
        conn.close()

    if row:
        cols = ['isin', 'cusip', 'name', 'sector', 'country', 'coupon',
                'maturity_date', 'issue_date', 'currency', 'accrual_date', 'effective_date',
                'first_seen_date', 'last_updated']
        return dict(zip(cols, row))
    return None


def get_latest_price(isin: str, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict]:
    """Get latest price for a single bond."""
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(get_db_path())
        close_conn = True

    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM bond_prices
        WHERE isin = ?
        ORDER BY price_date DESC
        LIMIT 1
    """, (isin,))
    row = cursor.fetchone()

    if close_conn:
        conn.close()

    if row:
        cols = ['isin', 'price_date', 'clean_price', 'market_value', 'par_value',
                'accrued_pct', 'accrued_value', 'duration', 'ytm', 'etf_source', 'weight_in_etf']
        return dict(zip(cols, row))
    return None


def get_database_summary(conn: Optional[sqlite3.Connection] = None) -> Dict:
    """Get summary statistics of the database."""
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(get_db_path())
        close_conn = True

    cursor = conn.cursor()

    # Count bonds
    cursor.execute("SELECT COUNT(*) FROM bond_static")
    total_bonds = cursor.fetchone()[0]

    # Count by country
    cursor.execute("""
        SELECT country, COUNT(*) as cnt
        FROM bond_static
        GROUP BY country
        ORDER BY cnt DESC
        LIMIT 10
    """)
    top_countries = cursor.fetchall()

    # Latest prices
    cursor.execute("SELECT MAX(price_date), COUNT(DISTINCT isin) FROM bond_prices")
    latest = cursor.fetchone()

    # Scrape history
    cursor.execute("""
        SELECT etf_ticker, scrape_date, bonds_found, status
        FROM scrape_log
        ORDER BY id DESC
        LIMIT 10
    """)
    recent_scrapes = cursor.fetchall()

    if close_conn:
        conn.close()

    return {
        'total_bonds': total_bonds,
        'top_countries': top_countries,
        'latest_price_date': latest[0],
        'bonds_with_prices': latest[1],
        'recent_scrapes': recent_scrapes
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape bond data from iShares ETFs")
    parser.add_argument('--etfs', nargs='+', help='ETFs to scrape (default: all)')
    parser.add_argument('--list', action='store_true', help='List available ETFs')
    parser.add_argument('--summary', action='store_true', help='Show database summary')
    args = parser.parse_args()

    if args.list:
        print("Available Bond ETFs:")
        for ticker, info in BOND_ETFS.items():
            print(f"  {ticker:6s} - {info['name']}")
            print(f"           Index: {info['index']}, Category: {info['category']}")

    elif args.summary:
        summary = get_database_summary()
        print(f"Database Summary:")
        print(f"  Total bonds: {summary['total_bonds']}")
        print(f"  Latest prices: {summary['latest_price_date']} ({summary['bonds_with_prices']} bonds)")
        print(f"\nTop countries:")
        for country, count in summary['top_countries']:
            print(f"  {country}: {count}")
        print(f"\nRecent scrapes:")
        for etf, date, count, status in summary['recent_scrapes']:
            print(f"  {etf} {date}: {count} bonds ({status})")

    else:
        results = scrape_all_etfs(etfs=args.etfs)
