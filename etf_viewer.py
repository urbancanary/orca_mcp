#!/usr/bin/env python3
"""
ETF Country Breakdown Viewer
Fetches live data from iShares and displays country allocations.
"""

import streamlit as st
import pandas as pd
import requests
import json
from collections import defaultdict

st.set_page_config(
    page_title="ETF Country Breakdown",
    page_icon="📊",
    layout="wide"
)

# ============================================================================
# ETF DATABASE - ~100 iShares ETFs organized by category
# ============================================================================

ISHARES_ETFS = {
    # === GLOBAL BROAD MARKET ===
    "🌍 URTH - MSCI World": {"product_id": "239696", "slug": "ishares-msci-world-etf", "category": "Global", "index": "MSCI World"},
    "🌍 ACWI - MSCI ACWI": {"product_id": "239600", "slug": "ishares-msci-acwi-etf", "category": "Global", "index": "MSCI ACWI"},
    "🌍 IXUS - Total International": {"product_id": "244048", "slug": "ishares-core-msci-total-international-stock-etf", "category": "Global", "index": "MSCI ACWI ex USA"},
    "🌍 IDEV - Intl Developed": {"product_id": "286762", "slug": "ishares-core-msci-international-developed-markets-etf", "category": "Global", "index": "MSCI World ex USA"},

    # === REGIONAL ===
    "🌐 EFA - EAFE": {"product_id": "239623", "slug": "ishares-msci-eafe-etf", "category": "Regional", "index": "MSCI EAFE"},
    "🌐 EEM - Emerging Markets": {"product_id": "239637", "slug": "ishares-msci-emerging-markets-etf", "category": "Regional", "index": "MSCI Emerging Markets"},
    "🌐 IEUR - Europe": {"product_id": "264617", "slug": "ishares-core-msci-europe-etf", "category": "Regional", "index": "MSCI Europe"},
    "🌐 IPAC - Pacific": {"product_id": "264619", "slug": "ishares-core-msci-pacific-etf", "category": "Regional", "index": "MSCI Pacific"},
    "🌐 EZU - Eurozone": {"product_id": "239658", "slug": "ishares-msci-eurozone-etf", "category": "Regional", "index": "MSCI EMU"},
    "🌐 ILF - Latin America 40": {"product_id": "239666", "slug": "ishares-latin-america-40-etf", "category": "Regional", "index": "S&P Latin America 40"},
    "🌐 FM - Frontier Markets": {"product_id": "244050", "slug": "ishares-msci-frontier-and-select-em-etf", "category": "Regional", "index": "MSCI Frontier & EM Select"},

    # === DEVELOPED MARKETS - EUROPE ===
    "🇬🇧 EWU - United Kingdom": {"product_id": "239690", "slug": "ishares-msci-united-kingdom-etf", "category": "Europe", "index": "MSCI United Kingdom"},
    "🇩🇪 EWG - Germany": {"product_id": "239660", "slug": "ishares-msci-germany-etf", "category": "Europe", "index": "MSCI Germany"},
    "🇫🇷 EWQ - France": {"product_id": "239659", "slug": "ishares-msci-france-etf", "category": "Europe", "index": "MSCI France"},
    "🇨🇭 EWL - Switzerland": {"product_id": "239686", "slug": "ishares-msci-switzerland-etf", "category": "Europe", "index": "MSCI Switzerland"},
    "🇳🇱 EWN - Netherlands": {"product_id": "239656", "slug": "ishares-msci-netherlands-etf", "category": "Europe", "index": "MSCI Netherlands"},
    "🇪🇸 EWP - Spain": {"product_id": "239657", "slug": "ishares-msci-spain-etf", "category": "Europe", "index": "MSCI Spain"},
    "🇮🇹 EWI - Italy": {"product_id": "239669", "slug": "ishares-msci-italy-etf", "category": "Europe", "index": "MSCI Italy"},
    "🇧🇪 EWK - Belgium": {"product_id": "239610", "slug": "ishares-msci-belgium-etf", "category": "Europe", "index": "MSCI Belgium"},
    "🇸🇪 EWD - Sweden": {"product_id": "239685", "slug": "ishares-msci-sweden-etf", "category": "Europe", "index": "MSCI Sweden"},
    "🇩🇰 EDEN - Denmark": {"product_id": "239668", "slug": "ishares-msci-denmark-etf", "category": "Europe", "index": "MSCI Denmark"},
    "🇳🇴 ENOR - Norway": {"product_id": "239672", "slug": "ishares-msci-norway-etf", "category": "Europe", "index": "MSCI Norway"},
    "🇫🇮 EFNL - Finland": {"product_id": "239652", "slug": "ishares-msci-finland-etf", "category": "Europe", "index": "MSCI Finland"},
    "🇵🇱 EPOL - Poland": {"product_id": "239680", "slug": "ishares-msci-poland-etf", "category": "Europe", "index": "MSCI Poland"},
    "🇮🇱 EIS - Israel": {"product_id": "239655", "slug": "ishares-msci-israel-etf", "category": "Europe", "index": "MSCI Israel"},

    # === DEVELOPED MARKETS - ASIA PACIFIC ===
    "🇯🇵 EWJ - Japan": {"product_id": "239665", "slug": "ishares-msci-japan-etf", "category": "Asia Pacific", "index": "MSCI Japan"},
    "🇯🇵 SCJ - Japan Small Cap": {"product_id": "239664", "slug": "ishares-msci-japan-small-cap-etf", "category": "Asia Pacific", "index": "MSCI Japan Small Cap"},
    "🇦🇺 EWA - Australia": {"product_id": "239607", "slug": "ishares-msci-australia-etf", "category": "Asia Pacific", "index": "MSCI Australia"},
    "🇸🇬 EWS - Singapore": {"product_id": "239681", "slug": "ishares-msci-singapore-etf", "category": "Asia Pacific", "index": "MSCI Singapore"},
    "🇭🇰 EWH - Hong Kong": {"product_id": "239661", "slug": "ishares-msci-hong-kong-etf", "category": "Asia Pacific", "index": "MSCI Hong Kong"},
    "🇳🇿 ENZL - New Zealand": {"product_id": "239673", "slug": "ishares-msci-new-zealand-etf", "category": "Asia Pacific", "index": "MSCI New Zealand"},
    "🇨🇦 EWC - Canada": {"product_id": "239618", "slug": "ishares-msci-canada-etf", "category": "Asia Pacific", "index": "MSCI Canada"},

    # === EMERGING MARKETS - ASIA ===
    "🇨🇳 MCHI - China": {"product_id": "239619", "slug": "ishares-msci-china-etf", "category": "EM Asia", "index": "MSCI China"},
    "🇮🇳 INDA - India": {"product_id": "239663", "slug": "ishares-msci-india-etf", "category": "EM Asia", "index": "MSCI India"},
    "🇰🇷 EWY - South Korea": {"product_id": "239678", "slug": "ishares-msci-south-korea-etf", "category": "EM Asia", "index": "MSCI South Korea"},
    "🇹🇼 EWT - Taiwan": {"product_id": "239688", "slug": "ishares-msci-taiwan-etf", "category": "EM Asia", "index": "MSCI Taiwan"},
    "🇮🇩 EIDO - Indonesia": {"product_id": "239662", "slug": "ishares-msci-indonesia-etf", "category": "EM Asia", "index": "MSCI Indonesia"},
    "🇲🇾 EWM - Malaysia": {"product_id": "239671", "slug": "ishares-msci-malaysia-etf", "category": "EM Asia", "index": "MSCI Malaysia"},
    "🇹🇭 THD - Thailand": {"product_id": "239689", "slug": "ishares-msci-thailand-etf", "category": "EM Asia", "index": "MSCI Thailand"},
    "🇵🇭 EPHE - Philippines": {"product_id": "239677", "slug": "ishares-msci-philippines-etf", "category": "EM Asia", "index": "MSCI Philippines"},

    # === EMERGING MARKETS - LATAM ===
    "🇧🇷 EWZ - Brazil": {"product_id": "239646", "slug": "ishares-msci-brazil-etf", "category": "EM LatAm", "index": "MSCI Brazil"},
    "🇲🇽 EWW - Mexico": {"product_id": "239675", "slug": "ishares-msci-mexico-etf", "category": "EM LatAm", "index": "MSCI Mexico"},
    "🇨🇴 ICOL - Colombia": {"product_id": "239653", "slug": "ishares-msci-colombia-etf", "category": "EM LatAm", "index": "MSCI Colombia"},
    "🇵🇪 EPU - Peru": {"product_id": "239676", "slug": "ishares-msci-peru-etf", "category": "EM LatAm", "index": "MSCI Peru"},

    # === EMERGING MARKETS - EMEA ===
    "🇿🇦 EZA - South Africa": {"product_id": "239684", "slug": "ishares-msci-south-africa-etf", "category": "EM EMEA", "index": "MSCI South Africa"},
    "🇹🇷 TUR - Turkey": {"product_id": "239691", "slug": "ishares-msci-turkey-etf", "category": "EM EMEA", "index": "MSCI Turkey"},
    "🇸🇦 KSA - Saudi Arabia": {"product_id": "239679", "slug": "ishares-msci-saudi-arabia-etf", "category": "EM EMEA", "index": "MSCI Saudi Arabia"},
    "🇦🇪 UAE - UAE": {"product_id": "239692", "slug": "ishares-msci-uae-etf", "category": "EM EMEA", "index": "MSCI UAE"},
    "🇶🇦 QAT - Qatar": {"product_id": "239670", "slug": "ishares-msci-qatar-etf", "category": "EM EMEA", "index": "MSCI Qatar"},

    # === US SIZE/STYLE ===
    "🇺🇸 IVV - S&P 500": {"product_id": "239710", "slug": "ishares-core-sp-500-etf", "category": "US Equity", "index": "S&P 500"},
    "🇺🇸 IJH - S&P MidCap 400": {"product_id": "239763", "slug": "ishares-core-sp-mid-cap-etf", "category": "US Equity", "index": "S&P MidCap 400"},
    "🇺🇸 IJR - S&P SmallCap 600": {"product_id": "239774", "slug": "ishares-core-sp-small-cap-etf", "category": "US Equity", "index": "S&P SmallCap 600"},
    "🇺🇸 IWB - Russell 1000": {"product_id": "239707", "slug": "ishares-russell-1000-etf", "category": "US Equity", "index": "Russell 1000"},
    "🇺🇸 IWM - Russell 2000": {"product_id": "239761", "slug": "ishares-russell-2000-etf", "category": "US Equity", "index": "Russell 2000"},
    "🇺🇸 IWV - Russell 3000": {"product_id": "239713", "slug": "ishares-russell-3000-etf", "category": "US Equity", "index": "Russell 3000"},
    "🇺🇸 IWR - Russell MidCap": {"product_id": "239714", "slug": "ishares-russell-mid-cap-etf", "category": "US Equity", "index": "Russell MidCap"},
    "🇺🇸 IWF - Russell 1000 Growth": {"product_id": "239706", "slug": "ishares-russell-1000-growth-etf", "category": "US Equity", "index": "Russell 1000 Growth"},
    "🇺🇸 IWD - Russell 1000 Value": {"product_id": "239708", "slug": "ishares-russell-1000-value-etf", "category": "US Equity", "index": "Russell 1000 Value"},
    "🇺🇸 IWO - Russell 2000 Growth": {"product_id": "239709", "slug": "ishares-russell-2000-growth-etf", "category": "US Equity", "index": "Russell 2000 Growth"},

    # === US SECTORS ===
    "💻 IYW - US Technology": {"product_id": "239507", "slug": "ishares-us-technology-etf", "category": "US Sector", "index": "DJ US Technology"},
    "🏦 IYF - US Financials": {"product_id": "239511", "slug": "ishares-us-financials-etf", "category": "US Sector", "index": "DJ US Financials"},
    "🏥 IYH - US Healthcare": {"product_id": "239510", "slug": "ishares-us-healthcare-etf", "category": "US Sector", "index": "DJ US Healthcare"},
    "⚡ IYE - US Energy": {"product_id": "239506", "slug": "ishares-us-energy-etf", "category": "US Sector", "index": "DJ US Energy"},
    "🛒 IYC - US Consumer Discr": {"product_id": "239514", "slug": "ishares-us-consumer-discretionary-etf", "category": "US Sector", "index": "DJ US Consumer Cyclical"},
    "🛍️ IYK - US Consumer Staples": {"product_id": "239508", "slug": "ishares-us-consumer-staples-etf", "category": "US Sector", "index": "DJ US Consumer Non-Cyclical"},
    "🏭 IYJ - US Industrials": {"product_id": "239513", "slug": "ishares-us-industrials-etf", "category": "US Sector", "index": "DJ US Industrials"},
    "💡 IDU - US Utilities": {"product_id": "239512", "slug": "ishares-us-utilities-etf", "category": "US Sector", "index": "DJ US Utilities"},
    "⛏️ IYM - US Basic Materials": {"product_id": "239509", "slug": "ishares-us-basic-materials-etf", "category": "US Sector", "index": "DJ US Basic Materials"},
    "🏠 IYR - US Real Estate": {"product_id": "239505", "slug": "ishares-us-real-estate-etf", "category": "US Sector", "index": "DJ US Real Estate"},
    "📡 IYZ - US Telecom": {"product_id": "239515", "slug": "ishares-us-telecommunications-etf", "category": "US Sector", "index": "DJ US Telecom"},

    # === GLOBAL SECTORS ===
    "💻 IXN - Global Tech": {"product_id": "239649", "slug": "ishares-global-tech-etf", "category": "Global Sector", "index": "S&P Global Tech"},
    "🏦 IXG - Global Financials": {"product_id": "239648", "slug": "ishares-global-financials-etf", "category": "Global Sector", "index": "S&P Global Financials"},
    "🏥 IXJ - Global Healthcare": {"product_id": "239650", "slug": "ishares-global-healthcare-etf", "category": "Global Sector", "index": "S&P Global Healthcare"},
    "⚡ IXC - Global Energy": {"product_id": "239644", "slug": "ishares-global-energy-etf", "category": "Global Sector", "index": "S&P Global Energy"},
    "🏭 EXI - Global Industrials": {"product_id": "239647", "slug": "ishares-global-industrials-etf", "category": "Global Sector", "index": "S&P Global Industrials"},
    "💡 JXI - Global Utilities": {"product_id": "239652", "slug": "ishares-global-utilities-etf", "category": "Global Sector", "index": "S&P Global Utilities"},
    "⛏️ MXI - Global Materials": {"product_id": "239645", "slug": "ishares-global-materials-etf", "category": "Global Sector", "index": "S&P Global Materials"},
    "🌱 ICLN - Global Clean Energy": {"product_id": "272821", "slug": "ishares-global-clean-energy-etf", "category": "Global Sector", "index": "S&P Global Clean Energy"},

    # === FACTOR ETFs ===
    "📊 QUAL - USA Quality": {"product_id": "239719", "slug": "ishares-msci-usa-quality-factor-etf", "category": "Factor", "index": "MSCI USA Quality"},
    "📈 MTUM - USA Momentum": {"product_id": "239724", "slug": "ishares-msci-usa-momentum-factor-etf", "category": "Factor", "index": "MSCI USA Momentum"},
    "💰 VLUE - USA Value": {"product_id": "239708", "slug": "ishares-msci-usa-value-factor-etf", "category": "Factor", "index": "MSCI USA Value"},
    "📏 SIZE - USA Size": {"product_id": "239706", "slug": "ishares-msci-usa-size-factor-etf", "category": "Factor", "index": "MSCI USA Size"},
    "🛡️ USMV - USA Min Vol": {"product_id": "239723", "slug": "ishares-msci-usa-min-vol-factor-etf", "category": "Factor", "index": "MSCI USA Min Vol"},
    "📊 IQLT - Intl Quality": {"product_id": "256101", "slug": "ishares-edge-msci-intl-quality-factor-etf", "category": "Factor", "index": "MSCI World ex USA Quality"},
    "📈 IMTM - Intl Momentum": {"product_id": "271540", "slug": "ishares-edge-msci-intl-momentum-factor-etf", "category": "Factor", "index": "MSCI World ex USA Momentum"},

    # === DIVIDEND ETFs ===
    "💵 DVY - Select Dividend": {"product_id": "239563", "slug": "ishares-select-dividend-etf", "category": "Dividend", "index": "DJ US Select Dividend"},
    "💵 IDV - Intl Select Dividend": {"product_id": "239654", "slug": "ishares-international-select-dividend-etf", "category": "Dividend", "index": "DJ EPAC Select Dividend"},
    "💵 DGRO - Dividend Growth": {"product_id": "264623", "slug": "ishares-core-dividend-growth-etf", "category": "Dividend", "index": "Morningstar Dividend Growth"},

    # === THEMATIC / SPECIALTY ===
    "🔬 SOXX - Semiconductor": {"product_id": "239738", "slug": "ishares-semiconductor-etf", "category": "Thematic", "index": "ICE Semiconductor"},
    "🧬 IBB - Biotechnology": {"product_id": "239737", "slug": "ishares-biotechnology-etf", "category": "Thematic", "index": "Nasdaq Biotechnology"},
    "🏗️ IFRA - Infrastructure": {"product_id": "239726", "slug": "ishares-us-infrastructure-etf", "category": "Thematic", "index": "NYSE FactSet Infrastructure"},
    "✈️ IYT - Transportation": {"product_id": "239739", "slug": "ishares-transportation-average-etf", "category": "Thematic", "index": "DJ Transportation"},
    "🛩️ ITA - Aerospace & Defense": {"product_id": "239740", "slug": "ishares-us-aerospace-defense-etf", "category": "Thematic", "index": "DJ US Aerospace"},
    "🏠 ITB - Home Construction": {"product_id": "239735", "slug": "ishares-us-home-construction-etf", "category": "Thematic", "index": "DJ US Home Construction"},
    "💊 IHI - Medical Devices": {"product_id": "239741", "slug": "ishares-us-medical-devices-etf", "category": "Thematic", "index": "DJ US Medical Equipment"},
    "🛢️ IEO - Oil & Gas E&P": {"product_id": "239729", "slug": "ishares-us-oil-gas-exploration-production-etf", "category": "Thematic", "index": "DJ US Oil & Gas E&P"},
    "🏦 IAI - Broker-Dealers": {"product_id": "239736", "slug": "ishares-us-broker-dealers-securities-exchanges-etf", "category": "Thematic", "index": "DJ US Broker-Dealers"},
    "🏦 IAT - Regional Banks": {"product_id": "239733", "slug": "ishares-us-regional-banks-etf", "category": "Thematic", "index": "DJ US Regional Banks"},

    # === ESG ===
    "🌿 ESGU - ESG Aware USA": {"product_id": "286007", "slug": "ishares-esg-aware-msci-usa-etf", "category": "ESG", "index": "MSCI USA ESG Focus"},

    # === FIXED INCOME - US TREASURY ===
    "📜 TLT - 20+ Year Treasury": {"product_id": "239454", "slug": "ishares-20-year-treasury-bond-etf", "category": "Treasury", "index": "ICE US Treasury 20+ Year", "asset_type": "bond"},
    "📜 IEF - 7-10 Year Treasury": {"product_id": "239453", "slug": "ishares-7-10-year-treasury-bond-etf", "category": "Treasury", "index": "ICE US Treasury 7-10 Year", "asset_type": "bond"},
    "📜 SHY - 1-3 Year Treasury": {"product_id": "239456", "slug": "ishares-1-3-year-treasury-bond-etf", "category": "Treasury", "index": "ICE US Treasury 1-3 Year", "asset_type": "bond"},
    "📜 SHV - Short Treasury": {"product_id": "239458", "slug": "ishares-short-treasury-bond-etf", "category": "Treasury", "index": "ICE Short US Treasury", "asset_type": "bond"},
    "📜 TIP - TIPS": {"product_id": "239451", "slug": "ishares-tips-bond-etf", "category": "Treasury", "index": "Bloomberg TIPS", "asset_type": "bond"},
    "📜 AGG - Core US Aggregate": {"product_id": "239458", "slug": "ishares-core-us-aggregate-bond-etf", "category": "Aggregate", "index": "Bloomberg US Aggregate", "asset_type": "bond"},

    # === FIXED INCOME - CORPORATE ===
    "📜 LQD - IG Corporate": {"product_id": "239566", "slug": "ishares-iboxx-investment-grade-corporate-bond-etf", "category": "IG Corporate", "index": "iBoxx USD IG Corporate", "asset_type": "bond"},
    "📜 HYG - High Yield Corp": {"product_id": "239565", "slug": "ishares-iboxx-high-yield-corporate-bond-etf", "category": "High Yield", "index": "iBoxx USD HY Corporate", "asset_type": "bond"},
    "📜 SLQD - 0-5 Year IG Corp": {"product_id": "258100", "slug": "ishares-0-5-year-investment-grade-corporate-bond-etf", "category": "IG Corporate", "index": "iBoxx USD IG 0-5 Year", "asset_type": "bond"},
    "📜 FLOT - Floating Rate": {"product_id": "239534", "slug": "ishares-floating-rate-bond-etf", "category": "Floating Rate", "index": "Bloomberg US Float Rate", "asset_type": "bond"},

    # === FIXED INCOME - EMERGING MARKETS ===
    "📜 EMB - EM USD Bond": {"product_id": "239572", "slug": "ishares-jp-morgan-usd-emerging-markets-bond-etf", "category": "EM Bonds", "index": "JPM EMBI Global Core", "asset_type": "bond"},
    "📜 EMHY - EM High Yield": {"product_id": "239535", "slug": "ishares-emerging-markets-high-yield-bond-etf", "category": "EM Bonds", "index": "Morningstar EM HY", "asset_type": "bond"},

    # === FIXED INCOME - MBS / AGENCY ===
    "📜 MBB - MBS": {"product_id": "239465", "slug": "ishares-mbs-etf", "category": "MBS/Agency", "index": "Bloomberg MBS", "asset_type": "bond"},
    "📜 AGZ - Agency Bond": {"product_id": "239461", "slug": "ishares-agency-bond-etf", "category": "MBS/Agency", "index": "Bloomberg Agency", "asset_type": "bond"},

    # === FIXED INCOME - MUNICIPAL ===
    "📜 MUB - National Muni": {"product_id": "239766", "slug": "ishares-national-muni-bond-etf", "category": "Municipal", "index": "S&P National AMT-Free Muni", "asset_type": "bond"},
    "📜 SUB - Short-Term Muni": {"product_id": "239771", "slug": "ishares-short-term-national-muni-bond-etf", "category": "Municipal", "index": "S&P Short-Term Muni", "asset_type": "bond"},
    "📜 CMF - California Muni": {"product_id": "239769", "slug": "ishares-california-muni-bond-etf", "category": "Municipal", "index": "S&P CA AMT-Free Muni", "asset_type": "bond"},
    "📜 NYF - New York Muni": {"product_id": "239770", "slug": "ishares-new-york-muni-bond-etf", "category": "Municipal", "index": "S&P NY AMT-Free Muni", "asset_type": "bond"},

    # === FIXED INCOME - INTERNATIONAL ===
    "📜 IGOV - Intl Treasury": {"product_id": "239620", "slug": "ishares-international-treasury-bond-etf", "category": "Intl Bonds", "index": "FTSE World Gov't ex-US", "asset_type": "bond"},
}


@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_etf_holdings(product_id: str, slug: str = "", is_bond: bool = False) -> pd.DataFrame:
    """Fetch holdings from iShares API and return as DataFrame."""
    urls_to_try = [
        f"https://www.ishares.com/us/products/{product_id}/{slug}/1467271812596.ajax?fileType=json&tab=all&dataType=fund",
        f"https://www.ishares.com/us/products/{product_id}/1467271812596.ajax?fileType=json&tab=all&dataType=fund",
    ]

    response = None
    for url in urls_to_try:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                break
        except:
            continue

    if response is None or response.status_code != 200:
        raise Exception(f"Could not fetch data for product {product_id}")

    content = response.text
    if content.startswith('\ufeff'):
        content = content[1:]

    data = json.loads(content)
    holdings = data.get('aaData', [])

    rows = []
    for h in holdings:
        if isinstance(h, list) and len(h) > 14:
            row = {
                'ticker': h[0],
                'name': h[1],
                'sector': h[2],
                'asset_class': h[3],
                'market_value': h[4]['raw'] if isinstance(h[4], dict) else h[4],
                'weight_pct': h[5]['raw'] if isinstance(h[5], dict) else h[5],
                'shares': h[7]['raw'] if isinstance(h[7], dict) else h[7],
                'isin': h[9],
                'country': h[12] if len(h) > 12 else '',
                'exchange': h[13] if len(h) > 13 else '',
                'currency': h[14] if len(h) > 14 else '',
            }
            # Add bond-specific fields if available
            if is_bond and len(h) > 18:
                row['duration'] = h[14]['raw'] if isinstance(h[14], dict) else 0
                row['yield_pct'] = h[15]['raw'] if isinstance(h[15], dict) else 0
                row['maturity'] = h[17]['display'] if isinstance(h[17], dict) else h[17]
                row['coupon'] = h[18]['raw'] if isinstance(h[18], dict) else 0
                row['country'] = h[11] if len(h) > 11 else ''  # Bond country at index 11
            rows.append(row)

    return pd.DataFrame(rows)


def get_country_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate holdings by country."""
    country_data = df.groupby('country').agg({
        'weight_pct': 'sum',
        'market_value': 'sum',
        'ticker': 'count'
    }).reset_index()

    country_data.columns = ['Country', 'Weight %', 'Market Value', 'Holdings']
    country_data = country_data.sort_values('Weight %', ascending=False)
    country_data['Weight %'] = country_data['Weight %'].round(2)
    country_data['Market Value'] = country_data['Market Value'].apply(lambda x: f"${x:,.0f}")

    return country_data


def get_sector_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate holdings by sector."""
    sector_data = df.groupby('sector').agg({
        'weight_pct': 'sum',
        'ticker': 'count'
    }).reset_index()

    sector_data.columns = ['Sector', 'Weight %', 'Holdings']
    sector_data = sector_data.sort_values('Weight %', ascending=False)
    sector_data['Weight %'] = sector_data['Weight %'].round(2)

    return sector_data


def get_bond_metrics(df: pd.DataFrame) -> dict:
    """Calculate portfolio-level bond metrics from holdings."""
    # Filter to actual bond holdings (exclude cash/derivatives)
    bond_df = df[df['asset_class'] == 'Fixed Income'].copy()

    if bond_df.empty or 'duration' not in bond_df.columns:
        return None

    total_weight = bond_df['weight_pct'].sum()
    if total_weight == 0:
        return None

    # Weighted averages
    weighted_duration = (bond_df['weight_pct'] * bond_df['duration']).sum() / total_weight
    weighted_yield = (bond_df['weight_pct'] * bond_df['yield_pct']).sum() / total_weight
    weighted_coupon = (bond_df['weight_pct'] * bond_df['coupon']).sum() / total_weight

    # Sector breakdown for bonds
    sector_breakdown = bond_df.groupby('sector').agg({
        'weight_pct': 'sum'
    }).reset_index()
    sector_breakdown.columns = ['Sector', 'Weight %']
    sector_breakdown = sector_breakdown.sort_values('Weight %', ascending=False)
    sector_breakdown['Weight %'] = sector_breakdown['Weight %'].round(2)

    return {
        'avg_duration': round(weighted_duration, 2),
        'avg_yield': round(weighted_yield, 2),
        'avg_coupon': round(weighted_coupon, 2),
        'bond_holdings': len(bond_df),
        'sector_breakdown': sector_breakdown
    }


# === UI ===

st.title("📊 ETF Country Breakdown")
st.markdown(f"Live data from iShares API • **{len(ISHARES_ETFS)} ETFs available**")

# Category filter and ETF selector
col1, col2 = st.columns([1, 3])

with col1:
    categories = ["All"] + sorted(set(e["category"] for e in ISHARES_ETFS.values()))
    selected_category = st.selectbox("Category", categories, index=0)

with col2:
    if selected_category == "All":
        etf_options = list(ISHARES_ETFS.keys())
    else:
        etf_options = [k for k, v in ISHARES_ETFS.items() if v["category"] == selected_category]

    selected_etf = st.selectbox("Select ETF", options=etf_options, index=0)

etf_info = ISHARES_ETFS[selected_etf]

# Display ETF info
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Index", etf_info["index"])
with col2:
    st.metric("Category", etf_info["category"])
with col3:
    ticker = selected_etf.split(" - ")[0].split(" ")[-1]
    st.metric("Ticker", ticker)

st.divider()

# Fetch data
is_bond_etf = etf_info.get("asset_type") == "bond"
with st.spinner("Fetching holdings data..."):
    try:
        df = fetch_etf_holdings(etf_info["product_id"], etf_info.get("slug", ""), is_bond=is_bond_etf)

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Holdings", len(df))
        with col2:
            st.metric("Countries", df['country'].nunique())
        with col3:
            st.metric("Sectors", df['sector'].nunique())
        with col4:
            total_value = df['market_value'].sum()
            st.metric("Total Value", f"${total_value/1e9:.1f}B")

        st.divider()

        # Tabs for different views - add Bond Metrics for bond ETFs
        if is_bond_etf:
            tab1, tab2, tab3, tab4 = st.tabs(["📊 Bond Metrics", "🌍 Country Breakdown", "🏢 Sector Breakdown", "📋 All Holdings"])
        else:
            tab1, tab2, tab3 = st.tabs(["🌍 Country Breakdown", "🏢 Sector Breakdown", "📋 All Holdings"])

        # Bond ETF: tab1=Bond Metrics, tab2=Country, tab3=Sector, tab4=Holdings
        # Equity ETF: tab1=Country, tab2=Sector, tab3=Holdings

        if is_bond_etf:
            # Bond Metrics Tab
            with tab1:
                bond_metrics = get_bond_metrics(df)

                if bond_metrics:
                    st.subheader("Portfolio Characteristics")

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Avg Duration", f"{bond_metrics['avg_duration']} yrs")
                    with col2:
                        st.metric("Avg Yield (YTM)", f"{bond_metrics['avg_yield']}%")
                    with col3:
                        st.metric("Avg Coupon", f"{bond_metrics['avg_coupon']}%")
                    with col4:
                        st.metric("Bond Holdings", bond_metrics['bond_holdings'])

                    st.divider()

                    col1, col2 = st.columns([1, 1])

                    with col1:
                        st.subheader("Sector Breakdown")
                        st.dataframe(
                            bond_metrics['sector_breakdown'],
                            use_container_width=True,
                            hide_index=True,
                            height=400
                        )

                    with col2:
                        st.subheader("Sector Distribution")
                        if not bond_metrics['sector_breakdown'].empty:
                            st.bar_chart(
                                bond_metrics['sector_breakdown'].set_index('Sector')['Weight %'],
                                use_container_width=True,
                                height=400
                            )
                else:
                    st.warning("Bond metrics not available for this ETF")

            # Country Tab for bonds
            with tab2:
                country_df = get_country_breakdown(df)
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.subheader("Country Weights")
                    st.dataframe(country_df, use_container_width=True, hide_index=True, height=500)
                with col2:
                    st.subheader("Top 10 Countries")
                    chart_data = country_df.head(10).copy()
                    chart_data['Weight'] = chart_data['Weight %']
                    st.bar_chart(chart_data.set_index('Country')['Weight'], use_container_width=True, height=500)

            # Sector Tab for bonds
            with tab3:
                sector_df = get_sector_breakdown(df)
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.subheader("Sector Weights")
                    st.dataframe(sector_df, use_container_width=True, hide_index=True, height=400)
                with col2:
                    st.subheader("Sector Distribution")
                    st.bar_chart(sector_df.set_index('Sector')['Weight %'], use_container_width=True, height=400)

            # Holdings Tab for bonds
            holdings_tab = tab4
        else:
            # Equity ETF tabs
            with tab1:
                country_df = get_country_breakdown(df)
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.subheader("Country Weights")
                    st.dataframe(country_df, use_container_width=True, hide_index=True, height=500)
                with col2:
                    st.subheader("Top 10 Countries")
                    chart_data = country_df.head(10).copy()
                    chart_data['Weight'] = chart_data['Weight %']
                    st.bar_chart(chart_data.set_index('Country')['Weight'], use_container_width=True, height=500)

            with tab2:
                sector_df = get_sector_breakdown(df)
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.subheader("Sector Weights")
                    st.dataframe(sector_df, use_container_width=True, hide_index=True, height=400)
                with col2:
                    st.subheader("Sector Distribution")
                    st.bar_chart(sector_df.set_index('Sector')['Weight %'], use_container_width=True, height=400)

            holdings_tab = tab3

        with holdings_tab:
            st.subheader("All Holdings")

            # Filters
            col1, col2 = st.columns(2)
            with col1:
                country_filter = st.multiselect(
                    "Filter by Country",
                    options=sorted(df['country'].unique()),
                    default=[]
                )
            with col2:
                sector_filter = st.multiselect(
                    "Filter by Sector",
                    options=sorted(df['sector'].unique()),
                    default=[]
                )

            filtered_df = df.copy()
            if country_filter:
                filtered_df = filtered_df[filtered_df['country'].isin(country_filter)]
            if sector_filter:
                filtered_df = filtered_df[filtered_df['sector'].isin(sector_filter)]

            # Format for display
            display_df = filtered_df[['ticker', 'name', 'country', 'sector', 'weight_pct', 'market_value']].copy()
            display_df.columns = ['Ticker', 'Name', 'Country', 'Sector', 'Weight %', 'Market Value']
            display_df['Weight %'] = display_df['Weight %'].round(3)
            display_df['Market Value'] = display_df['Market Value'].apply(lambda x: f"${x:,.0f}")

            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                height=600
            )

            st.caption(f"Showing {len(filtered_df)} of {len(df)} holdings")

        # Export button
        st.divider()
        csv = country_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Country Breakdown (CSV)",
            data=csv,
            file_name=f"{ticker}_country_breakdown.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"Error fetching data: {e}")
        st.info("The iShares API may be temporarily unavailable. Try again later.")
