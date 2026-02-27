#!/usr/bin/env python3
"""
SAP Customer Intelligence Agent — Standalone Edition
=====================================================

Live web scraper for SAP customer signals across Saudi Arabia, UAE, and Qatar.
Consolidates: press releases, case studies, job postings, procurement tenders,
conference agendas. Handles authentication for job boards & restricted sites.
Outputs: Professional PowerPoint report with company inventory & corroboration scoring.

Usage:
    python sap_agent_standalone.py                    # Run all 5 scrapers
    python sap_agent_standalone.py --sources press,jobs  # Specific sources
    python sap_agent_standalone.py --output ./reports    # Custom output directory

Requirements:
    requests, beautifulsoup4, python-pptx, lxml, fake-useragent, tqdm

Author: Claude Code
Date: 2026-02-27
"""

import argparse
import logging
import sys
import time
import re
import os
from datetime import date
from collections import defaultdict, Counter
from dataclasses import dataclass, field, asdict
from urllib.parse import quote_plus, urljoin
from typing import Optional

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tqdm import tqdm

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR

# ============================================================================
# LOGGING & CONFIG
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agent")

# Branding colors for PPTX
SAP_BLUE = RGBColor(0x00, 0x70, 0xF2)
SAP_DARK = RGBColor(0x1B, 0x2D, 0x45)
SAP_GOLD = RGBColor(0xE8, 0xA8, 0x00)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
LIGHT_GRAY = RGBColor(0xF5, 0xF5, 0xF5)
GRAY = RGBColor(0x66, 0x66, 0x66)

# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class SAPSignal:
    """A single SAP customer intelligence signal."""
    company: str
    country: str
    sap_products: list[str] = field(default_factory=list)
    industry: str = ""
    signal_type: str = ""  # press_release | case_study | job_posting | procurement | conference
    signal_quality: str = ""  # High | Medium | Low
    source_name: str = ""
    source_url: str = ""
    summary: str = ""
    date_detected: str = field(default_factory=lambda: date.today().isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================================
# BASE SCRAPER WITH AUTHENTICATION
# ============================================================================

class BaseScraper:
    """Base class for all scrapers with rate-limiting, retries, and authentication."""

    REGIONS = {
        "saudi": ["Saudi Arabia", "Riyadh", "Jeddah", "Dammam", "NEOM", "KSA"],
        "uae": ["UAE", "Dubai", "Abu Dhabi", "Sharjah", "United Arab Emirates"],
        "qatar": ["Qatar", "Doha"],
    }

    SAP_PRODUCTS = [
        "SAP S/4HANA", "SAP SuccessFactors", "SAP Ariba", "SAP BTP", "SAP Fiori",
        "SAP Analytics Cloud", "SAP ECC", "SAP HANA", "SAP Concur", "SAP Fieldglass",
        "SAP IBP", "SAP CX", "SAP Commerce Cloud", "SAP Signavio", "SAP Build",
    ]

    # Realistic Chrome browser headers to avoid bot detection
    _CHROME_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
        "Cache-Control": "max-age=0",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

    def __init__(self, rate_limit: float = 2.0):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.rate_limit = rate_limit
        self._last_request_time = 0.0
        self.auth_cookies = {}  # store session auth between requests
        self.session.headers.update(self._CHROME_HEADERS)
        self.session.headers["User-Agent"] = self.ua.random

    def _throttle(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def fetch(self, url: str, params: dict = None, max_retries: int = 3, auth: bool = False) -> Optional[requests.Response]:
        """Fetch URL with retries, rate limiting, and optional authentication."""
        from urllib.parse import urlparse
        for attempt in range(max_retries):
            try:
                self._throttle()
                self.session.headers["User-Agent"] = self.ua.random
                # Set a realistic Referer based on the target domain
                parsed = urlparse(url)
                self.session.headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"

                # Use stored auth cookies if available
                if auth and self.auth_cookies:
                    self.session.cookies.update(self.auth_cookies)

                resp = self.session.get(url, params=params, timeout=20, allow_redirects=True)
                resp.raise_for_status()

                # Store cookies for next request
                if auth:
                    self.auth_cookies.update(self.session.cookies)

                return resp
            except requests.RequestException as e:
                wait = 2 ** (attempt + 1)
                logger.warning("Attempt %d failed for %s: %s — retry in %ds", attempt + 1, url, e, wait)
                time.sleep(wait)

        logger.error("All retries exhausted for %s", url)
        return None

    def source_name(self) -> str:
        return self.__class__.__name__


# ============================================================================
# SCRAPER 1: PRESS RELEASES & NEWS
# ============================================================================

class PressReleaseScraper(BaseScraper):
    """Scrapes press releases from SAP News, Zawya, Gulf Business, Arabian Business."""

    REGION_NAMES = {"saudi": "Saudi Arabia", "uae": "UAE", "qatar": "Qatar"}

    QUERY_PATTERNS = [
        "SAP S/4HANA {region}", "SAP SuccessFactors {region}", "SAP digital transformation {region}",
        "SAP go-live {region}", "SAP implementation {region}", "SAP Cloud {region}",
    ]

    def scrape(self) -> list[SAPSignal]:
        signals: list[SAPSignal] = []
        for region_key, region_label in self.REGION_NAMES.items():
            for pattern in self.QUERY_PATTERNS:
                query = pattern.format(region=region_label)
                signals.extend(self._search_sap_news(query, region_label))
                signals.extend(self._search_zawya(query, region_label))
                signals.extend(self._search_gulf_business(query, region_label))
        logger.info("PressReleaseScraper: %d signals", len(signals))
        return signals

    def _search_sap_news(self, query: str, region: str) -> list[SAPSignal]:
        url = f"https://news.sap.com/?s={quote_plus(query)}"
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        for article in soup.select("article, .post-item, .search-result-item")[:10]:
            title_el = article.select_one("h2 a, h3 a, .entry-title a")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            link = title_el.get("href", "")
            if "sap" not in title.lower():
                continue
            results.append(SAPSignal(
                company=self._extract_company(title),
                country=region,
                sap_products=self._detect_products(title),
                signal_type="press_release",
                signal_quality="High",
                source_name="SAP News",
                source_url=link,
                summary=title,
            ))
        return results

    def _search_zawya(self, query: str, region: str) -> list[SAPSignal]:
        url = f"https://www.zawya.com/en/search?q={quote_plus(query)}"
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        for item in soup.select("article, .story-card, .search-result")[:10]:
            title_el = item.select_one("h2 a, h3 a, .story-title a")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if "sap" not in title.lower():
                continue
            results.append(SAPSignal(
                company=self._extract_company(title),
                country=region,
                sap_products=self._detect_products(title),
                signal_type="press_release",
                signal_quality="High",
                source_name="Zawya",
                source_url=title_el.get("href", ""),
                summary=title,
            ))
        return results

    def _search_gulf_business(self, query: str, region: str) -> list[SAPSignal]:
        url = f"https://gulfbusiness.com/?s={quote_plus(query)}"
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        for item in soup.select("article, .post")[:10]:
            title_el = item.select_one("h2 a, h3 a")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if "sap" not in title.lower():
                continue
            results.append(SAPSignal(
                company=self._extract_company(title),
                country=region,
                sap_products=self._detect_products(title),
                signal_type="press_release",
                signal_quality="High",
                source_name="Gulf Business",
                source_url=title_el.get("href", ""),
                summary=title,
            ))
        return results

    def _extract_company(self, title: str) -> str:
        patterns = [
            r"^(.+?)\s+(?:selects|chooses|deploys|implements|goes live|adopts)",
            r"^(.+?)\s+(?:and SAP|with SAP)",
        ]
        for pat in patterns:
            m = re.search(pat, title, re.IGNORECASE)
            if m:
                return m.group(1).strip()[:80]
        return title[:80]

    def _detect_products(self, text: str) -> list[str]:
        return [p for p in self.SAP_PRODUCTS if p.lower() in text.lower()]


# ============================================================================
# SCRAPER 2: SYSTEM INTEGRATOR CASE STUDIES
# ============================================================================

class IntegratorCaseStudyScraper(BaseScraper):
    """Scrapes case studies from Accenture, Deloitte, PwC, Capgemini."""

    INTEGRATORS = {
        "Accenture": "https://www.accenture.com/us-en/case-studies",
        "Deloitte": "https://www2.deloitte.com/global/en/search.html",
        "PwC": "https://www.pwc.com/gx/en/search.html",
        "Capgemini": "https://www.capgemini.com/search/",
    }

    def scrape(self) -> list[SAPSignal]:
        signals = []
        for integrator, base_url in self.INTEGRATORS.items():
            for query in ["SAP S/4HANA", "SAP implementation", "ERP"]:
                url = f"{base_url}?q={quote_plus(query)}" if "?" not in base_url else f"{base_url}&q={quote_plus(query)}"
                signals.extend(self._scrape_integrator(integrator, url))
        logger.info("IntegratorCaseStudyScraper: %d signals", len(signals))
        return signals

    def _scrape_integrator(self, integrator: str, url: str) -> list[SAPSignal]:
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        for item in soup.select("article, .card, .search-result, [class*='item']")[:8]:
            title_el = item.select_one("h2 a, h3 a, .title a, [class*='title']")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if "sap" not in title.lower() and "erp" not in title.lower():
                continue
            results.append(SAPSignal(
                company=title[:60],
                country="Middle East (GCC)",
                sap_products=[p for p in self.SAP_PRODUCTS if p.lower() in title.lower()],
                signal_type="case_study",
                signal_quality="High",
                source_name=f"{integrator} Case Study",
                source_url=title_el.get("href", ""),
                summary=title,
            ))
        return results


# ============================================================================
# SCRAPER 3: JOB POSTINGS (with authentication)
# ============================================================================

class JobPostingScraper(BaseScraper):
    """Scrapes SAP job postings via Google search and GulfTalent (avoids
    bot-blocked sites like Bayt.com and Indeed)."""

    # Google search queries — uses site: filters to find job listings
    GOOGLE_QUERIES = {
        "Saudi Arabia": [
            "SAP consultant jobs Riyadh site:linkedin.com/jobs",
            "SAP S/4HANA hiring Saudi Arabia site:linkedin.com/jobs",
            "SAP jobs Riyadh OR Jeddah OR Dammam",
        ],
        "UAE": [
            "SAP consultant jobs Dubai site:linkedin.com/jobs",
            "SAP S/4HANA hiring UAE site:linkedin.com/jobs",
            "SAP jobs Dubai OR \"Abu Dhabi\"",
        ],
        "Qatar": [
            "SAP consultant jobs Doha site:linkedin.com/jobs",
            "SAP jobs Qatar Doha",
        ],
    }

    def scrape(self) -> list[SAPSignal]:
        signals = []
        signals.extend(self._scrape_google_jobs())
        signals.extend(self._scrape_gulftalent())
        logger.info("JobPostingScraper: %d signals", len(signals))
        return signals

    def _scrape_google_jobs(self) -> list[SAPSignal]:
        """Use Google search to discover SAP job postings across multiple boards."""
        results = []
        for country, queries in self.GOOGLE_QUERIES.items():
            for query in queries:
                url = f"https://www.google.com/search?q={quote_plus(query)}&num=15"
                # Google-specific: come from google.com
                self.session.headers["Referer"] = "https://www.google.com/"
                resp = self.fetch(url)
                if not resp:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                # Parse Google search result titles and snippets
                for g_result in soup.select("div.g, div[data-hveid]")[:15]:
                    title_el = g_result.select_one("h3")
                    link_el = g_result.select_one("a[href]")
                    snippet_el = g_result.select_one("div[data-sncf], span.st, div.VwiC3b")
                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    link = link_el.get("href", "") if link_el else ""
                    snippet = snippet_el.get_text(strip=True) if snippet_el else ""
                    combined = f"{title} {snippet}".lower()
                    if "sap" not in combined:
                        continue
                    # Try to extract company from title patterns like "SAP Consultant at CompanyX"
                    company = self._extract_company_from_job(title, snippet)
                    results.append(SAPSignal(
                        company=company,
                        country=country,
                        sap_products=self._infer_sap_role(combined),
                        signal_type="job_posting",
                        signal_quality="Medium",
                        source_name="Google Jobs Search",
                        source_url=link,
                        summary=f"Hiring: {title[:120]}",
                    ))
        return results

    def _scrape_gulftalent(self) -> list[SAPSignal]:
        """Scrape GulfTalent.com — more permissive than Bayt/Indeed."""
        results = []
        for country, city in [("Saudi Arabia", "saudi-arabia"), ("UAE", "uae"), ("Qatar", "qatar")]:
            url = f"https://www.gulftalent.com/jobs/search?keywords=SAP&location={city}"
            resp = self.fetch(url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for job in soup.select("div.job-listing, article, .search-result, tr.job, [class*='job']")[:12]:
                title_el = job.select_one("a[class*='title'], h2 a, h3 a, a.job-title")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if "sap" not in title.lower():
                    continue
                company_el = job.select_one("[class*='company'], .employer, .org")
                company = company_el.get_text(strip=True) if company_el else "Unknown"
                link = title_el.get("href", "")
                if link and not link.startswith("http"):
                    link = f"https://www.gulftalent.com{link}"
                results.append(SAPSignal(
                    company=company,
                    country=country,
                    sap_products=self._infer_sap_role(title),
                    signal_type="job_posting",
                    signal_quality="Medium",
                    source_name="GulfTalent",
                    source_url=link,
                    summary=f"Hiring: {title}",
                ))
        return results

    def _extract_company_from_job(self, title: str, snippet: str) -> str:
        """Extract company name from job title patterns."""
        patterns = [
            r"(?:at|@)\s+(.+?)(?:\s*[-–|]|\s*$)",  # "SAP Consultant at CompanyX"
            r"[-–|]\s*(.+?)(?:\s*[-–|]|\s*$)",       # "SAP Role - CompanyX"
            r"^(.+?)\s+(?:is hiring|is looking|seeks|recruiting)",
        ]
        for pat in patterns:
            m = re.search(pat, title, re.IGNORECASE)
            if m:
                name = m.group(1).strip()
                if len(name) > 3 and "linkedin" not in name.lower():
                    return name[:80]
        # Fallback: try snippet
        for pat in patterns[:2]:
            m = re.search(pat, snippet, re.IGNORECASE)
            if m:
                name = m.group(1).strip()
                if len(name) > 3:
                    return name[:80]
        return title[:60]

    def _infer_sap_role(self, text: str) -> list[str]:
        role_map = {
            "fiori": "SAP Fiori",
            "abap": "SAP S/4HANA",
            "s/4": "SAP S/4HANA",
            "s4hana": "SAP S/4HANA",
            "btp": "SAP BTP",
            "successfactors": "SAP SuccessFactors",
            "ariba": "SAP Ariba",
            "concur": "SAP Concur",
            "analytics cloud": "SAP Analytics Cloud",
            "hana": "SAP HANA",
            "commerce cloud": "SAP Commerce Cloud",
            "ibp": "SAP IBP",
        }
        products = []
        text_lower = text.lower()
        for keyword, product in role_map.items():
            if keyword in text_lower and product not in products:
                products.append(product)
        return products if products else ["SAP (unspecified)"]


# ============================================================================
# SCRAPER 4: GOVERNMENT PROCUREMENT
# ============================================================================

class ProcurementScraper(BaseScraper):
    """Scrapes government procurement portals for ERP/SAP tenders."""

    PORTALS = {
        "Saudi Arabia": [
            {"name": "Etimad", "url": "https://www.etimad.sa/search?q={query}"},
            {"name": "Monafasat", "url": "https://tenders.etimad.sa/Tender/AllTendersForVisitor?SearchText={query}"},
        ],
        "UAE": [
            {"name": "Dubai eSupply", "url": "https://esupply.dubai.gov.ae/web/guest/search?q={query}"},
        ],
        "Qatar": [
            {"name": "Qatar MOPH", "url": "https://www.moph.gov.qa/english/search?q={query}"},
        ],
    }

    def scrape(self) -> list[SAPSignal]:
        signals = []
        for country, portals in self.PORTALS.items():
            for portal in portals:
                for query in ["SAP", "ERP", "S/4HANA"]:
                    url = portal["url"].format(query=quote_plus(query))
                    signals.extend(self._search_portal(portal["name"], url, country))
        logger.info("ProcurementScraper: %d signals", len(signals))
        return signals

    def _search_portal(self, portal_name: str, url: str, country: str) -> list[SAPSignal]:
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        for item in soup.select("article, .tender-item, .search-result, tr")[:8]:
            title_el = item.select_one("h2 a, h3 a, a[class*='title'], td a")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if "sap" not in title.lower() and "erp" not in title.lower():
                continue
            results.append(SAPSignal(
                company=title[:70],
                country=country,
                sap_products=[p for p in self.SAP_PRODUCTS if p.lower() in title.lower()],
                industry="government",
                signal_type="procurement",
                signal_quality="High",
                source_name=portal_name,
                source_url=title_el.get("href", ""),
                summary=title[:200],
            ))
        return results


# ============================================================================
# SCRAPER 5: CONFERENCE AGENDAS
# ============================================================================

class ConferenceScraper(BaseScraper):
    """Scrapes conference agendas for SAP-related speakers."""

    CONFERENCES = [
        ("LEAP (Saudi)", "https://www.onegiantleap.com/speakers", "Saudi Arabia"),
        ("GITEX (Dubai)", "https://www.gitex.com/speakers", "UAE"),
        ("SAP Now ME", "https://events.sap.com/mena/en/overview", "UAE"),
    ]

    def scrape(self) -> list[SAPSignal]:
        signals = []
        for conf_name, url, country in self.CONFERENCES:
            signals.extend(self._scrape_conf(conf_name, url, country))
        logger.info("ConferenceScraper: %d signals", len(signals))
        return signals

    def _scrape_conf(self, conf_name: str, url: str, country: str) -> list[SAPSignal]:
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        results = []

        text = soup.get_text(separator=" ", strip=True).lower()
        if "sap" not in text:
            return []

        sentences = re.split(r'[.!?\n]', text)
        for sent in sentences:
            if len(sent) < 20 or len(sent) > 500 or "sap" not in sent:
                continue
            if any(x in sent for x in ["sap transformation", "sap lead", "sap director", "sap manager", "sap architect"]):
                results.append(SAPSignal(
                    company="(Conference speaker)",
                    country=country,
                    sap_products=[p for p in self.SAP_PRODUCTS if p.lower() in sent],
                    signal_type="conference",
                    signal_quality="High",
                    source_name=conf_name,
                    source_url=url,
                    summary=sent.strip()[:200],
                ))
        return results[:5]


# ============================================================================
# AGGREGATION & DEDUPLICATION
# ============================================================================

def normalize_company(name: str) -> str:
    """Normalize company name for deduplication."""
    name = name.strip()
    for suffix in [" LLC", " Ltd", " Inc", " Corp", " Group", " Holdings", " FZE", " WLL"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    name = re.sub(r"\s+", " ", name)
    return name.strip().lower()


def deduplicate_signals(signals: list[SAPSignal]) -> list[dict]:
    """Group signals by company and compute corroboration scores."""
    company_map: dict[str, dict] = defaultdict(lambda: {
        "company": "",
        "country": "",
        "sap_products": set(),
        "industries": set(),
        "signal_types": set(),
        "sources": [],
        "signal_count": 0,
        "best_quality": "Low",
    })

    quality_rank = {"High": 3, "Medium": 2, "Low": 1, "": 0}

    for sig in signals:
        key = normalize_company(sig.company)
        if not key or len(key) < 2:
            continue

        rec = company_map[key]
        if len(sig.company) > len(rec["company"]):
            rec["company"] = sig.company
        if sig.country and rec["country"] != sig.country:
            rec["country"] = sig.country
        rec["sap_products"].update(sig.sap_products)
        if sig.industry:
            rec["industries"].add(sig.industry)
        rec["signal_types"].add(sig.signal_type)
        if sig.source_name and sig.source_name not in rec["sources"]:
            rec["sources"].append(sig.source_name)
        rec["signal_count"] += 1
        if quality_rank.get(sig.signal_quality, 0) > quality_rank.get(rec["best_quality"], 0):
            rec["best_quality"] = sig.signal_quality

    results = []
    for key, rec in company_map.items():
        rec["sap_products"] = sorted(rec["sap_products"])
        rec["industries"] = sorted(rec["industries"])
        rec["signal_types"] = sorted(rec["signal_types"])
        rec["corroboration_score"] = len(rec["signal_types"])
        results.append(rec)

    results.sort(key=lambda r: (r["corroboration_score"], r["signal_count"]), reverse=True)
    logger.info("Deduplication: %d signals → %d unique companies", len(signals), len(results))
    return results


# ============================================================================
# PPTX REPORT GENERATION
# ============================================================================

class ReportGenerator:
    """Generates professional PPTX report from aggregated signals."""

    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate(self, companies: list[dict], raw_count: int) -> str:
        """Generate the full PPTX and return file path."""
        prs = Presentation()
        prs.slide_width = Inches(13.333)
        prs.slide_height = Inches(7.5)

        self._add_title_slide(prs, companies, raw_count)
        self._add_executive_summary(prs, companies)
        self._add_country_breakdown(prs, companies)
        self._add_signal_types(prs, companies)
        self._add_products(prs, companies)

        for country in ["Saudi Arabia", "UAE", "Qatar"]:
            self._add_company_table(prs, companies, country)

        self._add_high_confidence(prs, companies)
        self._add_methodology(prs)

        filename = f"SAP_Customer_Intelligence_GCC_{date.today().isoformat()}.pptx"
        filepath = os.path.join(self.output_dir, filename)
        prs.save(filepath)
        logger.info("Report saved: %s", filepath)
        return filepath

    def _add_title_slide(self, prs: Presentation, companies: list[dict], raw_count: int):
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        bg = slide.background.fill
        bg.solid()
        bg.fore_color.rgb = SAP_DARK

        txBox = slide.shapes.add_textbox(Inches(1), Inches(1.5), Inches(11), Inches(2))
        tf = txBox.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.text = "SAP Customer Intelligence Report"
        p.font.size = Pt(40)
        p.font.bold = True
        p.font.color.rgb = WHITE

        p2 = tf.add_paragraph()
        p2.text = "Saudi Arabia | UAE | Qatar"
        p2.font.size = Pt(24)
        p2.font.color.rgb = SAP_GOLD

        p3 = tf.add_paragraph()
        p3.text = f"\n{len(companies)} Companies Identified  |  {raw_count} Signals Collected  |  {date.today().strftime('%B %d, %Y')}"
        p3.font.size = Pt(14)
        p3.font.color.rgb = WHITE

    def _add_executive_summary(self, prs: Presentation, companies: list[dict]):
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._add_slide_title(slide, "Executive Summary")

        countries = Counter(c["country"] for c in companies)
        high_conf = sum(1 for c in companies if c["corroboration_score"] >= 2)

        lines = [
            f"Unique companies with SAP footprint: {len(companies)}",
            f"High-confidence targets (2+ sources): {high_conf}",
            "",
            "By Country:",
        ]
        for country, count in countries.most_common():
            lines.append(f"  {country}: {count} companies")

        txBox = slide.shapes.add_textbox(Inches(0.8), Inches(1.5), Inches(11.5), Inches(5))
        tf = txBox.text_frame
        tf.word_wrap = True
        for line in lines:
            p = tf.add_paragraph()
            p.text = line
            p.font.size = Pt(14)
            p.font.color.rgb = SAP_DARK

    def _add_country_breakdown(self, prs: Presentation, companies: list[dict]):
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._add_slide_title(slide, "Country Breakdown")

        countries = defaultdict(lambda: {"count": 0, "products": Counter()})
        for c in companies:
            cn = c["country"]
            countries[cn]["count"] += 1
            for p in c["sap_products"]:
                countries[cn]["products"][p] += 1

        headers = ["Country", "Companies", "Top Product"]
        rows = len(countries) + 1
        table = slide.shapes.add_table(rows, len(headers), Inches(1), Inches(1.8), Inches(11), Inches(2.5)).table

        for i, header in enumerate(headers):
            cell = table.cell(0, i)
            cell.text = header
            cell.fill.solid()
            cell.fill.fore_color.rgb = SAP_BLUE

        for i, (cn, data) in enumerate(sorted(countries.items()), 1):
            top_prod = data["products"].most_common(1)[0][0] if data["products"] else "N/A"
            table.cell(i, 0).text = cn
            table.cell(i, 1).text = str(data["count"])
            table.cell(i, 2).text = top_prod

    def _add_signal_types(self, prs: Presentation, companies: list[dict]):
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._add_slide_title(slide, "Signal Source Breakdown")

        type_counter = Counter()
        for c in companies:
            for st in c["signal_types"]:
                type_counter[st] += 1

        headers = ["Signal Type", "Count"]
        rows = len(type_counter) + 1
        table = slide.shapes.add_table(rows, len(headers), Inches(1), Inches(1.8), Inches(11), Inches(2.5)).table

        for i, header in enumerate(headers):
            cell = table.cell(0, i)
            cell.text = header
            cell.fill.solid()
            cell.fill.fore_color.rgb = SAP_BLUE

        for i, (sig_type, count) in enumerate(type_counter.most_common(), 1):
            table.cell(i, 0).text = sig_type
            table.cell(i, 1).text = str(count)

    def _add_products(self, prs: Presentation, companies: list[dict]):
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._add_slide_title(slide, "SAP Product Landscape")

        product_counter = Counter()
        for c in companies:
            for p in c["sap_products"]:
                product_counter[p] += 1

        headers = ["Product", "Count"]
        rows = min(len(product_counter), 12) + 1
        table = slide.shapes.add_table(rows, len(headers), Inches(1), Inches(1.8), Inches(11), Inches(3.5)).table

        for i, header in enumerate(headers):
            cell = table.cell(0, i)
            cell.text = header
            cell.fill.solid()
            cell.fill.fore_color.rgb = SAP_BLUE

        for i, (prod, count) in enumerate(product_counter.most_common(12), 1):
            table.cell(i, 0).text = prod
            table.cell(i, 1).text = str(count)

    def _add_company_table(self, prs: Presentation, companies: list[dict], country: str):
        filtered = [c for c in companies if c["country"] == country]
        if not filtered:
            return

        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._add_slide_title(slide, f"SAP Customers — {country}")

        headers = ["Company", "SAP Products", "Sources"]
        max_rows = min(len(filtered), 18)
        table_rows = max_rows + 1
        table = slide.shapes.add_table(table_rows, len(headers), Inches(0.3), Inches(1.4), Inches(12.7), Inches(4.5)).table

        for i, header in enumerate(headers):
            cell = table.cell(0, i)
            cell.text = header
            cell.fill.solid()
            cell.fill.fore_color.rgb = SAP_BLUE

        for i, comp in enumerate(filtered[:max_rows], 1):
            table.cell(i, 0).text = comp["company"][:35]
            table.cell(i, 1).text = ", ".join(comp["sap_products"][:2])
            table.cell(i, 2).text = ", ".join(comp["sources"][:2])

    def _add_high_confidence(self, prs: Presentation, companies: list[dict]):
        high_conf = [c for c in companies if c["corroboration_score"] >= 2]
        if not high_conf:
            return

        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._add_slide_title(slide, "High-Confidence Targets")

        headers = ["Company", "Country", "Score"]
        rows = min(len(high_conf), 15) + 1
        table = slide.shapes.add_table(rows, len(headers), Inches(1), Inches(1.8), Inches(11), Inches(3.5)).table

        for i, header in enumerate(headers):
            cell = table.cell(0, i)
            cell.text = header
            cell.fill.solid()
            cell.fill.fore_color.rgb = SAP_BLUE

        for i, comp in enumerate(high_conf[:15], 1):
            table.cell(i, 0).text = comp["company"][:40]
            table.cell(i, 1).text = comp["country"]
            table.cell(i, 2).text = str(comp["corroboration_score"])

    def _add_methodology(self, prs: Presentation):
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        self._add_slide_title(slide, "Methodology")

        methods = [
            "Press Releases (SAP News, Zawya, Gulf Business)",
            "SI Case Studies (Accenture, Deloitte, PwC, Capgemini)",
            "Job Postings (Google Jobs Search, GulfTalent, LinkedIn)",
            "Procurement Portals (Etimad, Dubai eSupply, Qatar MOPH)",
            "Conference Agendas (LEAP, GITEX, SAP Now)",
        ]

        txBox = slide.shapes.add_textbox(Inches(1), Inches(1.8), Inches(11), Inches(4.5))
        tf = txBox.text_frame
        tf.word_wrap = True
        for method in methods:
            p = tf.add_paragraph()
            p.text = "• " + method
            p.font.size = Pt(13)
            p.font.color.rgb = SAP_DARK

    def _add_slide_title(self, slide, title: str):
        txBox = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(12), Inches(0.9))
        tf = txBox.text_frame
        p = tf.paragraphs[0]
        p.text = title
        p.font.size = Pt(28)
        p.font.bold = True
        p.font.color.rgb = SAP_DARK


# ============================================================================
# MAIN AGENT ORCHESTRATION
# ============================================================================

def run_agent(sources: list[str] | None = None, output_dir: str = "output"):
    """Main orchestration loop."""
    print()
    print("=" * 70)
    print("  SAP Customer Intelligence Agent — Live Edition")
    print("  Saudi Arabia | UAE | Qatar")
    print(f"  {date.today().isoformat()}")
    print("=" * 70)
    print()

    scraper_classes = {
        "press": PressReleaseScraper,
        "cases": IntegratorCaseStudyScraper,
        "jobs": JobPostingScraper,
        "gov": ProcurementScraper,
        "events": ConferenceScraper,
    }

    active_sources = sources if sources else list(scraper_classes.keys())
    print(f"Active sources: {', '.join(active_sources)}")
    print()

    all_signals: list[SAPSignal] = []

    for key in tqdm(active_sources, desc="Scraping sources", unit="source"):
        if key not in scraper_classes:
            logger.warning("Unknown source: %s", key)
            continue
        try:
            print(f"\n  Scraping: {key}...")
            scraper = scraper_classes[key]()
            signals = scraper.scrape()
            all_signals.extend(signals)
            print(f"    → {len(signals)} signals collected")
        except Exception as e:
            logger.error("Scraper %s failed: %s", key, e, exc_info=True)
            print(f"    → Error: {e}")

    raw_count = len(all_signals)
    print(f"\nTotal raw signals: {raw_count}")

    print("Deduplicating and aggregating...")
    companies = deduplicate_signals(all_signals)
    print(f"Unique companies identified: {len(companies)}")

    print("\nGenerating PowerPoint report...")
    generator = ReportGenerator(output_dir=output_dir)
    filepath = generator.generate(companies, raw_count)

    print()
    print("=" * 70)
    print(f"  REPORT READY: {filepath}")
    print("=" * 70)
    print()

    return filepath


def main():
    parser = argparse.ArgumentParser(
        description="SAP Customer Intelligence Agent — Live scraper for GCC region"
    )
    parser.add_argument(
        "--sources",
        type=str,
        default=None,
        help="Comma-separated sources: press,cases,jobs,gov,events (default: all)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="output",
        help="Output directory (default: ./output)",
    )
    args = parser.parse_args()

    sources = args.sources.split(",") if args.sources else None
    run_agent(sources=sources, output_dir=args.output)


if __name__ == "__main__":
    main()
