"""
Competitor scraping module.

Fetches similar sites from the AdMaven API and scrapes HTML content.
"""
import os
import re
import asyncio
import json
from datetime import date
from typing import List, Dict, Set, Optional
from dataclasses import dataclass, asdict
import aiohttp
from loguru import logger
from .ad_detection import check_ads

# Configurable API endpoint
SIMILAR_SITES_API_URL = os.getenv(
    'SIMILAR_SITES_API_URL',
    'http://leads-management.ad-maven.com:9777/similar_get_domains'
)
API_KEY = os.getenv('API_KEY')


@dataclass
class EnrichedSite:
    """Data model representing a processed competitor site."""
    scan_date: str
    site_domain: str
    competitor_name: str
    run_time_domain: str
    monthly_visitors: int
    contacts_json: str
    got_blocked: bool
    already_working: bool
    is_running_ads: bool
    ad_evidence: str


def normalize_domain(domain: str) -> str:
    """
    Normalize domain by removing protocol, www, and trailing slashes.
    Args:
        domain: Raw domain string
    Returns:
        Cleaned domain string
    """
    cleaned = domain.lower()
    cleaned = cleaned.replace('http://', '').replace('https://', '')
    cleaned = cleaned.replace('www.', '').strip('/')
    return cleaned


async def fetch_site_data(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
    timeout: int = 5
) -> str:
    """
    Fetches URL content asynchronously with semaphore control.
    Args:
        session: aiohttp client session
        url: Target URL
        sem: Concurrency limiting semaphore
        timeout: Request timeout in seconds
    Returns:
        HTML content string or empty string on failure
    """
    async with sem:
        try:
            async with session.get(
                url,
                timeout=timeout,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; AdMavenBot/1.0)'}
            ) as response:
                return await response.text() if response.status not in (403, 404, 500) else ''
        except Exception as e:
            logger.debug(f"Fetch failed for {url}: {e}")
            return ''


async def fetch_similar_sites(
    session: aiohttp.ClientSession,
    domain: str,
    log
) -> List[Dict]:
    """
    Fetches similar sites from the AdMaven API.
    Args:
        session: aiohttp client session
        domain: Domain to find similars for
        log: Logger instance
    Returns:
        List of site dictionaries with site_name and monthly_visitors
    """
    try:
        if not API_KEY:
            log.error("API_KEY environment variable is required")
            return []
        
        clean_domain = normalize_domain(domain)
        params = {
            'api_key': API_KEY,
            'domain': clean_domain,
        }
        async with session.get(SIMILAR_SITES_API_URL, params=params, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                sites = data.get('domain_list', []) or data.get('visitors_data', [])
                return sites if isinstance(sites, list) else []
            else:
                log.warning(f"API returned status {response.status} for {domain}")
                return []
    except Exception as e:
        log.error(f"API exception for {domain}: {e}")
        return []


async def process_site(
    session: aiohttp.ClientSession,
    site_domain: str,
    competitor_name: str,
    run_domain: str,
    monthly_visitors: int,
    clients_set: Set[str],
    sem: asyncio.Semaphore,
    use_llm: bool = True
) -> Optional[EnrichedSite]:
    """
    Process a single competitor site: fetch HTML, detect ads.
    Args:
        session: aiohttp client session
        site_domain: Site to process
        competitor_name: Competitor name
        run_domain: Competitor's runtime domain
        monthly_visitors: Visitor count
        clients_set: Set of existing client domains
        sem: Semaphore for concurrency control
        use_llm: Whether to use LLM for ad detection
    Returns:
        EnrichedSite object or None if processing failed
    """
    norm = normalize_domain(site_domain)
    if not norm:
        return None
    
    exists = norm in clients_set
    html = '' if exists else await fetch_site_data(session, f"http://{norm}", sem)
    contacts, is_ads, evidence = {}, False, "N/A"
    
    if exists:
        evidence = "Existing Client"
    elif not html:
        evidence = "Blocked / Unreachable"
    else:
        detection = await check_ads(html, use_llm=use_llm)
        is_ads = detection['is_running_ads']
        evidence = detection['ad_evidence']
        contacts = {
            'emails': re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html),
            'socials': re.findall(
                r'(?:https?://)?(?:www\.)?(?:facebook|twitter|linkedin|instagram|youtube|tiktok)\.com/[a-zA-Z0-9_\-\.]+',
                html
            ),
        }
    
    return EnrichedSite(
        scan_date=date.today().isoformat(),
        site_domain=norm,
        competitor_name=competitor_name,
        run_time_domain=run_domain,
        monthly_visitors=monthly_visitors,
        contacts_json=json.dumps(contacts, ensure_ascii=False),
        got_blocked=not html and not exists,
        already_working=exists,
        is_running_ads=is_ads,
        ad_evidence=evidence,
    )


async def scrape_competitor_domain(
    domain: str,
    clients_set: Optional[Set[str]] = None,
    max_concurrency: int = 5,
    use_llm: bool = True
) -> List[EnrichedSite]:
    """
    Scrape competitor domain and all similar sites.
    Args:
        domain: The competitor's runtime domain
        clients_set: Set of existing client domains (to skip)
        max_concurrency: Max concurrent HTTP requests
        use_llm: Enable LLM for ad detection
    Returns:
        List of EnrichedSite results
    """
    log = logger.bind(process="scraper")
    clients_set = clients_set or set()
    results = []
    sem = asyncio.Semaphore(max_concurrency)
    
    async with aiohttp.ClientSession() as session:
        sites = await fetch_similar_sites(session, domain, log)
        if not sites:
            log.warning(f"No similar sites found for {domain}")
            return []
        
        tasks = []
        for site in sites:
            if isinstance(site, dict):
                site_name = site.get('site_name', '')
                visitors = site.get('monthly_visitors', 0)
            else:
                site_name = str(site)
                visitors = 0
            
            task = process_site(
                session=session,
                site_domain=site_name,
                competitor_name='',  # will be filled from domain mapping
                run_domain=domain,
                monthly_visitors=visitors,
                clients_set=clients_set,
                sem=sem,
                use_llm=use_llm,
            )
            tasks.append(task)
        
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                if result:
                    results.append(result)
            except Exception as e:
                log.error(f"Site processing error: {e}")
    
    return results
