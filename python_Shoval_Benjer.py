import asyncio, json, re, sys, time, os
from dataclasses import dataclass, asdict
from datetime import date
from typing import List, Dict, Set, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
try:
    from dotenv import load_dotenv
    import aiohttp
    from huggingface_hub import InferenceClient
    from tenacity import retry, stop_after_attempt, wait_exponential
    from loguru import logger
    import polars as pl
    import duckdb
except ImportError as e:
    sys.exit(f"Missing dependency: {e}")
load_dotenv()
API_KEY = os.getenv('API_KEY')
HF_TOKEN = os.getenv('HF_TOKEN')
logger.remove()
logger.add(sys.stderr, format="{time:HH:mm:ss} | {level: <8} | {extra[process]} | {message}", level="INFO")
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
SOCIAL_RE = re.compile(r'(?:https?://)?(?:www\.)?(?:facebook|twitter|linkedin|instagram|youtube|tiktok)\.com/[a-zA-Z0-9_\-\.]+')
AD_SIGNATURES = {'googlesyndication': 1.5, 'doubleclick': 1.5, 'prebid': 1.2, 'criteo': 1.0, 'adnxs': 1.0, 'iframe': 0.2, 'width="300"': 0.3, 'height="250"': 0.3, 'sponsored': 0.5}
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
def heuristic_ad_detect(html: str) -> Dict[str, object]:
    """
    Analyzes HTML content using a weighted keyword dictionary to detect ad activity.
    Args:
        html (str): The raw HTML content of the page.
    Returns:
        Dict[str, object]: Dictionary containing 'is_running_ads' (bool) and 'ad_evidence' (str).
    """
    html_lower = html.lower()
    score = sum(AD_SIGNATURES.get(t, 0) for t in AD_SIGNATURES if t in html_lower)
    return {'is_running_ads': score > 2.0, 'ad_evidence': f"Heuristic score: {score:.2f}"}
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def detect_ads_with_qwen(html: str) -> Dict[str, object]:
    """
    Uses Qwen 2.5-72B via HuggingFace Inference API to semantically analyze HTML for ads.
    Args:
        html (str): The raw HTML content.
    Returns:
        Dict[str, object]: JSON analysis result.
    Raises:
        ValueError: If HF_TOKEN is missing.
    """
    if not HF_TOKEN: raise ValueError("No HF_TOKEN")
    client = InferenceClient(token=HF_TOKEN)
    prompt = f"Analyze HTML for ad activity: {html[:2000]}. Output JSON: {{'is_running_ads': true/false, 'ad_evidence': 'reason'}}"
    def _call():
        return client.chat_completion(model="Qwen/Qwen2.5-72B-Instruct", messages=[{"role": "user", "content": prompt}], max_tokens=100, temperature=0.1)
    res = await asyncio.to_thread(_call)
    content = res.choices[0].message.content.strip()
    return json.loads(content[7:-3] if content.startswith('```json') else content)
async def fetch_site_data(session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore) -> str:
    """
    Fetches URL content asynchronously with semaphore control.
    Args:
        session (aiohttp.ClientSession): The async HTTP session.
        url (str): Target URL.
        sem (asyncio.Semaphore): Concurrency limiter.
    Returns:
        str: HTML content or empty string on failure.
    """
    async with sem:
        try:
            async with session.get(url, timeout=5, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                return await r.text() if r.status not in (403, 404, 500) else ''
        except Exception: return ''
async def fetch_similar_sites(session: aiohttp.ClientSession, domain: str, log) -> List[Dict]:
    """Fetches similar sites from the API."""
    try:
        clean_domain = domain.lower().replace('http://', '').replace('https://', '').replace('www.', '').strip('/')
        url = "http://leads-management.ad-maven.com:9777/similar_get_domains"
        async with session.get(url, params={'api_key': API_KEY, 'domain': clean_domain}, timeout=10) as r:
            if r.status == 200: return await r.json()
            log.warning(f"API Error {r.status} for {domain} (sent: {clean_domain})")
            return []
    except Exception as e:
        log.error(f"API Exception for {domain}: {e}")
        return []
async def process_competitor_async(log, comp: str, domain: str, clients: Set[str]) -> List[EnrichedSite]:
    """Orchestrates pipeline for a single competitor."""
    log.info(f"Processing {domain}")
    results, sem = [], asyncio.Semaphore(5)
    async with aiohttp.ClientSession() as session:
        response = await fetch_similar_sites(session, domain, log)
        if not response: 
            log.warning(f"No response for {domain}")
            return []
        
        sites = response.get('domain_list', []) or response.get('visitors_data', [])
        if not sites:
            log.warning(f"No sites found in response for {domain}")
            return []

        for site in sites:
            if isinstance(site, dict):
                raw_site = site.get('site_name', '')
                visitors = site.get('monthly_visitors', 0)
            else:
                raw_site = str(site)
                visitors = 0

            norm = raw_site.lower().replace('http://', '').replace('https://', '').replace('www.', '').strip('/')
            if not norm: continue
            exists = norm in clients
            html = '' if exists else await fetch_site_data(session, f"http://{norm}", sem)
            contacts, is_ads, evidence = {}, False, "N/A"
            if exists: evidence = "Existing Client"
            elif not html: evidence = "Blocked"
            else:
                contacts = {'emails': EMAIL_RE.findall(html), 'socials': SOCIAL_RE.findall(html)}
                try:
                    q = await detect_ads_with_qwen(html)
                    is_ads, evidence = q.get('is_running_ads', False), q.get('ad_evidence', 'Silent')
                except Exception:
                    h = heuristic_ad_detect(html)
                    is_ads, evidence = h['is_running_ads'], h['ad_evidence']
            results.append(EnrichedSite(date.today().isoformat(), norm, comp, domain, visitors, json.dumps(contacts), not html and not exists, exists, is_ads, evidence))
    return results
def worker_entry(comp: str, dom: str, clients: Set[str]) -> List[EnrichedSite]:
    """Entry point for the worker process."""
    logger.remove()
    logger.add(sys.stderr, format="{time:HH:mm:ss} | {level: <8} | {extra[process]} | {message}", level="INFO")
    log = logger.bind(process=comp) 
    return asyncio.run(process_competitor_async(log, comp, dom, clients))

def main():
    """Main execution entry point."""
    log = logger.bind(process="Main")
    log.info("Starting Pipeline")
    try:
        wd = r"C:\Users\shova\Downloads\here\ADMAVEN"
        os.chdir(wd)
        clients = set(pl.read_csv("our_clients.csv")["domains"].str.to_lowercase().str.replace(r"https?://|www\.", "", literal=False).str.strip_chars("/").to_list())
        comp_df = pl.read_csv("comp_run_time_domains.csv")
        tasks = list(zip(comp_df["competitor"], comp_df["run_time_domain"]))
    except Exception as e: return log.critical(f"Setup Failed: {e}")
    all_res = []
    with ProcessPoolExecutor(max_workers=os.cpu_count() or 4) as exc:
        futures = {exc.submit(worker_entry, c, d, clients): c for c, d in tasks}
        for f in as_completed(futures):
            try: all_res.extend(f.result())
            except Exception as e: log.error(f"Crash: {e}")
    if all_res:
        pl.DataFrame([asdict(x) for x in all_res]).write_csv("final_output.csv", quote_style="always")
        log.success("Done")
if __name__ == "__main__": main()