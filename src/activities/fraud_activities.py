"""Fraud detection activities for Temporal workflows."""
import asyncio
import json
import re
import os
from dataclasses import dataclass, asdict
from datetime import date
from typing import List, Dict, Optional
from temporalio import activity
from temporalio.exceptions import ApplicationError
from opentelemetry import trace
from loguru import logger
import polars as pl
import aiohttp
from huggingface_hub import InferenceClient
from tenacity import retry, stop_after_attempt, wait_exponential

tracer = trace.get_tracer(__name__)

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
SOCIAL_RE = re.compile(r'(?:https?://)?(?:www\.)?(?:facebook|twitter|linkedin|instagram|youtube|tiktok)\.com/[a-zA-Z0-9_\-\.]+')
AD_SIGNATURES = {
    'googlesyndication': 1.5, 'doubleclick': 1.5, 'prebid': 1.2,
    'criteo': 1.0, 'adnxs': 1.0, 'iframe': 0.2,
    'width="300"': 0.3, 'height="250"': 0.3, 'sponsored': 0.5
}


@dataclass
class EnrichedSite:
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
    html_lower = html.lower()
    score = sum(AD_SIGNATURES.get(t, 0) for t in AD_SIGNATURES if t in html_lower)
    return {'is_running_ads': score > 2.0, 'ad_evidence': f"Heuristic score: {score:.2f}"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def detect_ads_with_qwen(html: str, hf_token: str) -> Dict[str, object]:
    if not hf_token:
        raise ValueError("Missing HF_TOKEN")
    client = InferenceClient(token=hf_token)
    prompt = f"Analyze HTML for ad activity: {html[:2000]}. Output JSON: {{'is_running_ads': true/false, 'ad_evidence': 'reason'}}"

    def _call():
        return client.chat_completion(
            model="z-ai/glm-5.1",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100, temperature=0.1
        )

    res = await asyncio.to_thread(_call)
    content = res.choices[0].message.content.strip()
    return json.loads(content[7:-3] if content.startswith('```json') else content)


async def fetch_site_data(session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore) -> str:
    async with sem:
        try:
            async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                return await r.text() if r.status not in (403, 404, 500) else ''
        except Exception:
            return ''


@activity.defn(name="fetch_similar_sites")
async def fetch_similar_sites_activity(domain: str, api_key: str, competitor_name: str) -> List[Dict]:
    logger.info(f"[{competitor_name}] Fetching similar sites for {domain}")
    with tracer.start_as_current_span("fetch_similar_sites") as span:
        span.set_attribute("competitor", competitor_name)
        span.set_attribute("domain", domain)
        span.set_attribute("activity.name", "fetch_similar_sites")
        try:
            clean_domain = domain.lower().replace('http://', '').replace('https://', '').replace('www.', '').strip('/')
            span.set_attribute("clean_domain", clean_domain)
            url = "http://leads-management.ad-maven.com:9777/similar_get_domains"
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params={'api_key': api_key, 'domain': clean_domain}) as r:
                    if r.status == 200:
                        data = await r.json()
                        sites = data.get('domain_list', []) or data.get('visitors_data', [])
                        span.set_attribute("sites.found", len(sites))
                        logger.info(f"[{competitor_name}] Found {len(sites)} similar sites")
                        span.set_attribute("activity.status", "success")
                        return sites
                    span.set_attribute("http.status", r.status)
                    logger.warning(f"[{competitor_name}] API error {r.status} for {domain}")
                    span.set_attribute("activity.status", "api_error")
                    return []
        except Exception as e:
            logger.error(f"[{competitor_name}] API exception: {e}")
            span.set_attribute("activity.status", "failed")
            span.set_attribute("activity.error", str(e))
            raise ApplicationError("API_ERROR", str(e))


@activity.defn(name="scrape_and_analyze_site")
async def scrape_and_analyze_site_activity(
    site_info: Dict,
    domain: str,
    competitor_name: str,
    clients: List[str],
    hf_token: str
) -> EnrichedSite:
    clients_set = set(clients)
    with tracer.start_as_current_span("scrape_and_analyze_site") as span:
        span.set_attribute("competitor", competitor_name)
        span.set_attribute("domain", domain)

        if isinstance(site_info, dict):
            raw_site = site_info.get('site_name', '')
            visitors = site_info.get('monthly_visitors', 0)
        else:
            raw_site = str(site_info)
            visitors = 0

        norm = raw_site.lower().replace('http://', '').replace('https://', '').replace('www.', '').strip('/')
        span.set_attribute("site_domain", norm)

        if not norm:
            span.set_attribute("activity.status", "invalid_domain")
            return EnrichedSite(
                scan_date=date.today().isoformat(),
                site_domain="",
                competitor_name=competitor_name,
                run_time_domain=domain,
                monthly_visitors=0,
                contacts_json="{}",
                got_blocked=False,
                already_working=False,
                is_running_ads=False,
                ad_evidence="Invalid domain"
            )

        exists = norm in clients_set
        span.set_attribute("is_existing_client", exists)
        sem = asyncio.Semaphore(5)
        timeout = aiohttp.ClientTimeout(total=10)

        html = ''
        if not exists:
            with tracer.start_as_current_span("fetch_html") as fetch_span:
                try:
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        html = await fetch_site_data(session, f"http://{norm}", sem)
                    fetch_span.set_attribute("html.length", len(html))
                    fetch_span.set_attribute("activity.status", "success")
                except Exception as e:
                    fetch_span.set_attribute("activity.status", "failed")
                    fetch_span.set_attribute("activity.error", str(e))
        else:
            span.set_attribute("fetch.skipped", "existing_client")

        contacts, is_ads, evidence = {}, False, "N/A"

        if exists:
            evidence = "Existing Client"
        elif not html:
            evidence = "Blocked"
            span.set_attribute("site.blocked", True)
        else:
            span.set_attribute("site.blocked", False)
            with tracer.start_as_current_span("analyze_content") as analyze_span:
                contacts = {'emails': EMAIL_RE.findall(html), 'socials': SOCIAL_RE.findall(html)}
                analyze_span.set_attribute("emails.count", len(contacts['emails']))
                analyze_span.set_attribute("socials.count", len(contacts['socials']))
                try:
                    q = await detect_ads_with_qwen(html, hf_token)
                    is_ads, evidence = q.get('is_running_ads', False), q.get('ad_evidence', 'Silent')
                    analyze_span.set_attribute("analysis.method", "llm")
                except Exception as e:
                    logger.warning(f"[{competitor_name}] LLM analysis failed for {norm}: {e}")
                    h = heuristic_ad_detect(html)
                    is_ads, evidence = h['is_running_ads'], h['ad_evidence']
                    analyze_span.set_attribute("analysis.method", "heuristic")
                    analyze_span.set_attribute("heuristic.fallback", True)
                analyze_span.set_attribute("is_running_ads", is_ads)

        span.set_attribute("is_running_ads", is_ads)
        span.set_attribute("ad_evidence", evidence)

        enriched = EnrichedSite(
            scan_date=date.today().isoformat(),
            site_domain=norm,
            competitor_name=competitor_name,
            run_time_domain=domain,
            monthly_visitors=visitors,
            contacts_json=json.dumps(contacts),
            got_blocked=not html and not exists,
            already_working=exists,
            is_running_ads=is_ads,
            ad_evidence=evidence
        )
        span.set_attribute("activity.status", "success")
        return enriched


@activity.defn(name="run_zscore_analysis")
async def run_zscore_analysis_activity(enriched_sites: List[EnrichedSite]) -> Dict:
    logger.info(f"Running Z-score analysis on {len(enriched_sites)} sites")

    with tracer.start_as_current_span("run_zscore_analysis") as span:
        span.set_attribute("sites.count", len(enriched_sites))

        if not enriched_sites:
            span.set_attribute("activity.status", "no_data")
            return {"error": "No data to analyze", "flagged_anomalies": []}

        with tracer.start_as_current_span("convert_to_dataframe"):
            df = pl.DataFrame([asdict(site) for site in enriched_sites])
            span.set_attribute("dataframe.rows", df.height)
            span.set_attribute("dataframe.columns", df.width)

        with tracer.start_as_current_span("filter_ads_sites"):
            ads_df = df.filter(pl.col("is_running_ads"))
            span.set_attribute("ads_sites.count", ads_df.height)

        if ads_df.height == 0:
            logger.info("No sites running ads found")
            span.set_attribute("activity.status", "no_ads_found")
            return {"total_sites": len(enriched_sites), "ads_sites": 0, "flagged_anomalies": []}

        with tracer.start_as_current_span("aggregate_advertiser_stats"):
            advertiser_stats = (
                ads_df.group_by("run_time_domain")
                .agg([
                    pl.count().alias("total_ads_sites"),
                    pl.sum("monthly_visitors").alias("total_visitors"),
                    pl.mean("monthly_visitors").alias("avg_visitors")
                ])
            )
            span.set_attribute("advertisers.count", advertiser_stats.height)

        with tracer.start_as_current_span("calculate_z_scores"):
            mean_visitors = advertiser_stats.select(pl.mean("total_visitors")).item()
            std_visitors = advertiser_stats.select(pl.std("total_visitors")).item()
            span.set_attribute("mean_visitors", mean_visitors)
            span.set_attribute("std_visitors", std_visitors or 0)

            if std_visitors and std_visitors > 0:
                all_stats = advertiser_stats.with_columns([
                    ((pl.col("total_visitors") - mean_visitors) / std_visitors).alias("z_score")
                ])
            else:
                all_stats = advertiser_stats.with_columns([
                    pl.lit(0.0).alias("z_score")
                ])

        with tracer.start_as_current_span("flag_anomalies"):
            anomalies_df = all_stats.filter(pl.col("z_score").abs() > 1.96)
            span.set_attribute("advertiser_anomalies.count", anomalies_df.height)

        site_indicators = []
        site_mean = ads_df.select(pl.mean("monthly_visitors")).item()
        site_std = ads_df.select(pl.std("monthly_visitors")).item()
        if site_std and site_std > 0:
            for row in ads_df.iter_rows(named=True):
                z = (row["monthly_visitors"] - site_mean) / site_std
                if abs(z) > 1.96:
                    site_indicators.append({
                        "site_domain": row["site_domain"],
                        "run_time_domain": row["run_time_domain"],
                        "monthly_visitors": row["monthly_visitors"],
                        "z_score": round(z, 2),
                        "status": "FRAUD_CONFIRMED" if z < -1.96 else "REVIEW_REQUIRED"
                    })

        span.set_attribute("anomalies.flagged", len(site_indicators))
        logger.info(f"Z-score analysis complete: {len(site_indicators)} flagged anomalies")
        span.set_attribute("activity.status", "success")

        return {
            "total_sites": len(enriched_sites),
            "ads_sites": ads_df.height,
            "advertiser_stats": advertiser_stats.to_dicts(),
            "flagged_anomalies": site_indicators,
            "analysis_date": date.today().isoformat()
        }


@activity.defn(name="generate_report")
def generate_report_activity(
    enriched_sites: List[EnrichedSite],
    zscore_results: Dict,
    output_path: str = "final_output.csv"
) -> Dict:
    logger.info(f"Generating report with {len(enriched_sites)} sites")

    with tracer.start_as_current_span("generate_report") as span:
        span.set_attribute("sites.count", len(enriched_sites))

        df = pl.DataFrame([asdict(site) for site in enriched_sites])
        span.set_attribute("dataframe.rows", df.height)

        output_dir = os.path.dirname(output_path) if os.path.dirname(output_path) else "."
        os.makedirs(output_dir, exist_ok=True)
        df.write_csv(output_path, quote_style="always")

        span.set_attribute("output.path", output_path)

        total_sites = len(enriched_sites)
        sites_with_ads = sum(1 for s in enriched_sites if s.is_running_ads)
        blocked_sites = sum(1 for s in enriched_sites if s.got_blocked)
        existing_clients = sum(1 for s in enriched_sites if s.already_working)
        anomalies = len(zscore_results.get("flagged_anomalies", []))

        span.set_attribute("sites.with_ads", sites_with_ads)
        span.set_attribute("sites.blocked", blocked_sites)
        span.set_attribute("sites.existing_clients", existing_clients)
        span.set_attribute("anomalies.flagged", anomalies)
        span.set_attribute("activity.status", "success")

        summary = {
            "output_file": output_path,
            "total_sites_processed": total_sites,
            "sites_running_ads": sites_with_ads,
            "sites_blocked": blocked_sites,
            "existing_clients": existing_clients,
            "flagged_anomalies": anomalies,
            "scan_date": date.today().isoformat(),
            "zscore_results": zscore_results
        }

        logger.success(f"Report generated: {output_path}")
        return summary
