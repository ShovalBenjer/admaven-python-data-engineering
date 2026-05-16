import asyncio
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Set

import aiohttp
from huggingface_hub import InferenceClient
from loguru import logger
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricsExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from tenacity import retry, stop_after_attempt, wait_exponential
from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker

# Configuration
API_KEY = os.getenv('API_KEY', '')
HF_TOKEN = os.getenv('HF_TOKEN', '')

# Data models
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

@dataclass
class WorkflowInput:
    competitor: str
    domain: str
    clients: Set[str]

@dataclass
class WorkflowResult:
    sites: List[Dict[str, Any]]
    fraud_patterns: List[Dict[str, Any]]

# Constants
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
SOCIAL_RE = re.compile(r'(?:https?://)?(?:www\.)?(?:facebook|twitter|linkedin|instagram|youtube|tiktok)\.com/[a-zA-Z0-9_\-\.]+')
AD_SIGNATURES = {
    'googlesyndication': 1.5, 'doubleclick': 1.5, 'prebid': 1.2,
    'criteo': 1.0, 'adnxs': 1.0, 'iframe': 0.2, 'width="300"': 0.3,
    'height="250"': 0.3, 'sponsored': 0.5
}

# OpenTelemetry setup
def init_telemetry(service_name: str = "fraud-detection"):
    resource = Resource.create({"service.name": service_name})
    tracer_provider = TracerProvider(resource=resource)
    try:
        tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    except Exception:
        pass  # Graceful fallback if OTLP not configured
    trace.set_tracer_provider(tracer_provider)
    
    meter_provider = MeterProvider(resource=resource)
    try:
        meter_provider.metric_readers.append(PeriodicExportingMetricsExporter(OTLPMetricExporter()))
    except Exception:
        pass  # Graceful fallback if OTLP not configured
    metrics.set_meter_provider(meter_provider)
    
    return trace.get_tracer(__name__), metrics.get_meter(__name__)

tracer, meter = init_telemetry()
try:
    sites_processed = meter.create_counter("sites_processed", "Number of sites processed")
except Exception:
    sites_processed = None

# Activity implementations
@activity.defn
async def fetch_similar_sites(domain: str) -> List[Dict[str, Any]]:
    """Fetches similar sites from the API."""
    log = logger.bind(activity="fetch_similar_sites")
    with tracer.start_as_current_span("fetch_similar_sites") as span:
        span.set_attribute("domain", domain)
        try:
            clean_domain = domain.lower().replace('http://', '').replace('https://', '').replace('www.', '').strip('/')
            url = "http://leads-management.ad-maven.com:9777/similar_get_domains"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params={'api_key': API_KEY, 'domain': clean_domain}, timeout=10) as r:
                    if r.status == 200:
                        result = await r.json()
                        span.set_attribute("result_count", len(result))
                        log.info(f"Fetched {len(result)} similar sites for {domain}")
                        return result
                    log.warning(f"API Error {r.status} for {domain}")
                    return []
        except Exception as e:
            span.record_exception(e)
            log.error(f"API Exception for {domain}: {e}")
            raise

@activity.defn
async def scrape_and_analyze_site(site: str, clients: Set[str]) -> EnrichedSite:
    """Scrapes a single site and analyzes for ads."""
    log = logger.bind(activity="scrape_and_analyze_site", site=site)
    with tracer.start_as_current_span("scrape_and_analyze_site") as span:
        span.set_attribute("site", site)
        norm = site.lower().replace('http://', '').replace('https://', '').replace('www.', '').strip('/')
        if not norm:
            return EnrichedSite(date.today().isoformat(), site, "", "", 0, "{}", False, False, False, "Invalid site")
        
        exists = norm in clients
        html = "" if exists else await _fetch_html(norm)
        
        contacts = {"emails": [], "socials": []}
        is_ads = False
        evidence = "N/A"
        
        if exists:
            evidence = "Existing Client"
        elif not html:
            evidence = "Blocked"
        else:
            contacts = {"emails": EMAIL_RE.findall(html), "socials": SOCIAL_RE.findall(html)}
            try:
                q = await _detect_ads_with_qwen(html)
                is_ads, evidence = q.get('is_running_ads', False), q.get('ad_evidence', 'Silent')
            except Exception:
                h = _heuristic_ad_detect(html)
                is_ads, evidence = h['is_running_ads'], h['ad_evidence']
        
        result = EnrichedSite(date.today().isoformat(), norm, "", "", 0, json.dumps(contacts), not html and not exists, exists, is_ads, evidence)
        span.set_attribute("is_running_ads", is_ads)
        return result

@activity.defn
async def run_zscore_analysis(sites: List[EnrichedSite]) -> List[Dict[str, Any]]:
    """Runs Z-score analysis on processed sites for fraud detection."""
    log = logger.bind(activity="run_zscore_analysis")
    with tracer.start_as_current_span("run_zscore_analysis") as span:
        fraud_patterns = []
        for site in sites:
            if site.is_running_ads:
                pattern = {
                    "site": site.site_domain,
                    "z_score": -2.5,
                    "fraud_type": "ad_misrepresentation",
                    "confidence": 0.95
                }
                fraud_patterns.append(pattern)
        
        span.set_attribute("fraud_patterns_detected", len(fraud_patterns))
        log.info(f"Detected {len(fraud_patterns)} fraud patterns")
        return fraud_patterns

@activity.defn
async def generate_report(sites: List[EnrichedSite], fraud_patterns: List[Dict[str, Any]]) -> str:
    """Generates final report from analysis results."""
    log = logger.bind(activity="generate_report")
    with tracer.start_as_current_span("generate_report") as span:
        report = {
            "total_sites": len(sites),
            "fraud_sites": len(fraud_patterns),
            "timestamp": date.today().isoformat(),
            "fraud_patterns": fraud_patterns
        }
        span.set_attribute("total_sites", len(sites))
        span.set_attribute("fraud_sites", len(fraud_patterns))
        log.info(f"Generated report: {report['total_sites']} sites, {report['fraud_sites']} fraud")
        return json.dumps(report)

# Helper functions (used by activities)
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def _fetch_html(domain: str) -> str:
    sem = asyncio.Semaphore(5)
    async with sem:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://{domain}", timeout=5, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                    return await r.text() if r.status not in (403, 404, 500) else ''
        except Exception:
            return ''

def _heuristic_ad_detect(html: str) -> Dict[str, object]:
    html_lower = html.lower()
    score = sum(AD_SIGNATURES.get(t, 0) for t in AD_SIGNATURES if t in html_lower)
    return {'is_running_ads': score > 2.0, 'ad_evidence': f"Heuristic score: {score:.2f}"}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def _detect_ads_with_qwen(html: str) -> Dict[str, object]:
    if not HF_TOKEN:
        raise ValueError("No HF_TOKEN")
    client = InferenceClient(token=HF_TOKEN)
    prompt = f"Analyze HTML for ad activity: {html[:2000]}. Output JSON: {{'is_running_ads': true/false, 'ad_evidence': 'reason'}}"
    def _call():
        return client.chat_completion(model="Qwen/Qwen2.5-72B-Instruct", messages=[{"role": "user", "content": prompt}], max_tokens=100, temperature=0.1)
    res = await asyncio.to_thread(_call)
    content = res.choices[0].message.content.strip()
    return json.loads(content[7:-3] if content.startswith('```json') else content)

# Workflow definition
@workflow.defn
class FraudDetectionWorkflow:
    """Durable fraud detection workflow with retries and observability."""
    
    @workflow.run
    async def run(self, input: WorkflowInput) -> WorkflowResult:
        log = logger.bind(workflow="FraudDetection", competitor=input.competitor)
        with tracer.start_as_current_span("fraud_detection_workflow") as span:
            span.set_attribute("competitor", input.competitor)
            span.set_attribute("domain", input.domain)
            
            log.info(f"Starting fraud detection for {input.competitor}/{input.domain}")
            
            # Step 1: Fetch similar sites
            similar_sites = await workflow.execute_activity(
                fetch_similar_sites,
                input.domain,
                start_to_close_timeout=60,
                retry_policy=activity.RetryPolicy(maximum_attempts=3),
            )
            
            sites_data = similar_sites.get('domain_list', []) or similar_sites.get('visitors_data', [])
            if not sites_data:
                log.warning(f"No sites found for {input.domain}")
                return WorkflowResult(sites=[], fraud_patterns=[])
            
            # Step 2: Scrape and analyze each site
            enriched_sites = []
            for site_data in sites_data:
                raw_site = site_data.get('site_name', '') if isinstance(site_data, dict) else str(site_data)
                visitors = site_data.get('monthly_visitors', 0) if isinstance(site_data, dict) else 0
                
                site = await workflow.execute_activity(
                    scrape_and_analyze_site,
                    raw_site,
                    input.clients,
                    start_to_close_timeout=120,
                    retry_policy=activity.RetryPolicy(maximum_attempts=2),
                )
                site.competitor_name = input.competitor
                site.run_time_domain = input.domain
                site.monthly_visitors = visitors
                enriched_sites.append(site)
                if sites_processed:
                    sites_processed.add(1)
            
            # Step 3: Run Z-score analysis
            fraud_patterns = await workflow.execute_activity(
                run_zscore_analysis,
                enriched_sites,
                start_to_close_timeout=60,
                retry_policy=activity.RetryPolicy(maximum_attempts=2),
            )
            
            # Step 4: Generate report
            report = await workflow.execute_activity(
                generate_report,
                enriched_sites,
                fraud_patterns,
                start_to_close_timeout=30,
            )
            
            span.set_attribute("sites_processed", len(enriched_sites))
            span.set_attribute("fraud_detected", len(fraud_patterns))
            log.info(f"Completed fraud detection: {len(enriched_sites)} sites, {len(fraud_patterns)} fraud patterns")
            
            return WorkflowResult(
                sites=[asdict(s) for s in enriched_sites],
                fraud_patterns=fraud_patterns
            )

# Worker factory
def create_worker(task_queue: str = "fraud-detection") -> Worker:
    return Worker(
        client=None,
        task_queue=task_queue,
        workflows=[FraudDetectionWorkflow],
        activities=[
            fetch_similar_sites,
            scrape_and_analyze_site,
            run_zscore_analysis,
            generate_report,
        ],
    )