"""
MCP Server exposing AdMaven fraud detection and competitor analysis tools.

Tools:
- detect_fraud: Run Z-score fraud analysis on SQL query results
- scrape_competitor: Fetch and analyze competitor websites
- check_ads: Detect advertising activity in HTML content
"""
import os
import sys
import json
from typing import Dict, Any, Optional
from datetime import date
from dataclasses import asdict

# Add src to path for module imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mcp.server.fastmcp import FastMCP
from lib.fraud_detection import detect_fraud_from_sql, validate_domain
from lib.ad_detection import check_ads as check_ads_func
from lib.scraper import scrape_competitor_domain, EnrichedSite

# Create MCP server
mcp = FastMCP(
    name="AdMaven Fraud Detection",
    instructions="Tools for fraud detection, competitor analysis, and ad detection",
    json_response=True,
)


@mcp.tool()
def detect_fraud(sql_query: str, db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute a SQL query and run Z-score fraud analysis on the results.
    The query should return data with columns: tag_id, advertiser_id, converted_pixel, user_ip, device_type, etc.
    
    Args:
        sql_query: SQL query returning impression-level or tag-aggregated data
        db_path: Optional path to DuckDB database file
    
    Returns:
        Dictionary with fraud flags and summary statistics
    """
    if not sql_query or not sql_query.strip():
        return {'error': 'SQL query cannot be empty', 'success': False}
    
    try:
        result = detect_fraud_from_sql(sql_query, db_path)
        return result
    except Exception as e:
        return {'error': str(e), 'success': False}


@mcp.tool()
def scrape_competitor(
    domain: str,
    include_ad_detection: bool = True
) -> Dict[str, Any]:
    """
    Scrape a competitor's domain and all similar sites found via the API.
    
    Args:
        domain: Competitor's runtime domain to analyze
        include_ad_detection: Whether to run ad detection on scraped sites
    
    Returns:
        Dictionary with list of processed sites and summary
    """
    if not domain or not validate_domain(domain):
        return {
            'error': f'Invalid domain format: {domain}',
            'success': False,
            'domains_processed': 0,
        }
    
    try:
        import asyncio
        from lib.scraper import scrape_competitor_domain
        
        results = asyncio.run(scrape_competitor_domain(
            domain=domain,
            clients_set=set(),
            max_concurrency=5,
            use_llm=include_ad_detection,
        ))
        
        # Convert dataclasses to dicts
        sites = []
        for site in results:
            site_dict = asdict(site)
            sites.append(site_dict)
        
        summary = {
            'total_sites': len(sites),
            'sites_with_ads': sum(1 for s in sites if s.get('is_running_ads')),
            'blocked_sites': sum(1 for s in sites if s.get('got_blocked')),
            'existing_clients': sum(1 for s in sites if s.get('already_working')),
        }
        
        return {
            'success': True,
            'competitor_domain': domain,
            'sites': sites,
            'summary': summary,
        }
    except Exception as e:
        return {'error': str(e), 'success': False}


@mcp.tool()
async def check_ads(html: str, use_llm: bool = True) -> Dict[str, Any]:
    """
    Detect advertising activity in HTML content using heuristic analysis and optional LLM.
    
    Args:
        html: Raw HTML content to analyze
        use_llm: Whether to use LLM for uncertain cases (requires HF_TOKEN)
    
    Returns:
        Dictionary with ad detection result and evidence
    """
    if not html or not isinstance(html, str):
        return {
            'error': 'HTML content must be a non-empty string',
            'success': False,
            'is_running_ads': False,
        }
    
    try:
        result = await check_ads_func(html, use_llm=use_llm)
        result['success'] = True
        return result
    except Exception as e:
        return {'error': str(e), 'success': False, 'is_running_ads': False}


# Run server
if __name__ == "__main__":
    # Default to stdio transport for MCP compatibility
    mcp.run(transport="stdio")
