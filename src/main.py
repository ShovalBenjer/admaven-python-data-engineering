import asyncio
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from typing import List, Set
from loguru import logger
import polars as pl

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Import refactored modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.scraper import (
    normalize_domain,
    scrape_competitor_domain,
    EnrichedSite as ScraperEnrichedSite,
)
from lib.ad_detection import check_ads
from lib.fraud_detection import validate_domain, detect_fraud_from_sql

# Configure logging
logger.remove()
logger.add(
    sys.stderr,
    format="{time:HH:mm:ss} | {level: <8} | {extra[process]:<10} | {message}",
    level="INFO",
    colorize=True,
)

# Environment configuration
DATA_DIR = os.getenv('DATA_DIR', os.path.join(os.path.dirname(__file__), '..', 'data'))
DATA_DIR = os.path.abspath(DATA_DIR)
API_KEY = os.getenv('API_KEY')


def load_clients(csv_path: str) -> Set[str]:
    """
    Load client domains from CSV and normalize them.
    Args:
        csv_path: Path to our_clients.csv
    Returns:
        Set of normalized domain strings
    """
    try:
        df = pl.read_csv(csv_path)
        domain_col = next((c for c in df.columns if 'domain' in c.lower()), df.columns[0])
        domains = (
            df[domain_col]
            .str.to_lowercase()
            .str.replace(r"https?://|www\.", "", literal=False)
            .str.strip_chars("/")
            .to_list()
        )
        return set(filter(None, domains))
    except Exception as e:
        logger.error(f"Failed to load clients from {csv_path}: {e}")
        return set()


def worker_entry(comp: str, dom: str, clients: Set[str]) -> List[ScraperEnrichedSite]:
    """
    Entry point for worker process - scrapes a competitor domain.
    Args:
        comp: Competitor name
        dom: Runtime domain
        clients: Set of existing client domains
    Returns:
        List of EnrichedSite results
    """
    logger.remove()
    logger.add(
        sys.stderr,
        format="{time:HH:mm:ss} | {level: <8} | {extra[process]:<10} | {message}",
        level="INFO",
        colorize=False,
    )
    log = logger.bind(process=comp[:10])
    log.info(f"Scraping competitor: {dom}")
    
    try:
        results = asyncio.run(scrape_competitor_domain(dom, clients, max_concurrency=5, use_llm=True))
        log.info(f"Completed {dom}: {len(results)} sites processed")
        return results
    except Exception as e:
        log.error(f"Scraping failed for {dom}: {e}")
        return []


def main():
    """Main execution entry point."""
    log = logger.bind(process="Main")
    log.info("Starting AdMaven Pipeline")
    
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    clients_csv = os.path.join(DATA_DIR, 'our_clients.csv')
    comp_csv = os.path.join(DATA_DIR, 'comp_run_time_domains.csv')
    
    try:
        clients = load_clients(clients_csv)
        log.info(f"Loaded {len(clients)} client domains")
        
        comp_df = pl.read_csv(comp_csv)
        if comp_df.is_empty():
            log.warning("No competitor domains found in CSV")
            return
        
        tasks = []
        for row in comp_df.iter_rows(named=True):
            comp_name = row.get('competitor', '')
            run_domain = row.get('run_time_domain', '')
            if comp_name and run_domain and validate_domain(run_domain):
                tasks.append((comp_name, run_domain))
        
        if not tasks:
            log.warning("No valid competitor tasks to process")
            return
        
        log.info(f"Processing {len(tasks)} competitors")
        
        all_res = []
        cpu_count = os.cpu_count() or 4
        
        with ProcessPoolExecutor(max_workers=min(cpu_count, len(tasks))) as exc:
            futures = {
                exc.submit(worker_entry, comp, dom, clients): comp
                for comp, dom in tasks
            }
            for f in as_completed(futures):
                try:
                    all_res.extend(f.result())
                except Exception as e:
                    log.error(f"Worker crash: {e}")
        
        if all_res:
            output_path = os.path.join(DATA_DIR, 'final_output.csv')
            df = pl.DataFrame([asdict(x) for x in all_res])
            df.write_csv(output_path, quote_style="always")
            log.success(f"Pipeline complete: {len(all_res)} records written to {output_path}")
        else:
            log.warning("No results produced")
            
    except FileNotFoundError as e:
        log.critical(f"Required file missing: {e}")
    except Exception as e:
        log.critical(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()
