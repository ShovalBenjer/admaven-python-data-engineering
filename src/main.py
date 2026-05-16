import asyncio
import json
import os
import sys
from typing import Set

import polars as pl
from loguru import logger
from temporalio.client import Client

from workflows.fraud_detection import (
    FraudDetectionWorkflow,
    WorkflowInput,
    create_worker,
)

logger.remove()
logger.add(sys.stderr, format="{time:HH:mm:ss} | {level: <8} | {message}", level="INFO")

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = "fraud-detection"

async def load_clients(csv_path: str) -> Set[str]:
    """Load existing clients from CSV."""
    return set(
        pl.read_csv(csv_path)["domains"]
        .str.to_lowercase()
        .str.replace(r"https?://|www\.", "", literal=False)
        .str.strip_chars("/")
        .to_list()
    )

async def run_workflow(client: Client, workflow_id: str, competitor: str, domain: str, clients: Set[str]) -> dict:
    """Run a single fraud detection workflow."""
    handle = await client.start_workflow(
        FraudDetectionWorkflow.run,
        WorkflowInput(competitor=competitor, domain=domain, clients=clients),
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return result

async def main():
    """Main entry point for fraud detection workflows."""
    log = logger.bind(process="Main")
    log.info(f"Connecting to Temporal server at {TEMPORAL_ADDRESS}")
    
    client = await Client.connect(TEMPORAL_ADDRESS)
    
    try:
        wd = r"C:\Users\shova\Downloads\here\ADMAVEN"
        os.chdir(wd)
        clients = await load_clients("our_clients.csv")
        comp_df = pl.read_csv("comp_run_time_domains.csv")
    except Exception as e:
        log.critical(f"Setup Failed: {e}")
        return
    
    tasks = list(zip(comp_df["competitor"], comp_df["run_time_domain"]))
    log.info(f"Starting {len(tasks)} fraud detection workflows")
    
    results = []
    for competitor, domain in tasks:
        workflow_id = f"fraud-{competitor}-{domain}".replace(".", "-")
        try:
            result = await run_workflow(client, workflow_id, competitor, domain, clients)
            results.append(result)
            log.info(f"Completed workflow for {competitor}/{domain}")
        except Exception as e:
            log.error(f"Workflow failed for {competitor}/{domain}: {e}")
    
    # Aggregate results
    all_sites = []
    all_fraud = []
    for r in results:
        all_sites.extend(r.sites)
        all_fraud.extend(r.fraud_patterns)
    
    if all_sites:
        output = {
            "sites": all_sites,
            "fraud_patterns": all_fraud,
            "summary": {
                "total_sites": len(all_sites),
                "fraud_sites": len(all_fraud)
            }
        }
        with open("final_output.json", "w") as f:
            json.dump(output, f, indent=2)
        log.success(f"Done. Processed {len(all_sites)} sites, detected {len(all_fraud)} fraud patterns")

async def run_worker():
    """Run the Temporal worker."""
    client = await Client.connect(TEMPORAL_ADDRESS)
    worker = create_worker(TASK_QUEUE)
    worker.client = client
    log = logger.bind(process="Worker")
    log.info(f"Starting worker on task queue: {TASK_QUEUE}")
    await worker.run()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true", help="Run as Temporal worker")
    args = parser.parse_args()
    
    if args.worker:
        asyncio.run(run_worker())
    else:
        asyncio.run(main())