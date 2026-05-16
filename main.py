"""
Main entry point for the fraud detection Temporal workflow.

This script:
1. Loads competitor domains and client lists from CSV files
2. Initializes OpenTelemetry observability
3. Creates a Temporal worker with workflow and activities
4. Starts the fraud detection workflow
"""
import asyncio
import json
import os
import sys
from datetime import date, timedelta
from typing import List
import polars as pl
from temporalio.client import Client
from temporalio.worker import Worker
from opentelemetry import trace
from dotenv import load_dotenv

# Add src directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.telemetry import setup_telemetry, get_tracer
from src.workflows.fraud_detection import FraudDetectionWorkflow, FraudDetectionWorkflowInput
from src.activities.fraud_activities import (
    fetch_similar_sites_activity,
    scrape_and_analyze_site_activity,
    run_zscore_analysis_activity,
    generate_report_activity,
)


# Get tracer for this module
tracer = trace.get_tracer(__name__)


def load_competitors(csv_path: str) -> List[dict]:
    """Load competitor list from CSV."""
    df = pl.read_csv(csv_path)
    required_cols = {"competitor", "run_time_domain"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {required_cols}")
    return [{"name": row["competitor"], "domain": row["run_time_domain"]} for row in df.to_dicts()]


def load_clients(csv_path: str) -> List[str]:
    """Load client domains from CSV."""
    df = pl.read_csv(csv_path)
    col = df.columns[0]
    return df[col].str.to_lowercase().str.replace(r"https?://|www\.", "", literal=False).str.strip_chars("/").to_list()


async def run_worker(client: Client) -> None:
    """Start and run the Temporal worker."""
    worker = Worker(
        client,
        task_queue="fraud-detection-queue",
        workflows=[FraudDetectionWorkflow],
        activities=[
            fetch_similar_sites_activity,
            scrape_and_analyze_site_activity,
            run_zscore_analysis_activity,
            generate_report_activity,
        ],
    )
    print("[worker] Worker started, listening for workflow tasks...")
    await worker.run()


async def start_workflow_async(client: Client, input_data: FraudDetectionWorkflowInput) -> dict:
    """Start a new fraud detection workflow."""
    handle = await client.start_workflow(
        FraudDetectionWorkflow.run,
        input_data,
        id=f"fraud-detection-{date.today().isoformat()}",
        task_queue="fraud-detection-queue",
        execution_timeout=timedelta(hours=4),
        run_timeout=timedelta(hours=2),
        task_timeout=timedelta(minutes=10),
    )
    print(f"[main] Workflow started: {handle.workflow_id}")
    print(f"[main] Waiting for completion...")
    result = await handle.result()
    return result


async def main():
    """Main entry point."""
    load_dotenv()

    # Environment variables
    API_KEY = os.getenv("API_KEY")
    HF_TOKEN = os.getenv("HF_TOKEN")
    TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "final_output.csv")
    MODE = os.getenv("MODE", "client")  # "client" or "worker"

    if not API_KEY:
        print("ERROR: API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)
    if not HF_TOKEN:
        print("ERROR: HF_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)

    # Initialize OpenTelemetry
    otlp_endpoint = os.getenv("OTLP_ENDPOINT", "http://localhost:4317")
    setup_telemetry(
        service_name="fraud-detection-workflow",
        otlp_endpoint=otlp_endpoint if os.getenv("ENABLE_OTLP") else None,
        enable_console=os.getenv("DEBUG_TELEMETRY") == "1",
    )

    with tracer.start_as_current_span("main_setup"):
        # Connect to Temporal
        print(f"[main] Connecting to Temporal at {TEMPORAL_HOST}")
        client = await Client.connect(TEMPORAL_HOST)

        if MODE == "worker":
            # Run as worker
            await run_worker(client)
        else:
            # Run as client - load data and start workflow
            workdir = os.getenv("WORKDIR", os.getcwd())
            competitors_csv = os.path.join(workdir, "data", "comp_run_time_domains.csv")
            clients_csv = os.path.join(workdir, "data", "our_clients.csv")

            print(f"[main] Loading competitors from {competitors_csv}")
            competitors = load_competitors(competitors_csv)
            print(f"[main] Loaded {len(competitors)} competitors")

            print(f"[main] Loading clients from {clients_csv}")
            clients = load_clients(clients_csv)
            print(f"[main] Loaded {len(clients)} client domains")

            # Build workflow input
            workflow_input = FraudDetectionWorkflowInput(
                competitors=competitors,
                clients=clients,
                api_key=API_KEY,
                hf_token=HF_TOKEN,
                output_path=OUTPUT_PATH,
            )

            result = await start_workflow_async(client, workflow_input)

            print(f"[main] Output file: {result.get('summary', {}).get('output_file', OUTPUT_PATH)}")
            return result

    return None


if __name__ == "__main__":
    result = asyncio.run(main())
    if result:
        print("\n=== FINAL RESULT ===")
        print(json.dumps(result, indent=2, default=str))
