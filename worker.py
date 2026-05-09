"""
Temporal worker process for fraud detection workflows.

Run this separately to start the worker that executes workflows and activities.
"""
import asyncio
import os
import sys

from temporalio.client import Client
from temporalio.worker import Worker
# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.telemetry import setup_telemetry
from src.workflows.fraud_detection import FraudDetectionWorkflow
from src.activities.fraud_activities import (
    fetch_similar_sites_activity,
    scrape_and_analyze_site_activity,
    run_zscore_analysis_activity,
    generate_report_activity,
)


async def run_worker():
    """Start and run the Temporal worker."""
    # Load env
    from dotenv import load_dotenv
    load_dotenv()

    # Initialize telemetry
    otlp_endpoint = os.getenv("OTLP_ENDPOINT", "http://localhost:4317")
    setup_telemetry(
        service_name="fraud-detection-worker",
        otlp_endpoint=otlp_endpoint if os.getenv("ENABLE_OTLP") else None,
        enable_console=os.getenv("DEBUG_TELEMETRY") == "1",
    )

    # Connect to Temporal
    temporal_host = os.getenv("TEMPORAL_HOST", "localhost:7233")
    client = await Client.connect(temporal_host)

    # Run worker
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

    print(f"[worker] Connected to Temporal at {temporal_host}")
    print("[worker] Starting worker on 'fraud-detection-queue'...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_worker())
