"""Fraud detection Temporal workflow definition."""
import asyncio
from datetime import timedelta
from typing import List, Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy
from dataclasses import dataclass

with workflow.unsafe.imports_passed_through():
    from src.activities.fraud_activities import (
        fetch_similar_sites_activity,
        scrape_and_analyze_site_activity,
        run_zscore_analysis_activity,
        generate_report_activity,
        EnrichedSite,
    )

DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=["API_ERROR"],
)

SOFT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=5,
)


@dataclass
class FraudDetectionWorkflowInput:
    competitors: List[Dict[str, str]]
    clients: List[str]
    api_key: str
    hf_token: str
    output_path: str = "final_output.csv"


@workflow.defn(name="fraud_detection_workflow")
class FraudDetectionWorkflow:

    @workflow.run
    async def run(self, input: FraudDetectionWorkflowInput) -> Dict[str, Any]:
        workflow.logger.info(f"Starting fraud detection workflow for {len(input.competitors)} competitors")

        clients_set = set(
            c.lower().replace('http://', '').replace('https://', '').replace('www.', '').strip('/')
            for c in input.clients
        )

        workflow.logger.info("Step 1: Fetching similar sites for all competitors")
        fetch_tasks = [
            workflow.execute_activity(
                fetch_similar_sites_activity,
                competitor["domain"],
                input.api_key,
                competitor["name"],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=DEFAULT_RETRY_POLICY,
            )
            for competitor in input.competitors
        ]
        similar_sites_results = await asyncio.gather(*fetch_tasks)

        all_sites_to_process = []
        for competitor, sites in zip(input.competitors, similar_sites_results):
            for site in sites:
                all_sites_to_process.append((site, competitor["domain"], competitor["name"]))

        workflow.logger.info(f"Found {len(all_sites_to_process)} total sites to analyze")

        if not all_sites_to_process:
            workflow.logger.warning("No sites found to process")
            return {"error": "No similar sites found", "results": []}

        workflow.logger.info("Step 2: Scraping and analyzing sites")
        batch_size = 10
        all_enriched_sites = []

        for i in range(0, len(all_sites_to_process), batch_size):
            batch = all_sites_to_process[i:i + batch_size]
            batch_tasks = [
                workflow.execute_activity(
                    scrape_and_analyze_site_activity,
                    site_info,
                    domain,
                    competitor_name,
                    list(clients_set),
                    input.hf_token,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=SOFT_RETRY_POLICY,
                )
                for site_info, domain, competitor_name in batch
            ]
            batch_results = await asyncio.gather(*batch_tasks)
            all_enriched_sites.extend(batch_results)
            total_batches = (len(all_sites_to_process) + batch_size - 1) // batch_size
            workflow.logger.info(f"Processed batch {i // batch_size + 1}/{total_batches}")

        valid_sites = [s for s in all_enriched_sites if s.site_domain]

        workflow.logger.info(f"Successfully processed {len(valid_sites)} valid sites")

        workflow.logger.info("Step 3: Running Z-score analysis")
        zscore_results = await workflow.execute_activity(
            run_zscore_analysis_activity,
            valid_sites,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info("Step 4: Generating report")
        report_summary = await workflow.execute_activity(
            generate_report_activity,
            valid_sites,
            zscore_results,
            input.output_path,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info("Workflow completed successfully")

        return {
            "status": "completed",
            "summary": report_summary,
            "zscore_findings": zscore_results.get("flagged_anomalies", []),
        }
