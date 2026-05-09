"""Fraud detection Temporal workflow definition."""
import os
from datetime import timedelta
from typing import List, Dict, Set, Any
from temporalio import workflow, activity
from temporalio.common import RetryPolicy
from temporalio.workflow import execute_local
from dataclasses import dataclass, asdict
from datetime import date

with workflow.unsafe.imports_passed_through():
    from src.activities.fraud_activities import (
        fetch_similar_sites_activity,
        scrape_and_analyze_site_activity,
        run_zscore_analysis_activity,
        generate_report_activity,
        EnrichedSite,
    )

# Retry policy for activities - exponential backoff with 3 attempts
DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=["ApplicationError"],  # Don't retry on certain errors
)


@dataclass
class FraudDetectionWorkflowInput:
    """Input for the fraud detection workflow."""
    competitors: List[Dict[str, str]]  # [{"name": "...", "domain": "..."}, ...]
    clients: List[str]  # List of client domains
    api_key: str
    hf_token: str
    output_path: str = "final_output.csv"


@workflow.defn(name="fraud_detection_workflow")
class FraudDetectionWorkflow:
    """Temporal workflow for fraud detection across competitor domains."""

    @workflow.run
    async def run(self, input: FraudDetectionWorkflowInput) -> Dict[str, Any]:
        """
        Main workflow execution: fetch similar sites, scrape & analyze,
        run Z-score analysis, and generate report.

        Args:
            input: FraudDetectionWorkflowInput with all required parameters

        Returns:
            Dict with workflow results and summary
        """
        workflow.logger.info(f"Starting fraud detection workflow for {len(input.competitors)} competitors")

        # Convert clients list to set for O(1) lookup
        clients_set = set(c.lower().replace('http://', '').replace('https://', '').replace('www.', '').strip('/') for c in input.clients)

        # Step 1: Fetch similar sites for each competitor (parallel)
        workflow.logger.info("Step 1: Fetching similar sites for all competitors")
        similar_sites_results = await asyncio.gather(*[
            workflow.execute_activity(
                fetch_similar_sites_activity,
                input.domain,
                input.api_key,
                competitor["name"],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=DEFAULT_RETRY_POLICY,
            )
            for competitor in input.competitors
        ])

        # Collect all sites to process
        all_sites_to_process = []
        for competitor, sites in zip(input.competitors, similar_sites_results):
            for site in sites:
                all_sites_to_process.append((site, competitor["domain"], competitor["name"]))

        workflow.logger.info(f"Found {len(all_sites_to_process)} total sites to analyze")

        if not all_sites_to_process:
            workflow.logger.warning("No sites found to process")
            return {"error": "No similar sites found", "results": []}

        # Step 2: Scrape and analyze each site (batched parallel)
        workflow.logger.info("Step 2: Scraping and analyzing sites")
        # Process in batches of 10 to avoid overwhelming resources
        batch_size = 10
        all_enriched_sites = []

        for i in range(0, len(all_sites_to_process), batch_size):
            batch = all_sites_to_process[i:i+batch_size]
            batch_results = await asyncio.gather(*[
                workflow.execute_activity(
                    scrape_and_analyze_site_activity,
                    site_info,
                    domain,
                    competitor_name,
                    clients_set,
                    input.hf_token,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=DEFAULT_RETRY_POLICY,
                )
                for site_info, domain, competitor_name in batch
            ])
            all_enriched_sites.extend(batch_results)
            workflow.logger.info(f"Processed batch {i//batch_size + 1}/{(len(all_sites_to_process) + batch_size - 1)//batch_size}")

        # Filter out any sites with empty domain (errors)
        valid_sites = [s for s in all_enriched_sites if s.site_domain]

        workflow.logger.info(f"Successfully processed {len(valid_sites)} valid sites")

        # Step 3: Run Z-score analysis
        workflow.logger.info("Step 3: Running Z-score analysis")
        zscore_results = await workflow.execute_activity(
            run_zscore_analysis_activity,
            valid_sites,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        # Step 4: Generate final report
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
