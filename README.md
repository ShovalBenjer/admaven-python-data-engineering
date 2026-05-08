# AdMaven Analytics: Detection & Intelligence Suite

<p align="center">
  <img width="799" height="776" alt="image" src="https://github.com/user-attachments/assets/5a5f6451-140f-4071-971a-3457068b0bc9" />
</p>

<p align="center">
  <a href="https://lookerstudio.google.com/reporting/1c93893d-c3a3-4ac8-8bb0-1ce742053349" target="_blank">
    <img src="https://img.shields.io/badge/%F0%9F%93%8A%20Interactive%20Dashboard-Looker%20Studio-blue?style=for-the-badge"/>
  </a>
  <a href="AdMaven_SQL_Investigation_Report.pdf" target="[blank](https://github.com/ShovalBenjer/admaven-python-data-engineering/blob/main/SQL_Analysis.pdf)">
    <img src="https://img.shields.io/badge/%F0%9F%93%84%20Analytical%20Report-PDF-red?style=for-the-badge"/>
  </a>
</p>

Two tools for ad-tech: a forensic SQL investigation into network fraud using Z-score anomaly detection, and a concurrent Python scraper for competitive domain intelligence. One catches bot farms. The other maps competitor publisher networks at scale.

---

## What It Found

- Advertiser `601040`: 400% traffic spike, near-zero conversions -- classic click flood
- Campaign `653344`: 75% CR across 10k+ impressions -- mathematical impossibility, likely pixel stuffing
- Philippines traffic surge + 60% CR drop: geographic arbitrage on cheap inventory
- Z-score threshold: flags any tag CR deviating >1.96 std deviations from advertiser baseline

---

## How It Works

**SQL (Snowflake):** Dynamic Z-score analysis instead of static thresholds. Adapts to each advertiser's baseline, so naturally low-converting verticals don't generate false positives.

```sql
CASE 
    WHEN (tag_cr - avg_cr) / NULLIF(std_cr, 0) < -1.96 THEN 'FRAUD_CONFIRMED'
    ELSE 'REVIEW_REQUIRED'
END
```

**Python pipeline:** Map-Reduce architecture. `ProcessPoolExecutor` for CPU-bound HTML parsing, `asyncio` for I/O-bound requests. Both CPU cores and network bandwidth saturated simultaneously. Polars for sub-millisecond set operations against the existing client list, DuckDB for pre-export QA.

**Ad detection:** Lightweight heuristic engine -- no ML model needed. Scans HTML for weighted tokens (`googlesyndication: 1.5`, `prebid: 1.2`, `iframe: 0.2`). Score > 2.0 = running ads.

---

## Architecture

<p align="center">
  <img width="1024" height="565" alt="image" src="https://github.com/user-attachments/assets/07a2b2eb-d257-420d-ac17-4fe1bb065575" />
</p>

Pipeline flow: load competitor domains via Polars -> spawn process per competitor -> each process fetches API data, deduplicates against client list, scrapes HTML async -> reduce to single Polars DataFrame -> DuckDB QA -> CSV export.

---

## Repository Structure

```
.
├── queries_shoval_benjer.sql   # Snowflake SQL fraud logic
├── python_Shoval_Benjer.py     # Competitive intelligence pipeline
├── final_output.csv            # Generated dataset
├── AdMaven_SQL_Investigation_Report.pdf
├── requirements.txt            # Polars, DuckDB, Loguru
└── README.md
```

---

## Setup

```bash
pip install -r requirements.txt
```

Optional `.env` for GenAI features:
```env
API_KEY=your_email@example.com
HF_TOKEN=your_huggingface_token
```

```bash
python python_Shoval_Benjer.py
# Input:  comp_run_time_domains.csv, our_clients.csv
# Output: final_output.csv
```
