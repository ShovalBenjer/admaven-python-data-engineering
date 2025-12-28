# **AdMaven Analytics: Detection & Intelligence Suite**

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

A comprehensive, dual-stack solution for the modern ad-tech ecosystem. This project delivers a forensic SQL investigation into network fraud and a high-throughput Python automation pipeline for competitive intelligence.

It demonstrates the transition from raw log analysis to production-grade anomaly detection and automated lead qualification.

---

# **1. Executive Summary**

### **Challenges**

*   **Fraud Detection:** In a high-volume ad network, distinguishing legitimate traffic from bot farms or click-spam requires more than simple aggregation. It demands statistical rigor to identify "invisible" anomalies in conversion rates (CR).
*   **Competitive Intelligence:** Manually identifying competitor publishers is slow and unscalable. The challenge is to automate the discovery, validation, and enrichment of thousands of domains while bypassing WAFs and detecting ad tech stacks without rendering.

### **Solution**

#### **Component A: Forensic SQL Analytics (Snowflake)**
A series of advanced SQL queries utilizing **Window Functions** and **Statistical Process Control (SPC)**.
*   **Methodology:** Moved beyond static thresholds to dynamic Z-Score analysis, flagging traffic sources that deviate >1.96 standard deviations from the advertiser's mean.
*   **Outcome:** Pinpointed specific tag IDs responsible for "Click Spam" (low CR, high volume) and "Attribution Fraud" (impossible CRs >70%).

#### **Component B: High-Performance Python Pipeline**
A robust, concurrent scraper utilizing a **Map-Reduce** architecture.
*   **Architecture:** `ProcessPoolExecutor` for CPU-bound parsing + `asyncio` for I/O-bound requests.
*   **Data Engine:** Integrated **Polars** for O(1) filtering and **DuckDB** for in-memory QA.
*   **Intelligence:** Deployed a custom "Manual ML" heuristic engine to detect ad signatures (e.g., Prebid, Google Syndication) with zero external dependencies.

---

# **2. System Architecture**
<p align="center">
  <img width="1024" height="565" alt="image" src="https://github.com/user-attachments/assets/07a2b2eb-d257-420d-ac17-4fe1bb065575" />
</p>


## **Python Pipeline: Map-Reduce Intelligence**

1.  **Ingest:** Loads competitor runtime domains and existing client lists via **Polars** for sub-millisecond set operations.
2.  **Map (Parallel Execution):** Spawns a dedicated process per competitor.
    *   **Worker:** Fetches API data → Deduplicates against client list → Scrapes HTML asynchronously.
    *   **Enrichment:** Extracts contacts (Regex) and detects ads (Heuristic/GenAI).
3.  **Reduce (Aggregation):** Consolidates results into a single **Polars DataFrame**.
4.  **QA:** Runs SQL integrity checks via **DuckDB** before CSV export.

## **SQL Analytics: The Fraud Detection DAG**

1.  **Ingest:** Raw `impressions` and `campaigns` tables.
2.  **Aggregation:** Hourly/Daily rollups via CTEs.
3.  **Metrics:** Calculation of CR, Fill Rate, and eCPM.
4.  **Anomaly Detection:**
    *   **Z-Score Calculation:** `(Tag_CR - Avg_Adv_CR) / StdDev_Adv_CR`
    *   **Bot Fingerprinting:** High IP density + Device Monoculture (100% Mobile).

---

# **3. Repository Structure**

```
.
├── queries_shoval_benjer.sql   # Snowflake SQL investigation & fraud logic
├── python_Shoval_Benjer.py     # Competitive intelligence automation pipeline
├── final_output.csv            # Generated dataset from the Python pipeline
├── AdMaven_SQL_Investigation_Report.pdf  # Detailed analytical findings
├── requirements.txt            # Dependencies (Polars, DuckDB, Loguru, etc.)
└── README.md                   # This file
```

---

# **4. Technical Deep Dive**

### **4.1 SQL: Statistical Anomaly Detection**

Instead of arbitrary rules (e.g., "CR < 0.1%"), I implemented a statistical framework:
```sql
CASE 
    WHEN (tag_cr - avg_cr) / NULLIF(std_cr, 0) < -1.96 THEN 'FRAUD_CONFIRMED'
    ELSE 'REVIEW_REQUIRED'
END
```
This dynamically adapts to each advertiser's baseline, reducing false positives for naturally low-converting verticals.

### **4.2 Python: Hybrid Concurrency**

The script creates a "Process Pool" where each process manages its own "Event Loop".
*   **Why?** Parsing HTML (CPU) blocks the Event Loop. Network requests (I/O) block the CPU.
*   **Result:** By combining `multiprocessing` and `asyncio`, we saturate both the Network Bandwidth and CPU Cores, achieving maximum throughput.

### **4.3 Heuristic Ad Engine**

A lightweight, zero-latency classifier replacing heavy ML models.
*   **Logic:** Scans HTML for weighted tokens: `{'googlesyndication': 1.5, 'prebid': 1.2, 'iframe': 0.2}`.
*   **Inference:** `Score = Sum(Weights)`. If `Score > 2.0`, the site is classified as `is_running_ads=True`.

---

# **5. Setup & Usage**

### **Installation**
```bash
pip install -r requirements.txt
```

### **Configuration**
Create a `.env` file for API keys (optional for GenAI features):
```env
API_KEY=your_email@example.com
HF_TOKEN=your_huggingface_token
```

### **Execution**
Run the Python pipeline:
```bash
python python_Shoval_Benjer.py
```
*   **Input:** `comp_run_time_domains.csv`, `our_clients.csv`
*   **Output:** `final_output.csv`

---

# **6. Key Insights (from Dashboard)**

*   **The "Bot Blast":** Advertiser `601040` experienced a 400% traffic spike with near-zero conversions, a classic DDoS-style click flood.
*   **The "Impossible Campaign":** Campaign `653344` showed a 75% conversion rate across 10k+ impressions, a mathematical impossibility suggesting pixel stuffing.
*   **Geographic Arbitrage:** Traffic from the Philippines (PH) surged while CR dropped by 60%, indicating a shift to cheaper, lower-quality inventory sources.

