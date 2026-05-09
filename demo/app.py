import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="AdMaven Demo", layout="centered")

st.title("AdMaven Analytics Demo")
st.markdown("**·Solution Engineer projects — Data Engineering + Fraud Detection**")

st.write("""
AdMaven is a dual-tool ad-tech analytics suite:
- **SQL Investigation**: Snowflake-based Z-score anomaly detection (dynamic thresholds)
- **Competitive Scraper**: Async Python pipeline mapping publisher networks at scale
""")

# Insert key findings from README
st.header("🔍 What it found")
findings = pd.DataFrame([
    {"advertiser_id": "601040", "issue": "400% traffic spike, near-zero conversions", "verdict": "Click flood detected"},
    {"advertiser_id": "653344", "issue": "75% CR across 10k+ impressions", "verdict": "Mathematical impossibility — likely pixel stuffing"},
    {"advertiser_id": "PH_Traffic", "issue": "Philippines surge + 60% CR drop", "verdict": "Geographic arbitrage on cheap inventory"},
])
st.dataframe(findings, use_container_width=True)

st.header("📊 Architecture")
st.markdown("""
- **Z-score threshold**: `(tag_cr - avg_cr) / std_cr < -1.96` → FRAUD_CONFIRMED
- **Tech**: asyncio + ProcessPoolExecutor for I/O + CPU parallelism; Polars for set ops; DuckDB for QA
""")

st.header("📈 Interactive Dashboard")
st.markdown("[Open Looker Studio →](https://lookerstudio.google.com/reporting/1c93893d-c3a3-4ac8-8bb0-1ce742053349)")

st.header("📄 Analytical Report")
st.markdown("[Download PDF →](https://github.com/ShovalBenjer/admaven-python-data-engineering/blob/main/SQL_Analysis.pdf)")

st.caption("Built by Shoval Benjer · Powered by Fly.io")
