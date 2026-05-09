"""
Fraud detection module using statistical Z-score analysis.

Implements the Z-score based fraud indicator from the SQL queries:
- Z-score = (tag_cr - avg_cr) / std_cr
- Threshold: Z-score < -1.96 indicates confirmed fraud
- Additional metrics: IP density and device monoculture
"""
import os
import re
from typing import Dict, List, Optional, Tuple
import duckdb
import polars as pl

# Configurable threshold via environment variable
Z_SCORE_THRESHOLD = float(os.getenv('Z_SCORE_THRESHOLD', '-1.96'))


def validate_domain(domain: str) -> bool:
    """
    Validate that a domain string is properly formatted.
    Args:
        domain: Domain string to validate
    Returns:
        bool: True if domain is valid, False otherwise
    """
    if not domain or not isinstance(domain, str):
        return False
    cleaned = domain.strip()
    if not cleaned:
        return False
    # Basic domain pattern: alphanumeric, hyphens, dots
    domain_pattern = r'^[a-zA-Z0-9][a-zA-Z0-9\-\.]*[a-zA-Z0-9]$'
    return bool(re.match(domain_pattern, cleaned))


def compute_fraud_metrics(
    impressions_data: pl.DataFrame,
    advertiser_id: str,
    z_score_threshold: float = -1.96
) -> List[Dict]:
    """
    Compute fraud metrics per tag_id using Z-score analysis.
    
    Args:
        impressions_data: Polars DataFrame with impression logs
        advertiser_id: Advertiser to analyze
        z_score_threshold: Threshold for fraud flag (default -1.96)
    
    Returns:
        List of fraud indicator dicts per tag_id
    """
    if impressions_data.is_empty():
        return []

    # Filter for advertiser and valid data
    filtered = impressions_data.filter(
        (pl.col('advertiser_id') == advertiser_id) &
        (pl.col('tag_id').is_not_null())
    )

    if filtered.is_empty():
        return []

    # Aggregate per tag
    tag_stats = filtered.group_by('tag_id').agg([
        pl.count().alias('impressions'),
        pl.col('user_ip').n_unique().alias('unique_ips'),
        (pl.col('device_type') == 'desktop').sum().alias('desktop_count'),
        pl.col('converted_pixel').sum().alias('conversions'),
    ]).with_columns([
        (pl.col('conversions') / pl.col('impressions')).alias('tag_cr'),
        (pl.col('desktop_count') / pl.col('impressions')).alias('pct_desktop'),
        (pl.col('impressions') / pl.col('unique_ips')).alias('ip_density'),
    ])

    # Compute advertiser-level stats
    adv_stats = filtered.group_by('advertiser_id').agg([
        pl.col('converted_pixel').sum().alias('total_conversions'),
        pl.count().alias('total_impressions'),
    ]).with_columns([
        (pl.col('total_conversions') / pl.col('total_impressions')).alias('adv_avg_cr'),
    ])

    # Calculate standard deviation of conversion rates across tags
    cr_std = tag_stats.select(pl.col('tag_cr').std()).item()
    adv_avg_cr = adv_stats.select(pl.col('adv_avg_cr')).item()

    results = []
    for row in tag_stats.iter_rows(named=True):
        z_score = (row['tag_cr'] - adv_avg_cr) / cr_std if cr_std else 0.0
        final_status = 'FRAUD_CONFIRMED' if z_score < z_score_threshold else 'REVIEW_REQUIRED'
        
        results.append({
            'tag_id': row['tag_id'],
            'indicator_z_score': round(z_score, 2),
            'indicator_ip_density': round(row['ip_density'], 2),
            'indicator_device_monoculture': round(row['pct_desktop'], 2),
            'final_status': final_status,
            'impressions': row['impressions'],
            'conversions': row['conversions'],
        })

    return results


def detect_fraud_from_sql(sql_query: str, db_path: Optional[str] = None) -> Dict:
    """
    Execute SQL query and run fraud detection on the results.
    
    Args:
        sql_query: SQL query that returns tag-level metrics
        db_path: Optional DuckDB database path
    
    Returns:
        Dict with fraud detection results and summary
    """
    db = duckdb.connect(db_path) if db_path else duckdb.connect(':memory:')
    try:
        # Execute query and get results as Polars DataFrame
        result = db.execute(sql_query).pl()
        
        if result.is_empty():
            return {'error': 'No data returned from query', 'fraud_flags': []}
        
        # Identify advertiser_id column
        adv_col = next((c for c in result.columns if 'advertiser' in c.lower()), None)
        if not adv_col:
            return {'error': 'No advertiser_id column found in query results', 'fraud_flags': []}
        
        # Get unique advertisers
        advertisers = result[adv_col].unique().to_list()
        
        all_frauds = []
        for adv in advertisers:
            metrics = compute_fraud_metrics(result, str(adv), Z_SCORE_THRESHOLD)
            for m in metrics:
                m['advertiser_id'] = adv
                all_frauds.append(m)
        
        summary = {
            'total_tags_analyzed': len(all_frauds),
            'confirmed_fraud': sum(1 for f in all_frauds if f['final_status'] == 'FRAUD_CONFIRMED'),
            'review_required': sum(1 for f in all_frauds if f['final_status'] == 'REVIEW_REQUIRED'),
            'threshold_used': Z_SCORE_THRESHOLD,
        }
        
        return {
            'success': True,
            'fraud_flags': all_frauds,
            'summary': summary,
        }
    finally:
        db.close()
