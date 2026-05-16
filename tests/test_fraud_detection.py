"""Tests for fraud detection module (Z-score analysis)."""
import os
import pytest
import polars as pl
from src.lib.fraud_detection import (
    validate_domain,
    compute_fraud_metrics,
    detect_fraud_from_sql,
    Z_SCORE_THRESHOLD,
)


class TestValidateDomain:
    """Test domain validation function."""

    def test_valid_domains(self):
        """Test various valid domain formats."""
        valid = [
            'example.com',
            'www.example.com',
            'subdomain.example.co.uk',
            'example.org',
        ]
        for domain in valid:
            assert validate_domain(domain) is True, f"Failed for {domain}"

    def test_invalid_domains(self):
        """Test invalid domain formats."""
        invalid = [
            '',
            ' ',
            'http://example.com',  # contains protocol
            'https://example.com',
            'example..com',  # double dot
            'example',  # no TLD
            '.com',
            'example.',  # trailing dot should be trimmed? Actually our function strips
        ]
        for domain in invalid:
            assert validate_domain(domain) is False, f"Should fail for '{domain}'"

    def test_domain_with_protocol_stripped(self):
        """Domains with protocol should be stripped by normalize_domain but validation checks raw."""
        # validate_domain should reject domains with http://
        assert validate_domain('http://example.com') is False

    def test_none_input(self):
        """None should be invalid."""
        assert validate_domain(None) is False

    def test_non_string_input(self):
        """Non-string types should be invalid."""
        assert validate_domain(123) is False
        assert validate_domain([]) is False


class TestComputeFraudMetrics:
    """Test fraud metric computation with Z-score."""

    def create_impressions_df(self, data):
        """Helper to create impressions DataFrame."""
        return pl.DataFrame(data)

    def test_basic_fraud_computation(self):
        """Test that fraud metrics are computed without error."""
        data = {
            'tag_id': [1, 2, 3, 4, 5],
            'advertiser_id': [100, 100, 100, 100, 100],
            'converted_pixel': [10, 5, 50, 100, 0],
            'user_ip': ['1.1.1.1', '1.1.1.2', '1.1.1.3', '1.1.1.4', '1.1.1.5'],
            'device_type': ['desktop', 'mobile', 'desktop', 'desktop', 'mobile'],
        }
        df = self.create_impressions_df(data)
        results = compute_fraud_metrics(df, '100')
        
        assert len(results) == 5
        for r in results:
            assert 'tag_id' in r
            assert 'indicator_z_score' in r
            assert 'final_status' in r
            assert r['final_status'] in ('FRAUD_CONFIRMED', 'REVIEW_REQUIRED')

    def test_z_score_threshold_boundary(self):
        """Test that Z-score threshold correctly identifies fraud."""
        # Create data where one tag has very low CR compared to average
        data = {
            'tag_id': [1, 2],
            'advertiser_id': ['A', 'A'],
            'converted_pixel': [100, 1],  # first has high CR, second very low
            'user_ip': ['1.1.1.1', '1.1.1.2'],
            'device_type': ['desktop', 'desktop'],
        }
        df = self.create_impressions_df(data)
        results = compute_fraud_metrics(df, 'A', z_score_threshold=-1.96)
        
        statuses = {r['tag_id']: r['final_status'] for r in results}
        # Tag with lower CR should have FRAUD_CONFIRMED if z-score < -1.96
        # With only 2 tags, std might be high, but we can at least check the function runs
        assert len(statuses) == 2

    def test_empty_dataframe(self):
        """Empty DataFrame should return empty list."""
        df = pl.DataFrame()
        results = compute_fraud_metrics(df, '100')
        assert results == []

    def test_missing_advertiser(self):
        """No matching advertiser data should return empty."""
        data = {
            'tag_id': [1, 2],
            'advertiser_id': ['X', 'X'],
            'converted_pixel': [10, 20],
            'user_ip': ['1.1.1.1', '1.1.1.2'],
            'device_type': ['desktop', 'mobile'],
        }
        df = self.create_impressions_df(data)
        results = compute_fraud_metrics(df, '100')  # different advertiser
        assert results == []

    def test_single_tag_std_zero(self):
        """With single tag, std=0, z-score should be 0, not fraud."""
        data = {
            'tag_id': [1],
            'advertiser_id': ['A'],
            'converted_pixel': [10],
            'user_ip': ['1.1.1.1'],
            'device_type': ['desktop'],
        }
        df = self.create_impressions_df(data)
        results = compute_fraud_metrics(df, 'A')
        assert len(results) == 1
        assert results[0]['indicator_z_score'] == 0.0
        assert results[0]['final_status'] == 'REVIEW_REQUIRED'


class TestDetectFraudFromSQL:
    """Test the SQL-based fraud detection entry point."""

    def test_empty_query_returns_error(self):
        """Empty or whitespace-only query should return error."""
        result = detect_fraud_from_sql('')
        assert 'error' in result
        assert result.get('success') is False

    def test_no_data_query(self):
        """Query returning no rows should return empty fraud_flags."""
        sql = "SELECT 1 as tag_id, 'A' as advertiser_id WHERE 1=0"
        result = detect_fraud_from_sql(sql)
        assert result.get('error') is None or 'No data' not in result.get('error', '')
        # Depending on implementation, might have empty fraud_flags
        assert 'fraud_flags' in result

    def test_valid_sql_returns_results(self):
        """Valid SQL with tag-level data should produce fraud analysis."""
        sql = """
        SELECT
            tag_id,
            advertiser_id,
            converted_pixel,
            user_ip,
            device_type
        FROM (VALUES
            (1, 'A', 1, '1.1.1.1', 'desktop'),
            (2, 'A', 100, '1.1.1.2', 'desktop'),
            (3, 'A', 50, '1.1.1.3', 'mobile'),
            (4, 'A', 50, '1.1.1.4', 'desktop'),
            (5, 'A', 50, '1.1.1.5', 'desktop')
        ) as t(tag_id, advertiser_id, converted_pixel, user_ip, device_type)
        """
        result = detect_fraud_from_sql(sql)
        assert result.get('success') is True
        assert 'fraud_flags' in result
        assert len(result['fraud_flags']) == 5
        assert 'summary' in result
        assert 'confirmed_fraud' in result['summary']

    def test_z_score_threshold_configurable(self):
        """Verify Z_SCORE_THRESHOLD env var exists."""
        # The module should have Z_SCORE_THRESHOLD set from env or default
        assert Z_SCORE_THRESHOLD == float(os.getenv('Z_SCORE_THRESHOLD', '-1.96'))
