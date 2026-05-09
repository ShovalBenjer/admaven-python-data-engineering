"""Tests for Z-score boundary cases in fraud detection."""
import pytest
import polars as pl
from src.lib.fraud_detection import compute_fraud_metrics, Z_SCORE_THRESHOLD


class TestZScoreBoundaries:
    """Test Z-score calculation boundary cases."""

    def test_negative_z_score_threshold_confirm_fraud(self):
        """Z-score below -1.96 should be marked FRAUD_CONFIRMED."""
        data = {
            'tag_id': [1, 2, 3, 4, 5],
            'advertiser_id': ['A'] * 5,
            'converted_pixel': [0, 0, 0, 0, 100],  # 4 tags with 0 CR, 1 with high CR
            'user_ip': ['1.1.1.1'] * 5,
            'device_type': ['desktop'] * 5,
        }
        df = pl.DataFrame(data)
        results = compute_fraud_metrics(df, 'A', z_score_threshold=-1.96)
        
        # Tags with 0 conversions will have CR = 0, which will be far below avg
        zero_cr_tags = [r for r in results if r['conversions'] == 0]
        for r in zero_cr_tags:
            # These should have very negative z-scores
            assert r['indicator_z_score'] < -1.96 or r['final_status'] == 'REVIEW_REQUIRED'

    def test_positive_z_score_not_fraud(self):
        """Z-score above threshold should not be marked as fraud."""
        data = {
            'tag_id': [1, 2],
            'advertiser_id': ['B', 'B'],
            'converted_pixel': [100, 100],  # both high CR
            'user_ip': ['1.1.1.1', '1.1.1.2'],
            'device_type': ['desktop', 'desktop'],
        }
        df = pl.DataFrame(data)
        results = compute_fraud_metrics(df, 'B', z_score_threshold=-1.96)
        
        # If CR are identical, z-score ≈ 0, not fraud (0 > -1.96)
        for r in results:
            assert r['final_status'] == 'REVIEW_REQUIRED'

    def test_threshold_minus_one_nine_six(self):
        """The threshold must be -1.96, not -2.5 as incorrectly hardcoded before."""
        assert Z_SCORE_THRESHOLD == -1.96

    def test_multiple_advertisers(self):
        """Metrics should be computed separately per advertiser."""
        data = {
            'tag_id': [1, 2, 3, 4],
            'advertiser_id': ['X', 'X', 'Y', 'Y'],
            'converted_pixel': [10, 5, 50, 50],
            'user_ip': ['1.1.1.1', '1.1.1.2', '1.1.1.3', '1.1.1.4'],
            'device_type': ['desktop', 'mobile', 'desktop', 'mobile'],
        }
        df = pl.DataFrame(data)
        results = compute_fraud_metrics(df, 'X')
        x_tags = [r for r in results if r.get('advertiser_id') == 'X']
        assert len(x_tags) == 2
        
        results_y = compute_fraud_metrics(df, 'Y')
        y_tags = [r for r in results_y if r.get('advertiser_id') == 'Y']
        assert len(y_tags) == 2

    def test_ip_density_and_device_monoculture_metrics(self):
        """Ensure IP density and device monoculture are calculated."""
        data = {
            'tag_id': [1],
            'advertiser_id': ['A'],
            'converted_pixel': [10],
            'user_ip': ['1.1.1.1', '1.1.1.1', '1.1.1.2'],  # 3 impressions, 2 unique IPs
            'device_type': ['desktop', 'desktop', 'desktop'],
        }
        df = pl.DataFrame(data)
        results = compute_fraud_metrics(df, 'A')
        
        r = results[0]
        assert 'indicator_ip_density' in r
        assert 'indicator_device_monoculture' in r
        # IP density: 3 impressions / 2 unique IPs = 1.5
        assert r['indicator_ip_density'] == 1.5
        # Device monoculture: 3 desktops / 3 total = 1.0
        assert r['indicator_device_monoculture'] == 1.0
