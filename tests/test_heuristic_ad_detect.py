"""Tests for heuristic ad detection module."""
import pytest
from src.lib.ad_detection import heuristic_ad_detect


class TestHeuristicAdDetect:
    """Test heuristic_ad_detect function with various HTML inputs."""

    def test_no_ads_simple_html(self):
        """Simple HTML with no ad signatures should return False."""
        html = "<html><body><p>Hello world</p></body></html>"
        result = heuristic_ad_detect(html)
        assert result['is_running_ads'] is False
        assert 'Heuristic score' in result['ad_evidence']
        assert result.get('heuristic_score', 0) <= 2.0

    def test_google_syndication_detected(self):
        """Google AdSense signature should trigger ad detection."""
        html = '<script src="https://googlesyndication.com/pagead/js/adsbygoogle.js"></script>'
        result = heuristic_ad_detect(html)
        assert result['is_running_ads'] is True
        assert result['heuristic_score'] >= 1.5

    def test_doubleclick_detected(self):
        """DoubleClick signature should contribute to ad detection."""
        html = '<img src="https://ad.doubleclick.net/addynam/...">'
        result = heuristic_ad_detect(html)
        assert result['is_running_ads'] is True

    def test_multiple_ad_networks(self):
        """Multiple ad networks should increase score significantly."""
        html = """
        <script src="googlesyndication.com/pagead/js"></script>
        <script src="doubleclick.net/adj/"></script>
        <div class="criteo">Ad content</div>
        """
        result = heuristic_ad_detect(html)
        assert result['is_running_ads'] is True
        assert result['heuristic_score'] > 2.0

    def test_iframe_ad_detected(self):
        """Ad iframe should count toward score."""
        html = '<iframe width="300" height="250" src="adserver.com"></iframe>'
        result = heuristic_ad_detect(html)
        # Score: iframe(0.2) + width="300"(0.3) + height="250"(0.3) = 0.8, not > 2.0
        assert result['is_running_ads'] is False
        assert result['heuristic_score'] == pytest.approx(0.8)

    def test_sponsored_content_detected(self):
        """Sponsored content keyword should contribute."""
        html = '<div class="ad-sponsor">Sponsored content</div>'
        result = heuristic_ad_detect(html)
        assert result['is_running_ads'] is False  # Only 0.5, not enough alone
        assert result['heuristic_score'] >= 0.5

    def test_case_insensitive_matching(self):
        """Matching should be case-insensitive."""
        html = '<SCRIPT SRC="GoOgLeSyNdIcAtIoN.cOm"></SCRIPT>'
        result = heuristic_ad_detect(html)
        assert result['is_running_ads'] is True

    def test_empty_html(self):
        """Empty HTML should return no ads."""
        result = heuristic_ad_detect("")
        assert result['is_running_ads'] is False
        assert result['heuristic_score'] == 0.0

    def test_boundary_score_exactly_2(self):
        """Score exactly at threshold should return False (threshold is > 2.0)."""
        # Construct HTML with score exactly 2.0
        # googlesyndication (1.5) + iframe (0.2) + width="300" (0.3) = 2.0
        html = '''
        <script src="googlesyndication.com"></script>
        <iframe width="300"></iframe>
        '''
        result = heuristic_ad_detect(html)
        # 1.5 + 0.2 + 0.3 = 2.0, which is NOT > 2.0
        assert result['is_running_ads'] is False

    def test_boundary_score_just_above_2(self):
        """Score just above 2.0 should return True."""
        # googlesyndication (1.5) + doubleclick (1.5) = 3.0 > 2
        html = '''
        <script src="googlesyndication.com"></script>
        <script src="doubleclick.net"></script>
        '''
        result = heuristic_ad_detect(html)
        assert result['is_running_ads'] is True
