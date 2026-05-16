"""Tests for MCP server tools."""
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from src.mcp_server import mcp, detect_fraud, scrape_competitor, check_ads


class TestMCPDetectFraud:
    """Test detect_fraud MCP tool."""

    def test_empty_sql_returns_error(self):
        """Empty SQL query should return error."""
        result = detect_fraud('')
        assert result.get('success') is False
        assert 'error' in result

    def test_valid_sql_returns_fraud_flags(self):
        """Valid SQL should produce fraud analysis."""
        sql = """
        SELECT
            tag_id,
            advertiser_id,
            converted_pixel,
            user_ip,
            device_type
        FROM (VALUES
            (1, 'A', 10, '1.1.1.1', 'desktop'),
            (2, 'A', 100, '1.1.1.2', 'desktop'),
            (3, 'A', 50, '1.1.1.3', 'mobile'),
            (4, 'A', 50, '1.1.1.4', 'desktop'),
            (5, 'A', 50, '1.1.1.5', 'desktop')
        ) as t(tag_id, advertiser_id, converted_pixel, user_ip, device_type)
        """
        result = detect_fraud(sql)
        assert result.get('success') is True
        assert 'fraud_flags' in result
        assert len(result['fraud_flags']) == 5
        assert 'summary' in result
        assert result['summary']['total_tags_analyzed'] == 5

    def test_sql_with_no_advertiser_column_returns_error(self):
        """SQL without advertiser_id column returns error."""
        sql = "SELECT tag_id, converted_pixel FROM (VALUES (1, 10)) as t(tag_id, converted_pixel)"
        result = detect_fraud(sql)
        assert result.get('success') is False or 'error' in result


class TestMCPCheckAds:
    """Test check_ads MCP tool."""

    def test_empty_html_returns_error(self):
        """Empty HTML should return error."""
        result = check_ads('', use_llm=False)
        assert result.get('success') is False
        assert 'error' in result

    def test_no_ads_simple_html(self):
        """HTML with no ad signatures should return is_running_ads=False."""
        html = '<html><body><p>Hello world</p></body></html>'
        result = check_ads(html, use_llm=False)
        assert result.get('success') is True
        assert result['is_running_ads'] is False
        assert 'method' in result

    def test_ads_detected_by_heuristic(self):
        """HTML with ad signatures should be detected."""
        html = '<script src="googlesyndication.com/pagead/js"></script>'
        result = check_ads(html, use_llm=False)
        assert result['is_running_ads'] is True
        assert result['method'] == 'heuristic_confirm'


class TestMCPScrapeCompetitor:
    """Test scrape_competitor MCP tool."""

    def test_invalid_domain_returns_error(self):
        """Invalid domain format should return error."""
        result = scrape_competitor('http://example.com')  # contains protocol
        assert result.get('success') is False
        assert 'error' in result

    def test_valid_domain_starts_processing(self):
        """Valid domain should initiate scraping (mocked)."""
        with patch('src.mcp_server.scrape_competitor_domain', new=AsyncMock(return_value=[])):
            result = scrape_competitor('example.com', include_ad_detection=False)
            assert result.get('success') is True
            assert 'competitor_domain' in result
