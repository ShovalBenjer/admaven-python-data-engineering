"""Tests for scraper module error handling."""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from src.lib.scraper import (
    normalize_domain,
    fetch_site_data,
    fetch_similar_sites,
    process_site,
    scrape_competitor_domain,
    EnrichedSite,
)


class TestNormalizeDomain:
    """Test domain normalization."""

    def test_basic_domain(self):
        assert normalize_domain('example.com') == 'example.com'

    def test_with_protocol(self):
        assert normalize_domain('https://example.com') == 'example.com'
        assert normalize_domain('http://example.com') == 'example.com'

    def test_with_www(self):
        assert normalize_domain('www.example.com') == 'example.com'

    def test_with_trailing_slash(self):
        assert normalize_domain('example.com/') == 'example.com'
        assert normalize_domain('example.com/path') == 'example.com/path'  # only strips trailing /

    def test_mixed_case(self):
        assert normalize_domain('Example.COM') == 'example.com'


class TestFetchSiteData:
    """Test HTTP fetching with error handling."""

    @pytest.mark.asyncio
    async def test_successful_fetch(self):
        """Successful fetch returns HTML."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value='<html>test</html>')
        
        mock_session = MagicMock()
        mock_session.get = MagicMock()
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
        
        sem = asyncio.Semaphore(1)
        result = await fetch_site_data(mock_session, 'http://example.com', sem)
        assert result == '<html>test</html>'

    @pytest.mark.asyncio
    async def test_403_forbidden_returns_empty(self):
        """403 status returns empty string."""
        mock_response = MagicMock()
        mock_response.status = 403
        
        mock_session = MagicMock()
        mock_session.get = MagicMock()
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
        
        sem = asyncio.Semaphore(1)
        result = await fetch_site_data(mock_session, 'http://example.com', sem)
        assert result == ''

    @pytest.mark.asyncio
    async def test_timeout_returns_empty(self):
        """Timeout exception returns empty string."""
        import aiohttp
        mock_session = MagicMock()
        mock_session.get = MagicMock(side_effect=aiohttp.ClientTimeout)
        
        sem = asyncio.Semaphore(1)
        result = await fetch_site_data(mock_session, 'http://example.com', sem)
        assert result == ''


class TestFetchSimilarSites:
    """Test similar sites API fetching."""

    @pytest.mark.asyncio
    async def test_successful_api_response(self):
        """Valid API response returns site list."""
        mock_response_data = {
            'domain_list': [
                {'site_name': 'site1.com', 'monthly_visitors': 1000},
                {'site_name': 'site2.com', 'monthly_visitors': 500},
            ]
        }
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)
        
        mock_session = MagicMock()
        mock_session.get = MagicMock()
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
        
        log = MagicMock()
        sites = await fetch_similar_sites(mock_session, 'example.com', log)
        assert len(sites) == 2

    @pytest.mark.asyncio
    async def test_api_error_returns_empty(self):
        """API error status returns empty list."""
        mock_response = MagicMock()
        mock_response.status = 500
        
        mock_session = MagicMock()
        mock_session.get = MagicMock()
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
        
        log = MagicMock()
        sites = await fetch_similar_sites(mock_session, 'example.com', log)
        assert sites == []
        log.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_api_key_logs_error(self):
        """Missing API_KEY returns empty list and logs error."""
        with patch('src.lib.scraper.API_KEY', None):
            mock_session = MagicMock()
            log = MagicMock()
            sites = await fetch_similar_sites(mock_session, 'example.com', log)
            assert sites == []
            log.error.assert_called_once_with("API_KEY environment variable is required")


class TestProcessSite:
    """Test individual site processing."""

    @pytest.mark.asyncio
    async def test_existing_client_skipped(self):
        """Existing client domain is detected and skipped."""
        mock_session = MagicMock()
        sem = asyncio.Semaphore(1)
        clients_set = {'knownclient.com'}
        
        result = await process_site(
            mock_session,
            'knownclient.com',
            'Competitor',
            'comp.com',
            1000,
            clients_set,
            sem,
            use_llm=False,
        )
        
        assert result is not None
        assert result.already_working is True
        assert result.is_running_ads is False
        assert result.got_blocked is False

    @pytest.mark.asyncio
    async def test_blocked_site_marked(self):
        """Site that fails to fetch is marked as blocked."""
        mock_session = MagicMock()
        sem = asyncio.Semaphore(1)
        clients_set = set()
        
        # Mock fetch_site_data to return empty (blocked)
        with patch('src.lib.scraper.fetch_site_data', AsyncMock(return_value='')):
            result = await process_site(
                mock_session,
                'blocked.com',
                'Competitor',
                'comp.com',
                1000,
                clients_set,
                sem,
                use_llm=False,
            )
        
        assert result is not None
        assert result.got_blocked is True
        assert result.is_running_ads is False
