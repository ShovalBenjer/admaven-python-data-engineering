"""Tests for lib.provenance: tagging, tier ranking, quarantine split."""
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lib.provenance import (  # noqa: E402
    TrustTier,
    is_quarantined,
    lowest_tier,
    split_quarantine,
    tag,
)


def _rec(tier=None):
    r = SimpleNamespace(site_domain="example.com")
    if tier is not None:
        r.trust_tier = tier
    return r


def test_lowest_tier_wins():
    assert lowest_tier(TrustTier.INTERNAL_API, TrustTier.SCRAPED_WEB) is TrustTier.SCRAPED_WEB
    assert lowest_tier(TrustTier.USER_SUPPLIED, TrustTier.PARTNER_FEED) is TrustTier.USER_SUPPLIED


def test_tag_sets_fields():
    r = tag(_rec(), "http://example.com", TrustTier.SCRAPED_WEB)
    assert r.source_id == "http://example.com"
    assert r.trust_tier == "scraped-web"


def test_quarantine_split():
    trusted = [_rec("internal-api"), _rec("user-supplied")]
    quarantined = [_rec("scraped-web"), _rec()]  # missing tier fails closed
    ok, held = split_quarantine(trusted + quarantined)
    assert ok == trusted
    assert held == quarantined


def test_is_quarantined():
    assert is_quarantined(TrustTier.SCRAPED_WEB)
    assert is_quarantined("scraped-web")
    assert not is_quarantined(TrustTier.INTERNAL_API)
    assert not is_quarantined(TrustTier.USER_SUPPLIED)
