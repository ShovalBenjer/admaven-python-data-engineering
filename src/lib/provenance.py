"""Data provenance tagging for ingested records.

Every ingested record carries two fields:

- ``source_id``: where the bytes came from (URL, API endpoint, file path).
- ``trust_tier``: how much we trust that origin (see :class:`TrustTier`).

Records from origins the operator does not control travel the quarantine
lane: they must not be written to ``results/`` or fed to another LLM prompt
without sanitization or explicit review. Use :func:`split_quarantine` to
enforce the split at write time.

The default tier for an unknown origin is the lowest one (fail closed).
"""
from __future__ import annotations

from enum import Enum
from typing import Sequence, Tuple, TypeVar


class TrustTier(str, Enum):
    INTERNAL_API = "internal-api"    # AdMaven first-party APIs
    PARTNER_FEED = "partner-feed"    # contracted third-party feeds
    USER_SUPPLIED = "user-supplied"  # operator-provided files (e.g. our_clients.csv)
    SCRAPED_WEB = "scraped-web"      # arbitrary third-party HTML — untrusted


_TRUST_RANK = {
    TrustTier.INTERNAL_API: 3,
    TrustTier.PARTNER_FEED: 2,
    TrustTier.USER_SUPPLIED: 1,
    TrustTier.SCRAPED_WEB: 0,
}

# Origins the operator does not control. Records at these tiers travel the
# quarantine lane: no writes to results/, no LLM prompts, without sanitization
# or explicit review.
QUARANTINED_TIERS = frozenset({TrustTier.SCRAPED_WEB})


def lowest_tier(*tiers: TrustTier) -> TrustTier:
    """A record is only as trustworthy as its least-trusted input."""
    return min(tiers, key=lambda t: _TRUST_RANK[t])


def is_quarantined(tier: TrustTier | str) -> bool:
    """True if records at this tier must travel the quarantine lane."""
    return TrustTier(tier) in QUARANTINED_TIERS


def tag(record, source_id: str, trust_tier: TrustTier):
    """Stamp ``source_id`` / ``trust_tier`` onto a record. Returns the record."""
    record.source_id = source_id
    record.trust_tier = TrustTier(trust_tier).value
    return record


T = TypeVar("T")


def split_quarantine(records: Sequence[T]) -> Tuple[list, list]:
    """Split records into (trusted, quarantined) by trust tier.

    Records with no ``trust_tier`` attribute are treated as quarantined
    (fail closed: unknown origin == untrusted origin).
    """
    trusted: list = []
    quarantined: list = []
    for record in records:
        tier = getattr(record, "trust_tier", TrustTier.SCRAPED_WEB.value)
        (quarantined if is_quarantined(tier) else trusted).append(record)
    return trusted, quarantined
