# Data provenance convention

Every ingested record carries two fields:

- `source_id` — where the bytes came from (URL, API endpoint, file path).
- `trust_tier` — how much we trust that origin.

## Why

`src/lib/scraper.py` fetches raw HTML from arbitrary third-party domains and
feeds it to an LLM for ad detection (`check_ads(use_llm=True)`). Per
OWASP LLM01:2025, attacker-controlled content in an LLM prompt is an indirect
prompt-injection channel: a poisoned page can steer detection output, and that
output lands in `results/` indistinguishable from clean data. Provenance is
the containment layer: when a record looks wrong, we can scope exactly which
records touched untrusted input. Sanitization alone is not enough — injection
payloads need no markup, so the tag must survive a sanitizer bypass.

## Tiers (highest trust first)

| tier | origin | example in this repo |
|---|---|---|
| `internal-api` | AdMaven first-party APIs | `similar_get_domains` |
| `partner-feed` | contracted third-party feeds | — (none yet) |
| `user-supplied` | operator-provided files | `data/our_clients.csv` |
| `scraped-web` | arbitrary third-party HTML | fetched page HTML |

A record's tier is the **lowest** tier of any input that shaped it
(`lowest_tier()`). Unknown origin defaults to `scraped-web` (fail closed).

## Quarantine lane

`scraped-web` records — origins the operator does not control — travel the
quarantine lane: they must not be written to `results/` or fed to another LLM
prompt without sanitization or explicit review. `user-supplied` is *not*
quarantined: an attacker who can rewrite the operator's own CSV already has
file access, so quarantining it adds ceremony without a threat model.

Enforce the split at write time with `split_quarantine()`:

```python
from lib.provenance import split_quarantine

trusted, quarantined = split_quarantine(sites)
write_results(trusted)          # the normal lane
write_quarantine(quarantined)   # review lane: sanitize or inspect first
```

## Code

`src/lib/provenance.py`: `TrustTier`, `tag()`, `lowest_tier()`,
`is_quarantined()`, `split_quarantine()`. `EnrichedSite` carries `source_id`
and `trust_tier`, set in `process_site()` per branch: fetched HTML →
`scraped-web`, existing client → `user-supplied`, unreachable/API-only →
`internal-api`.
