# REVIEW.md — judging policy for this repository

Per the Kilo Code `REVIEW.md` convention (see agentic-repo-standard, row 2):
this file is committed to the base branch that pull requests target, so a
branch cannot alter the criteria by which it is judged. Its reader is an agent
(or human) about to **judge** a change. Build facts live in `AGENTS.md` — this
file carries only judging policy, never build instructions.

## Severity calibration

- **Blocking** — request changes:
  - Secrets or credentials in the diff (API keys, tokens, private keys).
  - A change that weakens a security boundary: a new LLM call over untrusted
    input (scraped HTML, user text) without provenance tagging or sanitization;
    new network egress; a new dependency with install scripts.
  - Broken CI, or deleted / mocked-out tests that hide a regression.
  - A weakened oracle: a test that asserts less than before, without justification.
  - The `jev` router judging, filtering, or gating task content instead of only
    selecting a model endpoint.
- **Major** — fix before merge:
  - Untested changes to `src/lib/` core logic (scraper, ad/fraud detection, router).
  - Changed public contract (function signatures, `EnrichedSite` fields, CLI
    flags) without updating callers and docs.
  - Error handling that silently swallows failures in the ingest path.
- **Minor** — fix if cheap: unclear naming, missing docstring on a public
  function, duplicated logic.
- **Nit** — optional: formatting, typos. Never block on nits.

## Paths to skip (no style comments)

- `data/`, `results/` — ingest artifacts and generated output.
- `SQL_Analysis.pdf` — binary analysis artifact.
- `jev/decisions.jsonl` — append-only routing telemetry (ADR-0009); audit the
  code that writes it, not the ledger contents.
- `demo/` fixtures — frozen sample payloads.
- Lockfiles and vendored/generated clients.

## Verification expected

- CI green is required, not advisory.
- A reviewer must cite what was verified: which tests ran, or an explicit
  "could not run X because Y". "Looks fine" is not verification.
- `jev` router changes must show a decision-log line or a test exercising the
  new branch.
- Ingest changes (`scraper.py`, `main.py`) must show a dry run or a test; the
  reviewer checks trust-tier tagging on any new record field.

## Summary style

Decision first ("approve" / "request changes"), then blocking findings with
`file:line`, then everything else. A review longer than the diff is a smell.

## Sub-agent budget

- Diff < 200 lines: no sub-agents; read it directly.
- Diff 200–1000 lines: at most one sub-agent for an independent surface
  (e.g. tests vs implementation).
- Diff > 1000 lines: ask for the diff to be split before reviewing.

## Falsification

If conforming reviews do not catch more escaped defects than ad-hoc reviews
over two quarters, this file is ceremony — shrink it to the severity table or
delete it.
