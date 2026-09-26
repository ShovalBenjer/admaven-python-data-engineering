"""jev router: cost/latency-aware model routing.

Priority: local small LMs (Ollama) -> free tiers (GitHub Models, other free APIs)
-> paid escalation only when the task needs it.

Router contract (ADR-0009): the router ROUTES — it selects a model endpoint.
It never judges, filters, censors, or gates task content. Its outcome set is
only ``Route`` (or ``RuntimeError`` when no endpoint is reachable). A
content-based refusal would be a defect, not a feature.

Hardening:
- every routing decision is appended to ``jev/decisions.jsonl`` (append-only
  ledger; the raw task text is never logged, only its sha256 and length, per
  the ADR-0009 PII rule);
- the uncertainty escalation threshold is configurable via
  ``JEV_ESCALATE_ABOVE`` (default 0.55);
- the chosen route is schema-checked before it is accepted.

Usage:
    from jev.router import route
    r = route("summarize this log")
    print(r.name, r.model, r.base_url)
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class Route:
    name: str
    kind: str  # ollama | github-models | free-api | paid
    model: str
    base_url: str
    api_key_env: str | None
    cost_per_1k: float
    latency_ms_p50: int
    max_context: int
    available: bool = field(default=False, compare=False)


# Marker for the router contract: this module selects endpoints; it does not
# judge, filter, or gate task content. A named constant keeps the contract
# greppable and testable.
NEVER_JUDGES = True

ROUTE_KINDS = ("ollama", "github-models", "free-api", "paid")

DEFAULT_ESCALATE_ABOVE = 0.55


def _ollama_alive(url: str, timeout: float = 1.5) -> bool:
    try:
        with urllib.request.urlopen(url.rstrip("/") + "/api/tags", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def default_routes() -> list[Route]:
    return [
        Route("ollama-local", "ollama",
              os.getenv("JEV_LOCAL_MODEL", "qwen2.5:7b"),
              "http://localhost:11434", None, 0.0, 400, 32768),
        Route("github-models", "github-models",
              os.getenv("JEV_GH_MODEL", "gpt-4o-mini"),
              "https://models.github.ai/inference", "JEV_MODEL_TOKEN", 0.0, 1200, 128000),
    ]


def estimate_complexity(task: str) -> float:
    """0..1 heuristic: short/simple tasks stay local, hard ones escalate."""
    t = task.lower()
    score = min(len(task) / 4000, 1.0) * 0.4
    hard = ("prove", "security", "architecture", "refactor", "distributed",
            "concurrency", "formal", "cryptograph")
    score += 0.15 * sum(1 for m in hard if m in t)
    return min(score, 1.0)


def escalation_threshold() -> float:
    """Uncertainty escalation threshold: complexity >= this escalates off local."""
    try:
        return float(os.getenv("JEV_ESCALATE_ABOVE", str(DEFAULT_ESCALATE_ABOVE)))
    except (TypeError, ValueError):
        return DEFAULT_ESCALATE_ABOVE


def decisions_log_path() -> Path:
    return Path(os.getenv("JEV_DECISIONS_LOG",
                          str(Path(__file__).with_name("decisions.jsonl"))))


def validate_route(route: Route) -> Route:
    """Schema-check a chosen route before acceptance. Raises ValueError."""
    if route.kind not in ROUTE_KINDS:
        raise ValueError(f"unknown route kind: {route.kind!r}")
    for attr in ("name", "model", "base_url"):
        value = getattr(route, attr, "")
        if not isinstance(value, str) or not value:
            raise ValueError(f"route.{attr} must be a non-empty string")
    if not route.base_url.startswith(("http://", "https://")):
        raise ValueError(f"route.base_url is not a URL: {route.base_url!r}")
    if route.cost_per_1k < 0:
        raise ValueError("route.cost_per_1k must be >= 0")
    if route.max_context <= 0:
        raise ValueError("route.max_context must be > 0")
    return route


def decision_confidence(complexity: float, threshold: float) -> float:
    """Confidence in a routing decision: distance from the decision boundary.

    0.0 = sitting exactly on the threshold (coin flip); 1.0 = as far from it
    as the 0..1 complexity scale allows.
    """
    return round(min(1.0, abs(complexity - threshold) / 0.5), 3)


def log_decision(*, task: str, route: Route, complexity: float,
                 threshold: float, reason: str, kept_local: bool,
                 budget: str) -> dict:
    """Append one routing decision to the append-only ledger.

    The raw task text is never logged — only its sha256 and length — so the
    ledger stays PII-safe and usable as flywheel training data (ADR-0009).
    """
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "task_sha256": hashlib.sha256(task.encode("utf-8")).hexdigest(),
        "task_len": len(task),
        "budget": budget,
        "route": route.name,
        "kind": route.kind,
        "model": route.model,
        "complexity": round(complexity, 3),
        "threshold": threshold,
        "confidence": decision_confidence(complexity, threshold),
        "kept_local": kept_local,
        "reason": reason,
    }
    path = decisions_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")
    return entry


def route(task: str, budget: str = "free") -> Route:
    """Pick the cheapest route that can plausibly handle the task.

    budget: "free" (never paid), "balanced" (prefer free, allow paid when hard),
            "max-quality" (best capable route regardless of cost).

    This function never judges task content: the only outcomes are a Route
    or RuntimeError when no endpoint is reachable.
    """
    threshold = escalation_threshold()
    complexity = estimate_complexity(task)
    routes = [dataclasses.replace(r) for r in default_routes()]

    local = routes[0]
    local.available = _ollama_alive(local.base_url)
    gh = routes[1]
    gh.available = bool(os.getenv(gh.api_key_env or ""))

    # Local small LM wins for simple tasks when it is up.
    if local.available and (complexity < threshold or budget == "free"):
        if budget == "free" and complexity >= threshold:
            reason = "budget_free_forces_local_despite_complexity"
        else:
            reason = "complexity_below_threshold"
        chosen, kept_local = local, True
    elif gh.available:
        reason = "escalated_complexity_above_threshold"
        chosen, kept_local = gh, False
    elif local.available:
        reason = "fallback_local_only_available_route"
        chosen, kept_local = local, True
    else:
        raise RuntimeError("no model route available: start Ollama or set JEV_MODEL_TOKEN")

    chosen = validate_route(chosen)
    log_decision(task=task, route=chosen, complexity=complexity,
                 threshold=threshold, reason=reason,
                 kept_local=kept_local, budget=budget)
    return chosen
