"""Helpers for compact source-specific seed bank loading and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir, load_yaml

CORE_SEED_COUNTS: dict[str, int] = {
    "reddit": 10,
}

OPTIONAL_TEMPLATE_CAPS: dict[str, int] = {
    "business_communities": 8,
    "reddit": 8,
}

DISALLOWED_GENERIC_SEEDS = {
    "best bi tool",
    "analytics platform",
    "top dashboard software",
    "business intelligence",
    "best alternatives",
    "dashboard software",
}

DEV_HEAVY_TERMS = {
    "api auth",
    "oauth",
    "sdk",
    "npm",
    "docker",
    "selenium",
    "playwright",
    "build error",
    "package install",
    "deployment",
    "plugin development",
    "scraping setup",
}

SEED_STYLE_TERMS: dict[str, tuple[str, ...]] = {
    "business_communities": (
        "report",
        "dashboard",
        "numbers",
        "totals",
        "metric",
        "trust",
        "different",
        "changed",
        "breakdown",
        "segment",
        "export",
        "source of truth",
        "which number",
        "what changed",
        "what should i do",
        "mismatch",
        "wrong",
        "discrepancy",
        "not matching",
        "campaign",
        "revenue",
        "attribution",
        "tracking",
    ),
    "discourse": (
        "metabase",
        "dashboard",
        "query",
        "aggregation",
        "totals",
        "count",
        "date filter",
        "group by",
        "wrong",
        "mismatch",
        "source of truth",
        "csv",
        "metric",
        "breakdown",
    ),
    "reddit": (
        "numbers",
        "different",
        "trust",
        "makes no sense",
        "why did",
        "what changed",
        "breakdown",
        "boss",
        "stakeholder",
        "what should i do",
    ),
    "existing_forums": (
        "wrong total",
        "count distinct",
        "join",
        "date",
        "filter",
        "group by",
        "dashboard",
        "metric",
        "source of truth",
        "breakdown",
        "stakeholders",
    ),
}


@dataclass(slots=True)
class SeedItem:
    """One core source seed with a short explanation."""

    seed: str
    reason: str


@dataclass(slots=True)
class SeedTemplate:
    """One optional template stored for future expansion."""

    template: str
    reason: str


@dataclass(slots=True)
class SeedBank:
    """Resolved seed bank for one source."""

    source_id: str
    source_group: str
    max_query_count: int
    source_notes: str
    core_seeds: list[SeedItem]
    candidate_seed_pool: list[str]
    optional_templates: list[SeedTemplate]
    negative_terms: list[str]
    shared_negative_terms: list[str]
    products: list[str]

    @property
    def active_queries(self) -> list[str]:
        """Return the default active query seeds used by the pipeline."""
        return [item.seed for item in self.core_seeds][: self.max_query_count]

    @property
    def all_negative_terms(self) -> list[str]:
        """Return merged group and source-specific negative terms."""
        merged: list[str] = []
        for term in [*self.shared_negative_terms, *self.negative_terms]:
            lowered = str(term).strip().lower()
            if lowered and lowered not in merged:
                merged.append(lowered)
        return merged


@dataclass(slots=True)
class DiscoveryQuery:
    """One source-aware discovery query derived from a source seed."""

    seed_used: str
    expanded_query: str
    source_id: str
    source_group: str
    token_terms: tuple[str, ...]


PREFIX_RULES: dict[str, tuple[str, ...]] = {
    "support_community": ("issue", "wrong", "mismatch", "not working", "not showing"),
    "discussion_forum": ("cannot", "missing", "need", "wrong"),
}

PREFIX_SIGNAL_TERMS = {
    "issue",
    "wrong",
    "mismatch",
    "missing",
    "need",
    "cannot",
    "not",
    "discrepancy",
    "problem",
}

DISCOVERY_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "your",
    "into",
    "that",
    "this",
    "have",
    "help",
}


def get_seed_bank_path(root_dir: Path, source_group: str, source_id: str) -> Path:
    """Return the canonical seed bank path for one source."""
    return root_dir / "config" / "seeds" / source_group / f"{source_id}.yaml"


def _get_group_negative_terms(root_dir: Path, source_group: str) -> list[str]:
    """Load the shared negative terms for one source group."""
    path = root_dir / "config" / "seeds" / "shared" / f"{source_group}.yaml"
    if not path.exists():
        return []
    payload = load_yaml(path)
    values = payload.get("negative_terms", []) or []
    return [str(item).strip() for item in values if str(item).strip()]


def load_seed_bank(root_dir: Path, source_group: str, source_id: str) -> SeedBank | None:
    """Load one source-specific seed bank if it exists."""
    path = get_seed_bank_path(root_dir, source_group, source_id)
    if not path.exists():
        return None
    payload = load_yaml(path)
    core_seeds = _load_active_seed_items(payload)
    optional_templates = [
        SeedTemplate(template=str(item.get("template", "")).strip(), reason=str(item.get("reason", "")).strip())
        for item in payload.get("optional_templates", []) or []
        if str(item.get("template", "")).strip()
    ]
    negative_terms = [str(item).strip() for item in payload.get("negative_terms", []) or [] if str(item).strip()]
    products = [str(item).strip() for item in payload.get("products", []) or [] if str(item).strip()]
    return SeedBank(
        source_id=str(payload.get("source_id", source_id)),
        source_group=str(payload.get("source_group", source_group)),
        max_query_count=int(payload.get("max_query_count", len(core_seeds) or 0)),
        source_notes=str(payload.get("source_notes", "") or "").strip(),
        core_seeds=core_seeds,
        candidate_seed_pool=_load_candidate_seed_pool(payload),
        optional_templates=optional_templates,
        negative_terms=negative_terms,
        shared_negative_terms=_get_group_negative_terms(root_dir, source_group),
        products=products,
    )


def resolve_seed_queries(root_dir: Path, config: dict[str, Any], source_id: str, source_group: str) -> list[str]:
    """Resolve active query seeds from the compact seed bank."""
    seed_bank_path = str(config.get("seed_bank_path", "") or "").strip()
    if seed_bank_path:
        payload = load_yaml(root_dir / seed_bank_path)
        seed_bank = SeedBank(
            source_id=str(payload.get("source_id", source_id)),
            source_group=str(payload.get("source_group", source_group)),
            max_query_count=int(
                payload.get("max_query_count", len(payload.get("active_core_seeds", payload.get("core_seeds", [])) or []))
            ),
            source_notes=str(payload.get("source_notes", "") or "").strip(),
            core_seeds=_load_active_seed_items(payload),
            candidate_seed_pool=_load_candidate_seed_pool(payload),
            optional_templates=[
                SeedTemplate(template=str(item.get("template", "")).strip(), reason=str(item.get("reason", "")).strip())
                for item in payload.get("optional_templates", []) or []
                if str(item.get("template", "")).strip()
            ],
            negative_terms=[str(item).strip() for item in payload.get("negative_terms", []) or [] if str(item).strip()],
            shared_negative_terms=_get_group_negative_terms(root_dir, source_group),
            products=[str(item).strip() for item in payload.get("products", []) or [] if str(item).strip()],
        )
    else:
        seed_bank = load_seed_bank(root_dir, source_group=source_group, source_id=source_id)
    if seed_bank is None:
        return []
    return seed_bank.active_queries


def build_discovery_queries(
    root_dir: Path,
    config: dict[str, Any],
    source_id: str,
    source_group: str,
) -> list[DiscoveryQuery]:
    """Build source-specific discovery queries without falling back to shared query maps."""
    seeds = resolve_seed_queries(
        root_dir,
        config=config,
        source_id=source_id,
        source_group=source_group,
    )
    source_token = _source_query_token(config=config, source_id=source_id)
    style = _query_style(config=config, source_group=source_group)
    prefix_choices = PREFIX_RULES.get(style, ())

    queries: list[DiscoveryQuery] = []
    seen_queries: set[str] = set()
    for seed in seeds:
        normalized_seed = _normalize_query_text(seed)
        if not normalized_seed:
            continue
        canonical_query = normalized_seed
        if source_token and source_token not in canonical_query:
            canonical_query = f"{source_token} {canonical_query}".strip()
        for expanded_query in _expand_query_variants(canonical_query, prefix_choices):
            if expanded_query in seen_queries:
                continue
            seen_queries.add(expanded_query)
            queries.append(
                DiscoveryQuery(
                    seed_used=normalized_seed,
                    expanded_query=expanded_query,
                    source_id=source_id,
                    source_group=source_group,
                    token_terms=_token_terms(expanded_query, source_token),
                )
            )
    return queries


def _load_active_seed_items(payload: dict[str, Any]) -> list[SeedItem]:
    """Load promoted active seeds, falling back to the legacy core_seeds key."""
    rows = payload.get("active_core_seeds", None)
    if rows is None:
        rows = payload.get("core_seeds", []) or []
    return [
        SeedItem(seed=str(item.get("seed", "")).strip(), reason=str(item.get("reason", "")).strip())
        for item in rows or []
        if str(item.get("seed", "")).strip()
    ]


def _source_query_token(config: dict[str, Any], source_id: str) -> str:
    """Return the short source token preferred in expanded queries."""
    explicit = str(config.get("seed_source_token", "") or "").strip().lower()
    if explicit:
        return explicit
    product = str(config.get("product_or_tool", "") or "").strip().lower()
    if product:
        return re.sub(r"\s+", " ", product)
    source_name = str(config.get("source_name", source_id) or "").strip().lower()
    source_name = source_name.replace(" community", "").replace(" discussions", "").replace(" discussion", "")
    return re.sub(r"\s+", " ", source_name)


def _query_style(config: dict[str, Any], source_group: str) -> str:
    """Return which prefix family to use for discovery query expansion."""
    explicit = str(config.get("seed_query_style", "") or "").strip().lower()
    if explicit:
        return explicit
    collector_kind = str(config.get("collector_kind", "") or "").strip().lower()
    if collector_kind in {"business_communities", "google_ads_help_community"}:
        return "support_community"
    if source_group in {"discourse", "existing_forums"}:
        return "discussion_forum"
    return "support_community"


def _expand_query_variants(base_query: str, prefix_choices: tuple[str, ...]) -> list[str]:
    """Expand one source seed into one or two short discovery queries."""
    normalized = _normalize_query_text(base_query)
    if not normalized:
        return []
    variants = [normalized]
    if any(term in normalized for term in PREFIX_SIGNAL_TERMS):
        return variants
    if prefix_choices:
        variants.append(_normalize_query_text(f"{prefix_choices[0]} {normalized}"))
    deduped: list[str] = []
    for query in variants:
        if query and query not in deduped:
            deduped.append(query)
    return deduped


def _normalize_query_text(value: str) -> str:
    """Normalize a short source query while keeping operator language intact."""
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return normalized


def _token_terms(expanded_query: str, source_token: str) -> tuple[str, ...]:
    """Return meaningful token terms for title-level discovery matching."""
    parts = re.findall(r"[a-z0-9]+", expanded_query.lower())
    source_parts = set(re.findall(r"[a-z0-9]+", source_token.lower()))
    tokens: list[str] = []
    for part in parts:
        if part in DISCOVERY_STOPWORDS:
            continue
        if len(part) < 3 and part not in {"bi"}:
            continue
        if part in source_parts and len(parts) > 2:
            continue
        if part not in tokens:
            tokens.append(part)
    return tuple(tokens)


def _load_candidate_seed_pool(payload: dict[str, Any]) -> list[str]:
    """Load broad discovery vocabulary stored for future seed promotion."""
    values = payload.get("candidate_seed_pool", []) or []
    candidates: list[str] = []
    for item in values:
        seed = str(item.get("seed", "") if isinstance(item, dict) else item).strip()
        if seed and seed not in candidates:
            candidates.append(seed)
    return candidates


def render_optional_queries(seed_bank: SeedBank) -> list[str]:
    """Render optional product-aware templates for audit and manual use."""
    rendered: list[str] = []
    products = seed_bank.products or [""]
    for template in seed_bank.optional_templates:
        if "{product}" in template.template:
            for product in products:
                query = template.template.format(product=product).strip()
                if query and query not in rendered:
                    rendered.append(query)
        else:
            query = template.template.strip()
            if query and query not in rendered:
                rendered.append(query)
    return rendered


def validate_seed_bank(seed_bank: SeedBank) -> list[dict[str, str]]:
    """Return validation findings for one source-specific seed bank."""
    findings: list[dict[str, str]] = []
    required_core_count = CORE_SEED_COUNTS.get(seed_bank.source_group)
    if required_core_count is not None and len(seed_bank.core_seeds) != required_core_count:
        findings.append(
            {
                "level": "error",
                "code": "core_count",
                "message": f"expected {required_core_count} core seeds, found {len(seed_bank.core_seeds)}",
            }
        )

    optional_cap = OPTIONAL_TEMPLATE_CAPS.get(seed_bank.source_group)
    if optional_cap is not None and len(seed_bank.optional_templates) > optional_cap:
        findings.append(
            {
                "level": "error",
                "code": "optional_cap",
                "message": f"optional template count {len(seed_bank.optional_templates)} exceeds cap {optional_cap}",
            }
        )

    if len(seed_bank.active_queries) > seed_bank.max_query_count:
        findings.append(
            {
                "level": "error",
                "code": "max_query_count",
                "message": f"active query count {len(seed_bank.active_queries)} exceeds max_query_count {seed_bank.max_query_count}",
            }
        )

    seen_seeds: set[str] = set()
    for item in seed_bank.core_seeds:
        lowered = item.seed.lower()
        if lowered in seen_seeds:
            findings.append({"level": "error", "code": "duplicate_seed", "message": f"duplicate seed: {item.seed}"})
        seen_seeds.add(lowered)
        if lowered in DISALLOWED_GENERIC_SEEDS:
            findings.append({"level": "error", "code": "generic_seed", "message": f"over-generic seed: {item.seed}"})
        if any(term in lowered for term in DEV_HEAVY_TERMS):
            findings.append({"level": "error", "code": "dev_heavy_seed", "message": f"developer-heavy seed: {item.seed}"})
        if len(lowered.split()) < 2:
            findings.append({"level": "warning", "code": "too_short", "message": f"short seed may be noisy: {item.seed}"})
        if not item.reason:
            findings.append({"level": "warning", "code": "missing_reason", "message": f"missing reason for seed: {item.seed}"})

    style_terms = SEED_STYLE_TERMS.get(seed_bank.source_group, ())
    style_hits = sum(1 for item in seed_bank.core_seeds if any(term in item.seed.lower() for term in style_terms))
    minimum_style_hits = max(1, len(seed_bank.core_seeds) // 2)
    if style_hits < minimum_style_hits:
        findings.append(
            {
                "level": "warning",
                "code": "style_alignment",
                "message": f"only {style_hits} seeds match expected {seed_bank.source_group} language style",
            }
        )

    return findings


def build_seed_summary(root_dir: Path, definitions: list[Any]) -> pd.DataFrame:
    """Build one summary row per selected source definition."""
    rows: list[dict[str, Any]] = []
    for definition in definitions:
        seed_bank = load_seed_bank(root_dir, definition.source_group, definition.source_id)
        if seed_bank is None:
            continue
        findings = validate_seed_bank(seed_bank)
        rows.append(
            {
                "source_id": definition.source_id,
                "source_name": definition.source_name,
                "source_group": definition.source_group,
                "core_seed_count": len(seed_bank.core_seeds),
                "candidate_seed_count": len(seed_bank.candidate_seed_pool),
                "optional_template_count": len(seed_bank.optional_templates),
                "negative_term_count": len(seed_bank.all_negative_terms),
                "max_query_count": seed_bank.max_query_count,
                "active_query_count": len(seed_bank.active_queries),
                "validation_errors": sum(1 for row in findings if row["level"] == "error"),
                "validation_warnings": sum(1 for row in findings if row["level"] == "warning"),
                "active_queries": " | ".join(seed_bank.active_queries),
            }
        )
    return pd.DataFrame(rows)


def build_seed_audit_markdown(root_dir: Path, definitions: list[Any]) -> str:
    """Build a markdown audit of compact seed banks for selected sources."""
    lines = ["# Source Seed Bank Audit", ""]
    for definition in definitions:
        seed_bank = load_seed_bank(root_dir, definition.source_group, definition.source_id)
        if seed_bank is None:
            continue
        findings = validate_seed_bank(seed_bank)
        lines.append(f"## {definition.source_id}")
        lines.append("")
        lines.append(f"- Source group: `{definition.source_group}`")
        lines.append(f"- Max query count: `{seed_bank.max_query_count}`")
        lines.append(f"- Active query count: `{len(seed_bank.active_queries)}`")
        lines.append(f"- Candidate seed pool count: `{len(seed_bank.candidate_seed_pool)}`")
        lines.append(f"- Notes: {seed_bank.source_notes}")
        lines.append("- Core seeds:")
        for item in seed_bank.core_seeds:
            lines.append(f"  - `{item.seed}`: {item.reason}")
        if seed_bank.optional_templates:
            lines.append("- Optional templates:")
            for item in seed_bank.optional_templates:
                lines.append(f"  - `{item.template}`: {item.reason}")
        lines.append(f"- Shared/source negatives: {', '.join(seed_bank.all_negative_terms)}")
        if findings:
            lines.append("- Validation findings:")
            for finding in findings:
                lines.append(f"  - `{finding['level']}` `{finding['code']}`: {finding['message']}")
        else:
            lines.append("- Validation findings: none")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def build_before_after_seed_diff(root_dir: Path, definitions: list[Any]) -> str:
    """Compare new compact seed counts against the legacy query map where available."""
    query_map = load_yaml(root_dir / "config" / "query_map.yaml")
    legacy_banks = query_map.get("source_seed_banks", {}) or {}
    lines = ["# Before vs After Seed Diff", ""]
    for definition in definitions:
        seed_bank = load_seed_bank(root_dir, definition.source_group, definition.source_id)
        if seed_bank is None:
            continue
        legacy_rows = legacy_banks.get(definition.source_id, []) or []
        legacy_count = len(legacy_rows) if isinstance(legacy_rows, list) else 0
        lines.append(f"## {definition.source_id}")
        lines.append("")
        lines.append(f"- Legacy dedicated seed count: `{legacy_count}`")
        lines.append(f"- New compact core seed count: `{len(seed_bank.core_seeds)}`")
        lines.append(f"- Candidate seed pool count: `{len(seed_bank.candidate_seed_pool)}`")
        lines.append(f"- Optional template count: `{len(seed_bank.optional_templates)}`")
        if legacy_count:
            delta = len(seed_bank.core_seeds) - legacy_count
            lines.append(f"- Net change vs legacy dedicated bank: `{delta:+d}`")
        else:
            lines.append("- Net change vs legacy dedicated bank: `new dedicated compact bank`")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def export_seed_artifacts(root_dir: Path, definitions: list[Any]) -> dict[str, Path]:
    """Write summary and audit artifacts for the selected seed banks."""
    analysis_dir = ensure_dir(root_dir / "data" / "analysis")
    config_export_dir = ensure_dir(analysis_dir / "new_seed_configs")
    summary_df = build_seed_summary(root_dir, definitions)
    summary_path = analysis_dir / "source_seed_bank_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    audit_path = analysis_dir / "source_seed_bank_audit.md"
    audit_path.write_text(build_seed_audit_markdown(root_dir, definitions), encoding="utf-8")

    diff_path = analysis_dir / "before_after_seed_diff.md"
    diff_path.write_text(build_before_after_seed_diff(root_dir, definitions), encoding="utf-8")

    for definition in definitions:
        source_path = get_seed_bank_path(root_dir, definition.source_group, definition.source_id)
        if source_path.exists():
            target_name = f"{definition.source_group}__{definition.source_id}.yaml"
            (config_export_dir / target_name).write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")

    return {
        "summary_csv": summary_path,
        "audit_md": audit_path,
        "diff_md": diff_path,
        "config_dir": config_export_dir,
    }
