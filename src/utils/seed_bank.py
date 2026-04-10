"""Helpers for compact source-specific seed bank loading and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir, load_yaml

CORE_SEED_COUNTS: dict[str, int] = {
    "reddit": 8,
}

OPTIONAL_TEMPLATE_CAPS: dict[str, int] = {
    "reddit": 4,
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
    "reddit": (
        "every week",
        "takes too long",
        "don't",
        "can't",
        "still",
        "keep",
        "why did",
        "follow-up",
        "follow up",
        "breakdown",
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
    core_seeds = [
        SeedItem(seed=str(item.get("seed", "")).strip(), reason=str(item.get("reason", "")).strip())
        for item in payload.get("core_seeds", []) or []
        if str(item.get("seed", "")).strip()
    ]
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
            max_query_count=int(payload.get("max_query_count", len(payload.get("core_seeds", []) or []))),
            source_notes=str(payload.get("source_notes", "") or "").strip(),
            core_seeds=[
                SeedItem(seed=str(item.get("seed", "")).strip(), reason=str(item.get("reason", "")).strip())
                for item in payload.get("core_seeds", []) or []
                if str(item.get("seed", "")).strip()
            ],
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
