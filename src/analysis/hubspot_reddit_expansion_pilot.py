"""Bounded incremental expansion pilot for HubSpot Community and Reddit."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import md5
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from src.collectors.base import RawRecord
from src.collectors.business_community_collector import BusinessCommunityCollector
from src.collectors.reddit_collector import RedditCollector
from src.utils.io import ensure_dir, load_yaml, read_jsonl, write_jsonl

PILOT_SOURCE_NAME = "hubspot_reddit_existing_source_expansion"
HUBSPOT_PILOT_SEED = Path("config") / "seeds" / "business_communities" / "hubspot_community_expansion_pilot.yaml"
REDDIT_PILOT_SEED = Path("config") / "seeds" / "existing_forums" / "reddit_existing_balance_pilot.yaml"
PILOT_ARTIFACT_DIR = Path("artifacts") / "source_pilots"
PILOT_READINESS_JSON = Path("artifacts") / "readiness" / "hubspot_reddit_expansion_pilot_report.json"
PILOT_REPORT_MD = Path("docs") / "operational" / "HUBSPOT_REDDIT_EXPANSION_PILOT_REPORT.md"
DEDUP_SUMMARY_JSON = PILOT_ARTIFACT_DIR / "hubspot_reddit_existing_dedupe_index_summary.json"
RAW_JSONL = PILOT_ARTIFACT_DIR / "hubspot_reddit_expansion_raw.jsonl"
NEW_UNIQUE_CSV = PILOT_ARTIFACT_DIR / "hubspot_reddit_expansion_new_unique.csv"
DUPLICATES_CSV = PILOT_ARTIFACT_DIR / "hubspot_reddit_expansion_duplicates.csv"
SUMMARY_JSON = PILOT_ARTIFACT_DIR / "hubspot_reddit_expansion_summary.json"
SAMPLE_CSV = PILOT_ARTIFACT_DIR / "hubspot_reddit_expansion_sample.csv"

DEFAULT_USER_AGENT = "persona-research-bot/0.1 (+bounded existing-source expansion pilot; local-only audit)"
TARGET_RAW_ROWS = {"hubspot_community": 250, "reddit": 250}
MIN_NEW_UNIQUE_ROWS = {"hubspot_community": 200, "reddit": 200}
MIN_CORE_RATIO = {"hubspot_community": 0.80, "reddit": 0.75}

POSITIVE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "reporting_or_dashboard_pain": (
        "report",
        "reporting",
        "dashboard",
        "export",
        "excel",
        "spreadsheet",
        "recurring report",
        "monthly report",
        "weekly report",
    ),
    "CRM_or_pipeline_reporting_pain": (
        "crm",
        "pipeline",
        "lifecycle",
        "lead",
        "mql",
        "sql",
        "deal",
        "sales funnel",
        "attribution",
        "revenue report",
    ),
    "validation_or_reconciliation_pain": (
        "mismatch",
        "not matching",
        "doesn't match",
        "different numbers",
        "wrong number",
        "source of truth",
        "reconcile",
        "reconciliation",
        "trust",
        "incorrect",
    ),
    "stakeholder_reporting_context": (
        "leadership",
        "stakeholder",
        "exec",
        "boss",
        "client",
        "finance",
        "sales team",
        "board",
    ),
}

NOISE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "career_training_noise": (
        "career",
        "certificate",
        "certification",
        "course",
        "bootcamp",
        "tutorial",
    ),
    "generic_chatter_noise": (
        "anyone else",
        "just curious",
        "rant",
        "vent",
        "what do you think",
    ),
    "setup_support_noise": (
        "api",
        "oauth",
        "token",
        "permission",
        "auth",
        "install",
        "setup",
        "configuration",
        "dns",
        "script",
        "plugin",
        "code",
        "sdk",
        "bug report",
        "css",
        "html",
        "javascript",
        "python",
    ),
    "vendor_marketing_noise": (
        "announcement",
        "webinar",
        "launch",
        "new feature",
        "product update",
        "demo",
    ),
}


@dataclass(slots=True)
class ExpansionPilotRow:
    """One fetched pilot row with dedupe and scoring metadata."""

    source: str
    raw_id: str
    url: str
    canonical_url: str
    title: str
    body: str
    created_at: str
    query_seed: str
    subreddit_or_forum: str
    dedupe_status: str
    dedupe_reason: str
    duplicate_against: str
    normalized_title: str
    content_hash: str
    fetched_at: str
    valid_candidate: bool
    estimated_persona_core_candidate: bool
    persona_01_fit: str
    persona_02_fit: str
    persona_03_fit: str
    persona_04_fit: str
    persona_05_fit: str
    reporting_or_dashboard_pain: int
    CRM_or_pipeline_reporting_pain: int
    validation_or_reconciliation_pain: int
    stakeholder_reporting_context: int
    career_training_noise: int
    generic_chatter_noise: int
    setup_support_noise: int
    vendor_marketing_noise: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize the pilot row."""
        return asdict(self)


def load_pilot_source_configs(root_dir: Path) -> dict[str, dict[str, Any]]:
    """Load pilot-only source config overrides."""
    hubspot = dict(load_yaml(root_dir / "config" / "sources" / "hubspot_community.yaml"))
    hubspot["seed_bank_path"] = str(HUBSPOT_PILOT_SEED).replace("\\", "/")
    hubspot["query_mode"] = "source_config"
    api_cfg = dict(hubspot.get("api_discovery", {}) or {})
    api_cfg["max_items_per_board"] = 450
    hubspot["api_discovery"] = api_cfg
    hubspot["pilot_only"] = True
    hubspot["target_new_unique_rows"] = TARGET_RAW_ROWS["hubspot_community"]

    reddit = dict(load_yaml(root_dir / "config" / "sources" / "reddit.yaml"))
    reddit["query_mode"] = "source_config"
    reddit["seed_bank_path"] = str(REDDIT_PILOT_SEED).replace("\\", "/")
    reddit["preferred_subreddits"] = ["analytics", "businessintelligence", "marketinganalytics", "excel"]
    reddit["max_posts_per_seed"] = 50
    reddit["max_pages_per_query"] = 3
    reddit["default_per_seed_page_cap"] = 2
    reddit["max_queries_per_run"] = 5
    reddit["comment_expansion_mode"] = "none"
    reddit["pilot_only"] = True
    reddit["target_new_unique_rows"] = TARGET_RAW_ROWS["reddit"]
    return {"hubspot_community": hubspot, "reddit": reddit}


def is_pilot_output_path(path: Path, root_dir: Path) -> bool:
    """Return whether the path stays inside pilot artifacts or docs."""
    resolved = path.resolve()
    allowed_roots = [
        (root_dir / "artifacts" / "source_pilots").resolve(),
        (root_dir / "artifacts" / "readiness").resolve(),
        (root_dir / "docs" / "operational").resolve(),
    ]
    for allowed in allowed_roots:
        try:
            resolved.relative_to(allowed)
            return True
        except ValueError:
            continue
    return False


def normalize_title(value: str) -> str:
    """Normalize titles for fuzzy duplicate checks."""
    text = re.sub(r"\s+", " ", (value or "").strip().lower())
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return text.strip()


def canonicalize_url(value: str) -> str:
    """Normalize a URL for duplicate checks."""
    if not value:
        return ""
    parsed = urlparse(value.strip())
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = re.sub(r"/+", "/", parsed.path).rstrip("/")
    return f"{scheme}://{netloc}{path}"


def build_content_hash(title: str, body: str) -> str:
    """Build a compact content hash."""
    payload = f"{normalize_title(title)}||{re.sub(r'\\s+', ' ', (body or '').strip().lower())[:2000]}"
    return md5(payload.encode("utf-8")).hexdigest()


def build_existing_dedupe_index(root_dir: Path, source: str) -> tuple[dict[str, set[str]], dict[str, Any]]:
    """Build dedupe keys from active raw/normalized/labeled artifacts."""
    index: dict[str, set[str]] = {
        "raw_id": set(),
        "url": set(),
        "canonical_url": set(),
        "title": set(),
        "content_hash": set(),
    }
    raw_path = root_dir / "data" / "raw" / source / "raw.jsonl"
    raw_rows = read_jsonl(raw_path)
    for row in raw_rows:
        _add_dedupe_keys(index, row.get("raw_id", ""), row.get("url", ""), row.get("canonical_url", ""), row.get("title", ""), row.get("body") or row.get("body_text") or "")

    normalized_count = 0
    normalized_path = root_dir / "data" / "normalized" / f"{source}.parquet"
    if normalized_path.exists():
        normalized = pd.read_parquet(normalized_path)
        normalized_count = int(len(normalized))
        for _, row in normalized.iterrows():
            _add_dedupe_keys(index, row.get("raw_id", ""), row.get("url", ""), row.get("canonical_url", ""), row.get("title", ""), row.get("body") or row.get("body_text") or row.get("raw_text") or "")

    labeled_count = 0
    labeled_path = root_dir / "data" / "labeled" / "labeled_episodes.parquet"
    episode_path = root_dir / "data" / "episodes" / "episode_table.parquet"
    if labeled_path.exists() and episode_path.exists():
        labeled = pd.read_parquet(labeled_path, columns=["episode_id"])
        episode_schema = pd.read_parquet(episode_path, columns=None).columns
        episode_columns = [column for column in ["episode_id", "source", "raw_id", "url", "normalized_episode", "created_at"] if column in episode_schema]
        episode = pd.read_parquet(episode_path, columns=episode_columns)
        joined = labeled.merge(episode, on="episode_id", how="left")
        joined = joined[joined["source"] == source]
        labeled_count = int(len(joined))
        for _, row in joined.iterrows():
            _add_dedupe_keys(index, row.get("raw_id", ""), row.get("url", ""), "", row.get("normalized_episode", "")[:160], row.get("normalized_episode", ""))

    summary = {
        "source": source,
        "existing_raw_count": len(raw_rows),
        "existing_normalized_count": normalized_count,
        "existing_labeled_count": labeled_count,
        "existing_known_raw_id_count": len(index["raw_id"]),
        "existing_known_url_count": len(index["url"]),
        "existing_known_canonical_url_count": len(index["canonical_url"]),
        "existing_known_title_count": len(index["title"]),
        "existing_known_content_hash_count": len(index["content_hash"]),
        "dedupe_key_coverage": {
            "raw_id": len(index["raw_id"]) > 0,
            "url": len(index["url"]) > 0,
            "canonical_url": len(index["canonical_url"]) > 0,
            "title": len(index["title"]) > 0,
            "content_hash": len(index["content_hash"]) > 0,
        },
    }
    return index, summary


def _add_dedupe_keys(index: dict[str, set[str]], raw_id: Any, url: Any, canonical_url: Any, title: Any, body: Any) -> None:
    """Add dedupe keys when present."""
    raw_id_text = str(raw_id or "").strip()
    if raw_id_text:
        index["raw_id"].add(raw_id_text)
    url_text = canonicalize_url(str(url or ""))
    if url_text:
        index["url"].add(url_text)
    canonical_text = canonicalize_url(str(canonical_url or ""))
    if canonical_text:
        index["canonical_url"].add(canonical_text)
    title_text = normalize_title(str(title or ""))
    if title_text:
        index["title"].add(title_text)
    content_text = build_content_hash(str(title or ""), str(body or ""))
    if content_text:
        index["content_hash"].add(content_text)


def classify_duplicate(
    record: RawRecord,
    existing_index: dict[str, set[str]],
    seen_pilot: dict[str, set[str]],
) -> tuple[str, str]:
    """Classify one record against the existing corpus and pilot batch."""
    raw_id = str(record.raw_id or "").strip()
    url = canonicalize_url(record.url)
    canonical_url = canonicalize_url(record.canonical_url or "")
    title = normalize_title(record.title)
    content_hash = build_content_hash(record.title, record.body or record.body_text)

    for key_name, value, duplicate_reason in [
        ("raw_id", raw_id, "duplicate_raw_id"),
        ("url", url, "duplicate_url"),
        ("canonical_url", canonical_url, "duplicate_canonical_url"),
        ("content_hash", content_hash, "duplicate_content_hash"),
    ]:
        if value and (value in existing_index[key_name] or value in seen_pilot[key_name]):
            against = "existing" if value in existing_index[key_name] else "pilot"
            return duplicate_reason, against
    if title and (title in existing_index["title"] or title in seen_pilot["title"]):
        against = "existing" if title in existing_index["title"] else "pilot"
        return "near_duplicate_title", against
    if not any([raw_id, url, canonical_url, title, content_hash]):
        return "insufficient_dedupe_keys", "none"
    return "new_unique", "none"


def register_pilot_keys(record: RawRecord, seen_pilot: dict[str, set[str]]) -> None:
    """Track keys seen during this pilot run."""
    _add_dedupe_keys(
        seen_pilot,
        record.raw_id,
        record.url,
        record.canonical_url,
        record.title,
        record.body or record.body_text,
    )


def score_signals(source: str, title: str, body: str, query_seed: str, forum: str) -> dict[str, int]:
    """Score positive and negative pilot signals."""
    text = " ".join([source, title, body, query_seed, forum]).lower()
    scores: dict[str, int] = {}
    for name, keywords in POSITIVE_KEYWORDS.items():
        scores[name] = int(any(keyword in text for keyword in keywords))
    for name, keywords in NOISE_KEYWORDS.items():
        scores[name] = int(any(keyword in text for keyword in keywords))
    return scores


def _fit_label(score: int) -> str:
    """Map a fit score to a label."""
    if score >= 2:
        return "strong"
    if score == 1:
        return "medium"
    return "weak"


def score_persona_fit(signals: dict[str, int], source: str, title: str, body: str) -> dict[str, str]:
    """Estimate persona fit from pilot-only heuristics."""
    text = " ".join([source, title, body]).lower()
    p01 = min(2, signals["reporting_or_dashboard_pain"] + signals["stakeholder_reporting_context"] + int("export" in text or "excel" in text or "spreadsheet" in text))
    p02 = min(2, signals["stakeholder_reporting_context"] + int("why" in text or "explain" in text or "what changed" in text or "trust" in text))
    p03 = min(2, signals["reporting_or_dashboard_pain"] + int("workaround" in text or "manual" in text or "not enough" in text or "limitation" in text))
    p04 = min(2, signals["validation_or_reconciliation_pain"] + signals["CRM_or_pipeline_reporting_pain"] + int("pipeline" in text or "attribution" in text or "mismatch" in text))
    p05 = min(2, int("layout" in text or "presentation" in text or "deck" in text or "report builder" in text) + signals["stakeholder_reporting_context"])
    return {
        "persona_01_fit": _fit_label(p01),
        "persona_02_fit": _fit_label(p02),
        "persona_03_fit": _fit_label(p03),
        "persona_04_fit": _fit_label(p04),
        "persona_05_fit": _fit_label(p05),
    }


def fit_is_meaningful(value: str) -> bool:
    """Return whether a fit label is meaningful."""
    return value in {"medium", "strong"}


def estimate_valid_candidate(row: ExpansionPilotRow) -> bool:
    """Estimate whether a new unique row is candidate-worthy."""
    positive = row.reporting_or_dashboard_pain + row.CRM_or_pipeline_reporting_pain + row.validation_or_reconciliation_pain + row.stakeholder_reporting_context
    noise = row.career_training_noise + row.generic_chatter_noise + row.setup_support_noise + row.vendor_marketing_noise
    return positive >= 1 and noise == 0


def estimate_persona_core_candidate(row: ExpansionPilotRow) -> bool:
    """Estimate whether a row likely survives to persona-core."""
    if not estimate_valid_candidate(row):
        return False
    if row.setup_support_noise or row.vendor_marketing_noise:
        return False
    if row.source == "hubspot_community":
        return fit_is_meaningful(row.persona_01_fit) and (fit_is_meaningful(row.persona_02_fit) or fit_is_meaningful(row.persona_04_fit))
    return fit_is_meaningful(row.persona_01_fit) or fit_is_meaningful(row.persona_04_fit)


def build_pilot_row(record: RawRecord, dedupe_status: str, duplicate_against: str) -> ExpansionPilotRow:
    """Build one scored pilot row from a fetched raw record."""
    body = (record.body_text or record.body or "").strip()
    scores = score_signals(record.source, record.title, body, record.query_seed, record.subreddit_or_forum)
    fits = score_persona_fit(scores, record.source, record.title, body)
    row = ExpansionPilotRow(
        source=record.source,
        raw_id=str(record.raw_id or ""),
        url=record.url,
        canonical_url=record.canonical_url or "",
        title=record.title,
        body=body,
        created_at=record.created_at,
        query_seed=record.query_seed,
        subreddit_or_forum=record.subreddit_or_forum,
        dedupe_status=dedupe_status,
        dedupe_reason=dedupe_status,
        duplicate_against=duplicate_against,
        normalized_title=normalize_title(record.title),
        content_hash=build_content_hash(record.title, body),
        fetched_at=record.fetched_at,
        valid_candidate=False,
        estimated_persona_core_candidate=False,
        persona_01_fit=fits["persona_01_fit"],
        persona_02_fit=fits["persona_02_fit"],
        persona_03_fit=fits["persona_03_fit"],
        persona_04_fit=fits["persona_04_fit"],
        persona_05_fit=fits["persona_05_fit"],
        reporting_or_dashboard_pain=scores["reporting_or_dashboard_pain"],
        CRM_or_pipeline_reporting_pain=scores["CRM_or_pipeline_reporting_pain"],
        validation_or_reconciliation_pain=scores["validation_or_reconciliation_pain"],
        stakeholder_reporting_context=scores["stakeholder_reporting_context"],
        career_training_noise=scores["career_training_noise"],
        generic_chatter_noise=scores["generic_chatter_noise"],
        setup_support_noise=scores["setup_support_noise"],
        vendor_marketing_noise=scores["vendor_marketing_noise"],
    )
    row.valid_candidate = estimate_valid_candidate(row)
    row.estimated_persona_core_candidate = estimate_persona_core_candidate(row)
    return row


def baseline_metrics(root_dir: Path) -> dict[str, Any]:
    """Capture workbook and source-level baseline metrics."""
    overview = pd.read_csv(root_dir / "data" / "analysis" / "overview.csv")
    quality = pd.read_csv(root_dir / "data" / "analysis" / "quality_checks.csv")
    source_distribution = pd.read_csv(root_dir / "data" / "analysis" / "source_distribution.csv")
    episodes = pd.read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet")[["episode_id", "source"]]
    assignments = pd.read_parquet(root_dir / "data" / "analysis" / "persona_assignments.parquet")[["episode_id", "persona_id"]]
    source_balance = pd.read_csv(root_dir / "data" / "analysis" / "source_balance_audit.csv")
    overview_map = dict(zip(overview["metric"], overview["value"]))
    quality_map = dict(zip(quality["metric"], quality["value"]))
    raw_count_col = "raw_record_count" if "raw_record_count" in source_distribution.columns else "raw_count"
    core = assignments.merge(episodes, on="episode_id", how="left")
    persona_ct = pd.crosstab(core["source"], core["persona_id"])

    sources = {}
    for source in ["hubspot_community", "reddit"]:
        row = source_balance[source_balance["source"] == source].iloc[0]
        sources[source] = {
            "raw_rows": int(row["raw_record_count"]),
            "labeled_rows": int(row["labeled_episode_count"]),
            "persona_core_rows": int(row["promoted_persona_episode_count"]),
            "core_ratio": round(float(row["promoted_persona_episode_count"]) / float(row["labeled_episode_count"]), 3),
            "weak_source_status": bool(row["weak_source_cost_center"]),
            "core_readiness_weak_source_status": bool(row["core_readiness_weak_source_cost_center"]),
            "persona_contribution": {
                p: int(persona_ct.get(p, pd.Series(dtype=int)).get(source, 0))
                for p in ["persona_01", "persona_02", "persona_03", "persona_04", "persona_05"]
            },
        }
    return {
        "persona_readiness_state": str(overview_map["persona_readiness_state"]),
        "overall_status": str(overview_map["overall_status"]),
        "quality_flag": str(overview_map["quality_flag"]),
        "total_raw_rows": int(source_distribution[raw_count_col].sum()),
        "total_labeled_rows": int(quality_map["labeled_episode_rows"]),
        "total_persona_core_rows": int(quality_map["persona_core_labeled_rows"]),
        "effective_balanced_source_count": float(overview_map["effective_balanced_source_count"]),
        "persona_core_coverage_of_all_labeled_pct": float(overview_map["persona_core_coverage_of_all_labeled_pct"]),
        "weak_source_cost_center_count": int(quality_map["weak_source_cost_center_count"]),
        "core_readiness_weak_source_cost_center_count": int(quality_map["core_readiness_weak_source_cost_center_count"]),
        "final_usable_persona_count": int(overview_map["final_usable_persona_count"]),
        "production_ready_persona_count": int(overview_map["production_ready_persona_count"]),
        "review_ready_persona_count": int(overview_map["review_ready_persona_count"]),
        "deck_ready_claim_eligible_persona_count": int(overview_map["deck_ready_claim_eligible_persona_count"]),
        "sources": sources,
    }


def run_source_collectors(root_dir: Path) -> dict[str, list[RawRecord]]:
    """Run bounded pilot collectors without saving into production raw paths."""
    configs = load_pilot_source_configs(root_dir)
    data_dir = root_dir / "data"
    hubspot_records = BusinessCommunityCollector("hubspot_community", configs["hubspot_community"], data_dir).collect()
    reddit_records = RedditCollector(configs["reddit"], data_dir).collect()
    return {"hubspot_community": hubspot_records, "reddit": reddit_records}


def build_summary(root_dir: Path, baseline: dict[str, Any], rows: list[ExpansionPilotRow], dedupe_summaries: dict[str, Any]) -> dict[str, Any]:
    """Build per-source and combined pilot summary using only new unique rows."""
    source_balance = pd.read_csv(root_dir / "data" / "analysis" / "source_balance_audit.csv")
    blended_counts = (source_balance.set_index("source")["blended_influence_share_pct"] / 100.0) * baseline["total_persona_core_rows"]
    new_unique = [row for row in rows if row.dedupe_status == "new_unique"]
    by_source: dict[str, dict[str, Any]] = {}
    total_valid = 0
    total_core = 0
    for source in ["hubspot_community", "reddit"]:
        source_rows = [row for row in rows if row.source == source]
        unique_rows = [row for row in source_rows if row.dedupe_status == "new_unique"]
        valid_count = sum(1 for row in unique_rows if row.valid_candidate)
        core_count = sum(1 for row in unique_rows if row.estimated_persona_core_candidate)
        total_valid += valid_count
        total_core += core_count
        duplicate_rate = round((len(source_rows) - len(unique_rows)) / len(source_rows), 3) if source_rows else 0.0
        persona_counts = {
            "persona_01": sum(1 for row in unique_rows if fit_is_meaningful(row.persona_01_fit)),
            "persona_02": sum(1 for row in unique_rows if fit_is_meaningful(row.persona_02_fit)),
            "persona_03": sum(1 for row in unique_rows if fit_is_meaningful(row.persona_03_fit)),
            "persona_04": sum(1 for row in unique_rows if fit_is_meaningful(row.persona_04_fit)),
            "persona_05": sum(1 for row in unique_rows if fit_is_meaningful(row.persona_05_fit)),
        }
        by_source[source] = {
            "fetched_rows": len(source_rows),
            "duplicate_rows": len(source_rows) - len(unique_rows),
            "new_unique_rows": len(unique_rows),
            "duplicate_rate": duplicate_rate,
            "duplicate_reason_counts": pd.Series([row.dedupe_reason for row in source_rows if row.dedupe_status != "new_unique"]).value_counts().to_dict(),
            "valid_candidate_count": valid_count,
            "estimated_persona_core_candidate_count": core_count,
            "incremental_core_candidate_ratio": round(core_count / len(unique_rows), 3) if unique_rows else 0.0,
            "career_training_noise_count": sum(row.career_training_noise for row in unique_rows),
            "generic_chatter_noise_count": sum(row.generic_chatter_noise for row in unique_rows),
            "setup_support_noise_count": sum(row.setup_support_noise for row in unique_rows),
            "vendor_marketing_noise_count": sum(row.vendor_marketing_noise for row in unique_rows),
            "persona_fit_counts": persona_counts,
            "seed_window_exhausted": len(unique_rows) < TARGET_RAW_ROWS[source],
        }

    projected_labeled = baseline["total_labeled_rows"] + total_valid
    projected_core = baseline["total_persona_core_rows"] + total_core
    projected_coverage = round(projected_core / projected_labeled * 100, 2) if projected_labeled else baseline["persona_core_coverage_of_all_labeled_pct"]
    counts = blended_counts.copy()
    for source in ["hubspot_community", "reddit"]:
        counts[source] = counts.get(source, 0.0) + by_source[source]["estimated_persona_core_candidate_count"]
    shares = counts / counts.sum()
    projected_effective = round(float(1.0 / (shares.pow(2).sum())), 2)
    projected_weak_source_cost_center_count = baseline["weak_source_cost_center_count"]
    projected_core_readiness_weak_source_cost_center_count = baseline["core_readiness_weak_source_cost_center_count"]
    projected_final_usable_persona_count = baseline["final_usable_persona_count"]
    projected_production_ready_persona_count = baseline["production_ready_persona_count"]
    projected_review_ready_persona_count = baseline["review_ready_persona_count"]
    projected_deck_ready_claim_eligible_persona_count = baseline["deck_ready_claim_eligible_persona_count"]

    hubspot_pass = (
        by_source["hubspot_community"]["new_unique_rows"] >= MIN_NEW_UNIQUE_ROWS["hubspot_community"]
        and by_source["hubspot_community"]["incremental_core_candidate_ratio"] >= MIN_CORE_RATIO["hubspot_community"]
        and by_source["hubspot_community"]["setup_support_noise_count"] <= by_source["hubspot_community"]["new_unique_rows"] * 0.2
        and by_source["hubspot_community"]["persona_fit_counts"]["persona_01"] > 0
        and (by_source["hubspot_community"]["persona_fit_counts"]["persona_02"] > 0 or by_source["hubspot_community"]["persona_fit_counts"]["persona_04"] > 0)
    )
    reddit_pass = (
        by_source["reddit"]["new_unique_rows"] >= MIN_NEW_UNIQUE_ROWS["reddit"]
        and by_source["reddit"]["incremental_core_candidate_ratio"] >= MIN_CORE_RATIO["reddit"]
        and (by_source["reddit"]["career_training_noise_count"] + by_source["reddit"]["generic_chatter_noise_count"]) <= by_source["reddit"]["new_unique_rows"] * 0.25
        and (by_source["reddit"]["persona_fit_counts"]["persona_01"] > 0 or by_source["reddit"]["persona_fit_counts"]["persona_04"] > 0)
    )
    combined_pass = (
        hubspot_pass
        and reddit_pass
        and projected_effective >= 6.0
        and projected_coverage >= baseline["persona_core_coverage_of_all_labeled_pct"]
        and projected_weak_source_cost_center_count <= baseline["weak_source_cost_center_count"]
        and projected_core_readiness_weak_source_cost_center_count <= baseline["core_readiness_weak_source_cost_center_count"]
        and projected_final_usable_persona_count >= baseline["final_usable_persona_count"]
        and projected_production_ready_persona_count >= baseline["production_ready_persona_count"]
        and projected_review_ready_persona_count >= baseline["review_ready_persona_count"]
        and projected_deck_ready_claim_eligible_persona_count >= baseline["deck_ready_claim_eligible_persona_count"]
    )

    if combined_pass:
        decision = "propose_production_expansion"
    elif hubspot_pass and not reddit_pass:
        decision = "hubspot_only_expansion"
    elif reddit_pass and not hubspot_pass:
        decision = "reddit_only_expansion"
    elif by_source["hubspot_community"]["duplicate_rate"] > 0.8 or by_source["reddit"]["duplicate_rate"] > 0.8:
        decision = "adjust_seed_window"
    else:
        decision = "stop_existing_source_expansion"

    return {
        "baseline": baseline,
        "dedupe_index_summary": dedupe_summaries,
        "source_summaries": by_source,
        "projected_metric_movement": {
            "projected_labeled_rows": projected_labeled,
            "projected_persona_core_rows": projected_core,
            "projected_effective_balanced_source_count": projected_effective,
            "projected_persona_core_coverage_of_all_labeled_pct": projected_coverage,
            "projected_weak_source_cost_center_count": projected_weak_source_cost_center_count,
            "projected_core_readiness_weak_source_cost_center_count": projected_core_readiness_weak_source_cost_center_count,
            "projected_final_usable_persona_count": projected_final_usable_persona_count,
            "projected_production_ready_persona_count": projected_production_ready_persona_count,
            "projected_review_ready_persona_count": projected_review_ready_persona_count,
            "projected_deck_ready_claim_eligible_persona_count": projected_deck_ready_claim_eligible_persona_count,
            "projected_source_balance_impact": "improves" if projected_effective >= 6.0 else "insufficient",
            "confidence_level": "medium" if sum(len([row for row in rows if row.source == s and row.dedupe_status == 'new_unique']) for s in ['hubspot_community','reddit']) >= 400 else "low",
        },
        "pilot_gate_result": {
            "hubspot_pass": hubspot_pass,
            "reddit_pass": reddit_pass,
            "combined_pass": combined_pass,
            "decision": decision,
        },
    }


def render_report(summary: dict[str, Any]) -> str:
    """Render a markdown pilot report."""
    lines = ["# HubSpot + Reddit Expansion Pilot Report", ""]
    lines.append("## Baseline")
    for key, value in summary["baseline"].items():
        if key == "sources":
            continue
        lines.append(f"- `{key}` = `{value}`")
    lines.append("")
    lines.append("## Dedupe Index Summary")
    for source, details in summary["dedupe_index_summary"].items():
        lines.append(f"- `{source}` = `{details}`")
    lines.append("")
    lines.append("## Source Summaries")
    for source, details in summary["source_summaries"].items():
        lines.append(f"### {source}")
        for key, value in details.items():
            lines.append(f"- `{key}` = `{value}`")
    lines.append("")
    lines.append("## Projected Metric Movement")
    for key, value in summary["projected_metric_movement"].items():
        lines.append(f"- `{key}` = `{value}`")
    lines.append("")
    lines.append("## Gate Result")
    for key, value in summary["pilot_gate_result"].items():
        lines.append(f"- `{key}` = `{value}`")
    return "\n".join(lines) + "\n"
