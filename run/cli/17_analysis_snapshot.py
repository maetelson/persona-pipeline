"""Generate a no-xlsx analysis validation snapshot from canonical bundle artifacts."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.analysis.workbook_bundle import WORKBOOK_SHEET_NAMES, workbook_bundle_exists
from src.utils.io import read_parquet

DEFAULT_SNAPSHOT_PATH = ROOT / "data" / "analysis" / "validation_snapshot.json"
DEFAULT_MARKDOWN_PATH = ROOT / "data" / "analysis" / "validation_snapshot.md"
DEFAULT_DELTA_PATH = ROOT / "data" / "analysis" / "validation_delta.json"

OVERVIEW_KEYS = [
    "persona_readiness_state",
    "persona_readiness_label",
    "overall_status",
    "quality_flag",
    "effective_balanced_source_count",
    "weak_source_cost_center_count",
    "weak_source_cost_centers",
    "final_usable_persona_count",
    "top_3_cluster_share_of_core_labeled",
    "largest_source_influence_share_pct",
    "persona_core_coverage_of_all_labeled_pct",
    "overall_unknown_ratio",
    "promoted_persona_example_coverage_pct",
]

SOURCE_COLUMNS = [
    "source",
    "valid_post_count",
    "prefiltered_valid_post_count",
    "episode_count",
    "labelable_episode_count",
    "labeled_episode_count",
    "collapse_stage",
    "failure_reason_top",
    "priority_tier",
    "policy_action",
    "false_negative_hint",
    "source_specific_next_check",
]

PERSONA_COLUMNS = [
    "persona_id",
    "persona_name",
    "promotion_status",
    "promotion_action",
    "share_of_core_labeled",
    "grounding_status",
    "selected_example_count",
    "cross_source_robustness_score",
    "final_usable_persona",
]

DELTA_METRIC_KEYS = [
    "weak_source_cost_center_count",
    "final_usable_persona_count",
    "top_3_cluster_share_of_core_labeled",
    "largest_source_influence_share_pct",
    "persona_core_coverage_of_all_labeled_pct",
    "overall_unknown_ratio",
]

DELTA_SOURCE_KEYS = [
    "prefiltered_valid_post_count",
    "episode_count",
    "labelable_episode_count",
    "labeled_episode_count",
]

REGRESSION_DIRECTION = {
    "weak_source_cost_center_count": "lower_is_better",
    "final_usable_persona_count": "higher_is_better",
    "top_3_cluster_share_of_core_labeled": "lower_is_better",
    "largest_source_influence_share_pct": "lower_is_better",
    "persona_core_coverage_of_all_labeled_pct": "higher_is_better",
    "overall_unknown_ratio": "lower_is_better",
    "prefiltered_valid_post_count": "higher_is_better",
    "episode_count": "higher_is_better",
    "labelable_episode_count": "higher_is_better",
    "labeled_episode_count": "higher_is_better",
}

_AUDIT_SPEC = importlib.util.spec_from_file_location(
    "persona_workbook_audit_cli",
    ROOT / "run" / "cli" / "16_persona_workbook_audit.py",
)
if _AUDIT_SPEC is None or _AUDIT_SPEC.loader is None:
    raise RuntimeError("Unable to load workbook audit CLI helper.")
_AUDIT_MODULE = importlib.util.module_from_spec(_AUDIT_SPEC)
_AUDIT_SPEC.loader.exec_module(_AUDIT_MODULE)
build_audit_snapshot = _AUDIT_MODULE.build_audit_snapshot


def main() -> None:
    """Build a machine-readable validation snapshot without exporting xlsx."""
    parser = argparse.ArgumentParser(description="Create a no-xlsx validation snapshot from workbook bundle artifacts.")
    parser.add_argument("--json", action="store_true", help="Print the snapshot JSON to stdout.")
    parser.add_argument("--markdown", action="store_true", help="Print the snapshot markdown to stdout.")
    parser.add_argument("--compare-latest", action="store_true", help="Compare against the last validation snapshot if it exists.")
    parser.add_argument("--baseline", default="", help="Compare against a specific snapshot JSON path.")
    parser.add_argument("--fail-on-regression", action="store_true", help="Exit non-zero when the computed delta shows regressions.")
    args = parser.parse_args()

    snapshot = build_validation_snapshot(ROOT)
    baseline_snapshot = _load_baseline_snapshot(args)
    delta = build_validation_delta(snapshot, baseline_snapshot) if baseline_snapshot else {}

    DEFAULT_SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_SNAPSHOT_PATH.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    DEFAULT_MARKDOWN_PATH.write_text(render_snapshot_markdown(snapshot, delta if baseline_snapshot else None), encoding="utf-8")
    if baseline_snapshot:
        DEFAULT_DELTA_PATH.write_text(json.dumps(delta, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.json:
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    elif args.markdown:
        print(render_snapshot_markdown(snapshot, delta if baseline_snapshot else None))
    else:
        _print_compact_summary(snapshot, delta if baseline_snapshot else None)

    if args.fail_on_regression and baseline_snapshot and _has_regression(delta):
        raise SystemExit(1)


def build_validation_snapshot(root_dir: Path) -> dict[str, Any]:
    """Build a compact validation snapshot from canonical analysis outputs."""
    _ensure_bundle_contract(root_dir)
    analysis_dir = root_dir / "data" / "analysis"
    workbook_dir = analysis_dir / "workbook_bundle"

    overview_df = read_parquet(workbook_dir / "overview.parquet")
    source_diagnostics_df = read_parquet(workbook_dir / "source_diagnostics.parquet")
    source_balance_df = pd.read_csv(analysis_dir / "source_balance_audit.csv")
    persona_summary_df = pd.read_csv(analysis_dir / "persona_summary.csv")
    audit_snapshot = build_audit_snapshot(root_dir)

    overview = _metric_lookup(overview_df, "metric", "value")
    source_records = _frame_records(source_balance_df, SOURCE_COLUMNS, sort_by=["priority_tier", "source"])
    persona_records = _frame_records(persona_summary_df, PERSONA_COLUMNS, sort_by=["persona_id"])

    promoted_personas = [row for row in persona_records if str(row.get("promotion_status", "")) == "promoted_persona"]
    final_usable_personas = [row for row in persona_records if _coerce_bool(row.get("final_usable_persona", False))]
    source_actions = []
    if not source_diagnostics_df.empty and {"source", "row_kind", "section", "metric_name", "metric_value"}.issubset(source_diagnostics_df.columns):
        diag = source_diagnostics_df[
            source_diagnostics_df["row_kind"].astype(str).eq("diagnostic")
            & source_diagnostics_df["section"].astype(str).eq("diagnostic_reasons")
        ].copy()
        if not diag.empty:
            for source, group in diag.groupby("source", dropna=False):
                row = {
                    "source": str(source),
                    "diagnostics": {
                        str(metric_name): _normalize_scalar(group.loc[group["metric_name"] == metric_name, "metric_value"].iloc[0])
                        for metric_name in group["metric_name"].astype(str).tolist()
                    },
                }
                source_actions.append(row)

    return {
        "snapshot_schema_version": "v1",
        "snapshot_type": "analysis_validation",
        "generated_at": pd.Timestamp.now("UTC").isoformat(),
        "root_dir": str(root_dir),
        "uses_xlsx_export": False,
        "default_validation_tier": "tier_b_downstream_quality_snapshot",
        "overview_metrics": {key: _normalize_scalar(overview.get(key, "")) for key in OVERVIEW_KEYS},
        "source_balance": source_records,
        "promoted_personas": promoted_personas,
        "final_usable_personas": final_usable_personas,
        "source_actions": source_actions,
        "audit_snapshot": audit_snapshot,
    }


def build_validation_delta(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    """Compare two snapshots and classify metric/source movement."""
    current_overview = current.get("overview_metrics", {})
    baseline_overview = baseline.get("overview_metrics", {})
    metric_deltas = {
        key: _compare_metric(
            key=key,
            current_value=current_overview.get(key),
            baseline_value=baseline_overview.get(key),
        )
        for key in DELTA_METRIC_KEYS
    }

    baseline_sources = {
        str(row.get("source", "")): row
        for row in baseline.get("source_balance", [])
    }
    source_deltas = []
    for row in current.get("source_balance", []):
        source = str(row.get("source", ""))
        previous = baseline_sources.get(source, {})
        metric_changes = {
            key: _compare_metric(
                key=key,
                current_value=row.get(key),
                baseline_value=previous.get(key),
            )
            for key in DELTA_SOURCE_KEYS
        }
        source_deltas.append(
            {
                "source": source,
                "changes": metric_changes,
                "regressed": any(change["classification"] == "regressed" for change in metric_changes.values()),
            }
        )

    return {
        "baseline_path": str(baseline.get("_snapshot_path", "")),
        "summary": {
            "regressed": any(item["classification"] == "regressed" for item in metric_deltas.values())
            or any(row["regressed"] for row in source_deltas),
            "improved": any(item["classification"] == "improved" for item in metric_deltas.values())
            or any(
                change["classification"] == "improved"
                for row in source_deltas
                for change in row["changes"].values()
            ),
        },
        "overview_metric_deltas": metric_deltas,
        "source_deltas": source_deltas,
    }


def render_snapshot_markdown(snapshot: dict[str, Any], delta: dict[str, Any] | None = None) -> str:
    """Render the snapshot as markdown for quick human review."""
    overview = snapshot.get("overview_metrics", {})
    lines = [
        "# Analysis Validation Snapshot",
        "",
        "## Summary",
        f"- readiness: `{overview.get('persona_readiness_state', '')}`",
        f"- overall status: `{overview.get('overall_status', '')}`",
        f"- quality flag: `{overview.get('quality_flag', '')}`",
        f"- weak source count: `{overview.get('weak_source_cost_center_count', '')}`",
        f"- final usable persona count: `{overview.get('final_usable_persona_count', '')}`",
        f"- top-3 cluster share: `{overview.get('top_3_cluster_share_of_core_labeled', '')}`",
        f"- largest source influence share: `{overview.get('largest_source_influence_share_pct', '')}`",
        "",
        "## Sources",
    ]
    for row in snapshot.get("source_balance", []):
        lines.append(
            "- `{source}`: collapse=`{collapse}`, prefiltered=`{pref}`, episode=`{episode}`, "
            "labelable=`{labelable}`, action=`{action}`".format(
                source=row.get("source", ""),
                collapse=row.get("collapse_stage", ""),
                pref=row.get("prefiltered_valid_post_count", ""),
                episode=row.get("episode_count", ""),
                labelable=row.get("labelable_episode_count", ""),
                action=row.get("policy_action", ""),
            )
        )
    lines.extend(["", "## Promoted Personas"])
    for row in snapshot.get("promoted_personas", []):
        lines.append(
            "- `{persona_id}` `{persona_name}`: share=`{share}`, grounding=`{grounding}`, examples=`{examples}`".format(
                persona_id=row.get("persona_id", ""),
                persona_name=row.get("persona_name", ""),
                share=row.get("share_of_core_labeled", ""),
                grounding=row.get("grounding_status", ""),
                examples=row.get("selected_example_count", ""),
            )
        )
    if delta:
        lines.extend(["", "## Delta"])
        for key, change in delta.get("overview_metric_deltas", {}).items():
            lines.append(
                f"- `{key}`: `{change['baseline_value']}` -> `{change['current_value']}` "
                f"({change['classification']})"
            )
    return "\n".join(lines) + "\n"


def _print_compact_summary(snapshot: dict[str, Any], delta: dict[str, Any] | None = None) -> None:
    """Print a compact console summary."""
    overview = snapshot.get("overview_metrics", {})
    print("ANALYSIS VALIDATION SNAPSHOT")
    print("")
    for key in OVERVIEW_KEYS:
        print(f"- {key}: {overview.get(key, '')}")
    print("")
    print("Source Summary")
    for row in snapshot.get("source_balance", []):
        print(
            "- {source}: collapse={collapse} prefiltered={pref} episode={episode} labelable={labelable} action={action}".format(
                source=row.get("source", ""),
                collapse=row.get("collapse_stage", ""),
                pref=row.get("prefiltered_valid_post_count", ""),
                episode=row.get("episode_count", ""),
                labelable=row.get("labelable_episode_count", ""),
                action=row.get("policy_action", ""),
            )
        )
    if delta:
        print("")
        print("Delta Summary")
        for key, change in delta.get("overview_metric_deltas", {}).items():
            print(f"- {key}: {change['baseline_value']} -> {change['current_value']} ({change['classification']})")


def _load_baseline_snapshot(args: argparse.Namespace) -> dict[str, Any] | None:
    """Load an optional baseline snapshot for delta comparison."""
    baseline_path = Path(args.baseline).expanduser() if str(args.baseline).strip() else None
    if baseline_path is None and args.compare_latest and DEFAULT_SNAPSHOT_PATH.exists():
        baseline_path = DEFAULT_SNAPSHOT_PATH
    if baseline_path is None or not baseline_path.exists():
        return None
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    baseline["_snapshot_path"] = str(baseline_path)
    return baseline


def _metric_lookup(df: pd.DataFrame, key_column: str, value_column: str) -> dict[str, Any]:
    """Return a scalar metric mapping from a two-column frame."""
    if df.empty or key_column not in df.columns or value_column not in df.columns:
        return {}
    return dict(zip(df[key_column].astype(str), df[value_column]))


def _frame_records(df: pd.DataFrame, columns: list[str], sort_by: list[str] | None = None) -> list[dict[str, Any]]:
    """Return normalized records from selected columns."""
    if df.empty:
        return []
    selected = [column for column in columns if column in df.columns]
    frame = df[selected].copy()
    if sort_by:
        valid_sort = [column for column in sort_by if column in frame.columns]
        if valid_sort:
            frame = frame.sort_values(valid_sort, ascending=True, na_position="last")
    return [
        {column: _normalize_scalar(value) for column, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _compare_metric(key: str, current_value: Any, baseline_value: Any) -> dict[str, Any]:
    """Compare one metric using the repository's preferred improvement direction."""
    current_num = _maybe_float(current_value)
    baseline_num = _maybe_float(baseline_value)
    direction = REGRESSION_DIRECTION.get(key, "lower_is_better")
    if current_num is None or baseline_num is None:
        classification = "unchanged" if current_value == baseline_value else "changed"
        delta_value: Any = None
    else:
        delta_value = round(current_num - baseline_num, 6)
        if abs(delta_value) < 1e-9:
            classification = "unchanged"
        elif direction == "higher_is_better":
            classification = "improved" if delta_value > 0 else "regressed"
        else:
            classification = "improved" if delta_value < 0 else "regressed"
    return {
        "baseline_value": _normalize_scalar(baseline_value),
        "current_value": _normalize_scalar(current_value),
        "delta": _normalize_scalar(delta_value),
        "classification": classification,
    }


def _has_regression(delta: dict[str, Any]) -> bool:
    """Return whether the computed delta contains any regression."""
    if not delta:
        return False
    if delta.get("summary", {}).get("regressed", False):
        return True
    return False


def _maybe_float(value: Any) -> float | None:
    """Convert scalar-like values to float when possible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_scalar(value: Any) -> Any:
    """Normalize pandas/numpy scalars into JSON-safe builtins."""
    if pd.isna(value):
        return ""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lower() in {"true", "false"}:
            return stripped.lower() == "true"
        maybe_num = _maybe_float(stripped)
        if maybe_num is not None:
            if float(maybe_num).is_integer():
                return int(maybe_num)
            return round(float(maybe_num), 6)
        return value
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, float):
        return round(value, 6)
    return value


def _coerce_bool(value: Any) -> bool:
    """Interpret workbook-like boolean values safely."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _ensure_bundle_contract(root_dir: Path) -> None:
    """Raise a clear error when canonical workbook bundle inputs are missing."""
    if not workbook_bundle_exists(root_dir):
        missing = [sheet for sheet in WORKBOOK_SHEET_NAMES if not (root_dir / "data" / "analysis" / "workbook_bundle" / f"{sheet}.parquet").exists()]
        raise FileNotFoundError(
            "Missing canonical workbook bundle inputs for analysis snapshot: "
            + ", ".join(missing)
        )


if __name__ == "__main__":
    main()
