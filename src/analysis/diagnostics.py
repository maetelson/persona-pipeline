"""Source-level diagnostics and workbook quality gates."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.quality_status import QUALITY_STATUS_POLICY, flatten_quality_status_result
from src.utils.io import load_yaml, read_parquet
from src.utils.source_registry import load_enabled_source_ids
from src.utils.pipeline_schema import (
    CLUSTER_DOMINANCE_SHARE_PCT,
    DENOMINATOR_EPISODE_ROWS,
    DENOMINATOR_LABELED_EPISODE_ROWS,
    DENOMINATOR_NORMALIZED_POST_ROWS,
    DENOMINATOR_PERSONA_CORE_LABELED_ROWS,
    DENOMINATOR_PREFILTERED_VALID_ROWS,
    DENOMINATOR_RAW_RECORD_ROWS,
    DENOMINATOR_VALID_CANDIDATE_ROWS,
    LABELABLE_STATUSES,
    PIPELINE_STAGE_DEFINITIONS,
    QUALITY_FLAG_EXPLORATORY,
    QUALITY_FLAG_OK,
    QUALITY_FLAG_UNSTABLE,
    RAW_WITHOUT_LABEL_FAILURE_SOURCES,
    SOURCE_FIELD,
    aggregated_source_count,
    canonical_source_name,
    is_single_cluster_dominant,
    persona_min_cluster_size,
    round_pct,
    source_row_count,
)


def count_raw_jsonl_by_source(root_dir: Path) -> pd.DataFrame:
    """Count raw records by source, preferring staged raw_audit counts when available."""
    raw_root = root_dir / "data" / "raw"
    rows: list[dict[str, Any]] = []
    if raw_root.exists():
        for source_dir in sorted(path for path in raw_root.iterdir() if path.is_dir()):
            files = sorted(source_dir.glob("*.jsonl"))
            rows.append(
                {
                    "source": source_dir.name,
                    "raw_count": sum(_count_jsonl_lines(path) for path in files),
                    "raw_file_count": len(files),
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        grouped = pd.DataFrame(columns=["source", "raw_count", "raw_file_count"])
    else:
        frame["source"] = frame["source"].map(canonical_source_name)
        grouped = (
            frame.groupby("source", dropna=False, as_index=False)[["raw_count", "raw_file_count"]]
            .sum()
            .sort_values("source")
            .reset_index(drop=True)
        )
    raw_audit_path = root_dir / "data" / "analysis" / "raw_audit.parquet"
    if not raw_audit_path.exists():
        return grouped
    raw_audit_df = read_parquet(raw_audit_path)
    if raw_audit_df.empty or "source" not in raw_audit_df.columns:
        return grouped
    audit_grouped = (
        raw_audit_df.assign(source=raw_audit_df["source"].astype(str).map(canonical_source_name))
        .groupby("source", dropna=False, as_index=False)[["raw_record_count"]]
        .sum()
        .rename(columns={"raw_record_count": "raw_count"})
    )
    if grouped.empty:
        audit_grouped["raw_file_count"] = 0
        return audit_grouped.sort_values("source").reset_index(drop=True)
    merged = grouped.merge(audit_grouped, on="source", how="outer", suffixes=("", "_audit"))
    merged["raw_count"] = merged[["raw_count", "raw_count_audit"]].fillna(0).max(axis=1)
    merged["raw_file_count"] = merged["raw_file_count"].fillna(0).astype(int)
    return merged[["source", "raw_count", "raw_file_count"]].sort_values("source").reset_index(drop=True)


def build_metric_glossary() -> pd.DataFrame:
    """Build explicit metric definitions for workbook readers."""
    rows = [
        *[(metric, metric, str(payload.get("definition", "") or "")) for metric, payload in PIPELINE_STAGE_DEFINITIONS.items()],
        ("persona_core_labeled_rows", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Labeled episode rows with persona_core_eligible=true; this is the clustering denominator for persona shares."),
        ("promoted_candidate_persona_count", "persona_cluster_rows", "Count of clusters that passed the base size and dominance promotion gate before grounding review."),
        ("promotion_visibility_persona_count", "persona_cluster_rows", "Count of promoted personas that remain visible in the workbook for reviewer inspection after grounding policy merge. Under the current flag policy this includes grounded, weakly grounded, and ungrounded promoted personas."),
        ("headline_persona_count", "persona_cluster_rows", "Headline persona count shown to workbook readers. This must equal final_usable_persona_count so review-visible unsupported personas never inflate the apparent usable persona total."),
        ("final_usable_persona_count", "persona_cluster_rows", "Count of final usable personas for downstream reporting. Under the current policy this includes only structurally supported and grounded promoted personas, not structurally weak, weakly grounded, or ungrounded review-visible personas."),
        ("deck_ready_persona_count", "persona_cluster_rows", "Count of personas safe to present as deck-ready headline personas under the current workbook readiness gate. This is zero whenever persona_readiness_state is below deck_ready."),
        ("persona_readiness_state", "explicit_metric_value", "Workbook-level readiness state derived from policy-backed thresholds across overall unknown rate, persona-core coverage, promoted persona grounding coverage, final usable persona count, source influence concentration, and fragile tail share."),
        ("persona_readiness_label", "explicit_metric_value", "Reviewer-facing interpretation label for the current workbook state: Hypothesis Material, Reviewable Draft, or Final Persona Asset."),
        ("persona_asset_class", "explicit_metric_value", "Workbook asset class derived from persona_readiness_state. Below deck_ready this will never be final_persona_asset."),
        ("persona_readiness_gate_status", "explicit_metric_value", "Gate result for final persona claims: FAIL for exploratory_only, WARN for reviewable_but_not_deck_ready, and OK for deck_ready or production_persona_ready."),
        ("persona_completion_claim_allowed", "explicit_metric_value", "Boolean flag showing whether the workbook is allowed to present itself as a completed persona asset. This is true only for deck_ready and production_persona_ready."),
        ("persona_usage_restriction", "explicit_metric_value", "Explicit usage restriction text derived from persona_readiness_state so the workbook cannot be misread as final when blocked by readiness policy."),
        ("persona_readiness_blockers", "explicit_metric_value", "Pipe-delimited readiness thresholds not yet met for the next readiness tier."),
        ("persona_readiness_rule", "explicit_metric_value", "Single-line description of the current readiness policy tiers and the explicit thresholds for overall unknown, core coverage, grounding coverage, final usable count, source concentration, and tail fragility."),
        ("exploratory_bucket_count", "exploratory_cluster_rows", "Count of non-promoted exploratory clusters shown for context."),
        ("persona_core_unknown_ratio", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Ratio of persona-core labeled rows that still contain unresolved core label families."),
        ("overall_unknown_ratio", DENOMINATOR_LABELED_EPISODE_ROWS, "Ratio of all labeled rows with unresolved core label families, including rows outside persona-core clustering."),
        ("persona_core_coverage_of_all_labeled_pct", DENOMINATOR_LABELED_EPISODE_ROWS, "Percentage of all labeled rows that remain inside the persona-core subset used for clustering."),
        ("effective_labeled_source_count", "effective_labeled_source_count", "Effective count of contributing labeled sources after fractional down-weighting for very small labeled-source volumes. This is a source-count metric, not a row-count metric."),
        ("effective_balanced_source_count", "source_count", "Effective downstream source count after blending labeled share with persona-normalized promoted and grounded contribution shares. Promoted and grounded influence cap one source at the workbook source-influence ceiling within each persona before aggregation so one dominant source does not get counted as full downstream control across the same persona multiple times."),
        ("largest_cluster_share_of_core_labeled", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Largest promoted or exploratory persona cluster share over persona-core labeled rows."),
        ("top_3_cluster_share_of_core_labeled", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Combined share of the three largest persona clusters over persona-core labeled rows; high values indicate concentration even when the top cluster alone is below the dominance threshold."),
        ("robust_cluster_count", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Count of final clusters remaining after the robustness merge policy absorbs fragile adjacent fragments and collapses only the smallest residual buckets."),
        ("stable_cluster_count", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Count of final clusters that meet the configured size or share threshold for structural stability."),
        ("fragile_cluster_count", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Count of final clusters that remain above micro-cluster size but below the configured stability threshold."),
        ("micro_cluster_count", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Count of final clusters that remain at micro scale after robustness merging; this should usually stay near zero."),
        ("thin_evidence_cluster_count", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Count of final clusters whose cohesion and separation still do not clear the evidence sufficiency floors."),
        ("structurally_supported_cluster_count", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Count of final clusters that clear both stability and evidence sufficiency, meaning they have standalone structural support."),
        ("weak_separation_cluster_count", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Count of final clusters whose nearest-neighbor separation remains below the weak-separation floor, indicating adjacent personas may still be under-merged."),
        ("fragile_tail_cluster_count", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Count of final clusters that remain fragile or micro after robustness merging; this is the residual tail that should not be treated as mature personas."),
        ("fragile_tail_share_of_core_labeled", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Combined share of persona-core labeled rows held by fragile or micro tail clusters after robustness merging."),
        ("avg_cluster_separation", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Average nearest-neighbor separation across final clusters; higher values indicate stronger distinctiveness between adjacent personas."),
        ("min_cluster_separation", DENOMINATOR_PERSONA_CORE_LABELED_ROWS, "Lowest nearest-neighbor separation across final clusters; low values indicate at least one pair of personas may still be weakly separated."),
        ("largest_labeled_source_share_pct", DENOMINATOR_LABELED_EPISODE_ROWS, "Largest source contribution share over all labeled episode rows."),
        ("largest_promoted_source_share_pct", "promoted_cluster_rows", "Largest source contribution share over promoted persona episode contribution rows."),
        ("largest_grounded_source_share_pct", "grounded_persona_rows", "Largest source contribution share over grounded final-usable persona episode contribution rows."),
        ("largest_source_influence_share_pct", "source_count", "Largest blended downstream influence share after averaging raw labeled share with persona-normalized promoted and grounded contribution shares per source. Promoted and grounded influence contributions are capped at the workbook source-influence ceiling within each persona before cross-persona aggregation."),
        ("weak_source_cost_center_count", "source_count", "Count of high-input sources that still collapse before they become meaningful downstream contributors under the source-balance policy."),
        ("weak_source_cost_centers", "source_count", "Pipe-delimited source ids classified as weak-source cost centers under the source-balance policy."),
        ("promoted_persona_example_coverage_pct", "promoted_persona_rows", "Percentage of promoted personas that have any accepted grounding state, including weakly grounded personas."),
        ("promoted_persona_grounded_count", "promoted_persona_rows", "Count of promoted personas with at least one grounded representative example. This is a grounding metric, not a final-usability metric, so structurally weak grounded personas can contribute here without counting as final usable personas."),
        ("promoted_persona_weakly_grounded_count", "promoted_persona_rows", "Count of promoted personas whose evidence only meets weak fallback policy, not normal grounded selection."),
        ("promoted_persona_ungrounded_count", "promoted_persona_rows", "Count of promoted personas with no acceptable grounding evidence under policy. These can remain workbook-visible for review without counting as final usable personas."),
        ("promoted_persona_grounding_failure_count", "promoted_persona_rows", "Count of promoted personas that are not fully grounded for downstream reporting. Under the current policy this equals promoted_persona_weakly_grounded_count + promoted_persona_ungrounded_count and should not be confused with example-row issue counts."),
        ("promoted_personas_missing_examples", "promoted_persona_rows", "Pipe-delimited promoted persona ids with no accepted selected example rows."),
        ("promoted_personas_weakly_grounded", "promoted_persona_rows", "Pipe-delimited promoted persona ids that remain visible only with weak grounding coverage."),
        ("selected_example_grounding_issue_count", "persona_example_rows", "Count of selected example rows whose evidence is weak, fallback-only, reject-like, or otherwise degraded. This is example-level and can be zero even when persona-level grounding coverage fails."),
        ("promotion_status", "persona_cluster_row", "Final workbook promotion label after applying grounding policy merge. Grounded personas remain promoted_persona; weak or ungrounded visible personas are labeled review_visible_persona."),
        ("base_promotion_status", "persona_cluster_row", "Size and dominance based promotion label before grounding policy is applied. Promoted candidates use the explicit term promoted_candidate_persona."),
        ("structural_support_status", "persona_cluster_row", "Structural maturity label for one persona cluster before grounding merge. structurally_supported means the cluster clears stability and evidence sufficiency; review_visible_only means it stays workbook-visible but must not be treated as a mature persona."),
        ("structural_support_reason", "persona_cluster_row", "Reviewer-facing explanation for why a cluster is structurally supported or blocked, such as low separation or residual fragility."),
        ("visibility_state", "persona_cluster_row", "Explicit visibility state for one persona after policy merge. Promoted candidates that remain visible but unsupported use review_visible_persona."),
        ("usability_state", "persona_cluster_row", "Explicit downstream usability state. Only final_usable_persona rows count toward the headline persona total."),
        ("deck_readiness_state", "persona_cluster_row", "Explicit presentation-readiness state after workbook gating. Below deck_ready, rows must not present themselves as deck-ready even when they remain analytically usable."),
        ("promotion_action", "persona_cluster_row", "Recommended reviewer action after grounding merge: remain_promoted, remain_review_visible, promotion_candidate_pending_review, or downgraded_to_exploratory."),
        ("promoted_candidate_persona", "persona_cluster_row", "Boolean flag showing whether a row passed the base promotion gate before grounding review."),
        ("workbook_review_visible", "persona_cluster_row", "Boolean flag showing whether a row remains visible in the workbook's review set, including review_visible_persona rows."),
        ("final_usable_persona", "persona_cluster_row", "Boolean flag showing whether a persona is usable for downstream reporting under the current policy. This requires both structural support and acceptable grounding."),
        ("deck_ready_persona", "persona_cluster_row", "Boolean flag showing whether a persona is safe for deck-ready headline reporting under the current policy. Under the current policy this matches final_usable_persona."),
        ("reporting_readiness_status", "persona_cluster_row", "Reviewer-facing readiness class: deck_ready_persona, final_usable_persona, grounded_but_structurally_weak, promoted_but_weakly_grounded, promoted_but_ungrounded, review_visible_persona, promoted_candidate_persona, or not_final_usable."),
        ("grounding_status", "persona_cluster_row", "Reviewer-facing grounding state for a persona: grounded_single, grounded_bundle, weak_bundle, ungrounded, or not_applicable."),
        ("promotion_grounding_status", "persona_cluster_row", "Combined promotion-grounding state such as promoted_and_grounded, grounded_but_structurally_weak, or promoted_but_ungrounded."),
        ("context_evidence_count", "persona_cluster_row", "Count of distinct episode rows contributing recurring job-context evidence to the persona grounding bundle."),
        ("workaround_evidence_count", "persona_cluster_row", "Count of distinct episode rows contributing repeated bottleneck or workaround evidence to the persona grounding bundle."),
        ("trust_validation_evidence_count", "persona_cluster_row", "Count of distinct episode rows contributing trust, validation, or output-pressure evidence to the persona grounding bundle."),
        ("bundle_episode_count", "persona_cluster_row", "Distinct episode count across all accepted bundle-grounding evidence dimensions."),
        ("total_bundle_strength", "persona_cluster_row", "Additive strength of the grounding bundle, computed from context, workaround, and trust/output evidence counts."),
        ("bundle_grounding_status", "persona_cluster_row", "Bundle-only grounding outcome before final promotion merge: grounded_bundle, weak_bundle, or ungrounded."),
        ("bundle_grounding_reason", "persona_cluster_row", "Reviewer-facing explanation of exactly which evidence dimensions did or did not clear bundle-grounding policy."),
        ("selection_strength", "persona_example_row", "Workbook-facing selection label for an example row, distinguishing grounded selections from weak_grounding_fallback rows."),
        ("grounding_strength", "persona_example_row", "Policy bucket for one example candidate: strong, grounded, weak, or unacceptable."),
        ("coverage_selection_reason", "persona_example_row", "Why a row was selected: normal score_plus_diversity_policy or explicit minimum coverage policy."),
        ("grounding_fit_score", "persona_example_row", "Selection-time score component that rewards axis fit and penalizes mismatches for grounding quality."),
        ("top_failure_reason", "source_diagnostic_reason", "Top ranked source_diagnostics bottleneck reason. Values identify the strongest source-specific issue after comparing stage retention, episode yield, labelability, grounding contribution, concentration risk, and diversity contribution."),
        ("primary_collapse_stage", "source_diagnostic_reason", "Primary pipeline stage where the source collapses under current diagnostics policy."),
        ("recommended_action", "source_diagnostic_reason", "Single workbook-facing next action for the source after combining failure reason, concentration risk, and weak-source policy."),
        ("priority_tier", "source_diagnostic_reason", "Workbook-facing action priority tier for the source: fix_now, tune_soon, or monitor."),
        ("severity", "source_diagnostic_reason", "Workbook-facing severity for the current top source issue."),
        ("source_balance_status", "source_diagnostic_reason", "Source-balance classification summarizing whether the source is healthy, overdominant, weak, or on a watchlist."),
        ("weak_source_cost_center_status", "source_diagnostic_reason", "Explicit flag showing whether the source is currently treated as a weak-source cost center."),
        ("dominant_invalid_reason", "source_diagnostic_reason", "Most frequent invalid_reason observed for the source in invalid_candidates_with_prefilter.parquet. This is diagnostic context, not a funnel metric."),
        ("dominant_prefilter_reason", "source_diagnostic_reason", "Most frequent prefilter_reason observed for the source in relevance_drop.parquet. This is diagnostic context, not a funnel metric."),
        ("valid_retention_reason", "source_diagnostic_reason", "Post-funnel diagnosis for normalized_post_count to valid_post_count retention. Uses low_valid_post_retention with the dominant invalid reason when the stage is weak, otherwise a healthy_* status."),
        ("prefilter_retention_reason", "source_diagnostic_reason", "Post-funnel diagnosis for valid_post_count to prefiltered_valid_post_count retention. Uses low_prefilter_retention with the dominant prefilter reason when the stage is weak, otherwise a healthy_* status."),
        ("episode_yield_reason", "source_diagnostic_reason", "Cross-grain diagnosis for episode_count relative to prefiltered_valid_post_count. Uses low_episode_yield when retained posts do not produce enough episodes."),
        ("labelable_coverage_reason", "source_diagnostic_reason", "Episode-funnel diagnosis for labelable_episode_count relative to labeled_episode_count. Uses low_labelable_episode_ratio or label_output_missing_after_episode_build when labelability coverage is weak."),
        ("grounding_contribution_reason", "source_diagnostic_reason", "Source-level diagnosis for promoted persona contribution. Uses grounding_contribution_absent when labeled evidence exists but the source contributes no promoted persona episodes, and grounded_persona_contribution_absent when contribution stops at review-visible personas without final usable grounding."),
        ("concentration_risk_reason", "source_diagnostic_reason", "Source-level diagnosis for labeled, promoted, or grounded share concentration. Uses overconcentration_risk when one source contributes an outsized share of downstream evidence."),
        ("diversity_contribution_reason", "source_diagnostic_reason", "Source-level diagnosis for effective labeled-source diversity. Uses weak_diversity_contribution when the source contributes only a fractional diversity score."),
        ("recommended_seed_intervention", "source_diagnostic_intervention", "Optional source_diagnostics intervention payload. Populated only when the top issue is a relevance-prefilter bottleneck and an active local seed set exists for that source."),
        ("row_kind", "workbook_column_label", "Display-only export label that separates source_diagnostics metric rows from diagnostic rows."),
        ("diagnostic_level", "source_diagnostic_level", "Severity attached to a source_diagnostics diagnostic row: failure, warning, or pass. Metric rows keep this blank."),
        ("row_grain", "workbook_column_label", "Display-only export label for grain. In source_diagnostics this tells the reviewer whether a row is post-, episode-, or mixed-grain."),
        ("labelable_count", "labelability_rows", "Episode rows with labelability_status in labelable or borderline."),
        ("core_labeled", "persona_core_eligible_rows", "Labeled rows with persona_core_eligible=true, used for persona clustering."),
        ("promoted_to_persona_count", "promoted_cluster_rows", "Source rows assigned to clusters that pass persona promotion gates."),
        ("raw_record_count", DENOMINATOR_RAW_RECORD_ROWS, "Source-level raw JSONL record count in source_diagnostics; grain is raw record rows, not a count of sources and not normalized post rows."),
        ("normalized_post_count", DENOMINATOR_NORMALIZED_POST_ROWS, "Source-level normalized post count after source normalizers; grain is normalized post rows."),
        ("valid_post_count", DENOMINATOR_NORMALIZED_POST_ROWS, "Source-level valid post count after invalid filtering; numerator grain and denominator grain are normalized post rows."),
        ("prefiltered_valid_post_count", DENOMINATOR_VALID_CANDIDATE_ROWS, "Source-level post count retained by the relevance prefilter; numerator grain and denominator grain are valid post rows."),
        ("valid_posts_per_normalized_post_pct", DENOMINATOR_NORMALIZED_POST_ROWS, "Same-grain post funnel percentage: valid_post_count / normalized_post_count * 100."),
        ("prefiltered_valid_posts_per_valid_post_pct", DENOMINATOR_VALID_CANDIDATE_ROWS, "Same-grain post funnel percentage: prefiltered_valid_post_count / valid_post_count * 100."),
        ("episode_count", DENOMINATOR_EPISODE_ROWS, "Source-level episode count in episode_table.parquet; one prefiltered post can yield zero or more episodes."),
        ("labeled_episode_count", DENOMINATOR_EPISODE_ROWS, "Source-level episode count that appears in labeled_episodes.parquet; same grain as episode_count."),
        ("labelable_episode_count", DENOMINATOR_LABELED_EPISODE_ROWS, "Source-level labeled episode count whose labelability_status is labelable or borderline; same grain as labeled_episode_count."),
        ("labeled_episodes_per_episode_pct", DENOMINATOR_EPISODE_ROWS, "Same-grain episode funnel percentage: labeled_episode_count / episode_count * 100."),
        ("labelable_episodes_per_labeled_episode_pct", DENOMINATOR_LABELED_EPISODE_ROWS, "Same-grain episode funnel percentage: labelable_episode_count / labeled_episode_count * 100."),
        ("episodes_per_prefiltered_valid_post", DENOMINATOR_PREFILTERED_VALID_ROWS, "Cross-grain bridge metric: episode_count / prefiltered_valid_post_count. This can exceed 1.0 and is not a funnel rate."),
        ("labeled_episodes_per_prefiltered_valid_post", DENOMINATOR_PREFILTERED_VALID_ROWS, "Cross-grain bridge metric: labeled_episode_count / prefiltered_valid_post_count. This can exceed 1.0 and is not a funnel rate."),
        ("labelable_episodes_per_prefiltered_valid_post", DENOMINATOR_PREFILTERED_VALID_ROWS, "Cross-grain bridge metric: labelable_episode_count / prefiltered_valid_post_count. This can exceed 1.0 and is not a funnel rate."),
        ("effective_diversity_contribution", "source_quality_score", "Weak source-diversity contribution capped at 1.0 from labeled episode volume; this is a source-quality score, not a funnel metric."),
        ("promoted_persona_episode_count", "promoted_cluster_rows", "Source-level episode count assigned to promoted personas; grain is promoted persona assignment rows."),
        ("grounded_promoted_persona_episode_count", "grounded_persona_rows", "Source-level episode count assigned to final usable grounded personas; grain is grounded persona assignment rows."),
    ]
    return pd.DataFrame(rows, columns=["metric", "denominator_type", "definition"])


def build_source_stage_counts(
    root_dir: Path,
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build source-level counts before workbook formatting."""
    raw_counts_df = count_raw_jsonl_by_source(root_dir)
    prefiltered_df = read_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    labelability_df = read_parquet(root_dir / "data" / "labeled" / "labelability_audit.parquet")
    relevance_drop_df = read_parquet(root_dir / "data" / "prefilter" / "relevance_drop.parquet")
    invalid_with_prefilter_df = read_parquet(root_dir / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")

    episode_source = _episode_source_lookup(episodes_df)
    labeled_with_source = _with_episode_source(labeled_df, episode_source)
    labelable_df = labelability_df[labelability_df.get("labelability_status", pd.Series(dtype=str)).astype(str).isin(LABELABLE_STATUSES)]
    labelable_with_source = _with_episode_source(labelable_df, episode_source)
    workbook_review_visible = cluster_stats_df.get("workbook_review_visible", pd.Series(dtype=bool))
    if workbook_review_visible.empty:
        promoted_mask = cluster_stats_df.get("promotion_status", pd.Series(dtype=str)).astype(str).isin({"promoted_persona", "review_visible_persona"})
    else:
        promoted_mask = workbook_review_visible.fillna(False).astype(bool)
    promoted_ids = set(
        cluster_stats_df[
            promoted_mask
        ]
        .get("persona_id", pd.Series(dtype=str))
        .astype(str)
        .tolist()
    )
    promoted_assignments = persona_assignments_df[persona_assignments_df.get("persona_id", pd.Series(dtype=str)).astype(str).isin(promoted_ids)]
    promoted_with_source = _with_episode_source(promoted_assignments, episode_source)
    normalized_promoted_contributions = _persona_capped_source_contributions(promoted_with_source, _source_influence_cap_pct())
    final_usable_series = cluster_stats_df.get("final_usable_persona", pd.Series(dtype=bool))
    if final_usable_series.empty:
        promotion_grounding_status = cluster_stats_df.get(
            "promotion_grounding_status",
            pd.Series("", index=cluster_stats_df.index, dtype=str),
        )
        grounded_mask = promotion_grounding_status.astype(str).eq("promoted_and_grounded")
    else:
        grounded_mask = final_usable_series.fillna(False).astype(bool)
    grounded_ids = set(
        cluster_stats_df[
            grounded_mask
        ]
        .get("persona_id", pd.Series(dtype=str))
        .astype(str)
        .tolist()
    )
    grounded_assignments = persona_assignments_df[persona_assignments_df.get("persona_id", pd.Series(dtype=str)).astype(str).isin(grounded_ids)]
    grounded_with_source = _with_episode_source(grounded_assignments, episode_source)
    normalized_grounded_contributions = _persona_capped_source_contributions(grounded_with_source, _source_influence_cap_pct())

    sources = sorted(
        _workbook_visible_sources(
            root_dir=root_dir,
            source_sets=[
                set(normalized_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name)),
                set(valid_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name)),
                set(episodes_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name)),
                set(prefiltered_df.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name)),
                set(labelable_with_source.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name)),
                set(labeled_with_source.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name)),
                set(promoted_with_source.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name)),
                set(grounded_with_source.get("source", pd.Series(dtype=str)).astype(str).map(canonical_source_name)),
            ],
        )
    )
    rows: list[dict[str, Any]] = []
    for source in sources:
        normalized_count = source_row_count(normalized_df, source)
        raw_count = _source_count(raw_counts_df, source, "raw_count")
        # Partial reruns can leave raw_audit/jsonl coverage stale for a subset of
        # sources while normalized downstream artifacts still exist. Use the
        # observed normalized floor so source diagnostics stay internally
        # consistent instead of aborting analysis on a reporting-only mismatch.
        raw_count = max(raw_count, normalized_count)
        valid_count = source_row_count(valid_df, source)
        prefiltered_count = source_row_count(prefiltered_df, source)
        # Some historical valid_candidates artifacts preserve downstream rows
        # while dropping source fidelity on the broader valid set. Keep the
        # invariant valid>=prefiltered by using the observed downstream floor.
        valid_count = max(valid_count, prefiltered_count)
        normalized_count = max(normalized_count, valid_count)
        raw_count = max(raw_count, normalized_count)
        episode_count = source_row_count(episodes_df, source)
        labelable_count = source_row_count(labelable_with_source, source)
        labeled_count = source_row_count(labeled_with_source, source)
        promoted_count = source_row_count(promoted_with_source, source)
        grounded_count = source_row_count(grounded_with_source, source)
        rows.append(
            {
                "source": source,
                "raw_record_count": raw_count,
                "normalized_post_count": normalized_count,
                "valid_post_count": valid_count,
                "prefiltered_valid_post_count": prefiltered_count,
                "episode_count": episode_count,
                "labeled_episode_count": labeled_count,
                "labelable_episode_count": labelable_count,
                "effective_diversity_contribution": _effective_source_contribution(labeled_count),
                "promoted_persona_episode_count": promoted_count,
                "grounded_promoted_persona_episode_count": grounded_count,
                "source_normalized_promoted_persona_contribution": round(float(normalized_promoted_contributions.get(source, 0.0) or 0.0), 4),
                "source_normalized_grounded_persona_contribution": round(float(normalized_grounded_contributions.get(source, 0.0) or 0.0), 4),
                "dominant_invalid_reason": "reason_unavailable",
                "dominant_prefilter_reason": "reason_unavailable",
                "valid_retention_reason": "healthy_valid_post_retention",
                "valid_retention_level": "pass",
                "prefilter_retention_reason": "healthy_prefilter_retention",
                "prefilter_retention_level": "pass",
                "episode_yield_reason": "healthy_episode_yield",
                "episode_yield_level": "pass",
                "labelable_coverage_reason": "healthy_labelable_coverage",
                "labelable_coverage_level": "pass",
                "grounding_contribution_reason": "healthy_grounding_contribution",
                "grounding_contribution_level": "pass",
                "concentration_risk_reason": "concentration_risk_clear",
                "concentration_risk_level": "pass",
                "diversity_contribution_reason": "strong_diversity_contribution",
                "diversity_contribution_level": "pass",
                "failure_reason_top": "",
                "failure_level": "pass",
                "recommended_seed_set": "",
                "metric_glossary_note": "See metric_glossary for source_diagnostics metric definitions and row grains.",
            }
        )
    result = pd.DataFrame(rows)
    result = _apply_source_diagnostic_rules(
        result,
        root_dir=root_dir,
        relevance_drop_df=relevance_drop_df,
        invalid_with_prefilter_df=invalid_with_prefilter_df,
    )
    _validate_source_stage_counts(result)
    return result


def _workbook_visible_sources(root_dir: Path, source_sets: list[set[str]]) -> set[str]:
    """Return workbook-visible sources from enabled configs with downstream evidence."""
    evidence_sources = {source for source_set in source_sets for source in source_set if str(source).strip()}
    enabled_sources = {canonical_source_name(source_id) for source_id in load_enabled_source_ids(root_dir)}
    return {source for source in evidence_sources if source in enabled_sources}


def build_source_diagnostics(source_stage_counts_df: pd.DataFrame) -> pd.DataFrame:
    """Build workbook-friendly source diagnostics with explicit grain and section typing."""
    if source_stage_counts_df.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "section",
                "row_kind",
                "grain",
                "metric_name",
                "metric_value",
                "metric_type",
                "denominator_metric",
                "denominator_grain",
                "denominator_value",
                "bounded_range",
                "is_same_grain_funnel",
                "diagnostic_level",
                "metric_definition",
            ]
        )
    definitions = {
        row["metric"]: row["definition"]
        for _, row in build_metric_glossary().iterrows()
        if str(row.get("metric", "")).strip()
    }
    rows: list[dict[str, Any]] = []
    for _, source_row in source_stage_counts_df.iterrows():
        source = str(source_row.get("source", "") or "")
        raw_record_count = int(source_row.get("raw_record_count", 0) or 0)
        normalized_post_count = int(source_row.get("normalized_post_count", 0) or 0)
        valid_post_count = int(source_row.get("valid_post_count", 0) or 0)
        prefiltered_valid_post_count = int(source_row.get("prefiltered_valid_post_count", 0) or 0)
        episode_count = int(source_row.get("episode_count", 0) or 0)
        labeled_episode_count = int(source_row.get("labeled_episode_count", 0) or 0)
        labelable_episode_count = int(source_row.get("labelable_episode_count", 0) or 0)
        grounded_promoted_persona_episode_count = int(source_row.get("grounded_promoted_persona_episode_count", 0) or 0)
        failure_reason_top = str(source_row.get("failure_reason_top", "") or "")
        failure_level = str(source_row.get("failure_level", "") or "")
        recommended_seed_set = str(source_row.get("recommended_seed_set", "") or "")
        labeled_share_pct = round_pct(labeled_episode_count, int(pd.to_numeric(source_stage_counts_df.get("labeled_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()))
        prefilter_retention_pct = round_pct(prefiltered_valid_post_count, valid_post_count) if valid_post_count else 0.0
        episode_yield_ratio = round(float(episode_count) / float(prefiltered_valid_post_count), 2) if prefiltered_valid_post_count else 0.0
        labelable_ratio_pct = round_pct(labelable_episode_count, labeled_episode_count) if labeled_episode_count else 0.0
        diagnostic_policy_row = pd.Series(
            {
                **source_row.to_dict(),
                "labeled_share_pct": labeled_share_pct,
                "prefiltered_valid_posts_per_valid_post_pct": prefilter_retention_pct,
                "episodes_per_prefiltered_valid_post": episode_yield_ratio,
                "labelable_episode_ratio_pct": labelable_ratio_pct,
            }
        )
        collapse_stage = _collapse_stage_for_reason(failure_reason_top)
        weak_source_cost_center = _is_weak_source_cost_center_row(diagnostic_policy_row)
        source_balance_status = _source_balance_status(
            pd.Series(
                {
                    **diagnostic_policy_row.to_dict(),
                    "weak_source_cost_center": weak_source_cost_center,
                }
            )
        )
        recommended_action = _source_balance_action(
            pd.Series(
                {
                    **diagnostic_policy_row.to_dict(),
                    "weak_source_cost_center": weak_source_cost_center,
                }
            )
        )
        priority_tier = _source_priority_tier(
            failure_level=failure_level,
            source_balance_status=source_balance_status,
            weak_source_cost_center=weak_source_cost_center,
        )

        metric_specs = [
            ("raw_ingest", "other", "raw_record_count", raw_record_count, "count", "", "", "", "", False),
            ("post_funnel", "post", "normalized_post_count", normalized_post_count, "count", "", "", "", "", True),
            ("post_funnel", "post", "valid_post_count", valid_post_count, "count", "", "", "", "", True),
            ("post_funnel", "post", "prefiltered_valid_post_count", prefiltered_valid_post_count, "count", "", "", "", "", True),
            (
                "post_funnel",
                "post",
                "valid_posts_per_normalized_post_pct",
                round_pct(valid_post_count, normalized_post_count) if normalized_post_count else 0.0,
                "percentage",
                "normalized_post_count",
                "post",
                normalized_post_count,
                "0-100_pct",
                True,
            ),
            (
                "post_funnel",
                "post",
                "prefiltered_valid_posts_per_valid_post_pct",
                round_pct(prefiltered_valid_post_count, valid_post_count) if valid_post_count else 0.0,
                "percentage",
                "valid_post_count",
                "post",
                valid_post_count,
                "0-100_pct",
                True,
            ),
            ("episode_funnel", "episode", "episode_count", episode_count, "count", "", "", "", "", True),
            ("episode_funnel", "episode", "labeled_episode_count", labeled_episode_count, "count", "", "", "", "", True),
            ("episode_funnel", "episode", "labelable_episode_count", labelable_episode_count, "count", "", "", "", "", True),
            (
                "episode_funnel",
                "episode",
                "labeled_episodes_per_episode_pct",
                round_pct(labeled_episode_count, episode_count) if episode_count else 0.0,
                "percentage",
                "episode_count",
                "episode",
                episode_count,
                "0-100_pct",
                True,
            ),
            (
                "episode_funnel",
                "episode",
                "labelable_episodes_per_labeled_episode_pct",
                round_pct(labelable_episode_count, labeled_episode_count) if labeled_episode_count else 0.0,
                "percentage",
                "labeled_episode_count",
                "episode",
                labeled_episode_count,
                "0-100_pct",
                True,
            ),
            (
                "cross_grain_bridge",
                "mixed_grain_bridge",
                "episodes_per_prefiltered_valid_post",
                round(float(episode_count) / float(prefiltered_valid_post_count), 2) if prefiltered_valid_post_count else 0.0,
                "ratio",
                "prefiltered_valid_post_count",
                "post",
                prefiltered_valid_post_count,
                "unbounded_ratio",
                False,
            ),
            (
                "cross_grain_bridge",
                "mixed_grain_bridge",
                "labeled_episodes_per_prefiltered_valid_post",
                round(float(labeled_episode_count) / float(prefiltered_valid_post_count), 2) if prefiltered_valid_post_count else 0.0,
                "ratio",
                "prefiltered_valid_post_count",
                "post",
                prefiltered_valid_post_count,
                "unbounded_ratio",
                False,
            ),
            (
                "cross_grain_bridge",
                "mixed_grain_bridge",
                "labelable_episodes_per_prefiltered_valid_post",
                round(float(labelable_episode_count) / float(prefiltered_valid_post_count), 2) if prefiltered_valid_post_count else 0.0,
                "ratio",
                "prefiltered_valid_post_count",
                "post",
                prefiltered_valid_post_count,
                "unbounded_ratio",
                False,
            ),
            (
                "source_quality",
                "other",
                "effective_diversity_contribution",
                round(float(source_row.get("effective_diversity_contribution", 0.0) or 0.0), 2),
                "ratio",
                "",
                "",
                "",
                "0-1_ratio",
                False,
            ),
            (
                "source_quality",
                "episode",
                "promoted_persona_episode_count",
                int(source_row.get("promoted_persona_episode_count", 0) or 0),
                "count",
                "",
                "",
                "",
                "",
                False,
            ),
            (
                "source_quality",
                "episode",
                "grounded_promoted_persona_episode_count",
                grounded_promoted_persona_episode_count,
                "count",
                "",
                "",
                "",
                "",
                False,
            ),
        ]
        for section, grain, metric_name, metric_value, metric_type, denominator_metric, denominator_grain, denominator_value, bounded_range, is_same_grain_funnel in metric_specs:
            rows.append(
                {
                    "source": source,
                    "priority_tier": priority_tier,
                    "primary_collapse_stage": collapse_stage,
                    "recommended_action": recommended_action,
                    "severity": failure_level or "pass",
                    "failure_reason_top": failure_reason_top,
                    "source_balance_status": source_balance_status,
                    "weak_source_cost_center": weak_source_cost_center,
                    "section": section,
                    "row_kind": "metric",
                    "grain": grain,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "metric_type": metric_type,
                    "denominator_metric": denominator_metric,
                    "denominator_grain": denominator_grain,
                    "denominator_value": denominator_value,
                    "bounded_range": bounded_range,
                    "is_same_grain_funnel": is_same_grain_funnel,
                    "diagnostic_level": "",
                    "metric_definition": definitions.get(metric_name, ""),
                }
            )
        diagnostic_specs = [
            ("diagnostic_reasons", "other", "top_failure_reason", failure_reason_top, "diagnostic_reason", "", "", "", "", False, failure_level),
            ("diagnostic_reasons", "other", "severity", failure_level or "pass", "diagnostic_reason", "", "", "", "", False, failure_level or "pass"),
            ("diagnostic_reasons", "other", "priority_tier", priority_tier, "diagnostic_reason", "", "", "", "", False, failure_level or "pass"),
            ("diagnostic_reasons", "other", "primary_collapse_stage", collapse_stage, "diagnostic_reason", "", "", "", "", False, failure_level or "pass"),
            ("diagnostic_reasons", "other", "recommended_action", recommended_action, "diagnostic_intervention", "", "", "", "", False, failure_level or "pass"),
            ("diagnostic_reasons", "other", "source_balance_status", source_balance_status, "diagnostic_reason", "", "", "", "", False, failure_level or "pass"),
            ("diagnostic_reasons", "other", "weak_source_cost_center_status", "weak_source_cost_center" if weak_source_cost_center else "not_weak_source_cost_center", "diagnostic_reason", "", "", "", "", False, "warning" if weak_source_cost_center else "pass"),
            ("diagnostic_reasons", "post", "dominant_invalid_reason", str(source_row.get("dominant_invalid_reason", "reason_unavailable") or "reason_unavailable"), "diagnostic_reason", "", "", "", "", False, "pass"),
            ("diagnostic_reasons", "post", "dominant_prefilter_reason", str(source_row.get("dominant_prefilter_reason", "reason_unavailable") or "reason_unavailable"), "diagnostic_reason", "", "", "", "", False, "pass"),
            ("diagnostic_reasons", "post", "valid_retention_reason", str(source_row.get("valid_retention_reason", "") or ""), "diagnostic_reason", "normalized_post_count", "post", normalized_post_count, "", False, str(source_row.get("valid_retention_level", "pass") or "pass")),
            ("diagnostic_reasons", "post", "prefilter_retention_reason", str(source_row.get("prefilter_retention_reason", "") or ""), "diagnostic_reason", "valid_post_count", "post", valid_post_count, "", False, str(source_row.get("prefilter_retention_level", "pass") or "pass")),
            ("diagnostic_reasons", "mixed_grain_bridge", "episode_yield_reason", str(source_row.get("episode_yield_reason", "") or ""), "diagnostic_reason", "prefiltered_valid_post_count", "post", prefiltered_valid_post_count, "", False, str(source_row.get("episode_yield_level", "pass") or "pass")),
            ("diagnostic_reasons", "episode", "labelable_coverage_reason", str(source_row.get("labelable_coverage_reason", "") or ""), "diagnostic_reason", "labeled_episode_count", "episode", labeled_episode_count, "", False, str(source_row.get("labelable_coverage_level", "pass") or "pass")),
            ("diagnostic_reasons", "episode", "grounding_contribution_reason", str(source_row.get("grounding_contribution_reason", "") or ""), "diagnostic_reason", "labeled_episode_count", "episode", labeled_episode_count, "", False, str(source_row.get("grounding_contribution_level", "pass") or "pass")),
            ("diagnostic_reasons", "episode", "concentration_risk_reason", str(source_row.get("concentration_risk_reason", "") or ""), "diagnostic_reason", "labeled_episode_count", "episode", labeled_episode_count, "", False, str(source_row.get("concentration_risk_level", "pass") or "pass")),
            ("diagnostic_reasons", "other", "diversity_contribution_reason", str(source_row.get("diversity_contribution_reason", "") or ""), "diagnostic_reason", "", "", "", "", False, str(source_row.get("diversity_contribution_level", "pass") or "pass")),
        ]
        if recommended_seed_set:
            diagnostic_specs.append(
                ("diagnostic_reasons", "other", "recommended_seed_intervention", recommended_seed_set, "diagnostic_intervention", "", "", "", "", False, failure_level)
            )
        for section, grain, metric_name, metric_value, metric_type, denominator_metric, denominator_grain, denominator_value, bounded_range, is_same_grain_funnel, diagnostic_level in diagnostic_specs:
            rows.append(
                {
                    "source": source,
                    "priority_tier": priority_tier,
                    "primary_collapse_stage": collapse_stage,
                    "recommended_action": recommended_action,
                    "severity": failure_level or "pass",
                    "failure_reason_top": failure_reason_top,
                    "source_balance_status": source_balance_status,
                    "weak_source_cost_center": weak_source_cost_center,
                    "section": section,
                    "row_kind": "diagnostic",
                    "grain": grain,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "metric_type": metric_type,
                    "denominator_metric": denominator_metric,
                    "denominator_grain": denominator_grain,
                    "denominator_value": denominator_value,
                    "bounded_range": bounded_range,
                    "is_same_grain_funnel": is_same_grain_funnel,
                    "diagnostic_level": diagnostic_level,
                    "metric_definition": definitions.get(metric_name, ""),
                }
            )
    result = pd.DataFrame(rows)
    if not result.empty:
        row_order = {"diagnostic": 0, "metric": 1}
        section_order = {"diagnostic_reasons": 0, "raw_ingest": 1, "post_funnel": 2, "episode_funnel": 3, "cross_grain_bridge": 4, "source_quality": 5}
        metric_order = {
            "top_failure_reason": 0,
            "severity": 1,
            "priority_tier": 2,
            "primary_collapse_stage": 3,
            "recommended_action": 4,
            "source_balance_status": 5,
            "weak_source_cost_center_status": 6,
        }
        result["_row_order"] = result["row_kind"].astype(str).map(row_order).fillna(9)
        result["_section_order"] = result["section"].astype(str).map(section_order).fillna(9)
        result["_metric_order"] = result["metric_name"].astype(str).map(metric_order).fillna(99)
        result = (
            result.sort_values(["priority_tier", "severity", "source", "_row_order", "_section_order", "_metric_order", "metric_name"], ascending=[True, True, True, True, True, True, True])
            .drop(columns=["_row_order", "_section_order", "_metric_order"])
            .reset_index(drop=True)
        )
    _validate_source_diagnostics_frame(result)
    return result


def build_source_balance_audit(source_stage_counts_df: pd.DataFrame) -> pd.DataFrame:
    """Build an auditable per-source balance table with collapse stages and policy actions."""
    if source_stage_counts_df.empty:
        return pd.DataFrame()
    frame = source_stage_counts_df.copy()
    labeled_total = int(pd.to_numeric(frame.get("labeled_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).sum())
    promoted_contribution_column = "source_normalized_promoted_persona_contribution" if "source_normalized_promoted_persona_contribution" in frame.columns else "promoted_persona_episode_count"
    grounded_contribution_column = "source_normalized_grounded_persona_contribution" if "source_normalized_grounded_persona_contribution" in frame.columns else "grounded_promoted_persona_episode_count"
    promoted_total = float(pd.to_numeric(frame.get(promoted_contribution_column, pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum())
    grounded_total = float(pd.to_numeric(frame.get(grounded_contribution_column, pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum())
    frame["labeled_share_pct"] = frame.apply(
        lambda row: round_pct(row.get("labeled_episode_count", 0), labeled_total) if labeled_total else 0.0,
        axis=1,
    )
    frame["promoted_influence_share_pct"] = frame.apply(
        lambda row: round_pct(row.get(promoted_contribution_column, 0), promoted_total) if promoted_total else 0.0,
        axis=1,
    )
    frame["grounded_influence_share_pct"] = frame.apply(
        lambda row: round_pct(row.get(grounded_contribution_column, 0), grounded_total) if grounded_total else 0.0,
        axis=1,
    )
    frame["valid_posts_per_normalized_post_pct"] = frame.apply(
        lambda row: round_pct(row.get("valid_post_count", 0), row.get("normalized_post_count", 0)) if int(row.get("normalized_post_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["prefiltered_valid_posts_per_valid_post_pct"] = frame.apply(
        lambda row: round_pct(row.get("prefiltered_valid_post_count", 0), row.get("valid_post_count", 0)) if int(row.get("valid_post_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["episodes_per_prefiltered_valid_post"] = frame.apply(
        lambda row: round(float(row.get("episode_count", 0) or 0) / float(row.get("prefiltered_valid_post_count", 0) or 1), 2)
        if int(row.get("prefiltered_valid_post_count", 0) or 0)
        else 0.0,
        axis=1,
    )
    frame["labelable_episode_ratio_pct"] = frame.apply(
        lambda row: round_pct(row.get("labelable_episode_count", 0), row.get("labeled_episode_count", 0)) if int(row.get("labeled_episode_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["blended_influence_share_pct"] = frame.apply(
        lambda row: round(
            sum(
                share
                for share, total in [
                    (float(row.get("labeled_share_pct", 0.0) or 0.0), labeled_total),
                    (float(row.get("promoted_influence_share_pct", 0.0) or 0.0), promoted_total),
                    (float(row.get("grounded_influence_share_pct", 0.0) or 0.0), grounded_total),
                ]
                if total > 0
            )
            / max(sum(1 for total in [labeled_total, promoted_total, grounded_total] if total > 0), 1),
            1,
        ),
        axis=1,
    )
    frame["collapse_stage"] = frame["failure_reason_top"].astype(str).map(_collapse_stage_for_reason)
    frame["weak_source_cost_center"] = frame.apply(_is_weak_source_cost_center_row, axis=1)
    frame["source_balance_status"] = frame.apply(_source_balance_status, axis=1)
    frame["policy_action"] = frame.apply(_source_balance_action, axis=1)
    frame["priority_tier"] = frame.apply(
        lambda row: _source_priority_tier(
            failure_level=str(row.get("failure_level", "") or ""),
            source_balance_status=str(row.get("source_balance_status", "") or ""),
            weak_source_cost_center=bool(row.get("weak_source_cost_center", False)),
        ),
        axis=1,
    )
    preferred = [
        "source",
        "raw_record_count",
        "valid_post_count",
        "prefiltered_valid_post_count",
        "episode_count",
        "labelable_episode_count",
        "labeled_episode_count",
        "promoted_persona_episode_count",
        "grounded_promoted_persona_episode_count",
        "labeled_share_pct",
        "source_normalized_promoted_persona_contribution",
        "source_normalized_grounded_persona_contribution",
        "promoted_influence_share_pct",
        "grounded_influence_share_pct",
        "blended_influence_share_pct",
        "valid_posts_per_normalized_post_pct",
        "prefiltered_valid_posts_per_valid_post_pct",
        "episodes_per_prefiltered_valid_post",
        "labelable_episode_ratio_pct",
        "collapse_stage",
        "failure_reason_top",
        "failure_level",
        "source_balance_status",
        "weak_source_cost_center",
        "policy_action",
        "priority_tier",
    ]
    available = [column for column in preferred if column in frame.columns]
    return frame[available].sort_values(["blended_influence_share_pct", "labeled_episode_count", "source"], ascending=[False, False, True]).reset_index(drop=True)


def build_weak_source_triage(source_balance_audit_df: pd.DataFrame) -> pd.DataFrame:
    """Build explicit weak-source triage actions with confidence labels."""
    if source_balance_audit_df.empty:
        return pd.DataFrame(
            columns=[
                "source",
                "collapse_stage",
                "failure_reason_top",
                "raw_record_count",
                "labeled_episode_count",
                "grounded_promoted_persona_episode_count",
                "blended_influence_share_pct",
                "triage_recommendation",
                "recommendation_confidence",
                "triage_rationale",
                "policy_action",
            ]
        )
    frame = source_balance_audit_df.copy()
    weak_mask = frame.get("weak_source_cost_center", pd.Series(dtype=bool)).fillna(False).astype(bool)
    weak = frame[weak_mask].copy()
    if weak.empty:
        return pd.DataFrame(columns=[
            "source",
            "collapse_stage",
            "failure_reason_top",
            "raw_record_count",
            "labeled_episode_count",
            "grounded_promoted_persona_episode_count",
            "blended_influence_share_pct",
            "triage_recommendation",
            "recommendation_confidence",
            "triage_rationale",
            "policy_action",
        ])
    triage = weak.apply(_triage_weak_source_row, axis=1, result_type="expand")
    for column in triage.columns:
        weak[column] = triage[column]
    preferred = [
        "source",
        "collapse_stage",
        "failure_reason_top",
        "raw_record_count",
        "labeled_episode_count",
        "grounded_promoted_persona_episode_count",
        "blended_influence_share_pct",
        "triage_recommendation",
        "recommendation_confidence",
        "triage_rationale",
        "policy_action",
    ]
    available = [column for column in preferred if column in weak.columns]
    return weak[available].sort_values(["recommendation_confidence", "raw_record_count", "source"], ascending=[True, False, True]).reset_index(drop=True)


def build_quality_failures(
    quality_checks: dict[str, Any],
    source_stage_counts_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
    persona_examples_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build quality failures from the evaluated quality result plus source failure rows."""
    labeled_sources = int((source_stage_counts_df.get("labeled_episode_count", pd.Series(dtype=int)) > 0).sum()) if not source_stage_counts_df.empty else 0
    effective_labeled_sources = _effective_labeled_source_count(source_stage_counts_df)
    effective_balanced_sources = float(quality_checks.get("effective_balanced_source_count", effective_labeled_sources) or effective_labeled_sources)
    raw_sources = int((source_stage_counts_df.get("raw_record_count", pd.Series(dtype=int)) > 0).sum()) if not source_stage_counts_df.empty else 0
    largest_share = _largest_cluster_share(cluster_stats_df)
    largest_source_influence_share = float(quality_checks.get("largest_source_influence_share_pct", 0.0) or 0.0)
    weak_source_cost_centers = int(quality_checks.get("weak_source_cost_center_count", 0) or 0)
    min_cluster_size = int(quality_checks.get("min_cluster_size", 0))
    small_promoted = _small_promoted_count(cluster_stats_df, min_cluster_size)

    rows = [
        _gate_row("overall_uncertainty_gate", _gate_level_from_status(str(quality_checks.get("overall_unknown_status", "OK"))), round(float(quality_checks.get("overall_unknown_ratio", 0.0) or 0.0), 6), str(quality_checks.get("overall_unknown_reason_keys", "") or "")),
        _gate_row("core_coverage_gate", _gate_level_from_status(str(quality_checks.get("core_coverage_status", "OK"))), round(float(quality_checks.get("persona_core_coverage_of_all_labeled_pct", 0.0) or 0.0), 1), str(quality_checks.get("core_coverage_reason_keys", "") or "")),
        _gate_row(
            "source_diversity_gate",
            _gate_level_from_status(str(quality_checks.get("effective_source_diversity_status", "OK"))),
            round(effective_balanced_sources, 2),
            str(quality_checks.get("effective_source_diversity_reason_keys", "") or ""),
        ),
        _gate_row("source_concentration_gate", _gate_level_from_status(str(quality_checks.get("source_concentration_status", "OK"))), float(quality_checks.get("largest_labeled_source_share_pct", 0.0) or 0.0), str(quality_checks.get("source_concentration_reason_keys", "") or "")),
        _gate_row("source_influence_concentration_gate", _gate_level_from_status(str(quality_checks.get("source_influence_concentration_status", "OK"))), largest_source_influence_share, str(quality_checks.get("source_influence_concentration_reason_keys", "") or "")),
        _gate_row("weak_source_cost_center_gate", _gate_level_from_status(str(quality_checks.get("weak_source_yield_status", "OK"))), weak_source_cost_centers, str(quality_checks.get("weak_source_yield_reason_keys", "") or "")),
        _gate_row("cluster_dominance_gate", _gate_level_from_status(str(quality_checks.get("largest_cluster_dominance_status", "OK"))), largest_share, str(quality_checks.get("largest_cluster_dominance_reason_keys", "") or "")),
        _gate_row("persona_promotion_gate", "hard_fail" if small_promoted else "pass", small_promoted, f"0 promoted personas below min_cluster_size={min_cluster_size}"),
        _gate_row("raw_to_labeled_source_gate", "soft_fail" if raw_sources >= 3 and labeled_sources <= 2 else "pass", f"raw={raw_sources}, labeled={labeled_sources}", "avoid raw coverage collapsing to <=2 labeled sources"),
        _gate_row("promoted_persona_grounding_gate", _gate_level_from_status(str(quality_checks.get("grounding_coverage_status", "OK"))), int(quality_checks.get("promoted_persona_grounding_failure_count", 0) or 0), str(quality_checks.get("grounding_coverage_reason_keys", "") or "")),
        _gate_row("selected_example_grounding_issue_gate", "soft_fail" if int(quality_checks.get("selected_example_grounding_issue_count", 0) or 0) > 0 else "pass", int(quality_checks.get("selected_example_grounding_issue_count", 0) or 0), "selected example rows with weak or degraded grounding evidence"),
        _gate_row("promoted_example_coverage_gate", _gate_level_from_status(str(quality_checks.get("grounding_coverage_status", "OK"))), float(quality_checks.get("promoted_persona_example_coverage_pct", 0.0) or 0.0), str(quality_checks.get("grounding_coverage_reason_keys", "") or "")),
        _gate_row("denominator_consistency_check", "pass", quality_checks.get("denominator_consistency", "explicit"), "all summary rows expose denominator_type/value"),
    ]
    for _, row in source_stage_counts_df.iterrows():
        raw_count = int(row.get("raw_record_count", 0) or 0)
        labeled_count = int(row.get("labeled_episode_count", 0) or 0)
        source = str(row.get("source", "") or "")
        if raw_count > 0 and labeled_count == 0:
            rows.append(
                _gate_row(
                    f"source_failure:{source}",
                    "soft_fail",
                    labeled_count,
                    "raw_record_count > 0 should produce labeled_episode_count > 0",
                )
            )
    return pd.DataFrame(rows)


def finalize_quality_checks(evaluated_quality_result: dict[str, Any]) -> dict[str, Any]:
    """Flatten the evaluated quality policy result for workbook rendering only."""
    flattened = flatten_quality_status_result(evaluated_quality_result)
    readiness_state = str(flattened.get("persona_readiness_state", "exploratory_only") or "exploratory_only")
    if readiness_state not in {"deck_ready", "production_persona_ready"}:
        flattened["deck_ready_persona_count"] = 0
    else:
        flattened["deck_ready_persona_count"] = int(flattened.get("final_usable_persona_count", 0) or 0)
    return flattened


def _source_priority_tier(
    failure_level: str,
    source_balance_status: str,
    weak_source_cost_center: bool,
) -> str:
    """Return one workbook-facing priority tier for the source."""
    if weak_source_cost_center or source_balance_status == "overdominant_source_risk" or str(failure_level).strip() == "failure":
        return "fix_now"
    if str(failure_level).strip() == "warning" or source_balance_status == "watchlist":
        return "tune_soon"
    return "monitor"


def build_survival_funnel_by_source(source_stage_counts_df: pd.DataFrame) -> pd.DataFrame:
    """Build a compact by-source diagnostics table from validated source stage counts."""
    if source_stage_counts_df.empty:
        return pd.DataFrame()
    frame = source_stage_counts_df.copy()
    frame["valid_posts_per_normalized_post_pct"] = frame.apply(
        lambda row: round_pct(row.get("valid_post_count", 0), row.get("normalized_post_count", 0)) if int(row.get("normalized_post_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["prefiltered_valid_posts_per_valid_post_pct"] = frame.apply(
        lambda row: round_pct(row.get("prefiltered_valid_post_count", 0), row.get("valid_post_count", 0)) if int(row.get("valid_post_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["labeled_episodes_per_episode_pct"] = frame.apply(
        lambda row: round_pct(row.get("labeled_episode_count", 0), row.get("episode_count", 0)) if int(row.get("episode_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["labelable_episodes_per_labeled_episode_pct"] = frame.apply(
        lambda row: round_pct(row.get("labelable_episode_count", 0), row.get("labeled_episode_count", 0)) if int(row.get("labeled_episode_count", 0) or 0) else 0.0,
        axis=1,
    )
    frame["episodes_per_prefiltered_valid_post"] = frame.apply(
        lambda row: round(float(row.get("episode_count", 0) or 0) / float(row.get("prefiltered_valid_post_count", 0) or 1), 2)
        if int(row.get("prefiltered_valid_post_count", 0) or 0)
        else 0.0,
        axis=1,
    )
    frame["labeled_episodes_per_prefiltered_valid_post"] = frame.apply(
        lambda row: round(float(row.get("labeled_episode_count", 0) or 0) / float(row.get("prefiltered_valid_post_count", 0) or 1), 2)
        if int(row.get("prefiltered_valid_post_count", 0) or 0)
        else 0.0,
        axis=1,
    )
    frame["labelable_episodes_per_prefiltered_valid_post"] = frame.apply(
        lambda row: round(float(row.get("labelable_episode_count", 0) or 0) / float(row.get("prefiltered_valid_post_count", 0) or 1), 2)
        if int(row.get("prefiltered_valid_post_count", 0) or 0)
        else 0.0,
        axis=1,
    )
    return frame[
        [
            "source",
            "normalized_post_count",
            "valid_post_count",
            "prefiltered_valid_post_count",
            "valid_posts_per_normalized_post_pct",
            "prefiltered_valid_posts_per_valid_post_pct",
            "episode_count",
            "labeled_episode_count",
            "labelable_episode_count",
            "labeled_episodes_per_episode_pct",
            "labelable_episodes_per_labeled_episode_pct",
            "episodes_per_prefiltered_valid_post",
            "labeled_episodes_per_prefiltered_valid_post",
            "labelable_episodes_per_prefiltered_valid_post",
            "effective_diversity_contribution",
            "failure_reason_top",
            "failure_level",
        ]
    ].copy()


def _count_jsonl_lines(path: Path) -> int:
    """Count non-empty JSONL lines."""
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def _episode_source_lookup(episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Return unique episode-to-source mapping."""
    if episodes_df.empty or not {"episode_id", "source"}.issubset(episodes_df.columns):
        return pd.DataFrame(columns=["episode_id", "source"])
    return episodes_df[["episode_id", "source"]].drop_duplicates("episode_id")


def _with_episode_source(df: pd.DataFrame, episode_source: pd.DataFrame) -> pd.DataFrame:
    """Attach source from episode_id when needed."""
    if df.empty:
        return pd.DataFrame(columns=["source"])
    if "source" in df.columns:
        return df
    if "episode_id" not in df.columns:
        return pd.DataFrame(columns=["source"])
    return df.merge(episode_source, on="episode_id", how="left")


def _source_influence_cap_pct() -> float:
    """Return the per-persona cap used when aggregating downstream source influence."""
    return float(QUALITY_STATUS_POLICY["source_influence_concentration"]["fail_threshold"])


def _persona_capped_source_contributions(assignments_with_source: pd.DataFrame, cap_share_pct: float) -> dict[str, float]:
    """Return per-source downstream contribution after capping one source inside each persona."""
    if assignments_with_source.empty or not {"persona_id", "source"}.issubset(assignments_with_source.columns):
        return {}
    grouped = (
        assignments_with_source.assign(
            persona_id=assignments_with_source["persona_id"].astype(str),
            source=assignments_with_source["source"].astype(str).map(canonical_source_name),
        )
        .groupby(["source", "persona_id"], as_index=False)
        .size()
        .rename(columns={"size": "source_persona_episode_count"})
    )
    if grouped.empty:
        return {}
    persona_totals = grouped.groupby("persona_id", as_index=False)["source_persona_episode_count"].sum().rename(columns={"source_persona_episode_count": "persona_episode_total"})
    grouped = grouped.merge(persona_totals, on="persona_id", how="left")
    grouped["capped_contribution"] = grouped.apply(
        lambda row: min(float(row.get("source_persona_episode_count", 0) or 0), float(row.get("persona_episode_total", 0) or 0) * cap_share_pct / 100.0),
        axis=1,
    )
    return grouped.groupby("source")["capped_contribution"].sum().to_dict()


def _source_count(df: pd.DataFrame, source: str, column: str) -> int:
    """Return a numeric count for one source in a pre-aggregated table."""
    return aggregated_source_count(df, source, column)


def _top_reason(df: pd.DataFrame, source: str, column: str) -> str:
    """Return top reason text for one source."""
    if df.empty or column not in df.columns or SOURCE_FIELD not in df.columns:
        return "reason_unavailable"
    subset = df[df[SOURCE_FIELD].astype(str) == source]
    if subset.empty:
        return "reason_unavailable"
    return str(subset[column].fillna("unknown").astype(str).value_counts().idxmax())


def _apply_source_diagnostic_rules(
    source_stage_counts_df: pd.DataFrame,
    root_dir: Path,
    relevance_drop_df: pd.DataFrame,
    invalid_with_prefilter_df: pd.DataFrame,
) -> pd.DataFrame:
    """Assign one ranked diagnostic reason, severity, and intervention per source."""
    if source_stage_counts_df.empty:
        return source_stage_counts_df
    result = source_stage_counts_df.copy()
    total_labeled = int(pd.to_numeric(result.get("labeled_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).sum())
    total_promoted = int(pd.to_numeric(result.get("promoted_persona_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).sum())
    total_grounded = int(pd.to_numeric(result.get("grounded_promoted_persona_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).sum())
    diagnoses = result.apply(
        lambda row: _diagnose_source_row(
            root_dir=root_dir,
            source=str(row.get("source", "") or ""),
            raw_count=int(row.get("raw_record_count", 0) or 0),
            normalized_count=int(row.get("normalized_post_count", 0) or 0),
            valid_count=int(row.get("valid_post_count", 0) or 0),
            prefiltered_count=int(row.get("prefiltered_valid_post_count", 0) or 0),
            episode_count=int(row.get("episode_count", 0) or 0),
            labelable_count=int(row.get("labelable_episode_count", 0) or 0),
            labeled_count=int(row.get("labeled_episode_count", 0) or 0),
            promoted_count=int(row.get("promoted_persona_episode_count", 0) or 0),
            grounded_count=int(row.get("grounded_promoted_persona_episode_count", 0) or 0),
            effective_diversity_contribution=float(row.get("effective_diversity_contribution", 0.0) or 0.0),
            total_labeled_count=total_labeled,
            total_promoted_count=total_promoted,
            total_grounded_count=total_grounded,
            relevance_drop_df=relevance_drop_df,
            invalid_with_prefilter_df=invalid_with_prefilter_df,
        ),
        axis=1,
        result_type="expand",
    )
    for column in diagnoses.columns:
        result[column] = diagnoses[column]
    return result


def _diagnose_source_row(
    root_dir: Path,
    source: str,
    raw_count: int,
    normalized_count: int,
    valid_count: int,
    prefiltered_count: int,
    episode_count: int,
    labelable_count: int,
    labeled_count: int,
    promoted_count: int,
    grounded_count: int,
    effective_diversity_contribution: float,
    total_labeled_count: int,
    total_promoted_count: int,
    total_grounded_count: int,
    relevance_drop_df: pd.DataFrame,
    invalid_with_prefilter_df: pd.DataFrame,
) -> dict[str, str]:
    """Return source-specific diagnostics plus the strongest ranked intervention target."""
    if raw_count <= 0:
        level = "warning" if source in RAW_WITHOUT_LABEL_FAILURE_SOURCES else "pass"
        return {
            "dominant_invalid_reason": "reason_unavailable",
            "dominant_prefilter_reason": "reason_unavailable",
            "valid_retention_reason": "blocked_upstream_no_raw_records",
            "valid_retention_level": "pass",
            "prefilter_retention_reason": "blocked_upstream_no_raw_records",
            "prefilter_retention_level": "pass",
            "episode_yield_reason": "blocked_upstream_no_raw_records",
            "episode_yield_level": "pass",
            "labelable_coverage_reason": "blocked_upstream_no_raw_records",
            "labelable_coverage_level": "pass",
            "grounding_contribution_reason": "blocked_upstream_no_raw_records",
            "grounding_contribution_level": "pass",
            "concentration_risk_reason": "blocked_upstream_no_raw_records",
            "concentration_risk_level": "pass",
            "diversity_contribution_reason": "blocked_upstream_no_raw_records",
            "diversity_contribution_level": "pass",
            "failure_reason_top": "no_raw_records",
            "failure_level": level,
            "recommended_seed_set": "",
        }
    if normalized_count <= 0:
        return {
            "dominant_invalid_reason": "reason_unavailable",
            "dominant_prefilter_reason": "reason_unavailable",
            "valid_retention_reason": "raw_not_normalized",
            "valid_retention_level": "failure",
            "prefilter_retention_reason": "blocked_upstream_raw_not_normalized",
            "prefilter_retention_level": "pass",
            "episode_yield_reason": "blocked_upstream_raw_not_normalized",
            "episode_yield_level": "pass",
            "labelable_coverage_reason": "blocked_upstream_raw_not_normalized",
            "labelable_coverage_level": "pass",
            "grounding_contribution_reason": "blocked_upstream_raw_not_normalized",
            "grounding_contribution_level": "pass",
            "concentration_risk_reason": "blocked_upstream_raw_not_normalized",
            "concentration_risk_level": "pass",
            "diversity_contribution_reason": "blocked_upstream_raw_not_normalized",
            "diversity_contribution_level": "pass",
            "failure_reason_top": "raw_not_normalized",
            "failure_level": "failure",
            "recommended_seed_set": "",
        }

    valid_retention_pct = round_pct(valid_count, normalized_count) if normalized_count else 0.0
    prefilter_retention_pct = round_pct(prefiltered_count, valid_count) if valid_count else 0.0
    episode_yield = round(float(episode_count) / float(prefiltered_count), 2) if prefiltered_count else 0.0
    labelable_ratio_pct = round_pct(labelable_count, labeled_count) if labeled_count else 0.0
    labeled_share_pct = round_pct(labeled_count, total_labeled_count) if total_labeled_count else 0.0
    promoted_share_pct = round_pct(promoted_count, total_promoted_count) if total_promoted_count else 0.0
    grounded_share_pct = round_pct(grounded_count, total_grounded_count) if total_grounded_count else 0.0
    invalid_reason = _top_reason(invalid_with_prefilter_df, source, "invalid_reason")
    prefilter_reason = _top_reason(relevance_drop_df, source, "prefilter_reason")
    valid_reason = "healthy_valid_post_retention"
    valid_level = "pass"
    if valid_count <= 0:
        valid_reason = f"low_valid_post_retention: {invalid_reason}"
        valid_level = "failure"
    elif valid_retention_pct < 50.0:
        valid_reason = f"low_valid_post_retention: {invalid_reason}"
        valid_level = "failure" if valid_retention_pct < 20.0 else "warning"

    prefilter_reason_value = "healthy_prefilter_retention"
    prefilter_level = "pass"
    if valid_count <= 0:
        prefilter_reason_value = "blocked_upstream_valid_post_failure"
    elif prefiltered_count <= 0:
        prefilter_reason_value = f"low_prefilter_retention: {prefilter_reason}"
        prefilter_level = "failure"
    elif prefilter_retention_pct < 30.0:
        prefilter_reason_value = f"low_prefilter_retention: {prefilter_reason}"
        prefilter_level = "failure" if prefilter_retention_pct < 10.0 else "warning"

    episode_reason = "healthy_episode_yield"
    episode_level = "pass"
    if prefiltered_count <= 0:
        episode_reason = "blocked_upstream_prefilter_failure"
    elif episode_count <= 0:
        episode_reason = "low_episode_yield"
        episode_level = "failure"
    elif episode_yield < 0.75:
        episode_reason = "low_episode_yield"
        episode_level = "failure" if episode_yield < 0.25 else "warning"

    labelable_reason = "healthy_labelable_coverage"
    labelable_level = "pass"
    if episode_count <= 0:
        labelable_reason = "blocked_upstream_episode_failure"
    elif labeled_count <= 0:
        labelable_reason = "label_output_missing_after_episode_build"
        labelable_level = "failure"
    elif labelable_count <= 0:
        labelable_reason = "low_labelable_episode_ratio"
        labelable_level = "failure"
    elif labelable_ratio_pct < 50.0:
        labelable_reason = "low_labelable_episode_ratio"
        labelable_level = "failure" if labelable_ratio_pct < 25.0 else "warning"

    grounding_reason = "healthy_grounding_contribution"
    grounding_level = "pass"
    if labeled_count <= 0:
        grounding_reason = "blocked_upstream_label_output_missing"
    elif promoted_count <= 0 and labeled_count >= 3:
        grounding_reason = "grounding_contribution_absent"
        grounding_level = "warning"
    elif grounded_count <= 0 and promoted_count > 0:
        grounding_reason = "grounded_persona_contribution_absent"
        grounding_level = "warning"

    concentration_reason = "concentration_risk_clear"
    concentration_level = "pass"
    concentration_share = max(labeled_share_pct, promoted_share_pct, grounded_share_pct)
    has_multiple_contributing_sources = any(
        total_count > source_count
        for total_count, source_count in [
            (total_labeled_count, labeled_count),
            (total_promoted_count, promoted_count),
            (total_grounded_count, grounded_count),
        ]
        if total_count > 0
    )
    if has_multiple_contributing_sources and max(labeled_count, promoted_count, grounded_count) >= 3 and concentration_share >= 45.0:
        concentration_reason = "overconcentration_risk"
        concentration_level = "failure" if concentration_share >= 60.0 else "warning"

    diversity_reason = "strong_diversity_contribution"
    diversity_level = "pass"
    if labeled_count <= 0:
        diversity_reason = "blocked_upstream_label_output_missing"
    elif effective_diversity_contribution < 1.0:
        diversity_reason = "weak_diversity_contribution"
        diversity_level = "warning"

    ranked_issues = [
        (valid_reason, valid_level),
        (prefilter_reason_value, prefilter_level),
        (episode_reason, episode_level),
        (labelable_reason, labelable_level),
        (concentration_reason, concentration_level),
        (diversity_reason, diversity_level),
        (grounding_reason, grounding_level),
    ]
    top_reason = "healthy_source_contribution"
    top_level = "pass"
    for reason, level in ranked_issues:
        if level != "pass":
            top_reason = reason
            top_level = level
            break
    recommended_seed_set = _recommended_seed_set(root_dir, source, top_reason) if top_reason.startswith("low_prefilter_retention:") else ""
    return {
        "dominant_invalid_reason": invalid_reason,
        "dominant_prefilter_reason": prefilter_reason,
        "valid_retention_reason": valid_reason,
        "valid_retention_level": valid_level,
        "prefilter_retention_reason": prefilter_reason_value,
        "prefilter_retention_level": prefilter_level,
        "episode_yield_reason": episode_reason,
        "episode_yield_level": episode_level,
        "labelable_coverage_reason": labelable_reason,
        "labelable_coverage_level": labelable_level,
        "grounding_contribution_reason": grounding_reason,
        "grounding_contribution_level": grounding_level,
        "concentration_risk_reason": concentration_reason,
        "concentration_risk_level": concentration_level,
        "diversity_contribution_reason": diversity_reason,
        "diversity_contribution_level": diversity_level,
        "failure_reason_top": top_reason,
        "failure_level": top_level,
        "recommended_seed_set": recommended_seed_set,
    }


def _effective_source_contribution(labeled_count: int) -> float:
    """Count low-volume labeled sources as fractional diversity contributors."""
    if labeled_count <= 0:
        return 0.0
    return min(1.0, float(labeled_count) / 5.0)


def _effective_labeled_source_count(source_stage_counts_df: pd.DataFrame) -> float:
    """Return effective labeled source count using weak contributions below 5 labels."""
    if source_stage_counts_df.empty or "labeled_episode_count" not in source_stage_counts_df.columns:
        return 0.0
    counts = pd.to_numeric(source_stage_counts_df["labeled_episode_count"], errors="coerce").fillna(0).astype(int)
    return float(sum(_effective_source_contribution(int(count)) for count in counts.tolist()))


def _collapse_stage_for_reason(reason: str) -> str:
    """Map a ranked source failure reason into the pipeline stage that is collapsing."""
    value = str(reason or "").strip()
    if value in {"", "healthy_source_contribution"}:
        return "healthy"
    if value in {"no_raw_records", "raw_not_normalized"}:
        return "collection_or_normalization"
    if value.startswith("low_valid_post_retention: outside_time_window"):
        return "time_window"
    if value.startswith("low_valid_post_retention"):
        return "valid_filtering"
    if value.startswith("low_prefilter_retention"):
        return "relevance_prefilter"
    if value == "low_episode_yield":
        return "episode_yield"
    if value in {"low_labelable_episode_ratio", "label_output_missing_after_episode_build"}:
        return "labelability"
    if value in {"grounding_contribution_absent", "grounded_persona_contribution_absent"}:
        return "persona_contribution"
    if value == "overconcentration_risk":
        return "overdominant_source_risk"
    if value == "weak_diversity_contribution":
        return "low_yield_source_waste"
    return "other"


def _is_weak_source_cost_center_row(row: pd.Series) -> bool:
    """Return whether a source consumes upstream volume without meaningful downstream influence."""
    raw_count = int(row.get("raw_record_count", 0) or 0)
    labeled_share_pct = float(row.get("labeled_share_pct", 0.0) or 0.0)
    prefilter_pct = float(row.get("prefiltered_valid_posts_per_valid_post_pct", 0.0) or 0.0)
    episode_yield = float(row.get("episodes_per_prefiltered_valid_post", 0.0) or 0.0)
    labelable_pct = float(row.get("labelable_episode_ratio_pct", 0.0) or 0.0)
    grounded_count = int(row.get("grounded_promoted_persona_episode_count", 0) or 0)
    if raw_count < 100 or labeled_share_pct >= 10.0:
        return False
    return prefilter_pct < 10.0 or episode_yield < 0.5 or labelable_pct < 50.0 or grounded_count <= 0


def _source_balance_status(row: pd.Series) -> str:
    """Return one policy-backed source-balance classification for a source."""
    if str(row.get("concentration_risk_reason", "") or "") == "overconcentration_risk":
        return "overdominant_source_risk"
    if bool(row.get("weak_source_cost_center", False)):
        return "weak_source_cost_center"
    if str(row.get("failure_reason_top", "") or "") in {"", "healthy_source_contribution"}:
        return "balanced_or_healthy"
    return "watchlist"


def _source_balance_action(row: pd.Series) -> str:
    """Return the next source-balance action for a source under the current policy."""
    if str(row.get("concentration_risk_reason", "") or "") == "overconcentration_risk":
        return "diversify_other_sources_before_scaling_this_source"
    reason = str(row.get("failure_reason_top", "") or "")
    dominant_invalid_reason = str(row.get("dominant_invalid_reason", "") or "")
    if reason.startswith("low_prefilter_retention"):
        return "review_source_specific_prefilter_terms"
    if reason.startswith("low_valid_post_retention"):
        if dominant_invalid_reason == "outside_time_window":
            return "review_time_window_policy"
        return "review_source_specific_valid_filtering"
    if reason == "low_episode_yield":
        return "tighten_episode_segmentation_for_source"
    if reason in {"low_labelable_episode_ratio", "label_output_missing_after_episode_build"}:
        return "tighten_labelability_and_source_scope"
    if reason in {"grounding_contribution_absent", "grounded_persona_contribution_absent"}:
        return "expand_grounded_persona_coverage_before_promotion"
    if reason == "weak_diversity_contribution":
        return "raise_targeted_collection_on_underrepresented_source"
    if reason in {"no_raw_records", "raw_not_normalized"}:
        return "review_collect_depth"
    return "monitor_source"


def _triage_weak_source_row(row: pd.Series) -> dict[str, str]:
    """Return keep/investigate/drop recommendation for one weak source row."""
    collapse_stage = str(row.get("collapse_stage", "") or "")
    raw_count = int(row.get("raw_record_count", 0) or 0)
    prefilter_pct = float(row.get("prefiltered_valid_posts_per_valid_post_pct", 0.0) or 0.0)
    labelable_pct = float(row.get("labelable_episode_ratio_pct", 0.0) or 0.0)
    grounded = int(row.get("grounded_promoted_persona_episode_count", 0) or 0)
    blended = float(row.get("blended_influence_share_pct", 0.0) or 0.0)
    if collapse_stage == "relevance_prefilter" and raw_count >= 500 and prefilter_pct < 8.0 and grounded <= 0:
        return {
            "triage_recommendation": "DROP_OR_PAUSE_SOURCE",
            "recommendation_confidence": "high",
            "triage_rationale": "high raw input collapses in prefilter with no grounded contribution",
        }
    if collapse_stage in {"valid_filtering", "labelability", "episode_yield"} and raw_count >= 100:
        return {
            "triage_recommendation": "INVESTIGATE_RULES",
            "recommendation_confidence": "medium",
            "triage_rationale": "source has enough volume but loses yield in a fixable mid-pipeline stage",
        }
    if grounded > 0 or blended >= 2.0 or labelable_pct >= 60.0:
        return {
            "triage_recommendation": "KEEP_WITH_TARGETED_TUNING",
            "recommendation_confidence": "medium",
            "triage_rationale": "source already contributes downstream signal but needs focused tuning",
        }
    return {
        "triage_recommendation": "INVESTIGATE_RULES",
        "recommendation_confidence": "low",
        "triage_rationale": "weak-source profile detected but bottleneck severity is mixed",
    }


def _validate_source_stage_counts(df: pd.DataFrame) -> None:
    """Validate source stage counts before workbook formatting."""
    if df.empty:
        return
    raw = pd.to_numeric(df.get("raw_record_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    normalized = pd.to_numeric(df.get("normalized_post_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    valid = pd.to_numeric(df.get("valid_post_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    prefiltered = pd.to_numeric(df.get("prefiltered_valid_post_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    episodes = pd.to_numeric(df.get("episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    labeled = pd.to_numeric(df.get("labeled_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    labelable = pd.to_numeric(df.get("labelable_episode_count", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int)
    if (normalized > raw).any():
        raise ValueError("source_diagnostics invariant failed: normalized_post_count cannot exceed raw_record_count.")
    if (valid > normalized).any():
        raise ValueError("source_diagnostics invariant failed: valid_post_count cannot exceed normalized_post_count.")
    if (prefiltered > valid).any():
        raise ValueError("source_diagnostics invariant failed: prefiltered_valid_post_count cannot exceed valid_post_count.")
    if (labeled > episodes).any():
        raise ValueError("source_diagnostics invariant failed: labeled_episode_count cannot exceed episode_count.")
    if (labelable > labeled).any():
        raise ValueError("source_diagnostics invariant failed: labelable_episode_count cannot exceed labeled_episode_count.")


def _validate_source_diagnostics_frame(df: pd.DataFrame) -> None:
    """Validate workbook-facing source diagnostics rows and naming contracts."""
    if df.empty:
        return
    forbidden_columns = {
        "raw_count",
        "normalized_count",
        "valid_count",
        "prefiltered_valid_count",
        "prefilter_survival_rate",
        "episode_survival_rate",
        "labelable_count",
        "labeled_count",
        "labeling_survival_rate",
        "promoted_to_persona_count",
    }
    present_forbidden = sorted(column for column in forbidden_columns if column in df.columns)
    if present_forbidden:
        raise ValueError(
            "source_diagnostics invariant failed: legacy ambiguous columns present: "
            + ", ".join(present_forbidden)
        )
    required_columns = {
        "source",
        "section",
        "row_kind",
        "grain",
        "metric_name",
        "metric_value",
        "metric_type",
        "denominator_metric",
        "denominator_grain",
        "denominator_value",
        "bounded_range",
        "is_same_grain_funnel",
        "diagnostic_level",
        "metric_definition",
    }
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError("source_diagnostics invariant failed: missing required columns: " + ", ".join(missing))
    row_kind = df["row_kind"].astype(str)
    if not row_kind.isin({"metric", "diagnostic"}).all():
        raise ValueError("source_diagnostics invariant failed: row_kind must be metric or diagnostic.")
    metric_rows = df[row_kind.eq("metric")]
    diagnostic_rows = df[row_kind.eq("diagnostic")]
    if (metric_rows["section"].astype(str) == "diagnostic_reasons").any():
        raise ValueError("source_diagnostics invariant failed: metric rows cannot live in diagnostic_reasons.")
    if not diagnostic_rows.empty and (~diagnostic_rows["section"].astype(str).eq("diagnostic_reasons")).any():
        raise ValueError("source_diagnostics invariant failed: diagnostic rows must use section=diagnostic_reasons.")
    if not metric_rows.empty and metric_rows["diagnostic_level"].astype(str).str.strip().ne("").any():
        raise ValueError("source_diagnostics invariant failed: metric rows cannot carry diagnostic_level.")
    top_reason_rows = diagnostic_rows[diagnostic_rows["metric_name"].astype(str).eq("top_failure_reason")]
    if top_reason_rows.empty:
        raise ValueError("source_diagnostics invariant failed: missing top_failure_reason diagnostic rows.")
    generic_reason_rows = top_reason_rows[top_reason_rows["metric_value"].astype(str).isin({"labeled_output_present", "generic_source_pass", "pass"})]
    if not generic_reason_rows.empty:
        raise ValueError("source_diagnostics invariant failed: generic placeholder reasons are not allowed in top_failure_reason rows.")
    sources = df["source"].astype(str)
    for source in sorted(sources.unique().tolist()):
        source_top_rows = top_reason_rows[top_reason_rows["source"].astype(str).eq(source)]
        if len(source_top_rows) != 1:
            raise ValueError(f"source_diagnostics invariant failed: source {source} must have exactly one top_failure_reason row.")
    mixed_bridge = df[df["grain"].astype(str).eq("mixed_grain_bridge")]
    if not mixed_bridge.empty:
        invalid_names = mixed_bridge[
            mixed_bridge["metric_name"].astype(str).str.contains("rate|share|survival", case=False, regex=True)
        ]
        if not invalid_names.empty:
            bad = ", ".join(sorted(invalid_names["metric_name"].astype(str).unique().tolist()))
            raise ValueError(f"source_diagnostics invariant failed: mixed-grain bridge metrics cannot use rate/share/survival names: {bad}")
    percentage_rows = df[df["metric_type"].astype(str).eq("percentage")]
    if not percentage_rows.empty:
        values = pd.to_numeric(percentage_rows["metric_value"], errors="coerce").fillna(0.0)
        if ((values < 0.0) | (values > 100.0)).any():
            raise ValueError("source_diagnostics invariant failed: same-grain percentage metrics must be within 0-100.")


def _recommended_seed_set(root_dir: Path, source: str, reason: str) -> str:
    """Return active source-friendly seeds only for relevance-prefilter interventions."""
    if not reason.startswith("low_prefilter_retention:"):
        return ""
    seed_path = _seed_path(root_dir, source)
    if seed_path is None:
        return ""
    data = load_yaml(seed_path)
    seeds = data.get("active_core_seeds") or data.get("core_seeds") or []
    values: list[str] = []
    for item in seeds:
        if isinstance(item, dict):
            values.append(str(item.get("seed", "")).strip())
        else:
            values.append(str(item).strip())
    return " | ".join(value for value in values if value)


def _seed_path(root_dir: Path, source: str) -> Path | None:
    """Find the local seed file for a source."""
    candidates = [
        root_dir / "config" / "seeds" / "business_communities" / f"{source}.yaml",
        root_dir / "config" / "seeds" / "discourse" / f"{source}.yaml",
        root_dir / "config" / "seeds" / "existing_forums" / f"{source}.yaml",
        root_dir / "config" / "seeds" / "reddit" / f"{source}.yaml",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _largest_cluster_share(cluster_stats_df: pd.DataFrame) -> float:
    """Return largest cluster share percentage."""
    if cluster_stats_df.empty:
        return 0.0
    share_column = "share_of_core_labeled" if "share_of_core_labeled" in cluster_stats_df.columns else "share_of_total"
    if share_column not in cluster_stats_df.columns:
        return 0.0
    values = pd.to_numeric(cluster_stats_df[share_column], errors="coerce").fillna(0)
    return round(float(values.max()), 1) if not values.empty else 0.0


def _small_promoted_count(cluster_stats_df: pd.DataFrame, min_cluster_size: int) -> int:
    """Count promoted personas below the size floor."""
    if cluster_stats_df.empty or "promotion_status" not in cluster_stats_df.columns:
        return 0
    workbook_review_visible = cluster_stats_df.get("workbook_review_visible", pd.Series(dtype=bool))
    if workbook_review_visible.empty:
        promoted = cluster_stats_df[cluster_stats_df["promotion_status"].astype(str).isin({"promoted_persona", "review_visible_persona"})]
    else:
        promoted = cluster_stats_df[workbook_review_visible.fillna(False).astype(bool)]
    sizes = pd.to_numeric(promoted.get("persona_size", pd.Series(dtype=int)), errors="coerce").fillna(0)
    return int((sizes < min_cluster_size).sum())


def _example_failure_count(persona_examples_df: pd.DataFrame) -> int:
    """Count selected examples with weak grounding evidence."""
    if persona_examples_df.empty:
        return 0
    quality = persona_examples_df.get("quote_quality", pd.Series(dtype=str)).astype(str)
    text_len = pd.to_numeric(persona_examples_df.get("source_text_length", pd.Series(dtype=int)), errors="coerce").fillna(0)
    reasons = persona_examples_df.get("rejection_reason", pd.Series(dtype=str)).fillna("").astype(str)
    return int((quality.isin({"reject", "borderline"}) | (text_len < 80) | reasons.ne("")).sum())


def _gate_row(metric: str, level: str, value: Any, threshold: str) -> dict[str, Any]:
    """Build one quality gate row."""
    return {
        "metric": metric,
        "level": level,
        "value": value,
        "threshold": threshold,
        "passed": level == "pass",
    }


def _gate_level_from_status(status: str) -> str:
    """Map centralized status strings into quality failure gate levels."""
    if status == "FAIL":
        return "hard_fail"
    if status == "WARN":
        return "soft_fail"
    return "pass"
