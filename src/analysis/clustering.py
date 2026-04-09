"""Graph-style clustering for code co-occurrence outputs."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd


def build_code_clusters(code_freq_df: pd.DataFrame, edge_df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Assign cluster ids to codes using mutual-strong-tie communities."""
    columns = ["code", "cluster_id"]
    if code_freq_df.empty:
        return pd.DataFrame(columns=columns), []

    codes = [str(code) for code in code_freq_df.get("code", pd.Series(dtype=str)).tolist()]
    freq_lookup = {str(row["code"]): int(row["count"]) for _, row in code_freq_df.iterrows()}
    pair_score, pair_weight = _build_pair_maps(edge_df, freq_lookup)
    components = _build_mutual_topk_components(codes, pair_score)
    cluster_members = _merge_singletons(components, pair_score)

    cluster_rows: list[dict[str, str]] = []
    summaries: list[dict[str, Any]] = []
    for index, codes_in_cluster in enumerate(sorted(cluster_members, key=len, reverse=True), start=1):
        cluster_id = f"cluster_{index:03d}"
        for code in codes_in_cluster:
            cluster_rows.append({"code": code, "cluster_id": cluster_id})

        intra_weights = [
            weight
            for (code_a, code_b), weight in pair_weight.items()
            if code_a in codes_in_cluster and code_b in codes_in_cluster
        ]
        possible_edges = max((len(codes_in_cluster) * (len(codes_in_cluster) - 1)) // 2, 1)
        top_codes = sorted(codes_in_cluster, key=lambda code: (-freq_lookup.get(code, 0), code))[:8]
        summaries.append(
            {
                "cluster_id": cluster_id,
                "size": len(codes_in_cluster),
                "top_codes": top_codes,
                "edge_density": round(len(intra_weights) / possible_edges, 6) if len(codes_in_cluster) > 1 else 0.0,
                "total_edge_weight": int(sum(intra_weights)),
            }
        )

    return pd.DataFrame(cluster_rows, columns=columns), summaries


def cluster_summary_json(summary_rows: list[dict[str, Any]]) -> str:
    """Serialize cluster summary rows to stable JSON text."""
    return json.dumps(summary_rows, ensure_ascii=False, indent=2)


def _build_pair_maps(edge_df: pd.DataFrame, freq_lookup: dict[str, int]) -> tuple[dict[tuple[str, str], float], dict[tuple[str, str], int]]:
    """Create pairwise association and raw-weight lookups."""
    pair_score: dict[tuple[str, str], float] = {}
    pair_weight: dict[tuple[str, str], int] = {}
    for _, row in edge_df.iterrows():
        code_a = str(row.get("code_a", ""))
        code_b = str(row.get("code_b", ""))
        if not code_a or not code_b:
            continue
        edge = tuple(sorted((code_a, code_b)))
        raw_weight = int(row.get("count", 0) or 0)
        min_freq = max(min(freq_lookup.get(code_a, 1), freq_lookup.get(code_b, 1)), 1)
        pair_score[edge] = raw_weight / min_freq
        pair_weight[edge] = raw_weight
    return pair_score, pair_weight


def _build_mutual_topk_components(codes: list[str], pair_score: dict[tuple[str, str], float]) -> list[list[str]]:
    """Build communities from mutually strong code ties."""
    top_k = 2 if len(codes) <= 40 else 3
    top_neighbors: dict[str, list[str]] = {code: [] for code in codes}
    neighbor_rows: dict[str, list[tuple[str, float]]] = {code: [] for code in codes}
    for (code_a, code_b), score in pair_score.items():
        neighbor_rows.setdefault(code_a, []).append((code_b, score))
        neighbor_rows.setdefault(code_b, []).append((code_a, score))

    for code, rows in neighbor_rows.items():
        ranked = sorted(rows, key=lambda item: (-item[1], item[0]))
        top_neighbors[code] = [neighbor for neighbor, _ in ranked[:top_k]]

    adjacency: dict[str, set[str]] = {code: set() for code in codes}
    for code, neighbors in top_neighbors.items():
        for neighbor in neighbors:
            if code in top_neighbors.get(neighbor, []):
                adjacency[code].add(neighbor)
                adjacency[neighbor].add(code)
    return _connected_components(adjacency)


def _merge_singletons(components: list[list[str]], pair_score: dict[tuple[str, str], float]) -> list[list[str]]:
    """Attach singleton codes to the nearest existing community."""
    clusters = [set(component) for component in components if len(component) > 1]
    singletons = [component[0] for component in components if len(component) == 1]
    if not clusters:
        return [sorted(component) for component in components]

    for code in singletons:
        best_cluster_index: int | None = None
        best_score = -1.0
        for index, cluster in enumerate(clusters):
            score = max(
                (pair_score.get(tuple(sorted((code, other))), 0.0) for other in cluster),
                default=0.0,
            )
            if score > best_score:
                best_score = score
                best_cluster_index = index
        if best_cluster_index is None:
            clusters.append({code})
            continue
        clusters[best_cluster_index].add(code)

    return [sorted(cluster) for cluster in clusters]


def _connected_components(adjacency: dict[str, set[str]]) -> list[list[str]]:
    """Return connected components from an undirected graph."""
    components: list[list[str]] = []
    seen: set[str] = set()
    for node in sorted(adjacency):
        if node in seen:
            continue
        stack = [node]
        component: list[str] = []
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            component.append(current)
            stack.extend(sorted(adjacency.get(current, set()) - seen))
        components.append(sorted(component))
    return components
