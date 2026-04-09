"""Build compact batch labeling input files for later OpenAI batch execution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.labeling.prompt_payload import build_compact_episode_payload, extract_compact_rule_labels


def build_batch_requests(
    episode_rows: Iterable[pd.Series],
    labeled_df: pd.DataFrame,
    codebook: dict[str, object],
    prompt_cache_key: str,
    model: str,
    max_output_tokens: int,
) -> Path:
    """Write a JSONL file containing one compact request object per episode."""
    rows = list(episode_rows)
    output_dir = Path("data") / "labeled"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "llm_batch_requests.jsonl"
    labeled_lookup = labeled_df.set_index("episode_id", drop=False) if not labeled_df.empty else pd.DataFrame()

    with output_path.open("w", encoding="utf-8") as handle:
        for episode_row in rows:
            episode_id = str(episode_row.get("episode_id", ""))
            labeled_row = labeled_lookup.loc[episode_id] if episode_id in labeled_lookup.index else {}
            payload = {
                "episode_id": episode_id,
                "model": model,
                "max_output_tokens": max_output_tokens,
                "prompt_cache_key": prompt_cache_key,
                "episode": build_compact_episode_payload(episode_row),
                "rule_labels": (
                    extract_compact_rule_labels(
                        labeled_row,
                        [
                            "role_codes",
                            "moment_codes",
                            "question_codes",
                            "pain_codes",
                            "env_codes",
                            "workaround_codes",
                            "output_codes",
                            "fit_code",
                        ],
                    )
                    if isinstance(labeled_row, pd.Series)
                    else {}
                ),
                "codebook_keys": list((codebook or {}).keys())[:8],
            }
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return output_path
