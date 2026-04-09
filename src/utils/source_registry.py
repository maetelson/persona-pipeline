"""Config-driven registry helpers for source-group collection flows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.utils.io import load_yaml


@dataclass(slots=True)
class SourceDefinition:
    """One configured source definition resolved from YAML."""

    source_id: str
    source_name: str
    source_group: str
    collector_kind: str
    normalizer_kind: str
    config_path: Path
    config: dict[str, Any]

    @property
    def aliases(self) -> list[str]:
        """Return all names that may be used on the CLI."""
        raw_aliases = self.config.get("aliases", []) or []
        aliases = [self.source_id, self.source_name, *[str(item) for item in raw_aliases]]
        deduped: list[str] = []
        for item in aliases:
            lowered = str(item).strip().lower()
            if lowered and lowered not in deduped:
                deduped.append(lowered)
        return deduped

    @property
    def enabled(self) -> bool:
        """Return whether the source is enabled by default."""
        return bool(self.config.get("enabled", True))


def load_source_definitions(root_dir: Path, include_disabled: bool = True) -> list[SourceDefinition]:
    """Load all source YAMLs that declare a source group."""
    source_dir = root_dir / "config" / "sources"
    definitions: list[SourceDefinition] = []
    for config_path in sorted(source_dir.glob("*.yaml")):
        config = load_yaml(config_path)
        source_group = str(config.get("source_group", "") or "").strip()
        if not source_group:
            continue
        definition = SourceDefinition(
            source_id=str(config.get("source_id", config_path.stem)),
            source_name=str(config.get("source_name", config_path.stem)),
            source_group=source_group,
            collector_kind=str(config.get("collector_kind", source_group)),
            normalizer_kind=str(config.get("normalizer_kind", source_group)),
            config_path=config_path,
            config=config,
        )
        if include_disabled or definition.enabled:
            definitions.append(definition)
    return definitions


def filter_source_definitions(
    definitions: list[SourceDefinition],
    source_group: str | None = None,
    source_name: str | None = None,
    include_disabled: bool = True,
) -> list[SourceDefinition]:
    """Filter source definitions by group or CLI source alias."""
    selected = definitions
    if not include_disabled:
        selected = [definition for definition in selected if definition.enabled]

    if source_group:
        group_token = str(source_group).strip().lower()
        selected = [definition for definition in selected if definition.source_group.lower() == group_token]

    if source_name:
        source_token = str(source_name).strip().lower()
        selected = [definition for definition in selected if source_token in definition.aliases]

    return selected
