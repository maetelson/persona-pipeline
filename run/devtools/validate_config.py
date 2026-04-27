"""Validate repository YAML configs without running pipeline stages."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.dates import build_relative_time_window, build_time_slices
from src.utils.io import load_yaml
from src.utils.source_registry import load_source_definitions


def main() -> None:
    """Validate YAML parsing, source registry integrity, and key config invariants."""
    yaml_paths = sorted((ROOT / "config").rglob("*.yaml"))
    if not yaml_paths:
        raise RuntimeError("No YAML config files found under config/.")

    for path in yaml_paths:
        load_yaml(path)

    source_definitions = load_source_definitions(ROOT, include_disabled=True)
    if not source_definitions:
        raise RuntimeError("No source definitions were loaded from config/sources/*.yaml.")

    source_ids = [definition.source_id for definition in source_definitions]
    duplicate_source_ids = sorted({source_id for source_id in source_ids if source_ids.count(source_id) > 1})
    if duplicate_source_ids:
        raise RuntimeError(f"Duplicate source_id values found: {', '.join(duplicate_source_ids)}")

    alias_owner: dict[str, str] = {}
    collisions: list[str] = []
    for definition in source_definitions:
        for alias in definition.aliases:
            existing = alias_owner.get(alias)
            if existing and existing != definition.source_id:
                collisions.append(f"{alias} -> {existing}, {definition.source_id}")
            else:
                alias_owner[alias] = definition.source_id
    if collisions:
        raise RuntimeError("Conflicting source aliases found:\n- " + "\n- ".join(sorted(collisions)))

    time_window = load_yaml(ROOT / "config" / "time_window.yaml")
    build_relative_time_window(time_window)
    build_time_slices(time_window)

    export_schema = load_yaml(ROOT / "config" / "export_schema.yaml")
    workbook_name = str(export_schema.get("workbook_name", "")).strip()
    if not workbook_name:
        raise RuntimeError("config/export_schema.yaml is missing workbook_name.")
    if not workbook_name.lower().endswith(".xlsx"):
        raise RuntimeError("config/export_schema.yaml workbook_name must end with .xlsx.")

    print(f"Validated YAML files: {len(yaml_paths)}")
    print(f"Validated source definitions: {len(source_definitions)}")
    print(f"Validated source aliases: {len(alias_owner)}")
    print(f"Validated workbook target: {workbook_name}")


if __name__ == "__main__":
    main()
