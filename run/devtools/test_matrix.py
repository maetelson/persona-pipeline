"""Run fast validation suites tailored to the scope of pipeline changes."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

UNIT_TEST_MODULES = [
    "tests.test_time_window_filter",
    "tests.test_http_fetch",
    "tests.test_record_access",
    "tests.test_pipeline_schema",
    "tests.test_git_sync",
]

FIXTURE_TEST_MODULES = [
    "tests.test_business_community_sources",
    "tests.test_discourse_collector",
    "tests.test_reddit_public_collector",
    "tests.test_stackoverflow_collector",
    "tests.test_invalid_filter",
    "tests.test_relevance_prefilter",
    "tests.test_prefilter_stage_runner",
    "tests.test_episode_builder",
    "tests.test_label_quality",
    "tests.test_llm_labeler_runtime",
    "tests.test_source_aggregation",
    "tests.test_source_diagnostics",
    "tests.test_source_stage_profiler",
    "tests.test_review_ready_gap_analysis",
    "tests.test_deck_ready_feasibility_analysis",
    "tests.test_source_representativeness_audit",
    "tests.test_source_tier_evidence",
    "tests.test_workbook_policy_redesign",
    "tests.test_workbook_export",
]

CLI_SMOKE_TEST_MODULES = [
    "tests.test_analysis_snapshot_cli",
    "tests.test_source_cli_smoke",
    "tests.test_axis_cli_smoke",
    "tests.test_cluster_cli_smoke",
    "tests.test_example_cli_smoke",
    "tests.test_persona_cli_smoke",
]

FULL_PIPELINE_COMMANDS = [
    [sys.executable, "run/pipeline/00_run_all.py"],
    [sys.executable, "run/cli/17_analysis_snapshot.py", "--compare-latest"],
    [sys.executable, "run/cli/16_persona_workbook_audit.py"],
]

CHANGED_PATH_RULES: list[tuple[tuple[str, ...], list[str]]] = [
    (("src/utils/",), ["validate-config", "test-unit"]),
    (("src/collectors/", "src/normalizers/"), ["validate-config", "test-fixture"]),
    (("src/filters/", "config/invalid_rules.yaml", "config/relevance_rules.yaml"), ["validate-config", "test-fixture"]),
    (("src/episodes/", "config/segmentation_rules.yaml"), ["validate-config", "test-fixture"]),
    (("src/labeling/", "config/labeling_policy.yaml", "config/codebook.yaml"), ["validate-config", "test-fixture"]),
    (("src/analysis/", "config/scoring.yaml", "config/pipeline_thresholds.yaml"), ["validate-schema", "test-fixture"]),
    (
        (
            "config/sources/",
            "config/seeds/",
            "config/time_window.yaml",
            "config/query_map.yaml",
            "config/query_seed_taxonomy.yaml",
            "config/query_expansion_rules.yaml",
        ),
        ["validate-config", "test-fixture"],
    ),
    (("run/pipeline/08_smoke_pipeline.py", "run/pipeline/00_run_all.py"), ["test-smoke"]),
    (("run/pipeline/",), ["test-smoke"]),
    (("tests/",), ["test-unit"]),
]


def main() -> None:
    """Dispatch one validation suite or a changed-path-based recommendation."""
    parser = argparse.ArgumentParser(description="Run targeted test and validation suites.")
    parser.add_argument(
        "command",
        choices=[
            "test-unit",
            "test-fixture",
            "test-smoke",
            "test-full",
            "test-changed",
            "validate-config",
            "validate-schema",
        ],
    )
    parser.add_argument("paths", nargs="*", help="Optional changed paths for test-changed.")
    args = parser.parse_args()

    if args.command == "validate-config":
        _run_command([sys.executable, "run/devtools/validate_config.py"])
        return
    if args.command == "validate-schema":
        _run_command([sys.executable, "run/devtools/validate_schema.py"])
        return
    if args.command == "test-unit":
        _run_unittest_modules(UNIT_TEST_MODULES)
        return
    if args.command == "test-fixture":
        _run_unittest_modules(FIXTURE_TEST_MODULES)
        return
    if args.command == "test-smoke":
        _run_command([sys.executable, "run/devtools/validate_config.py"])
        _run_command([sys.executable, "run/devtools/validate_schema.py"])
        _run_unittest_modules(CLI_SMOKE_TEST_MODULES)
        _run_command([sys.executable, "run/pipeline/08_smoke_pipeline.py"])
        return
    if args.command == "test-full":
        for command in FULL_PIPELINE_COMMANDS:
            _run_command(command)
        return
    _run_changed_suite(args.paths)


def _run_changed_suite(paths: list[str]) -> None:
    """Run the smallest reasonable validation suites for the given changed paths."""
    changed_paths = paths or _git_changed_paths()
    if not changed_paths:
        print("No changed paths detected; running unit tests.")
        _run_unittest_modules(UNIT_TEST_MODULES)
        return

    selected: list[str] = []
    direct_test_modules = [
        _path_to_test_module(path)
        for path in changed_paths
        if path.startswith("tests/") and _path_to_test_module(path)
    ]
    for path in changed_paths:
        for prefixes, commands in CHANGED_PATH_RULES:
            if any(path.startswith(prefix) for prefix in prefixes):
                selected.extend(commands)

    if direct_test_modules:
        print("Running directly changed test modules:")
        for module in direct_test_modules:
            print(f"- {module}")
        _run_unittest_modules(_dedupe(direct_test_modules))

    ordered_commands = _dedupe(selected) or ["test-unit"]
    print("Changed-path validation plan:")
    for command in ordered_commands:
        print(f"- {command}")
    for command in ordered_commands:
        if command == "validate-config":
            _run_command([sys.executable, "run/devtools/validate_config.py"])
        elif command == "validate-schema":
            _run_command([sys.executable, "run/devtools/validate_schema.py"])
        elif command == "test-unit":
            _run_unittest_modules(UNIT_TEST_MODULES)
        elif command == "test-fixture":
            _run_unittest_modules(FIXTURE_TEST_MODULES)
        elif command == "test-smoke":
            _run_command([sys.executable, "run/devtools/validate_config.py"])
            _run_command([sys.executable, "run/devtools/validate_schema.py"])
            _run_unittest_modules(CLI_SMOKE_TEST_MODULES)
            _run_command([sys.executable, "run/pipeline/08_smoke_pipeline.py"])


def _run_unittest_modules(modules: list[str]) -> None:
    """Run unittest for the provided modules."""
    _run_command([sys.executable, "-m", "unittest", *modules])


def _run_command(command: list[str]) -> None:
    """Run one subprocess command in the repo root."""
    printable = " ".join(command)
    print(f"> {printable}")
    completed = subprocess.run(command, cwd=ROOT, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def _git_changed_paths() -> list[str]:
    """Return changed tracked and untracked paths in a compact form."""
    commands = [
        ["git", "diff", "--name-only", "HEAD"],
        ["git", "ls-files", "--others", "--exclude-standard"],
    ]
    paths: list[str] = []
    for command in commands:
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            continue
        paths.extend(line.strip().replace("\\", "/") for line in completed.stdout.splitlines() if line.strip())
    return _dedupe(paths)


def _path_to_test_module(path: str) -> str:
    """Convert a test file path to a unittest module when applicable."""
    if not path.startswith("tests/") or not path.endswith(".py"):
        return ""
    return path[:-3].replace("/", ".")


def _dedupe(values: list[str]) -> list[str]:
    """Deduplicate while preserving order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


if __name__ == "__main__":
    main()
