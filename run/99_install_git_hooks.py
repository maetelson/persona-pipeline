"""Install local git hooks for commit validation and auto-push after commit."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.git_sync import run_git


def main() -> None:
    """Point the repository to the tracked .githooks directory."""
    hooks_dir = ROOT / ".githooks"
    hooks_dir.mkdir(parents=True, exist_ok=True)
    for hook_name in ["_run_python.sh", "commit-msg", "post-commit"]:
        hook_path = hooks_dir / hook_name
        if hook_path.exists():
            hook_path.chmod(0o755)

    config_result = run_git(["config", "core.hooksPath", str(hooks_dir)], ROOT)
    if config_result.returncode != 0:
        raise SystemExit(config_result.stderr.strip() or "Failed to set core.hooksPath.")

    auto_setup = run_git(["config", "push.autoSetupRemote", "true"], ROOT)
    if auto_setup.returncode != 0:
        raise SystemExit(auto_setup.stderr.strip() or "Failed to enable push.autoSetupRemote.")

    print(f"Installed git hooks from {hooks_dir}")


if __name__ == "__main__":
    main()
