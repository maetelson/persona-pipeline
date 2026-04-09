#!/bin/sh
set -eu

if command -v python >/dev/null 2>&1; then
  exec python "$@"
fi

if command -v py >/dev/null 2>&1; then
  exec py -3 "$@"
fi

if [ -x "/c/Python313/python.exe" ]; then
  exec /c/Python313/python.exe "$@"
fi

echo "[git-hook] Could not find a Python interpreter for hook execution." >&2
exit 1
