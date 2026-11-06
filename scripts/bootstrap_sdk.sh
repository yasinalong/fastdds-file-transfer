#!/usr/bin/env bash
# Install workspace-local tooling and fetch the pinned SDK dependencies.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
command -v git >/dev/null
command -v cmake >/dev/null
export PIP_CACHE_DIR="$PROJECT_ROOT/out/cache/pip"
mkdir -p "$PROJECT_ROOT/sdk/src"
python3 -m venv "$PROJECT_ROOT/.venv"
"$PROJECT_ROOT/.venv/bin/python" -m pip install \
    -r "$PROJECT_ROOT/sdk/requirements.txt" \
    -c "$PROJECT_ROOT/sdk/requirements.lock.txt"
"$PROJECT_ROOT/.venv/bin/python" "$PROJECT_ROOT/scripts/fetch_sdk.py"
"$PROJECT_ROOT/.venv/bin/python" "$PROJECT_ROOT/scripts/check_sdk.py"
git -C "$PROJECT_ROOT/sdk/src/Fast-DDS" submodule update --init \
    thirdparty/asio thirdparty/tinyxml2

