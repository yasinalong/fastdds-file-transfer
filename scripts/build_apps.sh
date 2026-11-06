#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
if [[ ! -d "$PROJECT_ROOT/out/sdk/install/lib" ]]; then
    echo "Run ./scripts/build_sdk.sh first." >&2
    exit 1
fi
preset="${1:-dev}"
if [[ $# -gt 0 ]]; then shift; fi
cd "$PROJECT_ROOT"
cmake --preset "$preset"
cmake --build --preset "$preset" --parallel "$CMAKE_BUILD_PARALLEL_LEVEL" "$@"

