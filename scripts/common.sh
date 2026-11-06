#!/usr/bin/env bash
# Shared by the executable scripts; do not source into your login shell.
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CMAKE_BUILD_PARALLEL_LEVEL="${DDS_BUILD_JOBS:-4}"

