#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
# colcon-cmake passes its own make job count unless MAKEFLAGS supplies one.
export MAKEFLAGS="${MAKEFLAGS:+$MAKEFLAGS }-j$CMAKE_BUILD_PARALLEL_LEVEL -l$CMAKE_BUILD_PARALLEL_LEVEL"
if [[ ! -x "$PROJECT_ROOT/.venv/bin/colcon" ]]; then
    echo "Run ./scripts/bootstrap_sdk.sh first." >&2
    exit 1
fi
"$PROJECT_ROOT/.venv/bin/python" "$PROJECT_ROOT/scripts/check_sdk.py"
if [[ "$(uname -s)" == Darwin ]] && command -v brew >/dev/null; then
    openssl_prefix="$(brew --prefix openssl@3 2>/dev/null || true)"
    if [[ -n "$openssl_prefix" && -d "$openssl_prefix" ]]; then
        export OPENSSL_ROOT_DIR="$openssl_prefix"
    fi
fi
cd "$PROJECT_ROOT"
"$PROJECT_ROOT/.venv/bin/colcon" --log-base "$PROJECT_ROOT/out/sdk/log" build \
    --metas "$PROJECT_ROOT/sdk/colcon.meta" \
    --base-paths "$PROJECT_ROOT/sdk/src" \
    --build-base "$PROJECT_ROOT/out/sdk/build" \
    --install-base "$PROJECT_ROOT/out/sdk/install" \
    --merge-install --executor sequential --packages-up-to fastdds \
    --event-handlers console_cohesion+ desktop_notification- \
    --cmake-args \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_TESTING=OFF \
    "-DCMAKE_PROJECT_INCLUDE=$PROJECT_ROOT/sdk/cmake/macos_install_rpath.cmake" "$@"

