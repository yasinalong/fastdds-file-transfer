#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
generator="${DDS_FASTDDSGEN:-$root/out/tools/Fast-DDS-Gen/scripts/fastddsgen}"
if [[ ! -x "$generator" ]]; then
    echo "Prepare Fast DDS-Gen 4.3.0; see docs/build.md." >&2
    exit 1
fi
version="$("$generator" -version)"
if [[ "$version" != *"4.3.0"* ]]; then
    echo "Expected Fast DDS-Gen 4.3.0, got: $version" >&2
    exit 1
fi
mkdir -p "$root/apps/file_transfer/generated"
cd "$root/apps/file_transfer/idl"
"$generator" -replace -d "$root/apps/file_transfer/generated" FileTransfer.idl
