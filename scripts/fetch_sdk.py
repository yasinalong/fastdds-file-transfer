#!/usr/bin/env python3
"""Import only missing SDK repositories; never fetch or reset existing ones."""
from pathlib import Path
import subprocess
import sys
import yaml

root = Path(__file__).resolve().parents[1]
manifest = yaml.safe_load((root / "sdk/fastdds.repos").read_text())
missing = {}
for name, spec in manifest["repositories"].items():
    repo = root / "sdk/src" / name
    if not repo.exists():
        missing[name] = spec
    elif not (repo / ".git").exists():
        raise SystemExit(f"{repo} exists but is not a Git checkout; refusing to overwrite it")
if missing:
    subprocess.run(
        [str(Path(sys.executable).parent / "vcs"), "import", "--input", "-",
         str(root / "sdk/src")],
        input=yaml.safe_dump({"repositories": missing}), text=True, check=True,
    )
else:
    print("All SDK repositories are present; no import or fetch needed.", flush=True)

