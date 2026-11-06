#!/usr/bin/env python3
"""Fail before building if SDK checkouts do not match the pinned manifest."""
from pathlib import Path
import subprocess
import yaml

root = Path(__file__).resolve().parents[1]
manifest = yaml.safe_load((root / "sdk/fastdds.repos").read_text())
for name, spec in manifest["repositories"].items():
    repo = root / "sdk/src" / name
    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(repo), *args], text=True
        ).strip()
    expected = git("rev-parse", f"{spec['version']}^{{commit}}")
    actual = git("rev-parse", "HEAD")
    if actual != expected:
        raise SystemExit(f"{name}: expected {spec['version']}, found {actual}")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit(f"{name}: SDK source has local changes; review them before building")
    print(f"{name}: {spec['version']} ({actual[:12]})", flush=True)

