#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FSI Elliott 波浪分析。"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

DEFAULT_THRESHOLD = {"stock": 5, "index": 3, "etf": 3}


def find_fsi():
    """按优先级查找 fsi: venv标记 → skill同级venv → PATH → 自动安装。"""
    marker = Path.home() / ".claude" / "skills" / ".fsi-venv-path"
    if marker.exists():
        venv = Path(marker.read_text().strip())
        fsi = venv / "bin" / "fsi"
        if fsi.exists():
            return str(fsi)
    repo_venv = Path(__file__).resolve().parent.parent.parent / ".venv" / "bin" / "fsi"
    if repo_venv.exists():
        return str(repo_venv)
    path = shutil.which("fsi")
    if path:
        return path
    repo_root = Path(__file__).resolve().parent.parent.parent
    pkg_dir = repo_root / "fsi-pkg"
    if pkg_dir.exists():
        print("fsi 未找到，自动创建 venv 并安装...", file=sys.stderr)
        venv_dir = repo_root / ".venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
        pip = str(venv_dir / "bin" / "pip")
        subprocess.run([pip, "install", "-q", str(pkg_dir)], check=True)
        fsi = venv_dir / "bin" / "fsi"
        if fsi.exists():
            return str(fsi)
    print("Error: fsi 未安装。请运行 bash install.sh", file=sys.stderr)
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="FSI 波浪分析")
    parser.add_argument("--type", required=True, choices=["stock", "index", "etf"])
    parser.add_argument("--code", required=True)
    parser.add_argument("--days", type=int, default=120)
    parser.add_argument("--threshold", type=int, default=0)
    args = parser.parse_args()

    fsi = find_fsi()
    threshold = args.threshold or DEFAULT_THRESHOLD[args.type]
    cmd = [fsi, "wave", args.type, args.code, "-d", str(args.days), "-t", str(threshold)]

    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
