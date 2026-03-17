#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FSI K 线图生成。"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


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
    parser = argparse.ArgumentParser(description="FSI K 线图")
    parser.add_argument("--type", required=True, choices=["stock", "index", "etf", "intraday"])
    parser.add_argument("--code", required=True)
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--output")
    parser.add_argument("--asset-type", choices=["stock", "index", "etf"])
    args = parser.parse_args()

    fsi = find_fsi()
    if args.type == "intraday":
        cmd = [fsi, "chart", "intraday", args.code]
        if args.asset_type:
            cmd += ["-t", args.asset_type]
    else:
        cmd = [fsi, "chart", args.type, args.code, "--days", str(args.days)]
    if args.output:
        cmd += ["-o", args.output]

    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
