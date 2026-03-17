#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FSI 数据拉取。"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def find_fsi():
    """按优先级查找 fsi: venv标记 → skill同级venv → PATH → 自动安装。"""
    # 1) install.sh 写入的 venv 路径标记
    marker = Path.home() / ".claude" / "skills" / ".fsi-venv-path"
    if marker.exists():
        venv = Path(marker.read_text().strip())
        fsi = venv / "bin" / "fsi"
        if fsi.exists():
            return str(fsi)
    # 2) 与 skill repo 同级的 .venv
    repo_venv = Path(__file__).resolve().parent.parent.parent / ".venv" / "bin" / "fsi"
    if repo_venv.exists():
        return str(repo_venv)
    # 3) PATH
    path = shutil.which("fsi")
    if path:
        return path
    # 4) 自动从 bundled fsi-pkg 安装到 repo venv
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", required=True, choices=["list", "indices", "stock", "finance", "stock_all", "check-network"])
    parser.add_argument("--codes", nargs="*", default=[])
    args = parser.parse_args()

    fsi = find_fsi()
    if args.action == "check-network":
        cmd = [fsi, "check-network"]
    elif args.action in ("list", "indices"):
        cmd = [fsi, "fetch", args.action]
    else:
        if not args.codes:
            print(f"Error: --action {args.action} 需要 --codes", file=sys.stderr)
            sys.exit(1)
        cmd = [fsi, "fetch", args.action] + args.codes

    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
