#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FSI 综合分析。"""

import argparse
import os
import shlex
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
    parser = argparse.ArgumentParser(description="FSI 综合分析")
    parser.add_argument("--action", required=True, choices=["query", "report", "compare", "screen", "news", "finance"])
    parser.add_argument("--type", default="stock", choices=["stock", "index", "etf"])
    parser.add_argument("--code")
    parser.add_argument("--codes", nargs="*")
    parser.add_argument("--days", type=int, default=20)
    parser.add_argument("--screen-args", default="")
    args = parser.parse_args()

    fsi = find_fsi()

    if args.action == "query":
        assert args.code, "query 需要 --code"
        cmd = [fsi, "query", args.type, args.code, "--days", str(args.days), "--indicators"]
    elif args.action == "report":
        assert args.code, "report 需要 --code"
        cmd = [fsi, "report", args.code]
    elif args.action == "compare":
        assert args.codes and len(args.codes) >= 2, "compare 需要至少 2 个 --codes"
        cmd = [fsi, "compare"] + args.codes + ["--days", str(args.days)]
    elif args.action == "screen":
        cmd = [fsi, "screen"] + shlex.split(args.screen_args)
    elif args.action == "news":
        assert args.code, "news 需要 --code"
        cmd = [fsi, "news", args.code]
    elif args.action == "finance":
        assert args.code, "finance 需要 --code"
        cmd = [fsi, "finance", args.code]
    else:
        sys.exit(1)

    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
