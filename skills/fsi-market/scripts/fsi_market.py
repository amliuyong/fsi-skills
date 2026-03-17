#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FSI 市场总览。"""

import argparse
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

BJT = timezone(timedelta(hours=8))


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

def auto_action():
    now = datetime.now(BJT)
    t = now.hour * 100 + now.minute
    if t < 930:
        return "am"
    elif t < 1500:
        return "now"
    return "pm"


def main():
    parser = argparse.ArgumentParser(description="FSI 市场总览")
    parser.add_argument("--action", required=True, choices=["am", "now", "pm", "hot", "digest", "flow", "auto"])
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    fsi = find_fsi()
    action = args.action
    if action == "auto":
        action = auto_action()
        print(f"Auto → {action}", file=sys.stderr)

    if action == "digest":
        cmd = [fsi, "digest", "--days", str(args.days), "--limit", str(args.limit)]
    else:
        cmd = [fsi, action]

    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
