#!/usr/bin/env bash
# FSI Skills 一键安装
# 1. 在 repo 内创建 .venv 并安装 FSI
# 2. 复制 skills 到 ~/.claude/skills/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

info()  { echo -e "\033[1;34m[INFO]\033[0m $*"; }
ok()    { echo -e "\033[1;32m[OK]\033[0m $*"; }
error() { echo -e "\033[1;31m[ERROR]\033[0m $*"; exit 1; }

# ─── 选择安装位置 ───
echo ""
echo "请选择 Skills 安装位置："
echo "  1) 全局安装 → ~/.claude/skills/（所有项目共享）"
echo "  2) 项目安装 → 当前目录 .claude/skills/（仅当前项目）"
echo ""
read -rp "请输入 1 或 2 [默认 1]: " choice
case "${choice:-1}" in
    1) SKILLS_DIR="$HOME/.claude/skills" ;;
    2) SKILLS_DIR="$(pwd)/.claude/skills" ;;
    *) error "无效选择：$choice" ;;
esac

# ─── Python ───
PYTHON=""
for cmd in python3 python; do
    command -v "$cmd" &>/dev/null && PYTHON="$cmd" && break
done
[ -z "$PYTHON" ] && error "未找到 Python3"

# ─── 检查 Python 版本 ───
PY_VERSION=$($PYTHON -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJOR=$($PYTHON -c 'import sys; print(sys.version_info.major)')
PY_MINOR=$($PYTHON -c 'import sys; print(sys.version_info.minor)')
if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 11 ]; }; then
    error "需要 Python >= 3.11，当前为 $PY_VERSION"
fi
info "Python $PY_VERSION"

# ─── 创建 venv + 安装 FSI ───
info "创建 venv → $VENV_DIR"
$PYTHON -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

info "安装 FSI（从 fsi-pkg/）..."
pip install -q "$SCRIPT_DIR/fsi-pkg" || error "FSI 安装失败"
ok "FSI 安装完成: $(which fsi)"

# ─── 安装 Skills ───
info "安装 Skills → $SKILLS_DIR"
mkdir -p "$SKILLS_DIR"

count=0
for skill_dir in "$SCRIPT_DIR"/fsi-*/; do
    [ ! -f "$skill_dir/SKILL.md" ] && continue
    skill_name="$(basename "$skill_dir")"
    cp -r "$skill_dir" "$SKILLS_DIR/"
    chmod +x "$SKILLS_DIR/$skill_name"/scripts/*.py 2>/dev/null || true
    ok "  $skill_name"
    count=$((count + 1))
done
ok "$count 个 skills 已安装"

# ─── 写入 venv 路径供脚本使用 ───
echo "$VENV_DIR" > "$SKILLS_DIR/.fsi-venv-path"
ok "venv 路径已记录: $SKILLS_DIR/.fsi-venv-path"

echo ""
ok "完成！在 Claude Code 中试试："
echo "  > 帮我看看 000001 的技术指标"
echo "  > 今天大盘怎么样？"
echo ""
