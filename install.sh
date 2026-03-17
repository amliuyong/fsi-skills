#!/usr/bin/env bash
# FSI Skills 一键安装
# 用法：curl -sSL https://raw.githubusercontent.com/amliuyong/fsi-skills/main/install.sh | bash
# 也可 clone 后本地运行：bash install.sh
set -euo pipefail

REPO_URL="https://github.com/amliuyong/fsi-skills/archive/refs/heads/main.tar.gz"
FSI_HOME="$HOME/.fsi"

info()  { echo -e "\033[1;34m[INFO]\033[0m $*"; }
ok()    { echo -e "\033[1;32m[OK]\033[0m $*"; }
error() { echo -e "\033[1;31m[ERROR]\033[0m $*"; exit 1; }

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

# ─── 选择安装位置 ───
echo ""
echo "请选择 Skills 安装位置："
echo "  1) 全局安装 → ~/.claude/skills/（所有项目共享）"
echo "  2) 项目安装 → 指定项目目录 .claude/skills/（仅该项目）"
echo ""
read -rp "请输入 1 或 2 [默认 1]: " choice
INSTALL_MODE="${choice:-1}"
case "$INSTALL_MODE" in
    1) SKILLS_DIR="$HOME/.claude/skills" ;;
    2)
        read -rp "请输入项目路径 [默认当前目录]: " project_path
        project_path="${project_path:-.}"
        project_path="$(cd "$project_path" 2>/dev/null && pwd)" || error "路径不存在：$project_path"
        SKILLS_DIR="$project_path/.claude/skills"
        ;;
    *) error "无效选择：$INSTALL_MODE" ;;
esac

# ─── 下载源码（如果不在 repo 内） ───
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" 2>/dev/null && pwd || echo "")"
if [ -n "$SCRIPT_DIR" ] && [ -f "$SCRIPT_DIR/skills/fsi-fetch/SKILL.md" ]; then
    SRC_DIR="$SCRIPT_DIR"
    NEED_CLEANUP=false
else
    info "从 GitHub 下载..."
    TMP_DIR="$(mktemp -d)"
    curl -sSL "$REPO_URL" | tar -xz -C "$TMP_DIR"
    SRC_DIR="$TMP_DIR/fsi-skills-main"
    [ -f "$SRC_DIR/skills/fsi-fetch/SKILL.md" ] || error "下载失败"
    NEED_CLEANUP=true
    ok "下载完成"
fi

# ─── 创建 venv + 安装 FSI ───
VENV_DIR="$FSI_HOME/venv"
info "创建 venv → $VENV_DIR"
mkdir -p "$FSI_HOME"
$PYTHON -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

info "安装 FSI..."
pip install -q "$SRC_DIR/fsi-pkg" || error "FSI 安装失败"
ok "FSI 安装完成: $(which fsi)"

# ─── 安装 Skills ───
info "安装 Skills → $SKILLS_DIR"
mkdir -p "$SKILLS_DIR"

count=0
for skill_dir in "$SRC_DIR"/skills/fsi-*/; do
    [ ! -f "$skill_dir/SKILL.md" ] && continue
    skill_name="$(basename "$skill_dir")"
    target="$SKILLS_DIR/$skill_name"

    rm -rf "$target"
    cp -r "$skill_dir" "$target"
    chmod +x "$target"/scripts/*.py 2>/dev/null || true
    ok "  $skill_name"
    count=$((count + 1))
done
ok "$count 个 skills 已安装"

# ─── 写入 venv 路径供脚本使用 ───
echo "$VENV_DIR" > "$SKILLS_DIR/.fsi-venv-path"
ok "venv 路径已记录"

# ─── 清理临时文件 ───
if [ "$NEED_CLEANUP" = true ]; then
    rm -rf "$TMP_DIR"
    ok "临时文件已清理"
fi

echo ""
ok "完成！在 Claude Code 中试试："
echo "  > 帮我看看 000001 的技术指标"
echo "  > 今天大盘怎么样？"
echo ""
