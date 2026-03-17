# FSI Skills

A 股数据分析 [Claude Code](https://docs.anthropic.com/en/docs/claude-code) Skills 集合，基于 [FSI](fsi-pkg/)（Financial Stock Intelligence）。

**完全自包含** — FSI 源码已打包在 `fsi-pkg/` 中，无需外部依赖。

## Skills 列表

| Skill | 功能 | 触发场景 |
|-------|------|----------|
| **fsi-fetch** | 数据拉取 | 拉取股票列表、日线、财报、全量同步 |
| **fsi-analysis** | 综合分析 | 技术指标、综合报告、多股对比、条件筛选、新闻、财报 |
| **fsi-market** | 市场总览 | 盘前/盘中/盘后、热点快讯、新闻回顾、资金流向 |
| **fsi-chart** | K 线图表 | 生成个股/指数/ETF K 线图和分时图 |
| **fsi-wave** | 波浪分析 | Elliott 波浪理论分析，浪型识别 |

## 一键安装

```bash
git clone https://github.com/yonmzn/fsi-skills.git
cd fsi-skills
bash install.sh
```

`install.sh` 会自动：
1. 从本地 `fsi-pkg/` 安装 FSI（`pip install ./fsi-pkg`）
2. 复制所有 skills 到 `~/.claude/skills/`

### 安装单个 skill

```bash
# 先安装 FSI
pip install ./fsi-pkg

# 再复制需要的 skill
cp -r fsi-chart ~/.claude/skills/
```

## 目录结构

```
fsi-skills/
├── install.sh              # 一键安装脚本
├── fsi-pkg/                # FSI 源码（自包含）
│   ├── pyproject.toml
│   └── src/fsi/
├── fsi-fetch/              # 5 个 skill
│   ├── SKILL.md
│   └── scripts/fsi_fetch.py
├── fsi-analysis/
├── fsi-market/
├── fsi-chart/
└── fsi-wave/
```

## 使用示例

安装后在 Claude Code 中自然对话即可自动触发：

```
> 拉一下平安银行的数据
  → fsi fetch stock 000001

> 帮我看看茅台的技术指标
  → fsi query stock 600519 --days 20 --indicators

> 今天大盘怎么样？
  → 根据时段自动选择 fsi am / now / pm

> 画一下上证指数的 K 线图
  → fsi chart index 000001 --days 60

> 000001 目前在第几浪？
  → fsi wave stock 000001

```

## 系统要求

- Python >= 3.11
- Claude Code

## License

MIT
