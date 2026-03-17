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

## 系统要求

- Python >= 3.11
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
- 网络连接（用于拉取行情数据）

## 一键安装

```bash
git clone https://github.com/amliuyong/fsi-skills.git
cd fsi-skills
bash install.sh
```

`install.sh` 会自动：
1. 创建 Python 虚拟环境（`.venv/`）
2. 从本地 `fsi-pkg/` 安装 FSI 及所有依赖
3. 复制所有 skills 到 `~/.claude/skills/`
4. 记录 venv 路径供 skills 脚本使用

> **提示**：如果使用 SSH 方式克隆，可替换为：
> ```bash
> git clone git@github.com:amliuyong/fsi-skills.git
> ```

### 安装单个 skill

```bash
# 先安装 FSI
pip install ./fsi-pkg

# 再复制需要的 skill
cp -r fsi-chart ~/.claude/skills/
```

### 验证安装

```bash
# 确认 fsi 命令可用
fsi --help

# 确认 skills 已就位
ls ~/.claude/skills/fsi-*/SKILL.md
```

## 目录结构

```
fsi-skills/
├── install.sh              # 一键安装脚本
├── fsi-pkg/                # FSI 源码（自包含）
│   ├── pyproject.toml
│   └── src/fsi/
├── fsi-fetch/              # 数据拉取
│   ├── SKILL.md
│   └── scripts/fsi_fetch.py
├── fsi-analysis/           # 综合分析
│   ├── SKILL.md
│   └── scripts/fsi_analysis.py
├── fsi-market/             # 市场总览
│   ├── SKILL.md
│   └── scripts/fsi_market.py
├── fsi-chart/              # K 线图表
│   ├── SKILL.md
│   └── scripts/fsi_chart.py
└── fsi-wave/               # 波浪分析
    ├── SKILL.md
    └── scripts/fsi_wave.py
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

## 数据存储

所有数据存储在 `~/.fsi/data/market_data.duckdb`（DuckDB），5 个 skills 共享同一数据库。

首次使用建议按顺序初始化：
1. **检测网络** — `check-network`，检测数据源可用性
2. **拉取列表** — `list`，全市场 A 股列表
3. **拉取指数** — `indices`，6 大主要指数日线
4. **拉取个股** — `stock` 或 `stock_all`，目标个股数据

## License

MIT
