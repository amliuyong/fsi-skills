# FSI Skills

用自然语言和 Claude 聊天就能完成 A 股数据分析 — 拉数据、看指标、盯盘、画图、数浪，全部自动搞定。

**FSI Skills** 是一套 [Claude Code](https://docs.anthropic.com/en/docs/claude-code) Skills，基于 [FSI](fsi-pkg/)（Financial Stock Intelligence）构建。它让 Claude 理解你的股市分析意图，自动调用正确的工具完成任务。你只需要像平时聊天一样说话：

- "帮我看看茅台的技术指标" → 自动拉取数据、计算 MACD/KDJ/RSI/布林带，给出趋势判断
- "今天大盘怎么样？" → 根据当前时段自动选择盘前/盘中/盘后分析
- "对比一下银行股，哪个更强？" → 多维度对比 + AI 排名

**完全自包含** — FSI 源码已打包在 `fsi-pkg/` 中，一键安装，无需外部依赖。数据来源涵盖 AKShare、Yahoo Finance、腾讯行情等多个源，自动选择最优可用源。

## 核心能力

| Skill | 功能 | 你可以这样说 |
|-------|------|-------------|
| **fsi-fetch** | 数据拉取与同步 | "拉一下平安银行的数据"、"同步全量数据"、"检测数据源" |
| **fsi-analysis** | 技术分析与综合报告 | "看看茅台技术指标"、"出份报告"、"对比这几只股票"、"筛选超卖股" |
| **fsi-market** | 市场全时段覆盖 | "今天大盘怎么样"、"盘前看看"、"有什么新闻"、"资金流向" |
| **fsi-chart** | K 线图与分时图 | "画个 K 线图"、"看看走势"、"分时图" |
| **fsi-wave** | Elliott 波浪分析 | "现在第几浪"、"波浪结构分析" |

## 系统要求

- Python >= 3.11
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
- 网络连接（用于拉取行情数据）

## 安装

一行命令，无需 git clone，无残留文件：

```bash
bash <(curl -sSL https://raw.githubusercontent.com/amliuyong/fsi-skills/main/install.sh)
```

脚本会自动：
1. 从 GitHub 下载所需文件到临时目录
2. 创建 Python 虚拟环境（`~/.fsi/venv/`）
3. 安装 FSI 及所有依赖
4. 交互式选择安装位置 → 复制 skills
5. 清理临时文件，不留任何残留

安装时会提示选择：

```
请选择 Skills 安装位置：
  1) 全局安装 → ~/.claude/skills/（所有项目共享）
  2) 项目安装 → 指定项目目录 .claude/skills/（仅该项目）
```

| 选项 | 安装位置 | 作用范围 |
|------|----------|----------|
| 1 | `~/.claude/skills/` | 全局，所有项目共享 |
| 2 | `<项目>/.claude/skills/` | 仅该项目可用 |

### 验证安装

```bash
# 确认 fsi 命令可用（需先激活 venv）
source ~/.fsi/venv/bin/activate
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
└── skills/                 # 所有 Skills
    ├── fsi-fetch/          # 数据拉取
    │   ├── SKILL.md
    │   └── scripts/fsi_fetch.py
    ├── fsi-analysis/       # 综合分析
    │   ├── SKILL.md
    │   └── scripts/fsi_analysis.py
    ├── fsi-market/         # 市场总览
    │   ├── SKILL.md
    │   └── scripts/fsi_market.py
    ├── fsi-chart/          # K 线图表
    │   ├── SKILL.md
    │   └── scripts/fsi_chart.py
    └── fsi-wave/           # 波浪分析
        ├── SKILL.md
        └── scripts/fsi_wave.py
```

## 使用示例

安装后在 Claude Code 中自然对话即可自动触发：

### 数据拉取（fsi-fetch）

```
> 拉一下平安银行的数据
> 帮我同步一下茅台和招商银行的全量数据
> 更新一下股票列表
> 拉取 6 大指数日线
> 检测一下数据源是否正常
```

### 综合分析（fsi-analysis）

```
> 帮我看看茅台的技术指标
> 给我出一份 000001 的综合分析报告
> 对比一下茅台、五粮液和泸州老窖，哪个更强？
> 帮我筛选 RSI 低于 30 的超卖股票
> 平安银行最近有什么新闻？
> 看看比亚迪的财报怎么样
```

### 市场总览（fsi-market）

```
> 今天大盘怎么样？
> 盘前看看外盘情况
> 现在行情如何？哪些板块在涨？
> 收盘了，帮我复盘一下
> 最近有什么重要新闻？
> 这周市场回顾一下
> 看看北向资金流向和 QVIX
```

### K 线图表（fsi-chart）

```
> 画一下上证指数的 K 线图
> 看看茅台最近 120 天的走势
> 帮我画个沪深 300 ETF 的图
> 平安银行今天的分时图
```

### 波浪分析（fsi-wave）

```
> 000001 目前在第几浪？
> 分析一下茅台的波浪结构
> 上证指数的波浪走到哪了？
> 用大一点的阈值看看长期浪型
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
