---
name: fsi-analysis
description: >-
  A 股综合分析 - 技术指标查询、综合报告、多股对比、条件筛选、个股新闻、财务解读。
  当用户想分析一只股票（技术面/基本面/新闻）、对比多只股票、筛选股票时使用。
  覆盖场景：查技术指标、看报告、对比排名、条件选股、查新闻、看财报。
  即使用户只说"帮我看看 000001"也应触发。
---

# FSI 综合分析

A 股查询与分析工具集，覆盖技术指标、综合报告、多股对比、条件筛选、个股新闻和财务解读。

## 前置条件

- 已安装 FSI（`pip install fsi`），`fsi` 命令在 PATH 中可用
- 已拉取相关股票数据（可配合 fsi-fetch skill）

## 用法

```bash
# 技术指标查询（个股/指数/ETF）
python3 {baseDir}/scripts/fsi_analysis.py --action query --type stock --code 000001 --days 20
python3 {baseDir}/scripts/fsi_analysis.py --action query --type index --code 000001 --days 20
python3 {baseDir}/scripts/fsi_analysis.py --action query --type etf --code 510300 --days 20

# 综合报告（技术面+新闻+财报）
python3 {baseDir}/scripts/fsi_analysis.py --action report --code 000001

# 多股对比 + AI 排名
python3 {baseDir}/scripts/fsi_analysis.py --action compare --codes 000001 600519 --days 20

# 条件筛选 + AI 点评
python3 {baseDir}/scripts/fsi_analysis.py --action screen --screen-args "--rsi-below 30 --min-turnover 2"

# 个股新闻 + AI 情感分析
python3 {baseDir}/scripts/fsi_analysis.py --action news --code 000001

# 财务指标 + AI 解读
python3 {baseDir}/scripts/fsi_analysis.py --action finance --code 000001
```

## 参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--action` | 是 | - | `query` / `report` / `compare` / `screen` / `news` / `finance` |
| `--type` | 否 | `stock` | 资产类型：`stock` / `index` / `etf`（仅 query） |
| `--code` | 条件 | - | 6 位代码（query/report/news/finance 必填） |
| `--codes` | 条件 | - | 多个代码（compare 必填，至少 2 个） |
| `--days` | 否 | `20` | 查询天数 |
| `--screen-args` | 条件 | - | screen 筛选参数字符串 |

## 意图识别

| 用户说的 | action | type |
|----------|--------|------|
| "看看 000001" / "技术指标" | `query` | 按上下文判断 |
| "报告" / "详细分析" / "深度" | `report` | - |
| "对比" / "比较" / "哪个好" | `compare` | - |
| "筛选" / "超卖" / "RSI" | `screen` | - |
| "新闻" / "消息" / "舆情" | `news` | - |
| "财报" / "财务" / "利润" | `finance` | - |
| "指数" / "上证" / "创业板" | `query` | `index` |
| "ETF" / "基金" | `query` | `etf` |

## 输出

JSON 格式含 AI 分析。重点展示趋势判断、关键价位、买卖信号、风险提示。
