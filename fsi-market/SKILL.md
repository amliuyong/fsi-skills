---
name: fsi-market
description: "A 股市场总览 - 盘前速览、盘中速报、盘后复盘、实时快讯热点、新闻回顾、资金流向。当用户想了解今天大盘怎么样、市场行情、资金面、盘前/盘中/盘后情况时使用。覆盖：早盘预判、盘中实况、收盘总结、突发新闻、新闻回顾、资金流向和恐慌指数。"
---

# FSI 市场总览

A 股市场全时段覆盖：盘前、盘中、盘后、热点新闻、历史回顾和资金流向。

## 前置条件

- 已安装 FSI（`pip install fsi`），`fsi` 命令在 PATH 中可用
- 需要网络连接（实时数据）

## 用法

```bash
# 盘前速览（隔夜外盘 + 今日预判）
python3 {baseDir}/scripts/fsi_market.py --action am

# 盘中速报（实时指数 + 资金 + 行业）
python3 {baseDir}/scripts/fsi_market.py --action now

# 盘后复盘（全天回顾 + 后市展望）
python3 {baseDir}/scripts/fsi_market.py --action pm

# 自动选择（根据北京时间：<9:30→am, 9:30-15:00→now, >15:00→pm）
python3 {baseDir}/scripts/fsi_market.py --action auto

# 实时快讯 + 市场热点 + 经济日历
python3 {baseDir}/scripts/fsi_market.py --action hot

# 近 N 天新闻回顾 + AI 综合研判
python3 {baseDir}/scripts/fsi_market.py --action digest --days 3 --limit 50
python3 {baseDir}/scripts/fsi_market.py --action digest --days 7 --limit 100

# 资金流向 + QVIX + 美港指数
python3 {baseDir}/scripts/fsi_market.py --action flow
```

## 参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--action` | 是 | - | `am` / `now` / `pm` / `auto` / `hot` / `digest` / `flow` |
| `--days` | 否 | `3` | digest 回顾天数 |
| `--limit` | 否 | `50` | digest 新闻条数上限 |

## 意图识别

| 用户说的 | action |
|----------|--------|
| "今天大盘怎么样" | `auto`（按时段自动选） |
| "盘前" / "早盘" | `am` |
| "现在行情" / "盘中" | `now` |
| "收盘" / "复盘" / "总结" | `pm` |
| "有什么新闻" / "快讯" | `hot` |
| "这周新闻" / "回顾" | `digest`（调整 --days） |
| "资金" / "北向" / "QVIX" | `flow` |

## 输出

JSON 格式含 AI 分析。重点：指数涨跌、板块轮动、资金流向、关键事件。
