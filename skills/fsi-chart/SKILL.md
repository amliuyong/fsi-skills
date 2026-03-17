---
name: fsi-chart
description: >-
  A 股 K 线图生成 - 生成个股/指数/ETF 的 K 线图（蜡烛图 + 均线 + 成交量 + MACD）和分时图。
  当用户想看 K 线图、走势图、技术图形、分时走势时使用。
  支持自定义天数和输出目录。即使用户只说"画个图"或"看看走势"也应触发。
---

# FSI K 线图

生成专业 A 股 K 线图（蜡烛图 + MA5/10/20/60 均线 + 成交量 + MACD）或分时图，输出 PNG。

## 前置条件

- 已通过 `install.sh` 安装 FSI，`fsi` 命令可用（脚本会自动从 bundled fsi-pkg 安装）
- 已拉取相关证券数据

## 用法

```bash
# 个股 K 线图（默认 60 天）
python3 {baseDir}/scripts/fsi_chart.py --type stock --code 000001 --days 60

# 指数 K 线图
python3 {baseDir}/scripts/fsi_chart.py --type index --code 000001 --days 90

# ETF K 线图
python3 {baseDir}/scripts/fsi_chart.py --type etf --code 510300 --days 60

# 自定义天数和输出目录
python3 {baseDir}/scripts/fsi_chart.py --type stock --code 600519 --days 120 --output /tmp

# 分时图（自动检测资产类型）
python3 {baseDir}/scripts/fsi_chart.py --type intraday --code 000001

# 分时图（强制指定为股票，000001 = 平安银行而非上证指数）
python3 {baseDir}/scripts/fsi_chart.py --type intraday --code 000001 --asset-type stock
```

## 参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--type` | 是 | - | `stock` / `index` / `etf` / `intraday` |
| `--code` | 是 | - | 6 位证券代码 |
| `--days` | 否 | `60` | K 线天数（分时图忽略） |
| `--output` | 否 | `data/charts/` | 输出目录 |
| `--asset-type` | 否 | 自动检测 | 分时图强制资产类型 |

## 意图识别

| 用户说的 | type |
|----------|------|
| "K 线" / "日 K" / "走势图" | `stock`（默认） |
| "指数图" / "上证 K 线" | `index` |
| "ETF 图" | `etf` |
| "分时" / "今天走势" / "日内" | `intraday` |

## 输出

- 生成 PNG 文件，路径在 stdout 中输出
- 生成后应读取并展示图片给用户
