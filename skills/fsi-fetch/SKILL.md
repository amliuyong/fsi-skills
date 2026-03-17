---
name: fsi-fetch
description: >-
  A 股数据拉取与同步 - 拉取股票列表、指数日线、个股日线、财报数据，或一键全量同步。
  也包含网络健康检测（check-network），用于检测数据源可用性并生成最优源配置。
  当用户要求拉取数据、更新行情、同步股票信息、初始化数据库、检测网络/数据源时使用。
  即使用户只说了股票代码加"拉一下"/"更新"/"同步"，也应触发。
---

# FSI 数据拉取

根据用户请求拉取 A 股数据并同步到本地 DuckDB（`~/.fsi/data/market_data.duckdb`）。

## 前置条件

- 已安装 FSI，`fsi` 命令可用（脚本会自动从 bundled fsi-pkg 安装）

## 用法

```bash
# 检测数据源可用性（首次使用建议先跑一次）
python3 {baseDir}/scripts/fsi_fetch.py --action check-network

# 拉取 A 股列表
python3 {baseDir}/scripts/fsi_fetch.py --action list

# 拉取 6 大指数日线
python3 {baseDir}/scripts/fsi_fetch.py --action indices

# 拉取个股日线
python3 {baseDir}/scripts/fsi_fetch.py --action stock --codes 000001

# 拉取财报数据
python3 {baseDir}/scripts/fsi_fetch.py --action finance --codes 000001

# 一键全量（日线 + 新闻 + 财报）
python3 {baseDir}/scripts/fsi_fetch.py --action stock_all --codes 600938

# 多股全量
python3 {baseDir}/scripts/fsi_fetch.py --action stock_all --codes 600938 601398
```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| `--action` | 是 | `check-network` / `list` / `indices` / `stock` / `finance` / `stock_all` |
| `--codes` | 条件 | 6 位股票代码，`stock`/`finance`/`stock_all` 必填 |

## 首次使用流程

新环境建议按以下顺序初始化：

1. `check-network` — 检测各数据源（AKShare/Yahoo/Tencent 等）可用性，生成 `~/.fsi/data/api_health.json`
2. `list` — 拉取全市场 A 股列表
3. `indices` — 拉取 6 大主要指数日线
4. `stock` 或 `stock_all` — 拉取目标个股数据

## 意图识别

| 用户说的 | action | 说明 |
|----------|--------|------|
| "检测网络" / "检查数据源" / "check" | `check-network` | API 健康检测 |
| "拉取列表" / "股票列表" / "初始化" | `list` | 全市场列表 |
| "拉取指数" | `indices` | 6 大指数 |
| 只给了代码 | `stock` | 默认拉日线 |
| "全量" / "全部" / "所有数据" | `stock_all` | 日线+新闻+财报 |
| "财报" / "财务" | `finance` | 仅财报 |

## 数据存储

所有数据写入 `~/.fsi/data/market_data.duckdb`，其他 skill（fsi-analysis/fsi-chart/fsi-wave）从同一数据库读取。

## 输出

- 成功：stdout 输出拉取条数和同步状态
- 失败：stderr 输出错误信息，exit code 1
