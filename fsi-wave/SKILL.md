---
name: fsi-wave
description: "A 股 Elliott 波浪分析 - 基于 zigzag 算法识别波浪结构，判断当前所处浪型。当用户提到波浪理论、波浪分析、艾略特波浪、浪型判断、当前在第几浪时使用。支持个股/指数/ETF，可自定义天数和摆动阈值。"
---

# FSI 波浪分析

基于经典 zigzag 状态机算法进行 Elliott 波浪分析，识别摆动高低点，判断浪型结构。

## 前置条件

- 已安装 FSI（`pip install fsi`），`fsi` 命令在 PATH 中可用
- 已拉取相关证券数据

## 用法

```bash
# 个股波浪分析（默认 120 天，阈值 5%）
python3 {baseDir}/scripts/fsi_wave.py --type stock --code 000001

# 指数波浪分析（默认 120 天，阈值 3%）
python3 {baseDir}/scripts/fsi_wave.py --type index --code 000001

# ETF 波浪分析（默认 120 天，阈值 3%）
python3 {baseDir}/scripts/fsi_wave.py --type etf --code 510300

# 自定义天数和阈值
python3 {baseDir}/scripts/fsi_wave.py --type stock --code 600519 --days 250 --threshold 8
```

## 参数

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--type` | 是 | - | `stock` / `index` / `etf` |
| `--code` | 是 | - | 6 位证券代码 |
| `--days` | 否 | `120` | 分析天数 |
| `--threshold` | 否 | 按类型 | 摆动阈值%（stock=5, index/etf=3） |

阈值越小浪越多（细节），阈值越大浪越少（大趋势）。

## 意图识别

| 用户说的 | type |
|----------|------|
| "波浪" / "浪型" / "第几浪" | `stock`（默认） |
| "指数波浪" / "上证波浪" | `index` |
| "ETF 波浪" | `etf` |

## 输出

JSON 格式含：摆动点序列、浪型标注、当前浪位判断、AI 分析（目标位 + 风险提示）。
