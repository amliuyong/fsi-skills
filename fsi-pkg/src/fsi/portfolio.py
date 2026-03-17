"""持仓配置加载 + 共享工具函数（mid / pos / eod 三命令复用）"""

import json
from pathlib import Path

import requests

from fsi.config import FSI_DIR, MAJOR_INDICES
from fsi.cli.quote import _parse_tencent_quote


# ---------------------------------------------------------------------------
# 持仓加载
# ---------------------------------------------------------------------------

PORTFOLIO_PATH = FSI_DIR / "portfolio.json"


def load_portfolio() -> list[dict]:
    """读取 data/portfolio.json，文件不存在返回空列表。"""
    if not PORTFOLIO_PATH.exists():
        return []
    with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("positions", [])


# ---------------------------------------------------------------------------
# 指数实时行情（腾讯 API）
# ---------------------------------------------------------------------------

def fetch_index_quote(code: str, name: str = "") -> dict | None:
    """获取指数实时行情。指数前缀规则与个股不同。"""
    # 指数前缀：000xxx / 880xxx → sh，399xxx → sz
    if code.startswith("399"):
        prefix = "sz"
    else:
        prefix = "sh"
    symbol = f"{prefix}{code}"

    try:
        r = requests.get(f"https://qt.gtimg.cn/q={symbol}", timeout=10)
        r.raise_for_status()
    except Exception:
        return None

    parts = r.text.split("~")
    if len(parts) < 50:
        return None

    def safe_float(idx):
        try:
            return float(parts[idx])
        except (ValueError, IndexError):
            return None

    return {
        "code": code,
        "name": name or parts[1],
        "price": safe_float(3),
        "pct_change": safe_float(32),
        "change_amt": safe_float(31),
        "open": safe_float(5),
        "high": safe_float(33),
        "low": safe_float(34),
        "prev_close": safe_float(4),
        "volume": safe_float(6),
        "amount": safe_float(37),
        "time": parts[30] if len(parts) > 30 else None,
    }


def fetch_all_index_quotes() -> list[dict]:
    """获取主要指数（MAJOR_INDICES）的实时行情。"""
    results = []
    for code, name in MAJOR_INDICES.items():
        q = fetch_index_quote(code, name)
        if q:
            results.append(q)
    return results


# ---------------------------------------------------------------------------
# DB 新闻读取
# ---------------------------------------------------------------------------

def load_news_from_db(conn, code: str, limit: int = 5) -> list[dict]:
    """从 stock_news 表读取已存新闻，返回中文键字典列表。"""
    df = conn.execute(
        "SELECT title, content, source, pub_time FROM stock_news "
        "WHERE code = ? ORDER BY pub_time DESC LIMIT ?",
        [code, limit],
    ).fetchdf()
    if df.empty:
        return []
    items = []
    for _, row in df.iterrows():
        items.append({
            "标题": row.get("title", ""),
            "内容": row.get("content", ""),
            "来源": row.get("source", ""),
            "时间": str(row.get("pub_time", "")),
        })
    return items


# ---------------------------------------------------------------------------
# DB 财报读取
# ---------------------------------------------------------------------------

def load_finance_from_db(conn, code: str, limit: int = 2) -> list[dict]:
    """从 stock_finance 表读取财报，返回中文键字典列表。"""
    from fsi.cli.finance import load_finance_from_db as _load
    return _load(conn, code, limit)


# ---------------------------------------------------------------------------
# 持仓行情增强
# ---------------------------------------------------------------------------

def enrich_portfolio_with_quotes(positions: list[dict], conn) -> list[dict]:
    """为每个持仓获取实时行情，查 stock_list 获取名称/行业，计算盈亏。"""
    enriched = []
    for pos in positions:
        code = pos["code"]
        shares = pos["shares"]
        cost = pos["cost"]

        # 实时行情
        quote = _parse_tencent_quote(code)
        if not quote or quote.get("price") is None:
            enriched.append({
                "code": code,
                "shares": shares,
                "cost": cost,
                "error": "行情获取失败",
            })
            continue

        # 从 stock_list 查名称和行业，ETF 从 etf_daily 查
        info = conn.execute(
            "SELECT name, industry FROM stock_list WHERE code = ?", [code]
        ).fetchone()
        if not info:
            etf_info = conn.execute(
                "SELECT DISTINCT name FROM etf_daily WHERE code = ? LIMIT 1", [code]
            ).fetchone()
            name = etf_info[0] if etf_info else quote.get("name", "")
            industry = "ETF"
        else:
            name = info[0] if info else quote.get("name", "")
            industry = info[1] if info else ""

        price = quote["price"]
        cost_total = cost * shares
        market_value = price * shares
        profit = market_value - cost_total
        profit_pct = round((price - cost) / cost * 100, 2) if cost > 0 else 0

        enriched.append({
            "code": code,
            "name": name,
            "industry": industry,
            "shares": shares,
            "cost": cost,
            "price": price,
            "pct_change": quote.get("pct_change"),
            "cost_total": round(cost_total, 2),
            "market_value": round(market_value, 2),
            "profit": round(profit, 2),
            "profit_pct": profit_pct,
            "turnover": quote.get("turnover"),
            "volume": quote.get("volume"),
            "pe": quote.get("pe"),
            "pb": quote.get("pb"),
        })
    return enriched


# ---------------------------------------------------------------------------
# 组合汇总
# ---------------------------------------------------------------------------

def calc_portfolio_summary(enriched: list[dict]) -> dict:
    """汇总总成本、总市值、总盈亏、总收益率。"""
    total_cost = 0.0
    total_market_value = 0.0
    valid_count = 0

    for item in enriched:
        if "error" in item:
            continue
        total_cost += item["cost_total"]
        total_market_value += item["market_value"]
        valid_count += 1

    total_profit = total_market_value - total_cost
    total_profit_pct = round(total_profit / total_cost * 100, 2) if total_cost > 0 else 0

    return {
        "total_positions": len(enriched),
        "valid_positions": valid_count,
        "total_cost": round(total_cost, 2),
        "total_market_value": round(total_market_value, 2),
        "total_profit": round(total_profit, 2),
        "total_profit_pct": total_profit_pct,
    }
