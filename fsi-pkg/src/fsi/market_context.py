"""市场上下文组装 — 供各 CLI 命令共用

单一入口函数，所有 API 调用 graceful 处理，失败返回空 dict 不影响调用方。
使用线程池并行获取以减少总耗时。
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import click

from fsi.fetcher.market_news import fetch_breaking_news, fetch_caixin_news, fetch_economic_calendar


def fetch_market_context(
    include_breaking_news: bool = False,
    include_news: bool = True,
    include_calendar: bool = True,
    include_northbound: bool = False,
    include_qvix: bool = False,
    include_global_indices: bool = False,
    breaking_news_limit: int = 30,
    news_limit: int = 20,
    verbose: bool = False,
) -> dict:
    """获取市场热点上下文（并行）

    Returns:
        {"market_hot_news": [...], "economic_calendar": [...],
         "northbound_summary": [...], "qvix_recent": [...],
         "us_indices": [...], "hk_indices": [...]} 或部分/空 dict
    """
    tasks = []

    if include_breaking_news:
        def _breaking():
            return "breaking_news", fetch_breaking_news(limit=breaking_news_limit)
        tasks.append((_breaking, "实时快讯"))

    if include_news:
        def _news():
            return "market_hot_news", fetch_caixin_news(limit=news_limit)
        tasks.append((_news, "市场热点新闻"))

    if include_calendar:
        def _calendar():
            return "economic_calendar", fetch_economic_calendar()
        tasks.append((_calendar, "经济日历"))

    if include_northbound:
        def _northbound():
            from fsi.fetcher.capital_flow import fetch_hsgt_summary
            return "northbound_summary", fetch_hsgt_summary()
        tasks.append((_northbound, "北向资金"))

    if include_qvix:
        def _qvix():
            from fsi.fetcher.qvix import fetch_qvix_daily
            return "qvix_recent", fetch_qvix_daily(days=3)
        tasks.append((_qvix, "QVIX"))

    if include_global_indices:
        def _global():
            from fsi.fetcher.global_index import fetch_global_index_quotes
            return "global_indices", fetch_global_index_quotes()
        tasks.append((_global, "外盘指数"))

    if not tasks:
        return {}

    ctx = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {pool.submit(fn): label for fn, label in tasks}
        for future in as_completed(futures):
            label = futures[future]
            try:
                key, data = future.result()
                if data:
                    if key == "global_indices":
                        ctx.update(data)
                        if verbose:
                            us_n = len(data.get("us_indices", []))
                            hk_n = len(data.get("hk_indices", []))
                            click.echo(f"  ✓ {label}: 美股 {us_n} + 港股 {hk_n}", err=True)
                    else:
                        ctx[key] = data
                        if verbose:
                            n = len(data) if isinstance(data, list) else ""
                            click.echo(f"  ✓ {label}: {n} 条", err=True)
            except Exception as e:
                if verbose:
                    click.echo(f"  ✗ {label}获取失败: {e}", err=True)

    return ctx


def get_a_share_index_history(days: int = 5) -> list[dict]:
    """从 DuckDB 查 A 股三大指数最近 N 日收盘数据，用于注入 AI prompt 防止编造点位"""
    import duckdb
    from fsi.config import DB_PATH

    indices = []
    codes = [
        ("000001", "上证指数"),
        ("399001", "深证成指"),
        ("399006", "创业板指"),
    ]
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        for code, name in codes:
            rows = conn.execute(
                "SELECT trade_date, close, pct_change FROM index_daily "
                "WHERE code = ? ORDER BY trade_date DESC LIMIT ?",
                [code, days],
            ).fetchall()
            if rows:
                latest = rows[0]
                indices.append({
                    "code": code,
                    "name": name,
                    "last_close": float(latest[1]),
                    "last_date": str(latest[0]),
                    "pct_change": float(latest[2]) if latest[2] is not None else None,
                    "recent": [
                        {"date": str(r[0]), "close": float(r[1]),
                         "pct_change": float(r[2]) if r[2] is not None else None}
                        for r in rows
                    ],
                })
        conn.close()
    except Exception:
        pass
    return indices
