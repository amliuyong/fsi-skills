"""fsi pm — 盘后复盘（全日总结 + 资金全景 + 明日展望）"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from decimal import Decimal

import click

from fsi.config import today_bj
import pandas as pd

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import PM_SYSTEM_PROMPT, build_user_message
from fsi.output.formatter import output


def collect_pm_data(db_path=None, verbose=False):
    """收集盘后复盘所需的全部数据，返回 result dict（不含 AI 分析）。"""
    from datetime import datetime, timezone, timedelta

    beijing_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(beijing_tz).strftime("%Y-%m-%d")
    result = {"report_type": "盘后复盘", "report_date": today_str}

    # 所有数据源并行获取
    click.echo("获取盘后数据（并行）...", err=True)

    def _fetch_index_quotes():
        from fsi.portfolio import fetch_all_index_quotes
        return "index_quotes", fetch_all_index_quotes()

    def _fetch_global_indices():
        from fsi.fetcher.global_index import fetch_global_index_quotes
        return "global_indices", fetch_global_index_quotes()

    def _fetch_hsgt_summary():
        from fsi.fetcher.capital_flow import fetch_hsgt_summary
        return "hsgt_summary", fetch_hsgt_summary()

    def _fetch_northbound_hist():
        from fsi.fetcher.capital_flow import fetch_northbound_hist
        return "northbound_hist", fetch_northbound_hist(days=10)

    def _fetch_market_fund_flow():
        from fsi.fetcher.capital_flow import fetch_market_fund_flow
        return "market_fund_flow", fetch_market_fund_flow(days=10)

    def _fetch_sector_fund_flow():
        from fsi.fetcher.capital_flow import fetch_sector_fund_flow
        return "sector_fund_flow", fetch_sector_fund_flow(top=15)

    def _fetch_qvix_daily():
        from fsi.fetcher.qvix import fetch_qvix_daily
        return "qvix_daily", fetch_qvix_daily(days=20)

    def _fetch_market_news():
        from fsi.fetcher.market_news import fetch_caixin_news
        return "market_hot_news", fetch_caixin_news(limit=20)

    def _fetch_economic_calendar():
        from fsi.fetcher.market_news import fetch_economic_calendar
        return "economic_calendar", fetch_economic_calendar()

    tasks = [
        _fetch_index_quotes,
        _fetch_global_indices,
        _fetch_hsgt_summary,
        _fetch_northbound_hist,
        _fetch_market_fund_flow,
        _fetch_sector_fund_flow,
        _fetch_qvix_daily,
        _fetch_market_news,
        _fetch_economic_calendar,
    ]

    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {pool.submit(fn): fn.__name__ for fn in tasks}
        for future in as_completed(futures):
            fn_name = futures[future]
            try:
                key, data = future.result()
                if data:
                    if key == "global_indices":
                        result.update(data)
                        if verbose:
                            us_n = len(data.get("us_indices", []))
                            hk_n = len(data.get("hk_indices", []))
                            click.echo(f"  ✓ 外盘指数: 美股 {us_n} + 港股 {hk_n}", err=True)
                    else:
                        result[key] = data
                        if verbose:
                            n = len(data) if isinstance(data, list) else ""
                            click.echo(f"  ✓ {key}: {n} 条", err=True)
            except Exception as e:
                if verbose:
                    click.echo(f"  ✗ {fn_name}: {e}", err=True)

    # 校验至少有部分数据可用
    data_keys = [
        "index_quotes", "hsgt_summary", "market_fund_flow",
        "sector_fund_flow", "qvix_daily",
    ]
    if not any(k in result for k in data_keys):
        return None

    # 串行 DB 查询：6 大指数近 5 日走势
    click.echo("查询指数历史走势...", err=True)
    try:
        from fsi.db.connection import get_connection
        from fsi.config import MAJOR_INDICES
        conn = get_connection(db_path)
        index_history = {}
        cutoff_5d = (today_bj() - timedelta(days=15)).isoformat()
        for code in MAJOR_INDICES:
            hist = conn.execute(
                "SELECT trade_date, close, pct_change FROM index_daily "
                "WHERE code = ? AND trade_date >= ? ORDER BY trade_date DESC LIMIT 5",
                [code, cutoff_5d],
            ).fetchdf()
            if not hist.empty:
                index_history[code] = [
                    {
                        "date": _to_str(row.get("trade_date")),
                        "close": _dec(row.get("close")),
                        "pct_change": _dec(row.get("pct_change")),
                    }
                    for _, row in hist.iterrows()
                ]
        if index_history:
            result["index_history_5d"] = index_history
            if verbose:
                click.echo(f"  ✓ 指数历史: {len(index_history)} 只", err=True)
    except Exception as e:
        if verbose:
            click.echo(f"  ✗ 指数历史查询失败: {e}", err=True)

    return result


@click.command("pm")
@click.pass_context
def pm_cmd(ctx):
    """盘后复盘 — 全日总结 + 资金全景 + 明日展望"""
    fmt = ctx.obj["fmt"]
    verbose = ctx.obj.get("verbose", False)

    result = collect_pm_data(db_path=ctx.obj["db"], verbose=verbose)
    if result is None:
        output({"error": "盘后数据获取失败，请检查网络连接"}, fmt)
        return

    # AI 解读
    click.echo("正在生成 AI 盘后分析...", err=True)
    try:
        ai_text = call_bedrock(PM_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="盘后复盘")


def _to_str(val) -> str:
    if val is None:
        return ""
    if hasattr(val, "isoformat"):
        return val.isoformat()[:10]
    return str(val)


def _dec(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, Decimal):
        return float(val)
    return round(float(val), 4)
