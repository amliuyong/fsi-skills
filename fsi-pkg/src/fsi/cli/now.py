"""fsi now — 盘中速报（实时指数 + 资金流向 + 行业轮动）"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import click

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import NOW_SYSTEM_PROMPT, build_user_message
from fsi.output.formatter import output


def collect_now_data(verbose=False):
    """收集盘中速报所需的全部数据，返回 result dict（不含 AI 分析）。"""
    from datetime import datetime, timezone, timedelta

    beijing_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(beijing_tz).strftime("%Y-%m-%d")
    result = {"report_type": "盘中速报", "report_date": today_str}

    click.echo("获取盘中数据（并行）...", err=True)

    def _fetch_index_quotes():
        from fsi.portfolio import fetch_all_index_quotes
        return "index_quotes", fetch_all_index_quotes()

    def _fetch_hsgt_summary():
        from fsi.fetcher.capital_flow import fetch_hsgt_summary
        return "hsgt_summary", fetch_hsgt_summary()

    def _fetch_market_fund_flow():
        from fsi.fetcher.capital_flow import fetch_market_fund_flow
        return "market_fund_flow", fetch_market_fund_flow(days=5)

    def _fetch_sector_fund_flow():
        from fsi.fetcher.capital_flow import fetch_sector_fund_flow
        return "sector_fund_flow", fetch_sector_fund_flow(top=15)

    def _fetch_qvix_intraday():
        from fsi.fetcher.qvix import fetch_qvix_intraday
        return "qvix_intraday", fetch_qvix_intraday()

    def _fetch_market_news():
        from fsi.fetcher.market_news import fetch_caixin_news
        return "market_hot_news", fetch_caixin_news(limit=20)

    def _fetch_economic_calendar():
        from fsi.fetcher.market_news import fetch_economic_calendar
        return "economic_calendar", fetch_economic_calendar()

    tasks = [
        _fetch_index_quotes,
        _fetch_hsgt_summary,
        _fetch_market_fund_flow,
        _fetch_sector_fund_flow,
        _fetch_qvix_intraday,
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
                    result[key] = data
                    if verbose:
                        n = len(data) if isinstance(data, list) else ""
                        click.echo(f"  ✓ {key}: {n} 条", err=True)
            except Exception as e:
                if verbose:
                    click.echo(f"  ✗ {fn_name}: {e}", err=True)

    # 校验至少有部分数据可用
    data_keys = ["index_quotes", "hsgt_summary", "market_fund_flow", "sector_fund_flow"]
    if not any(k in result for k in data_keys):
        return None

    # 注入 A 股指数历史数据（防止 AI 编造点位）
    from fsi.market_context import get_a_share_index_history
    a_idx = get_a_share_index_history()
    if a_idx:
        result["a_share_index_history"] = a_idx

    return result


@click.command("now")
@click.pass_context
def now_cmd(ctx):
    """盘中速报 — 实时指数 + 资金流向 + 行业轮动（无持仓，适合公开分享）"""
    fmt = ctx.obj["fmt"]
    verbose = ctx.obj.get("verbose", False)

    result = collect_now_data(verbose=verbose)
    if result is None:
        output({"error": "盘中数据获取失败，请检查网络连接"}, fmt)
        return

    # AI 解读
    click.echo("正在生成 AI 盘中分析...", err=True)
    try:
        ai_text = call_bedrock(NOW_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="盘中速报")
