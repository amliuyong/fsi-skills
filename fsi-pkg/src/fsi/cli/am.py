"""fsi am — 盘前速览（隔夜外盘 + 今日预判）"""

import click

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import AM_SYSTEM_PROMPT, build_user_message
from fsi.market_context import fetch_market_context, get_a_share_index_history
from fsi.output.formatter import output


def collect_am_data(verbose=False):
    """收集盘前速览所需的全部数据，返回 result dict（不含 AI 分析）。"""
    from datetime import datetime, timezone, timedelta

    beijing_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(beijing_tz).strftime("%Y-%m-%d")
    result = {"report_type": "盘前速览", "report_date": today_str}

    click.echo("获取盘前数据...", err=True)
    market_ctx = fetch_market_context(
        include_news=True,
        include_calendar=True,
        include_qvix=True,
        include_global_indices=True,
        verbose=verbose,
    )
    result.update(market_ctx)

    # 注入 A 股三大指数最近收盘数据（避免 AI 编造点位）
    a_indices = get_a_share_index_history()
    if a_indices:
        result["a_share_indices"] = a_indices

    # 校验至少有部分数据
    if not any(k in result for k in ["us_indices", "hk_indices", "market_hot_news"]):
        return None

    return result


@click.command("am")
@click.pass_context
def am_cmd(ctx):
    """盘前速览 — 隔夜外盘 + 今日预判（无持仓，适合公开分享）"""
    fmt = ctx.obj["fmt"]
    verbose = ctx.obj.get("verbose", False)

    result = collect_am_data(verbose=verbose)
    if result is None:
        output({"error": "盘前数据获取失败，请检查网络连接"}, fmt)
        return

    # AI 分析
    click.echo("正在生成 AI 盘前分析...", err=True)
    try:
        ai_text = call_bedrock(AM_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="盘前速览")
