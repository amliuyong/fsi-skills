"""fsi hot — 市场热点 + AI 解读"""

import click

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import HOT_SYSTEM_PROMPT, build_user_message
from fsi.market_context import fetch_market_context
from fsi.output.formatter import output


def save_market_news_to_db(conn, market_ctx: dict) -> int:
    """将市场热点新闻存入 market_news 表，返回入库条数。按 title 去重。"""
    import pandas as pd
    from datetime import datetime
    from zoneinfo import ZoneInfo

    _tz_bj = ZoneInfo("Asia/Shanghai")
    now = datetime.now(tz=_tz_bj)
    rows = []

    # 东财快讯
    for item in market_ctx.get("breaking_news", []):
        title = item.get("标题", "").strip()
        if not title:
            continue
        rows.append({
            "title": title,
            "summary": item.get("摘要", ""),
            "source": "快讯",
            "pub_time": item.get("发布时间", None),
            "tags": None,
            "fetched_at": now,
        })

    # 财新热点
    for item in market_ctx.get("market_hot_news", []):
        summary = item.get("摘要", "").strip()
        if not summary:
            continue
        rows.append({
            "title": summary,
            "summary": None,
            "source": "财新",
            "pub_time": None,
            "tags": item.get("标签", ""),
            "fetched_at": now,
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows)
    # 先按 title 去重（本批内部）
    df = df.drop_duplicates(subset=["title"])
    conn.execute("INSERT OR IGNORE INTO market_news SELECT * FROM df")
    return len(df)


@click.command("hot")
@click.pass_context
def hot_cmd(ctx):
    """市场热点新闻 + 经济日历 + AI 解读"""
    fmt = ctx.obj["fmt"]
    verbose = ctx.obj.get("verbose", False)

    click.echo("获取市场热点新闻...", err=True)
    market_ctx = fetch_market_context(
        include_breaking_news=True,
        include_news=True,
        include_calendar=True,
        breaking_news_limit=30,
        news_limit=20,
        verbose=verbose,
    )

    if not market_ctx:
        output({"error": "市场热点数据获取失败，请检查网络连接"}, fmt)
        return

    # 新闻入库
    try:
        from fsi.db.connection import get_connection
        conn = get_connection(ctx.obj.get("db"))
        saved = save_market_news_to_db(conn, market_ctx)
        if verbose:
            click.echo(f"市场新闻入库 {saved} 条", err=True)
    except Exception as e:
        if verbose:
            click.echo(f"市场新闻入库失败: {e}", err=True)

    result = {"report_type": "市场热点"}
    result.update(market_ctx)

    # AI 解读
    click.echo("正在生成 AI 市场解读...", err=True)
    try:
        ai_text = call_bedrock(HOT_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="市场热点")
