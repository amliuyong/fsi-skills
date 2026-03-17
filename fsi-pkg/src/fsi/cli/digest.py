"""fsi digest — 近N天新闻回顾 + AI 综合研判"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import click

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import DIGEST_SYSTEM_PROMPT, build_user_message
from fsi.output.formatter import output

_tz_bj = ZoneInfo("Asia/Shanghai")


def _query_news(conn, days: int, limit: int) -> list[dict]:
    """从 stock_news + market_news 查询近 N 天新闻，合并去重，按 pub_time 倒序。"""
    cutoff = datetime.now(tz=_tz_bj) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    news = []

    # stock_news（个股新闻）
    try:
        rows = conn.execute(
            "SELECT title, source, pub_time, content AS summary, NULL AS tags "
            "FROM stock_news WHERE pub_time >= ? "
            "ORDER BY pub_time DESC",
            [cutoff_str],
        ).fetchall()
        for r in rows:
            news.append({
                "title": r[0] or "",
                "source": r[1] or "",
                "pub_time": str(r[2]) if r[2] else "",
                "summary": r[3] or "",
                "tags": r[4] or "",
            })
    except Exception:
        pass

    # market_news（市场热点）
    try:
        rows = conn.execute(
            "SELECT title, source, pub_time, summary, tags "
            "FROM market_news WHERE pub_time >= ? OR "
            "(pub_time IS NULL AND fetched_at >= ?) "
            "ORDER BY COALESCE(pub_time, fetched_at) DESC",
            [cutoff_str, cutoff_str],
        ).fetchall()
        for r in rows:
            news.append({
                "title": r[0] or "",
                "source": r[1] or "",
                "pub_time": str(r[2]) if r[2] else "",
                "summary": r[3] or "",
                "tags": r[4] or "",
            })
    except Exception:
        pass

    # 按 title 去重
    seen = set()
    deduped = []
    for item in news:
        title = item["title"].strip()
        if not title or title in seen:
            continue
        seen.add(title)
        deduped.append(item)

    # 按 pub_time 倒序（无时间的排最后）
    deduped.sort(key=lambda x: x["pub_time"] or "0000", reverse=True)

    return deduped[:limit]


@click.command("digest")
@click.option("--days", "-d", default=3, show_default=True, help="回溯天数")
@click.option("--limit", "-n", default=50, show_default=True, help="最终输出条数")
@click.pass_context
def digest_cmd(ctx, days, limit):
    """近N天新闻回顾 + AI 综合研判"""
    from fsi.db.connection import get_connection

    fmt = ctx.obj.get("fmt", "json")
    verbose = ctx.obj.get("verbose", False)

    conn = get_connection(ctx.obj.get("db"))

    click.echo(f"查询近 {days} 天新闻...", err=True)
    news = _query_news(conn, days, limit)

    if not news:
        output({"error": f"近 {days} 天无新闻数据，请先运行 fsi hot 或 fsi news 获取新闻"}, fmt)
        return

    now = datetime.now(tz=_tz_bj)
    start = now - timedelta(days=days)
    period = f"{start.strftime('%Y-%m-%d')} ~ {now.strftime('%Y-%m-%d')}"

    click.echo(f"共 {len(news)} 条新闻，正在生成 AI 分析...", err=True)

    result = {
        "report_type": "新闻回顾",
        "days": days,
        "period": period,
        "total_count": len(news),
        "news": news,
    }

    # AI 分析
    try:
        ai_text = call_bedrock(DIGEST_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="新闻回顾")
