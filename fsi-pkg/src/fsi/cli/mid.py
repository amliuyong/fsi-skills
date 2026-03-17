"""fsi mid — 盘中解读命令"""

from datetime import timedelta

import click

from fsi.config import today_bj

from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.output.formatter import output
from fsi.portfolio import (
    load_portfolio,
    fetch_all_index_quotes,
    enrich_portfolio_with_quotes,
    calc_portfolio_summary,
    load_news_from_db,
)
from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import MID_SYSTEM_PROMPT, build_user_message


@click.command("mid")
@click.pass_context
def mid_cmd(ctx):
    """盘中解读（大盘 + 持仓实时快照 + AI 分析）"""
    conn = get_connection(ctx.obj["db"])
    fmt = ctx.obj["fmt"]
    engine = IndicatorEngine()

    # 1. 加载持仓
    positions = load_portfolio()
    if not positions:
        output({"error": "无持仓配置，请先编辑 data/portfolio.json"}, fmt)
        return

    # 2. 大盘指数行情
    click.echo("获取大盘指数行情...", err=True)
    index_quotes = fetch_all_index_quotes()

    # 3. 持仓实时行情 + 盈亏
    click.echo("获取持仓实时行情...", err=True)
    enriched = enrich_portfolio_with_quotes(positions, conn)
    summary = calc_portfolio_summary(enriched)

    # 4. 每只持仓技术信号
    cutoff = (today_bj() - timedelta(days=365)).isoformat()
    stock_details = []
    for item in enriched:
        if "error" in item:
            stock_details.append(item)
            continue

        code = item["code"]
        df = conn.execute(
            "SELECT * FROM stock_daily WHERE code = ? AND trade_date >= ? ORDER BY trade_date",
            [code, cutoff],
        ).fetchdf()

        detail = {
            "code": code,
            "name": item.get("name", ""),
            "price": item["price"],
            "pct_change": item.get("pct_change"),
            "profit": item["profit"],
            "profit_pct": item["profit_pct"],
        }

        if not df.empty:
            df = engine.calculate(df)
            detail["signals"] = engine.detect_signals(df)
            detail["indicators"] = engine.get_latest_indicators(df)

        # 最近 3 条新闻
        news = load_news_from_db(conn, code, limit=3)
        if news:
            detail["recent_news"] = news

        stock_details.append(detail)

    result = {
        "report_type": "盘中解读",
        "index_quotes": index_quotes,
        "portfolio_summary": summary,
        "stock_details": stock_details,
    }

    # 市场热点上下文
    click.echo("获取市场热点...", err=True)
    from fsi.market_context import fetch_market_context
    market_ctx = fetch_market_context(
        include_northbound=True,
        include_qvix=True,
        include_global_indices=True,
        verbose=ctx.obj.get("verbose", False),
    )
    result.update(market_ctx)

    # 注入 A 股指数历史数据（防止 AI 编造点位）
    from fsi.market_context import get_a_share_index_history
    a_idx = get_a_share_index_history()
    if a_idx:
        result["a_share_index_history"] = a_idx

    # AI 分析
    click.echo("正在生成 AI 盘中解读...", err=True)
    try:
        ai_text = call_bedrock(MID_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="盘中解读")
