"""report 综合分析报告命令"""

from datetime import timedelta
from decimal import Decimal

import click

from fsi.config import today_bj
import pandas as pd

from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.output.formatter import output
from fsi.cli.news import fetch_stock_news
from fsi.cli.finance import load_finance_from_db
from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import STOCK_ANALYSIS_SYSTEM_PROMPT, build_user_message


@click.command()
@click.argument("code")
@click.option("--days", "-d", default=60, help="分析天数")
@click.pass_context
def report(ctx, code, days):
    """生成综合分析报告（技术面 + 新闻 + 财报）"""
    conn = get_connection(ctx.obj["db"])
    fmt = ctx.obj["fmt"]
    engine = IndicatorEngine()

    # 拉取足够的数据用于计算长周期指标
    cutoff = (today_bj() - timedelta(days=int(days * 6))).isoformat()

    df = conn.execute(
        "SELECT * FROM stock_daily WHERE code = ? AND trade_date >= ? ORDER BY trade_date",
        [code, cutoff],
    ).fetchdf()

    if df.empty:
        output({"error": f"无 {code} 数据，请先 fsi fetch stock {code}"}, fmt)
        return

    # 股票信息
    info = conn.execute(
        "SELECT name, exchange, industry FROM stock_list WHERE code = ?", [code]
    ).fetchone()
    name = info[0] if info else ""
    exchange = info[1] if info else ""
    industry = info[2] if info else ""

    # 计算指标
    df = engine.calculate(df)

    # 取最近 days 天的数据用于报告
    df_recent = df.tail(days).reset_index(drop=True)
    latest = df.iloc[-1]

    # 价格摘要
    close_now = float(latest["close"])
    price_summary = {"latest_close": close_now, "latest_date": _to_str(latest["trade_date"])}

    for period, label in [(5, "5d_change"), (20, "20d_change"), (60, "60d_change")]:
        if len(df_recent) > period:
            old_close = float(df_recent.iloc[-period - 1]["close"])
            price_summary[label] = round((close_now - old_close) / old_close * 100, 2)

    # 趋势判断
    trend_parts = []
    ma5 = latest.get("ma5")
    ma20 = latest.get("ma20")
    ma60 = latest.get("ma60")
    if ma5 is not None and ma20 is not None and pd.notna(ma5) and pd.notna(ma20):
        if float(ma5) > float(ma20):
            trend_parts.append("短期上升")
        else:
            trend_parts.append("短期下降")
    if ma20 is not None and ma60 is not None and pd.notna(ma20) and pd.notna(ma60):
        if float(ma20) > float(ma60):
            trend_parts.append("中期上升")
        else:
            trend_parts.append("中期震荡偏弱")
    trend = "，".join(trend_parts) if trend_parts else "趋势不明"

    # 支撑位和压力位
    support_levels = []
    resistance_levels = []
    for k in ["ma20", "ma60", "ma120"]:
        v = latest.get(k)
        if v is not None and pd.notna(v):
            fv = float(v)
            if fv < close_now:
                support_levels.append(round(fv, 2))
            else:
                resistance_levels.append(round(fv, 2))
    boll_upper = latest.get("boll_upper")
    boll_lower = latest.get("boll_lower")
    if boll_upper is not None and pd.notna(boll_upper):
        resistance_levels.append(round(float(boll_upper), 2))
    if boll_lower is not None and pd.notna(boll_lower):
        support_levels.append(round(float(boll_lower), 2))

    support_levels.sort(reverse=True)
    resistance_levels.sort()

    # 成交量分析
    vol_analysis = {}
    if "turnover" in df_recent.columns:
        tv = df_recent["turnover"].astype(float)
        if len(tv) >= 5:
            vol_analysis["avg_turnover_5d"] = round(float(tv.tail(5).mean()), 2)
        if len(tv) >= 20:
            vol_analysis["avg_turnover_20d"] = round(float(tv.tail(20).mean()), 2)
        if len(tv) >= 5 and len(tv) >= 20:
            t5 = float(tv.tail(5).mean())
            t20 = float(tv.tail(20).mean())
            if t5 > t20 * 1.5:
                vol_analysis["volume_trend"] = "明显放量"
            elif t5 > t20 * 1.1:
                vol_analysis["volume_trend"] = "温和放量"
            elif t5 < t20 * 0.7:
                vol_analysis["volume_trend"] = "明显缩量"
            else:
                vol_analysis["volume_trend"] = "量能平稳"

    # 信号检测
    signals = engine.detect_signals(df)

    # 与大盘对比
    comparison = {}
    for idx_code, idx_name in [("000001", "上证指数"), ("000300", "沪深300")]:
        idx_df = conn.execute(
            "SELECT trade_date, close FROM index_daily WHERE code = ? "
            "AND trade_date >= ? ORDER BY trade_date",
            [idx_code, (today_bj() - timedelta(days=int(days * 2))).isoformat()],
        ).fetchdf()
        if len(idx_df) >= 20:
            idx_recent = idx_df.tail(20)
            idx_first = float(idx_recent.iloc[0]["close"])
            idx_last = float(idx_recent.iloc[-1]["close"])
            idx_change = round((idx_last - idx_first) / idx_first * 100, 2)
            stock_20d = price_summary.get("20d_change")
            if stock_20d is not None:
                comparison[f"vs_{idx_name}"] = {
                    "stock_20d": stock_20d,
                    "index_20d": idx_change,
                    "alpha": round(stock_20d - idx_change, 2),
                }

    # 最近 20 天原始数据
    raw_20d = []
    for _, row in df_recent.tail(20).iterrows():
        raw_20d.append({
            "date": _to_str(row["trade_date"]),
            "open": _dec(row.get("open")),
            "close": _dec(row.get("close")),
            "high": _dec(row.get("high")),
            "low": _dec(row.get("low")),
            "volume": _int(row.get("volume")),
            "amount": _dec(row.get("amount")),
            "pct_change": _dec(row.get("pct_change")),
            "turnover": _dec(row.get("turnover")),
        })

    # 个股新闻（最近 5 条）
    recent_news = fetch_stock_news(code, limit=5)

    # 财务指标（最近 4 期）：只读库，需先 fsi fetch finance
    financial_summary = load_finance_from_db(conn, code, limit=4)
    if not financial_summary:
        click.echo(f"提示: 无 {code} 财报数据，请先运行 fsi fetch finance {code}", err=True)

    result = {
        "stock_info": {
            "code": code,
            "name": name,
            "exchange": exchange,
            "industry": industry,
        },
        "price_summary": price_summary,
        "technical_analysis": {
            "trend": trend,
            "support_levels": support_levels,
            "resistance_levels": resistance_levels,
            "indicators": engine.get_latest_indicators(df),
        },
        "volume_analysis": vol_analysis,
        "signals": signals,
        "comparison_with_index": comparison,
        "financial_summary": financial_summary,
        "recent_news": recent_news,
        "raw_data_recent_20d": raw_20d,
    }

    # 市场热点上下文
    click.echo("获取市场热点...", err=True)
    from fsi.market_context import fetch_market_context
    market_ctx = fetch_market_context(
        include_calendar=False, news_limit=10,
        verbose=ctx.obj.get("verbose", False),
    )
    result.update(market_ctx)

    # AI 分析
    click.echo("正在生成 AI 分析报告...", err=True)
    try:
        ai_text = call_bedrock(STOCK_ANALYSIS_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title=f"{code} {name} 综合报告")


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


def _int(val) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return int(val)
