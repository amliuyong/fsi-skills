"""fsi eod — 盘后回顾命令"""

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import click

from fsi.config import FSI_DIR, today_bj
import pandas as pd

from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.output.formatter import DecimalEncoder, output

_REPORT_DIR = FSI_DIR / "reports"
from fsi.portfolio import (
    load_portfolio,
    fetch_all_index_quotes,
    enrich_portfolio_with_quotes,
    calc_portfolio_summary,
    load_news_from_db,
    load_finance_from_db,
)
from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import EOD_SYSTEM_PROMPT, build_user_message


@click.command("eod")
@click.pass_context
def eod_cmd(ctx):
    """盘后回顾（最全面：大盘+持仓+技术+量价+新闻+财报 + AI 复盘）"""
    conn = get_connection(ctx.obj["db"])
    fmt = ctx.obj["fmt"]
    engine = IndicatorEngine()

    # 1. 加载持仓
    positions = load_portfolio()
    if not positions:
        output({"error": "无持仓配置，请先编辑 data/portfolio.json"}, fmt)
        return

    # 2. 大盘指数行情 + 近 5 日历史
    click.echo("获取大盘指数行情...", err=True)
    index_quotes = fetch_all_index_quotes()

    index_history = {}
    cutoff_5d = (today_bj() - timedelta(days=15)).isoformat()
    for iq in index_quotes:
        code = iq["code"]
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

    # 3. 持仓实时行情 + 盈亏
    click.echo("获取持仓实时行情...", err=True)
    enriched = enrich_portfolio_with_quotes(positions, conn)
    summary = calc_portfolio_summary(enriched)

    # 4. 逐股深度分析
    cutoff = (today_bj() - timedelta(days=365)).isoformat()
    stock_details = []
    total_mv = summary["total_market_value"]

    for item in enriched:
        if "error" in item:
            stock_details.append(item)
            continue

        code = item["code"]
        detail = {
            "code": code,
            "name": item.get("name", ""),
            "industry": item.get("industry", ""),
            "shares": item["shares"],
            "cost": item["cost"],
            "price": item["price"],
            "pct_change": item.get("pct_change"),
            "cost_total": item["cost_total"],
            "market_value": item["market_value"],
            "profit": item["profit"],
            "profit_pct": item["profit_pct"],
            "weight_pct": round(item["market_value"] / total_mv * 100, 2) if total_mv > 0 else 0,
        }

        # 技术指标 + 信号
        df = conn.execute(
            "SELECT * FROM stock_daily WHERE code = ? AND trade_date >= ? ORDER BY trade_date",
            [code, cutoff],
        ).fetchdf()

        if not df.empty:
            df = engine.calculate(df)
            detail["indicators"] = engine.get_latest_indicators(df)
            detail["signals"] = engine.detect_signals(df)

            # 成交量分析：5 日均量 vs 20 日均量
            if "turnover" in df.columns:
                tv = df["turnover"].astype(float)
                if len(tv) >= 5:
                    avg_5d = round(float(tv.tail(5).mean()), 2)
                    detail["avg_turnover_5d"] = avg_5d
                if len(tv) >= 20:
                    avg_20d = round(float(tv.tail(20).mean()), 2)
                    detail["avg_turnover_20d"] = avg_20d
                if len(tv) >= 20:
                    t5 = float(tv.tail(5).mean())
                    t20 = float(tv.tail(20).mean())
                    if t5 > t20 * 1.5:
                        detail["volume_trend"] = "明显放量"
                    elif t5 > t20 * 1.1:
                        detail["volume_trend"] = "温和放量"
                    elif t5 < t20 * 0.7:
                        detail["volume_trend"] = "明显缩量"
                    else:
                        detail["volume_trend"] = "量能平稳"

            # 最近 3 天原始数据
            raw_3d = []
            for _, row in df.tail(3).iterrows():
                raw_3d.append({
                    "date": _to_str(row.get("trade_date")),
                    "open": _dec(row.get("open")),
                    "close": _dec(row.get("close")),
                    "high": _dec(row.get("high")),
                    "low": _dec(row.get("low")),
                    "volume": _int(row.get("volume")),
                    "pct_change": _dec(row.get("pct_change")),
                    "turnover": _dec(row.get("turnover")),
                })
            detail["raw_data_3d"] = raw_3d

        # 最近 5 条新闻
        news = load_news_from_db(conn, code, limit=5)
        if news:
            detail["recent_news"] = news

        # 最近 2 期财报
        finance = load_finance_from_db(conn, code, limit=2)
        if finance:
            detail["financial_summary"] = finance

        stock_details.append(detail)

    result = {
        "report_type": "盘后回顾",
        "index_quotes": index_quotes,
        "index_history_5d": index_history,
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

    # AI 分析
    click.echo("正在生成 AI 盘后回顾...", err=True)
    try:
        ai_text = call_bedrock(EOD_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    # 保存完整报告到文件
    _save_report(result)

    output(result, fmt, title="盘后回顾")


def _save_report(result: dict):
    """保存完整盘后报告到 data/reports/{date}-eod.json"""
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    today = today_bj()
    path = _REPORT_DIR / f"{today.isoformat()}-eod.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result, cls=DecimalEncoder, ensure_ascii=False, indent=2, fp=f)
        click.echo(f"报告已保存: {path}", err=True)
    except OSError as e:
        click.echo(f"报告保存失败: {e}", err=True)


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
