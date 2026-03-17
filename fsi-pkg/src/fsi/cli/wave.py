"""wave 子命令 — 基于波浪理论的技术分析"""

from datetime import timedelta
from decimal import Decimal

import click

from fsi.config import today_bj
import pandas as pd

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import WAVE_SYSTEM_PROMPT, build_user_message
from fsi.cli.chart import generate_kline_chart
from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.indicators.swing import detect_swings, summarize_swings
from fsi.output.formatter import output


@click.group()
@click.pass_context
def wave(ctx):
    """波浪理论分析命令"""
    pass


@wave.command("stock")
@click.argument("code")
@click.option("--days", "-d", default=120, help="分析天数（默认 120）")
@click.option("--threshold", "-t", default=5.0, help="转折点最小幅度百分比（默认 5%）")
@click.pass_context
def wave_stock(ctx, code, days, threshold):
    """个股波浪分析"""
    _wave_analysis("stock_daily", code, days, threshold,
                   ctx.obj["fmt"], ctx.obj["db"])


@wave.command("index")
@click.argument("code")
@click.option("--days", "-d", default=120, help="分析天数（默认 120）")
@click.option("--threshold", "-t", default=3.0, help="转折点最小幅度百分比（默认 3%，指数波动较小）")
@click.pass_context
def wave_index(ctx, code, days, threshold):
    """指数波浪分析"""
    _wave_analysis("index_daily", code, days, threshold,
                   ctx.obj["fmt"], ctx.obj["db"], name_col=True)


@wave.command("etf")
@click.argument("code")
@click.option("--days", "-d", default=120, help="分析天数（默认 120）")
@click.option("--threshold", "-t", default=3.0, help="转折点最小幅度百分比（默认 3%，ETF 波动较小）")
@click.pass_context
def wave_etf(ctx, code, days, threshold):
    """ETF 波浪分析"""
    _wave_analysis("etf_daily", code, days, threshold,
                   ctx.obj["fmt"], ctx.obj["db"], name_col=True)


def _wave_analysis(table: str, code: str, days: int, threshold: float,
                   fmt: str, db_path: str | None, name_col: bool = False):
    conn = get_connection(db_path)

    # 多取数据用于指标预热
    multiplier = 6
    cutoff = (today_bj() - timedelta(days=int(days * multiplier))).isoformat()
    where = "code = ? AND trade_date >= ?"
    params = [code, cutoff]

    df = conn.execute(
        f"SELECT * FROM {table} WHERE {where} ORDER BY trade_date", params
    ).fetchdf()

    if df.empty:
        output({"error": f"无 {code} 数据，请先 fetch"}, fmt)
        return

    # 解析名称
    name = ""
    if table == "stock_daily":
        info = conn.execute(
            "SELECT name FROM stock_list WHERE code = ?", [code]
        ).fetchone()
        if info:
            name = info[0]
    elif name_col and "name" in df.columns:
        name = df["name"].iloc[0] if not df["name"].isna().all() else ""

    # 计算技术指标
    engine = IndicatorEngine()
    df = engine.calculate(df)

    # 截取分析窗口
    df = df.tail(days).reset_index(drop=True)

    if len(df) < 10:
        output({"error": f"{code} 数据不足，至少需要 10 个交易日"}, fmt)
        return

    # 检测转折点
    swings = detect_swings(df, pct_threshold=threshold)
    segments = summarize_swings(swings)

    latest = df.iloc[-1]
    result = {
        "code": code,
        "name": name,
        "analysis_type": "elliott_wave",
        "period": {
            "start": _to_str(df["trade_date"].iloc[0]),
            "end": _to_str(df["trade_date"].iloc[-1]),
            "days": len(df),
        },
        "latest": {
            "date": _to_str(latest["trade_date"]),
            "close": _dec(latest.get("close")),
            "pct_change": _dec(latest.get("pct_change")),
            "volume": _int(latest.get("volume")),
        },
        "indicators": engine.get_latest_indicators(df),
        "signals": engine.detect_signals(df),
        "swing_points": swings,
        "swing_segments": segments,
    }

    # 价格区间统计
    closes = pd.to_numeric(df["close"], errors="coerce")
    result["price_range"] = {
        "high": _dec(closes.max()),
        "low": _dec(closes.min()),
        "current": _dec(closes.iloc[-1]),
        "from_high_pct": round(float((closes.iloc[-1] - closes.max()) / closes.max() * 100), 2),
        "from_low_pct": round(float((closes.iloc[-1] - closes.min()) / closes.min() * 100), 2),
    }

    # 近期 K 线数据（最近 20 日供 AI 参考细节）
    data_cols = ["trade_date", "open", "close", "high", "low", "volume", "pct_change"]
    recent = df.tail(20)
    data_rows = []
    for _, row in recent[data_cols].iterrows():
        r = {}
        for c in data_cols:
            v = row[c]
            if c == "trade_date":
                r["date"] = _to_str(v)
            elif c == "volume":
                r[c] = _int(v)
            else:
                r[c] = _dec(v)
        data_rows.append(r)
    result["recent_data"] = data_rows

    # 生成带波浪标注的 K 线图
    kind = table.replace("_daily", "")          # stock / index / etf
    chart_fname = f"wave_{kind}_{code}_{today_bj().isoformat()}.png"
    chart_path = generate_kline_chart(
        table, code, days, db_path=db_path, name_col=name_col,
        filename=chart_fname, swing_points=swings,
    )
    if chart_path:
        result["chart"] = chart_path
        click.echo(f"K 线图: {chart_path}", err=True)

    # AI 波浪分析
    click.echo("正在生成 AI 波浪分析...", err=True)
    try:
        ai_text = call_bedrock(WAVE_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title=f"波浪分析: {code} {name}")


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
