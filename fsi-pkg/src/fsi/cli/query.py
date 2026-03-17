"""query 子命令"""

from datetime import timedelta
from decimal import Decimal

import click

from fsi.config import today_bj
import pandas as pd

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import QUERY_SYSTEM_PROMPT, build_user_message
from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.output.formatter import output


@click.group()
@click.pass_context
def query(ctx):
    """数据查询命令"""
    pass


def _query_daily(table: str, code: str, days: int, start: str | None,
                 end: str | None, indicators: bool, fmt: str,
                 db_path: str | None, name_col: bool = False):
    conn = get_connection(db_path)

    if start and end:
        start = start.replace("-", "")
        end = end.replace("-", "")
        where = f"code = ? AND trade_date >= ? AND trade_date <= ?"
        params = [code, start, end]
    else:
        # 当需要计算指标时，多取历史数据用于指标预热（MA250 需要 250+ 交易日）
        multiplier = 6 if indicators else 1.6
        where = f"code = ? AND trade_date >= ?"
        cutoff = (today_bj() - timedelta(days=int(days * multiplier))).isoformat()
        params = [code, cutoff]

    cols = "*"
    df = conn.execute(
        f"SELECT {cols} FROM {table} WHERE {where} ORDER BY trade_date", params
    ).fetchdf()

    if df.empty:
        output({"error": f"无 {code} 数据，请先 fetch"}, fmt)
        return

    # 查询股票名称
    name = ""
    exchange = ""
    if table == "stock_daily":
        info = conn.execute(
            "SELECT name, exchange FROM stock_list WHERE code = ?", [code]
        ).fetchone()
        if info:
            name, exchange = info
    elif name_col and "name" in df.columns:
        name = df["name"].iloc[0] if not df["name"].isna().all() else ""

    # 先在完整数据上计算指标（确保足够的预热期），再截取输出
    engine = IndicatorEngine()
    if indicators:
        df = engine.calculate(df)

    # 截取最后 days 条用于输出
    if not (start and end):
        df = df.tail(days).reset_index(drop=True)

    latest = df.iloc[-1]
    result = {
        "code": code,
        "name": name,
    }
    if exchange:
        result["exchange"] = exchange

    result["period"] = {
        "start": _to_str(df["trade_date"].iloc[0]),
        "end": _to_str(df["trade_date"].iloc[-1]),
    }
    result["latest"] = {
        "date": _to_str(latest["trade_date"]),
        "close": _dec(latest.get("close")),
        "pct_change": _dec(latest.get("pct_change")),
        "volume": _int(latest.get("volume")),
        "amount": _dec(latest.get("amount")),
    }
    if "turnover" in latest and pd.notna(latest.get("turnover")):
        result["latest"]["turnover"] = _dec(latest["turnover"])

    if indicators:
        result["indicators"] = engine.get_latest_indicators(df)
        result["signals"] = engine.detect_signals(df)

    # 原始数据
    data_cols = ["trade_date", "open", "close", "high", "low", "volume",
                 "amount", "pct_change"]
    if "turnover" in df.columns:
        data_cols.append("turnover")
    data_rows = []
    for _, row in df[data_cols].iterrows():
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
    result["data"] = data_rows

    if indicators:
        click.echo("获取市场热点...", err=True)
        from fsi.market_context import fetch_market_context
        market_ctx = fetch_market_context(
            include_calendar=False, news_limit=10, verbose=False,
        )
        result.update(market_ctx)

        click.echo("正在生成 AI 分析...", err=True)
        try:
            ai_text = call_bedrock(QUERY_SYSTEM_PROMPT, build_user_message(result))
            result["ai_analysis"] = ai_text
        except Exception as e:
            click.echo(f"AI 分析失败: {e}", err=True)
            result["ai_analysis"] = None

    output(result, fmt, title=f"{code} {name}")


@query.command("stock")
@click.argument("code")
@click.option("--days", "-d", default=60, help="查询天数")
@click.option("--start", default=None, help="开始日期")
@click.option("--end", default=None, help="结束日期")
@click.option("--indicators", "-i", is_flag=True, help="计算技术指标")
@click.pass_context
def query_stock(ctx, code, days, start, end, indicators):
    """查询个股数据"""
    _query_daily("stock_daily", code, days, start, end, indicators,
                 ctx.obj["fmt"], ctx.obj["db"])


@query.command("index")
@click.argument("code")
@click.option("--days", "-d", default=60, help="查询天数")
@click.option("--indicators", "-i", is_flag=True, help="计算技术指标")
@click.pass_context
def query_index(ctx, code, days, indicators):
    """查询指数数据"""
    _query_daily("index_daily", code, days, None, None, indicators,
                 ctx.obj["fmt"], ctx.obj["db"], name_col=True)


@query.command("etf")
@click.argument("code")
@click.option("--days", "-d", default=60, help="查询天数")
@click.option("--indicators", "-i", is_flag=True, help="计算技术指标")
@click.pass_context
def query_etf(ctx, code, days, indicators):
    """查询 ETF 数据"""
    _query_daily("etf_daily", code, days, None, None, indicators,
                 ctx.obj["fmt"], ctx.obj["db"], name_col=True)


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
