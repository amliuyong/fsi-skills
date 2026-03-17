"""screen 筛选命令"""

from datetime import timedelta
from decimal import Decimal

import click

from fsi.config import today_bj
import pandas as pd

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import SCREEN_SYSTEM_PROMPT, build_user_message
from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.output.formatter import output


@click.command()
@click.option("--rsi-below", type=float, default=None, help="RSI6 低于此值")
@click.option("--rsi-above", type=float, default=None, help="RSI6 高于此值")
@click.option("--macd-golden-cross", is_flag=True, help="MACD 金叉")
@click.option("--above-ma", type=int, default=None, help="收盘价在此均线之上")
@click.option("--below-ma", type=int, default=None, help="收盘价在此均线之下")
@click.option("--min-turnover", type=float, default=None, help="最低换手率%")
@click.option("--min-amount", type=float, default=None, help="最低成交额（元）")
@click.option("--limit", "-n", type=int, default=20, help="返回数量")
@click.option("--sort", type=str, default="pct_change", help="排序字段")
@click.pass_context
def screen(ctx, rsi_below, rsi_above, macd_golden_cross, above_ma, below_ma,
           min_turnover, min_amount, limit, sort):
    """多条件筛选股票"""
    conn = get_connection(ctx.obj["db"])
    fmt = ctx.obj["fmt"]

    # 获取所有股票代码
    codes = conn.execute("SELECT code, name FROM stock_list ORDER BY code").fetchall()
    if not codes:
        output({"error": "股票列表为空，请先 fsi fetch list"}, fmt)
        return

    engine = IndicatorEngine()
    cutoff = (today_bj() - timedelta(days=400)).isoformat()
    results = []

    for code, name in codes:
        df = conn.execute(
            "SELECT * FROM stock_daily WHERE code = ? AND trade_date >= ? ORDER BY trade_date",
            [code, cutoff],
        ).fetchdf()

        if df.empty or len(df) < 30:
            continue

        latest = df.iloc[-1]

        # 预过滤：成交额和换手率（在计算指标前先筛掉）
        if min_amount is not None:
            amt = latest.get("amount")
            if amt is None or (isinstance(amt, float) and pd.isna(amt)) or float(amt) < min_amount:
                continue
        if min_turnover is not None:
            tv = latest.get("turnover")
            if tv is None or (isinstance(tv, float) and pd.isna(tv)) or float(tv) < min_turnover:
                continue

        # 需要指标的筛选条件
        need_indicators = any([rsi_below, rsi_above, macd_golden_cross,
                               above_ma is not None, below_ma is not None])
        if need_indicators:
            df = engine.calculate(df)
            curr = df.iloc[-1]
            prev = df.iloc[-2] if len(df) >= 2 else None

            if rsi_below is not None:
                rsi6 = curr.get("rsi6")
                if rsi6 is None or pd.isna(rsi6) or float(rsi6) >= rsi_below:
                    continue

            if rsi_above is not None:
                rsi6 = curr.get("rsi6")
                if rsi6 is None or pd.isna(rsi6) or float(rsi6) <= rsi_above:
                    continue

            if macd_golden_cross and prev is not None:
                dif_c = curr.get("dif")
                dea_c = curr.get("dea")
                dif_p = prev.get("dif")
                dea_p = prev.get("dea")
                if any(v is None or (isinstance(v, float) and pd.isna(v))
                       for v in [dif_c, dea_c, dif_p, dea_p]):
                    continue
                if not (float(dif_p) <= float(dea_p) and float(dif_c) > float(dea_c)):
                    continue

            if above_ma is not None:
                ma_key = f"ma{above_ma}"
                ma_val = curr.get(ma_key)
                if ma_val is None or pd.isna(ma_val) or float(curr["close"]) <= float(ma_val):
                    continue

            if below_ma is not None:
                ma_key = f"ma{below_ma}"
                ma_val = curr.get(ma_key)
                if ma_val is None or pd.isna(ma_val) or float(curr["close"]) >= float(ma_val):
                    continue

        row = df.iloc[-1]
        entry = {
            "code": code,
            "name": name or "",
            "close": _dec(row.get("close")),
            "pct_change": _dec(row.get("pct_change")),
            "volume": _int(row.get("volume")),
            "amount": _dec(row.get("amount")),
            "turnover": _dec(row.get("turnover")),
        }

        if need_indicators:
            if "rsi6" in row and pd.notna(row.get("rsi6")):
                entry["rsi6"] = round(float(row["rsi6"]), 2)
            if "dif" in row and pd.notna(row.get("dif")):
                entry["dif"] = round(float(row["dif"]), 4)
                entry["dea"] = round(float(row["dea"]), 4)

        results.append(entry)

    # 排序
    reverse = True
    results.sort(key=lambda x: x.get(sort, 0) or 0, reverse=reverse)
    results = results[:limit]

    result = {"count": len(results), "results": results}

    # 市场热点上下文
    click.echo("获取市场热点...", err=True)
    from fsi.market_context import fetch_market_context
    market_ctx = fetch_market_context(
        include_calendar=False, news_limit=15,
        verbose=ctx.obj.get("verbose", False),
    )
    result.update(market_ctx)

    click.echo("正在生成 AI 分析...", err=True)
    try:
        ai_text = call_bedrock(SCREEN_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="筛选结果")


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
