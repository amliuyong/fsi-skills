"""compare 多股对比命令"""

from datetime import timedelta
from decimal import Decimal

import click

from fsi.config import today_bj
import pandas as pd

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import COMPARE_SYSTEM_PROMPT, build_user_message
from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.output.formatter import output


@click.command()
@click.argument("codes", nargs=-1, required=True)
@click.option("--days", "-d", default=20, help="对比天数")
@click.option("--metrics", default="pct_change,turnover,rsi,macd",
              help="对比指标（逗号分隔）")
@click.pass_context
def compare(ctx, codes, days, metrics):
    """多股对比"""
    conn = get_connection(ctx.obj["db"])
    fmt = ctx.obj["fmt"]
    engine = IndicatorEngine()
    metric_list = [m.strip() for m in metrics.split(",")]

    cutoff = (today_bj() - timedelta(days=int(days * 2.5))).isoformat()
    comparisons = []

    for code in codes:
        df = conn.execute(
            "SELECT * FROM stock_daily WHERE code = ? AND trade_date >= ? ORDER BY trade_date",
            [code, cutoff],
        ).fetchdf()

        if df.empty:
            comparisons.append({"code": code, "error": "无数据"})
            continue

        df = engine.calculate(df)
        df = df.tail(days).reset_index(drop=True)

        info = conn.execute(
            "SELECT name FROM stock_list WHERE code = ?", [code]
        ).fetchone()
        name = info[0] if info else ""

        latest = df.iloc[-1]
        entry = {"code": code, "name": name}

        # 区间涨跌幅
        if len(df) >= 2:
            first_close = float(df.iloc[0]["close"])
            last_close = float(latest["close"])
            entry["period_change"] = round((last_close - first_close) / first_close * 100, 2)

        entry["latest_close"] = _dec(latest.get("close"))
        entry["latest_pct_change"] = _dec(latest.get("pct_change"))

        if "turnover" in metric_list and "turnover" in df.columns:
            entry["avg_turnover"] = round(float(df["turnover"].astype(float).mean()), 2)

        if "rsi" in metric_list:
            for k in ["rsi6", "rsi12", "rsi24"]:
                if k in latest and pd.notna(latest[k]):
                    entry[k] = round(float(latest[k]), 2)

        if "macd" in metric_list:
            for k in ["dif", "dea", "macd_hist"]:
                if k in latest and pd.notna(latest[k]):
                    entry[k] = round(float(latest[k]), 4)

        if "pct_change" in metric_list:
            entry["avg_pct_change"] = round(
                float(df["pct_change"].astype(float).mean()), 2
            )
            entry["max_pct_change"] = round(float(df["pct_change"].astype(float).max()), 2)
            entry["min_pct_change"] = round(float(df["pct_change"].astype(float).min()), 2)

        comparisons.append(entry)

    result = {
        "period_days": days,
        "metrics": metric_list,
        "stocks": comparisons,
    }

    # 市场热点上下文
    click.echo("获取市场热点...", err=True)
    from fsi.market_context import fetch_market_context
    market_ctx = fetch_market_context(
        include_calendar=False, news_limit=10,
        verbose=ctx.obj.get("verbose", False),
    )
    result.update(market_ctx)

    click.echo("正在生成 AI 分析...", err=True)
    try:
        ai_text = call_bedrock(COMPARE_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="多股对比")


def _dec(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, Decimal):
        return float(val)
    return round(float(val), 4)
