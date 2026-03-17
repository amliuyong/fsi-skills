"""个股财务数据（东方财富）"""

import click
import akshare as ak
import pandas as pd

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import FINANCE_SYSTEM_PROMPT, build_user_message
from fsi.output.formatter import output


# 代码转东方财富格式：000001 -> 000001.SZ
def _to_em_symbol(code: str) -> str:
    if code.startswith("6"):
        return f"{code}.SH"
    elif code.startswith(("0", "3")):
        return f"{code}.SZ"
    elif code.startswith(("4", "8")):
        return f"{code}.BJ"
    return code


# API 字段 -> DB 列名映射
_DB_FIELD_MAP = {
    "REPORT_DATE": "report_date",
    "REPORT_DATE_NAME": "report_name",
    "EPSJB": "eps",
    "BPS": "bps",
    "TOTALOPERATEREVE": "revenue",
    "PARENTNETPROFIT": "net_profit",
    "KCFJCXSYJLR": "net_profit_ded",
    "TOTALOPERATEREVETZ": "revenue_yoy",
    "PARENTNETPROFITTZ": "net_profit_yoy",
    "ROEJQ": "roe",
    "XSMLL": "gross_margin",
    "XSJLL": "net_margin",
    "ZCFZL": "debt_ratio",
    "MGJYXJJE": "ocf_per_share",
}

# DB 列名 -> 中文输出名
_DISPLAY_MAP = {
    "report_name": "报告期",
    "eps": "每股收益",
    "bps": "每股净资产",
    "revenue": "营业总收入(亿)",
    "net_profit": "归母净利润(亿)",
    "net_profit_ded": "扣非净利润(亿)",
    "revenue_yoy": "营收同比(%)",
    "net_profit_yoy": "净利润同比(%)",
    "roe": "ROE(%)",
    "gross_margin": "毛利率(%)",
    "net_margin": "净利率(%)",
    "debt_ratio": "资产负债率(%)",
    "ocf_per_share": "每股经营现金流",
}


def _fetch_from_akshare(code: str, limit: int = 8) -> list[dict]:
    """从东方财富 API 拉取财务指标，返回 DB 格式字典列表。"""
    symbol = _to_em_symbol(code)
    df = ak.stock_financial_analysis_indicator_em(symbol=symbol, indicator="按报告期")
    rows = []
    for _, row in df.head(limit).iterrows():
        item = {"code": code}
        for api_field, db_col in _DB_FIELD_MAP.items():
            val = row.get(api_field)
            if val is not None and pd.notna(val):
                if db_col == "report_date":
                    item[db_col] = pd.to_datetime(val).strftime("%Y-%m-%d")
                elif db_col == "report_name":
                    item[db_col] = str(val)
                else:
                    item[db_col] = round(float(val), 4)
            else:
                item[db_col] = None
        rows.append(item)
    return rows


def _fetch_from_api(code: str, limit: int = 8) -> list[dict]:
    """获取财务指标，按 api_health.json 配置选源。"""
    from fsi.health import get_source
    source = get_source("finance")

    if source == "yahoo":
        # 健康检查指定优先 Yahoo
        return _fetch_finance_ordered(code, limit, primary="yahoo", fallback="akshare")
    else:
        return _fetch_finance_ordered(code, limit, primary="akshare", fallback="yahoo")


def _fetch_finance_ordered(code: str, limit: int, primary: str, fallback: str) -> list[dict]:
    """按指定顺序尝试获取财务数据"""
    fetchers = {
        "akshare": lambda: _fetch_from_akshare(code, limit),
        "yahoo": lambda: _fetch_yahoo_finance(code, limit),
    }
    for source_name in [primary, fallback]:
        fn = fetchers.get(source_name)
        if fn is None:
            continue
        try:
            rows = fn()
            if rows:
                return rows
        except Exception:
            pass
    return []


def _fetch_yahoo_finance(code: str, limit: int) -> list[dict]:
    from fsi.fetcher.yahoo import fetch_stock_finance
    return fetch_stock_finance(code, limit)


def save_finance_to_db(conn, code: str, rows: list[dict]) -> int:
    """将财务数据存入 DuckDB，返回写入条数。基于 (code, report_date) 去重。"""
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    conn.execute("INSERT OR REPLACE INTO stock_finance SELECT *, current_timestamp FROM df")
    return len(df)


def load_finance_from_db(conn, code: str, limit: int = 4) -> list[dict]:
    """从 DuckDB 读取财务指标，返回中文输出格式。"""
    df = conn.execute(
        "SELECT * FROM stock_finance WHERE code = ? ORDER BY report_date DESC LIMIT ?",
        [code, limit],
    ).fetchdf()
    if df.empty:
        return []
    items = []
    for _, row in df.iterrows():
        item = {}
        for db_col, label in _DISPLAY_MAP.items():
            val = row.get(db_col)
            if val is not None and pd.notna(val):
                if db_col in ("revenue", "net_profit", "net_profit_ded"):
                    item[label] = round(float(val) / 1e8, 2)
                elif db_col == "report_name":
                    item[label] = str(val)
                else:
                    item[label] = round(float(val), 4)
            else:
                item[label] = None
        items.append(item)
    return items


def fetch_stock_finance(code: str, limit: int = 4) -> list[dict]:
    """获取个股财务指标（实时 API），返回中文输出格式。供 CLI 和 report 调用。"""
    try:
        rows = _fetch_from_api(code, limit)
        items = []
        for row in rows:
            item = {}
            for db_col, label in _DISPLAY_MAP.items():
                val = row.get(db_col)
                if val is not None:
                    if db_col in ("revenue", "net_profit", "net_profit_ded"):
                        item[label] = round(val / 1e8, 2)
                    elif db_col == "report_name":
                        item[label] = str(val)
                    else:
                        item[label] = round(val, 4)
                else:
                    item[label] = None
            items.append(item)
        return items
    except Exception:
        return []


def _output_finance(results: list[dict], fmt: str):
    """统一输出逻辑。"""
    if len(results) == 1:
        data = results[0]
        if fmt == "table" and "finance" in data:
            output(data["finance"], fmt, title=f"财务指标 {data['code']}")
        else:
            output(data, fmt)
    else:
        if fmt == "table":
            for data in results:
                if "finance" in data:
                    output(data["finance"], fmt, title=f"财务指标 {data['code']}")
                else:
                    output(data, fmt)
        else:
            output(results, fmt)


@click.command("finance")
@click.argument("codes", nargs=-1, required=True)
@click.option("--limit", "-n", default=4, show_default=True, help="显示最近 N 期报告")
@click.pass_context
def finance_cmd(ctx, codes, limit):
    """查看个股财务指标（实时拉取 + 入库）"""
    from fsi.db.connection import get_connection

    fmt = ctx.obj.get("fmt", "json")
    conn = get_connection(ctx.obj.get("db"))
    results = []
    for code in codes:
        try:
            rows = _fetch_from_api(code, limit)
            if rows:
                saved = save_finance_to_db(conn, code, rows)
                click.echo(f"{code} 获取 {len(rows)} 期财报，入库 {saved} 条", err=True)
                items = fetch_stock_finance(code, limit)
                results.append({"code": code, "periods": len(items), "finance": items})
            else:
                results.append({"code": code, "error": "获取失败"})
        except Exception as e:
            results.append({"code": code, "error": f"获取失败: {e}"})

    # AI 财报解读
    ai_data = {"stocks": results}
    click.echo("正在生成 AI 分析...", err=True)
    try:
        ai_text = call_bedrock(FINANCE_SYSTEM_PROMPT, build_user_message(ai_data))
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        ai_text = None

    if ai_text:
        for r in results:
            r["ai_analysis"] = ai_text

    _output_finance(results, fmt)
