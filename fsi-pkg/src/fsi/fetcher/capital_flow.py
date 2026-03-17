"""资金流向采集 — 沪深港通 + 大盘资金流 + 行业资金流

不继承 BaseFetcher（无需 DB 写入/sync_log），直接用 tenacity 重试。
数据不持久化，每次分析时实时获取。
"""

import akshare as ak
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_hsgt_summary() -> list[dict]:
    """获取今日沪深港通资金流向汇总（沪股通/深股通/北向合计等）

    Returns:
        [{"类型": "沪股通", "净流入": ..., ...}, ...]
    """
    df = ak.stock_hsgt_fund_flow_summary_em()
    if df is None or df.empty:
        return []

    _TRADE_STATUS = {1: "未开盘(1)", 2: "交易中(2)", 3: "已收盘(3)"}
    _MONEY_FIELDS = {"成交净买额", "资金净流入", "当日资金余额"}

    records = []
    for _, row in df.iterrows():
        entry = {}
        for col in df.columns:
            val = row[col]
            if val is not None and str(val).strip():
                if col == "交易状态" and isinstance(val, (int, float)):
                    entry[col] = _TRADE_STATUS.get(int(val), f"未知({int(val)})")
                elif isinstance(val, (int, float)):
                    entry[col] = round(float(val), 4)
                else:
                    entry[col] = str(val).strip()
        # 北向资金：2024年8月起交易所不再实时披露净买入数据
        if entry.get("资金方向") == "北向":
            if all(entry.get(f, None) == 0.0 for f in _MONEY_FIELDS if f in entry):
                entry["数据状态"] = "不可用（交易所已停止披露北向逐笔数据）"
        if entry:
            records.append(entry)

    return records


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_northbound_hist(days: int = 10) -> list[dict]:
    """获取北向资金近 N 日历史流入数据

    注意：2024年8月起交易所不再实时披露北向资金逐笔净买入数据，
    东方财富接口返回的成交净买额等字段可能全部为 NaN。
    此时返回空列表，避免误导。

    Returns:
        [{"日期": "2026-03-04", "当日成交净买额": ..., ...}, ...] 或空列表
    """
    try:
        df = ak.stock_hsgt_hist_em(symbol="北向资金")
    except Exception:
        return []
    if df is None or df.empty:
        return []

    # 取最近 N 日
    df = df.tail(days)

    # 检查核心字段是否全为 NaN — 2024年8月后交易所不再披露
    key_col = "当日成交净买额"
    if key_col in df.columns and df[key_col].isna().all():
        return []  # 数据不可用，返回空

    records = []
    for _, row in df.iterrows():
        entry = {}
        for col in df.columns:
            val = row[col]
            if val is not None:
                if isinstance(val, (pd.Timestamp,)):
                    entry[col] = val.strftime("%Y-%m-%d")
                elif hasattr(val, "isoformat"):
                    entry[col] = val.isoformat()[:10]
                elif isinstance(val, (int, float)) and not pd.isna(val):
                    entry[col] = round(float(val), 4)
                elif str(val).strip():
                    entry[col] = str(val).strip()
        if entry:
            records.append(entry)

    return records


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_market_fund_flow(days: int = 10) -> list[dict]:
    """获取大盘资金流向（主力/超大单/大单/中单/小单净流入）

    Returns:
        [{"日期": "2026-03-04", "主力净流入": ..., ...}, ...]
    """
    df = ak.stock_market_fund_flow()
    if df is None or df.empty:
        return []

    # 取最近 N 日
    df = df.tail(days)

    records = []
    for _, row in df.iterrows():
        entry = {}
        for col in df.columns:
            val = row[col]
            if val is not None:
                if isinstance(val, (pd.Timestamp,)):
                    entry[col] = val.strftime("%Y-%m-%d")
                elif hasattr(val, "isoformat"):
                    entry[col] = val.isoformat()[:10]
                elif isinstance(val, (int, float)) and not pd.isna(val):
                    entry[col] = round(float(val), 4)
                elif str(val).strip():
                    entry[col] = str(val).strip()
        if entry:
            records.append(entry)

    return records


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_sector_fund_flow(top: int = 15) -> list[dict]:
    """获取今日行业资金流排行（新浪 MoneyFlow API，海外可用）

    Returns:
        [{"行业": "半导体", "净流入(亿)": 12.5, "流入(亿)": 50.2, ...}, ...]
    """
    import requests

    url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_bk"
    params = {
        "page": 1,
        "num": top,
        "sort": "netamount",
        "asc": 0,
        "fenlei": 0,  # 0=行业, 1=概念
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return []

    def _safe_float(val):
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    records = []
    for item in data:
        net_amt = _safe_float(item.get("netamount"))
        in_amt = _safe_float(item.get("inamount"))
        out_amt = _safe_float(item.get("outamount"))
        change_pct = _safe_float(item.get("avg_changeratio"))
        net_ratio = _safe_float(item.get("ratioamount"))
        ts_change = _safe_float(item.get("ts_changeratio"))
        records.append({
            "行业": item.get("name", ""),
            "涨跌幅(%)": round(change_pct * 100, 2) if change_pct is not None else None,
            "净流入(亿)": round(net_amt / 1e8, 2) if net_amt is not None else None,
            "流入(亿)": round(in_amt / 1e8, 2) if in_amt is not None else None,
            "流出(亿)": round(out_amt / 1e8, 2) if out_amt is not None else None,
            "净占比(%)": round(net_ratio * 100, 2) if net_ratio is not None else None,
            "领涨股": item.get("ts_name", ""),
            "领涨股涨跌(%)": round(ts_change * 100, 2) if ts_change is not None else None,
        })

    return records
