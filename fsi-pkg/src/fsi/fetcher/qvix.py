"""QVIX 恐慌指数采集 — 50ETF 波动率指数

不继承 BaseFetcher（无需 DB 写入/sync_log），直接用 tenacity 重试。
数据不持久化，每次分析时实时获取。

QVIX（中国波指）反映市场对未来 30 日波动率的预期，
类似美股 VIX，是 A 股市场恐慌/贪婪情绪的核心指标。
"""

from io import StringIO

import akshare as ak
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


def _parse_qvix_csv(text: str) -> pd.DataFrame:
    """解析 optbbs.com CSV 格式的 QVIX 数据（AKShare 失败时的 fallback）"""
    df = pd.read_csv(StringIO(text))
    # optbbs CSV 列名可能为：日期,开盘,最高,最低,收盘,涨跌,涨跌幅
    # 统一列名
    rename_map = {
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "涨跌": "change",
        "涨跌幅": "pct_change",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
    return df


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _fetch_qvix_from_optbbs(days: int) -> list[dict]:
    """直接从 optbbs.com 获取 QVIX CSV（AKShare 接口的数据源）"""
    url = "http://1.optbbs.com/d/csv/d/k.csv"
    resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    text = resp.content.decode("gbk", errors="replace")
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return []
    # 格式: date,open,high,low,close,...  (第一行是header，跳过)
    records = []
    for line in lines[1:]:
        cols = line.strip().split(",")
        if len(cols) < 5 or not cols[0]:
            continue
        try:
            date_str = cols[0].strip().replace("/", "-")
            records.append({
                "date": date_str,
                "open": round(float(cols[1]), 4),
                "high": round(float(cols[2]), 4),
                "low": round(float(cols[3]), 4),
                "close": round(float(cols[4]), 4),
            })
        except (ValueError, IndexError):
            continue
    return records[-days:] if records else []


def _fetch_qvix_from_akshare(days: int) -> list[dict]:
    """通过 AKShare 获取 QVIX 日线数据"""
    df = ak.index_option_50etf_qvix()
    if df is None or df.empty:
        return []
    # 确保日期列为字符串（AKShare 可能返回 datetime.date 或 Timestamp）
    for col in df.columns:
        if df[col].dtype == object or hasattr(df[col].iloc[0], 'strftime'):
            try:
                df[col] = df[col].astype(str)
            except Exception:
                pass
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


def fetch_qvix_daily(days: int = 20) -> list[dict]:
    """获取 50ETF QVIX 日线数据

    按 api_health.json 配置选源（akshare 或 optbbs），失败后回退另一个源。

    Returns:
        [{"date": "2026-03-04", "open": 15.2, "close": 14.8, ...}, ...]
    """
    from fsi.health import get_source
    source = get_source("qvix")

    if source == "optbbs":
        fetchers = [("optbbs", _fetch_qvix_from_optbbs), ("akshare", _fetch_qvix_from_akshare)]
    else:
        fetchers = [("akshare", _fetch_qvix_from_akshare), ("optbbs", _fetch_qvix_from_optbbs)]

    for name, fn in fetchers:
        try:
            records = fn(days)
            if records:
                return records
        except Exception:
            pass
    return []


def fetch_qvix_intraday() -> list[dict]:
    """获取 50ETF QVIX 盘中分时数据（交易时段）

    Returns:
        [{"time": "09:30", "qvix": 15.2}, ...] 或空列表（非交易时段/获取失败）
    """
    try:
        df = ak.index_option_50etf_min_qvix()
        if df is None or df.empty:
            return []

        records = []
        for _, row in df.iterrows():
            entry = {}
            for col in df.columns:
                val = row[col]
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    if isinstance(val, (pd.Timestamp,)):
                        entry[col] = val.strftime("%Y-%m-%d %H:%M:%S")
                    elif hasattr(val, "isoformat"):
                        entry[col] = str(val)
                    elif isinstance(val, (int, float)):
                        entry[col] = round(float(val), 4)
                    elif str(val).strip():
                        entry[col] = str(val).strip()
            # 盘中数据必须有 qvix 值才有意义
            if entry and "qvix" in entry:
                records.append(entry)
        return records
    except Exception:
        return []
