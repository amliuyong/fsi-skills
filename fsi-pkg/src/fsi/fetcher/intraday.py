"""分时数据采集 — 腾讯分时 API（主） + 东方财富 fallback + 文件缓存

腾讯 API 无需代理，直连可用；东方财富在海外服务器需代理。
缓存保存为 JSON，自动清理旧日期文件。
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from fsi.config import FSI_DIR, TZ_BJ

# 缓存目录
CACHE_DIR = FSI_DIR / "cache"


def _is_trading_time() -> bool:
    """判断当前是否处于 A 股交易时间（北京时间 9:15-15:05，留 5 分钟余量）。"""
    now = datetime.now(tz=TZ_BJ)
    # 周末不是交易时间
    if now.weekday() >= 5:
        return False
    t = now.hour * 100 + now.minute
    return 915 <= t <= 1505


# ── 腾讯前缀 ─────────────────────────────────────────────────

def _tencent_symbol(code: str, asset_type: str) -> str:
    """返回腾讯 API 符号，如 sh600519、sz000001。"""
    if asset_type == "index":
        prefix = "sz" if code.startswith("399") else "sh"
    elif code.startswith(("6", "5")):
        prefix = "sh"
    elif code.startswith(("0", "3", "1")):
        prefix = "sz"
    else:
        prefix = "bj"
    return f"{prefix}{code}"


# ── 缓存 ─────────────────────────────────────────────────────

def _cache_path(code: str, date_str: str, asset_type: str = "") -> Path:
    tag = f"_{asset_type}" if asset_type else ""
    return CACHE_DIR / f"intraday_{code}{tag}_{date_str}.json"


def _clean_old_cache(code: str, keep_date: str, asset_type: str = ""):
    if not CACHE_DIR.exists():
        return
    tag = f"_{asset_type}" if asset_type else ""
    keep_name = f"intraday_{code}{tag}_{keep_date}.json"
    for f in CACHE_DIR.glob(f"intraday_{code}{tag}_*.json"):
        if f.name != keep_name:
            f.unlink(missing_ok=True)


def _load_cache(code: str, date_str: str, asset_type: str = "") -> pd.DataFrame | None:
    path = _cache_path(code, date_str, asset_type)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)
        if not records:
            return None
        df = pd.DataFrame(records)
        for col in ("收盘", "成交量", "成交额", "均价"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception:
        return None


def _save_cache(code: str, date_str: str, df: pd.DataFrame, asset_type: str = ""):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(code, date_str, asset_type)
    records = df.to_dict(orient="records")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
    _clean_old_cache(code, date_str, asset_type)


# ── 腾讯分时 API（主源）────────────────────────────────────────

@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _fetch_tencent(code: str, asset_type: str) -> tuple[pd.DataFrame, str, dict]:
    """从腾讯 ifzq API 获取当日分时数据。

    返回: (DataFrame, trade_date, qt_info)
    - DataFrame 列：时间, 收盘, 成交量, 成交额, 均价
    - qt_info: {"name": ..., "prev_close": ...}

    腾讯分时数据格式："HHMM price cum_volume cum_amount"
    成交量和成交额为累计值，需差分还原每分钟增量。
    """
    symbol = _tencent_symbol(code, asset_type)
    url = f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={symbol}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()

    inner = data.get("data", {}).get(symbol, {})
    minute_data = inner.get("data", {})
    items = minute_data.get("data", [])
    trade_date = minute_data.get("date", "")

    if not items:
        return pd.DataFrame(), "", {}

    # 解析 qt 行情信息
    qt_info = {}
    qt = inner.get("qt", {}).get(symbol, [])
    if len(qt) > 5:
        qt_info["name"] = qt[1]
        try:
            qt_info["prev_close"] = float(qt[4])
        except (ValueError, IndexError):
            pass

    # 格式化日期
    if len(trade_date) == 8:
        trade_date_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    else:
        trade_date_fmt = trade_date

    # 解析分钟数据
    records = []
    prev_vol = 0
    prev_amt = 0.0
    for item in items:
        parts = item.split(" ")
        if len(parts) < 4:
            continue
        hhmm = parts[0]
        price = float(parts[1])
        cum_vol = int(parts[2])
        cum_amt = float(parts[3])

        # 差分还原每分钟增量
        vol = cum_vol - prev_vol
        amt = cum_amt - prev_amt
        prev_vol = cum_vol
        prev_amt = cum_amt

        # 计算均价（累计成交额 / 累计成交量）
        avg_price = cum_amt / cum_vol if cum_vol > 0 else price

        time_str = f"{trade_date_fmt} {hhmm[:2]}:{hhmm[2:]}"
        records.append({
            "时间": time_str,
            "收盘": price,
            "成交量": vol,
            "成交额": amt,
            "均价": avg_price,
        })

    df = pd.DataFrame(records)
    return df, trade_date_fmt, qt_info


# ── 东方财富 API（fallback）───────────────────────────────────

def _get_em_market_code(code: str, asset_type: str) -> int:
    if asset_type == "index":
        return 0 if code.startswith("399") else 1
    return 1 if code.startswith(("6", "5")) else 0


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _fetch_eastmoney(code: str, asset_type: str) -> pd.DataFrame:
    """从东方财富 trends2 API 获取近 5 天 1 分钟数据（fallback）。"""
    market_code = _get_em_market_code(code, asset_type)
    url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "ndays": "5",
        "iscr": "0",
        "secid": f"{market_code}.{code}",
    }
    r = requests.get(url, timeout=15, params=params)
    r.raise_for_status()
    data_json = r.json()

    if not data_json.get("data") or not data_json["data"].get("trends"):
        return pd.DataFrame()

    rows = [item.split(",") for item in data_json["data"]["trends"]]
    df = pd.DataFrame(rows,
                      columns=["时间", "开盘", "收盘", "最高", "最低",
                               "成交量", "成交额", "均价"])
    for col in ("收盘", "成交量", "成交额", "均价"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── 主入口 ────────────────────────────────────────────────────

def fetch_intraday(code: str, asset_type: str = "stock",
                   verbose: bool = False) -> tuple[pd.DataFrame | None, dict]:
    """获取分时数据（交易时间拿新数据，收盘后走缓存）。

    返回: (DataFrame, meta)
    - DataFrame 列：时间, 收盘, 成交量, 成交额, 均价
    - meta: {"name": ..., "prev_close": ..., "trade_date": ..., "cache_file": ...}
    - 失败返回 (None, {})
    """
    today = datetime.now(tz=TZ_BJ).strftime("%Y-%m-%d")
    meta = {}
    trading = _is_trading_time()

    # 非交易时间：优先使用缓存（需完整性校验）
    if not trading:
        cached = _load_cache(code, today, asset_type)
        if cached is not None and not cached.empty:
            # 收盘后（>=15:05）要求缓存包含下午数据，否则重新拉取
            now = datetime.now(tz=TZ_BJ)
            after_close = now.weekday() < 5 and now.hour * 100 + now.minute >= 1505
            cache_complete = True
            if after_close and "时间" in cached.columns:
                last_time = cached["时间"].iloc[-1]  # "2026-03-09 14:57"
                hhmm = last_time.split(" ")[-1].replace(":", "")
                if hhmm.isdigit() and int(hhmm) < 1455:
                    cache_complete = False
                    if verbose:
                        print(f"[cache] 缓存不完整（最后 {last_time}），重新拉取",
                              file=sys.stderr)
            if cache_complete:
                if verbose:
                    print(f"[cache] 使用缓存 {_cache_path(code, today, asset_type).name}",
                          file=sys.stderr)
                meta["trade_date"] = today
                meta["cache_file"] = str(_cache_path(code, today, asset_type))
                return cached, meta

    # 腾讯 API（主源）
    try:
        df, trade_date, qt_info = _fetch_tencent(code, asset_type)
        if not df.empty:
            meta.update(qt_info)
            meta["trade_date"] = trade_date
            meta["source"] = "tencent"
            _save_cache(code, trade_date, df, asset_type)
            meta["cache_file"] = str(_cache_path(code, trade_date, asset_type))
            if verbose:
                print(f"[fetch/tencent] {code} 分时数据 {trade_date}，{len(df)} 条",
                      file=sys.stderr)
            return df, meta
    except Exception as e:
        if verbose:
            print(f"[warn] 腾讯分时获取失败 {code}: {e}", file=sys.stderr)

    # 东方财富 fallback
    try:
        df = _fetch_eastmoney(code, asset_type)
        if not df.empty:
            df["_date"] = df["时间"].str[:10]
            latest_date = df["_date"].iloc[-1]
            day_df = df[df["_date"] == latest_date].drop(
                columns=["_date"]).reset_index(drop=True)
            keep_cols = [c for c in ("时间", "收盘", "成交量", "成交额", "均价")
                         if c in day_df.columns]
            day_df = day_df[keep_cols]
            meta["trade_date"] = latest_date
            meta["source"] = "eastmoney"
            _save_cache(code, latest_date, day_df, asset_type)
            meta["cache_file"] = str(_cache_path(code, latest_date, asset_type))
            if verbose:
                print(f"[fetch/eastmoney] {code} 分时数据 {latest_date}，{len(day_df)} 条",
                      file=sys.stderr)
            return day_df, meta
    except Exception as e:
        if verbose:
            print(f"[error] 分时数据获取失败 {code}: {e}", file=sys.stderr)

    # 全部失败，尝试读取已有缓存兜底
    cached = _load_cache(code, today, asset_type)
    if cached is not None and not cached.empty:
        if verbose:
            print(f"[cache/fallback] 使用缓存 {_cache_path(code, today, asset_type).name}",
                  file=sys.stderr)
        meta["trade_date"] = today
        meta["cache_file"] = str(_cache_path(code, today, asset_type))
        return cached, meta

    return None, {}
