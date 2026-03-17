"""API 健康检查 — 检测各数据源可用性，写入配置供各模块选源

数据任务 → 候选源映射：
  stock_daily:      akshare, yahoo
  stock_list:       akshare
  index_daily:      akshare
  etf_list:         akshare
  etf_daily:        akshare
  finance:          akshare, yahoo
  stock_news:       akshare, sina
  qvix:             akshare, optbbs
  hsgt:             akshare
  market_fund_flow: akshare
  sector_fund_flow: sina
  breaking_news:    eastmoney
  caixin_news:      akshare
  baidu_calendar:   akshare
  tencent_quote:    tencent
  global_index:     tencent
"""

import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ_BEIJING = ZoneInfo("Asia/Shanghai")
from pathlib import Path

# 配置文件路径
from fsi.config import FSI_DIR as _DATA_DIR
_HEALTH_FILE = _DATA_DIR / "api_health.json"

# 内存缓存
_cached_health: dict | None = None


def _data_dir() -> Path:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    return _DATA_DIR


# ── 各源检测函数 ─────────────────────────────────────────

def _check_akshare_stock_daily() -> dict:
    """测试 ak.stock_zh_a_daily()"""
    import akshare as ak
    t0 = time.time()
    df = ak.stock_zh_a_daily(symbol="sz000001", start_date="20260101", end_date="20260301", adjust="qfq")
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_yahoo_stock_daily() -> dict:
    """测试 yfinance 单股获取"""
    t0 = time.time()
    from fsi.fetcher.yahoo import fetch_stock_daily
    df = fetch_stock_daily("000001", "20260101", "20260301")
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_stock_list() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.stock_info_a_code_name()
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_index_daily() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.stock_zh_index_daily(symbol="sh000001")
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_etf_list() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.fund_etf_category_sina(symbol="ETF基金")
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_etf_daily() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.fund_etf_hist_sina(symbol="sh510300")
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_finance() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.stock_financial_analysis_indicator_em(symbol="000001.SZ", indicator="按报告期")
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回 None 或空"}
    return {"ok": True, "ms": ms}


def _check_yahoo_finance() -> dict:
    t0 = time.time()
    from fsi.fetcher.yahoo import fetch_stock_finance
    rows = fetch_stock_finance("000001", limit=4)
    ms = int((time.time() - t0) * 1000)
    if not rows:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_stock_news() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.stock_news_em(symbol="000001")
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_sina_stock_news() -> dict:
    """测试新浪 feed API"""
    import requests
    t0 = time.time()
    url = "https://feed.mix.sina.com.cn/api/roll/get"
    params = {"pageid": "153", "lid": "2516", "k": "000001", "num": 5, "page": 1}
    resp = requests.get(url, params=params, timeout=10,
                        headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    data = resp.json()
    ms = int((time.time() - t0) * 1000)
    result = data.get("result", {})
    items = result.get("data", [])
    if not items:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_qvix() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.index_option_50etf_qvix()
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回 None 或空"}
    return {"ok": True, "ms": ms}


def _check_optbbs_qvix() -> dict:
    import requests
    t0 = time.time()
    url = "http://1.optbbs.com/d/csv/d/k.csv"
    resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    ms = int((time.time() - t0) * 1000)
    text = resp.content.decode("gbk", errors="replace")
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return {"ok": False, "ms": ms, "error": "CSV 无数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_hsgt() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.stock_hsgt_fund_flow_summary_em()
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_market_fund_flow() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.stock_market_fund_flow()
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_sina_sector_fund_flow() -> dict:
    import requests
    t0 = time.time()
    url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_bk"
    params = {"page": 1, "num": 5, "sort": "netamount", "asc": 0, "fenlei": 0}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    ms = int((time.time() - t0) * 1000)
    if not data:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_eastmoney_breaking_news() -> dict:
    """测试东方财富 7x24 实时快讯"""
    import akshare as ak
    t0 = time.time()
    df = ak.stock_info_global_em()
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_caixin_news() -> dict:
    import akshare as ak
    t0 = time.time()
    df = ak.stock_news_main_cx()
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        return {"ok": False, "ms": ms, "error": "返回空数据"}
    return {"ok": True, "ms": ms}


def _check_akshare_baidu_calendar() -> dict:
    import akshare as ak
    from fsi.config import today_bj
    t0 = time.time()
    df = ak.news_economic_baidu(date=today_bj().strftime("%Y%m%d"))
    ms = int((time.time() - t0) * 1000)
    if df is None or df.empty:
        # 某些日期可能无经济数据，不算失败
        return {"ok": True, "ms": ms}
    return {"ok": True, "ms": ms}


def _check_tencent_quote() -> dict:
    import requests
    t0 = time.time()
    r = requests.get("https://qt.gtimg.cn/q=sh000001", timeout=10)
    r.raise_for_status()
    ms = int((time.time() - t0) * 1000)
    if "~" not in r.text:
        return {"ok": False, "ms": ms, "error": "解析异常"}
    return {"ok": True, "ms": ms}


def _check_tencent_global_index() -> dict:
    import requests
    t0 = time.time()
    r = requests.get("https://qt.gtimg.cn/q=us.DJI,hkHSI", timeout=10)
    r.raise_for_status()
    ms = int((time.time() - t0) * 1000)
    if "~" not in r.text:
        return {"ok": False, "ms": ms, "error": "解析异常"}
    return {"ok": True, "ms": ms}


# ── 任务 → 候选源 → 检测函数 映射 ─────────────────────────

TASK_SOURCES = {
    "stock_daily": {
        "akshare": _check_akshare_stock_daily,
        "yahoo": _check_yahoo_stock_daily,
    },
    "stock_list": {
        "akshare": _check_akshare_stock_list,
    },
    "index_daily": {
        "akshare": _check_akshare_index_daily,
    },
    "etf_list": {
        "akshare": _check_akshare_etf_list,
    },
    "etf_daily": {
        "akshare": _check_akshare_etf_daily,
    },
    "finance": {
        "akshare": _check_akshare_finance,
        "yahoo": _check_yahoo_finance,
    },
    "stock_news": {
        "akshare": _check_akshare_stock_news,
        "sina": _check_sina_stock_news,
    },
    "qvix": {
        "akshare": _check_akshare_qvix,
        "optbbs": _check_optbbs_qvix,
    },
    "hsgt": {
        "akshare": _check_akshare_hsgt,
    },
    "market_fund_flow": {
        "akshare": _check_akshare_market_fund_flow,
    },
    "sector_fund_flow": {
        "sina": _check_sina_sector_fund_flow,
    },
    "breaking_news": {
        "eastmoney": _check_eastmoney_breaking_news,
    },
    "caixin_news": {
        "akshare": _check_akshare_caixin_news,
    },
    "baidu_calendar": {
        "akshare": _check_akshare_baidu_calendar,
    },
    "tencent_quote": {
        "tencent": _check_tencent_quote,
    },
    "global_index": {
        "tencent": _check_tencent_global_index,
    },
}


def check_all(verbose: bool = False) -> dict:
    """测试所有数据源端点，返回完整结果 dict

    Returns:
        {"last_check": "...", "sources": {"stock_daily": {"akshare": {...}, "yahoo": {...}, "preferred": "..."}, ...}}
    """
    results = {"last_check": datetime.now(tz=_TZ_BEIJING).isoformat(timespec="seconds"), "sources": {}}

    for task, sources in TASK_SOURCES.items():
        task_result = {}
        for source_name, check_fn in sources.items():
            if verbose:
                print(f"  检测 {task}/{source_name} ...", end=" ", flush=True)
            try:
                r = check_fn()
            except Exception as e:
                r = {"ok": False, "ms": 0, "error": str(e)[:200]}
            task_result[source_name] = r
            if verbose:
                status = "✓" if r["ok"] else "✗"
                print(f"{status} ({r['ms']}ms)")

        # 选择 preferred：最快的可用源
        ok_sources = [(name, info["ms"]) for name, info in task_result.items() if info["ok"]]
        if ok_sources:
            ok_sources.sort(key=lambda x: x[1])
            task_result["preferred"] = ok_sources[0][0]
        else:
            # 全部不可用，默认选第一个候选源（运行时仍会尝试）
            task_result["preferred"] = list(sources.keys())[0]

        results["sources"][task] = task_result

    return results


def save_health(results: dict) -> str:
    """将检测结果写入 data/api_health.json，返回文件路径"""
    global _cached_health
    path = _data_dir() / "api_health.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    _cached_health = results
    return str(path)


def load_health() -> dict:
    """读取 api_health.json（带内存缓存）

    文件不存在则返回默认值（所有源标记为可用，preferred 取第一个候选）。
    """
    global _cached_health
    if _cached_health is not None:
        return _cached_health

    if _HEALTH_FILE.exists():
        try:
            with open(_HEALTH_FILE, "r", encoding="utf-8") as f:
                _cached_health = json.load(f)
                return _cached_health
        except (json.JSONDecodeError, OSError):
            pass

    # 默认值：所有源标记可用
    defaults = {"last_check": None, "sources": {}}
    for task, sources in TASK_SOURCES.items():
        task_entry = {}
        first_source = None
        for name in sources:
            task_entry[name] = {"ok": True, "ms": 0}
            if first_source is None:
                first_source = name
        task_entry["preferred"] = first_source
        defaults["sources"][task] = task_entry
    _cached_health = defaults
    return _cached_health


def get_source(task: str) -> str:
    """获取指定数据任务的 preferred 源名称

    Args:
        task: 数据任务名，如 "stock_daily", "finance", "qvix" 等

    Returns:
        源名称，如 "akshare", "yahoo", "optbbs", "sina", "tencent"
    """
    health = load_health()
    sources = health.get("sources", {})
    task_info = sources.get(task, {})
    preferred = task_info.get("preferred")
    if preferred:
        return preferred
    # 未配置时返回默认第一候选
    if task in TASK_SOURCES:
        return list(TASK_SOURCES[task].keys())[0]
    return "akshare"


def reload_health():
    """强制清除缓存，下次 get_source() 会重新读取文件"""
    global _cached_health
    _cached_health = None
