"""全球主要指数行情采集 — 美股隔夜 + 港股实时 + A50 期货

通过腾讯财经 qt.gtimg.cn 批量获取美股/港股，新浪期货接口获取 A50。
不继承 BaseFetcher（无需 DB 写入），直接用 tenacity 重试。
数据不持久化，每次分析时实时获取。

时区处理：
- 腾讯 API 返回的美股 time 字段为美东时间，需转换为北京时间
- 港股 time 字段为 HKT（与北京时间一致，仅格式化）
- 新浪期货 A50 返回新加坡时间（SGT = 北京时间，直接透传）
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

_TZ_BEIJING = ZoneInfo("Asia/Shanghai")
_TZ_US_EASTERN = ZoneInfo("America/New_York")


# 美股三大指数
US_INDICES = {
    "us.DJI": "道琼斯",
    "usINX": "标普500",
    "us.IXIC": "纳斯达克",
}

# 港股主要指数
HK_INDICES = {
    "hkHSI": "恒生指数",
    "hkHSTECH": "恒生科技指数",
    "hkHSCEI": "国企指数",
}


def _convert_us_time_to_beijing(time_str: str) -> str:
    """将美股时间（美东 ET）转换为北京时间

    腾讯 API 返回格式: "2026-03-04 16:39:34" (美东时间)
    输出: "2026-03-05 05:39:34" (北京时间)
    """
    if not time_str or not time_str.strip():
        return time_str
    try:
        dt = datetime.strptime(time_str.strip(), "%Y-%m-%d %H:%M:%S")
        dt_et = dt.replace(tzinfo=_TZ_US_EASTERN)
        dt_bj = dt_et.astimezone(_TZ_BEIJING)
        return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return time_str


def _normalize_hk_time(time_str: str) -> str:
    """标准化港股时间格式（HKT = 北京时间，仅统一格式）

    腾讯 API 返回格式: "2026/03/05 15:25:26"
    输出: "2026-03-05 15:25:26"
    """
    if not time_str or not time_str.strip():
        return time_str
    return time_str.strip().replace("/", "-")


def _parse_tencent_global(text: str, symbol_map: dict, market: str = "") -> list[dict]:
    """解析腾讯全球指数批量返回（与 A 股字段位置一致）

    Args:
        market: "us" 或 "hk"，用于时区转换
    """
    results = []
    lines = [l for l in text.strip().split(";") if l.strip()]

    for line in lines:
        parts = line.split("~")
        if len(parts) < 35:
            continue

        def safe_float(idx):
            try:
                return float(parts[idx])
            except (ValueError, IndexError):
                return None

        # 从 v_usXXX= 中提取符号用于匹配名称
        name = parts[1] if parts[1] else ""

        raw_time = parts[30] if len(parts) > 30 else None

        # 时区转换：美股 ET → 北京，港股 HKT 仅格式化
        if market == "us" and raw_time:
            display_time = _convert_us_time_to_beijing(raw_time)
        elif market == "hk" and raw_time:
            display_time = _normalize_hk_time(raw_time)
        else:
            display_time = raw_time

        results.append({
            "name": name,
            "price": safe_float(3),
            "pct_change": safe_float(32),
            "change_amt": safe_float(31),
            "open": safe_float(5),
            "high": safe_float(33),
            "low": safe_float(34),
            "prev_close": safe_float(4),
            "time": display_time,
        })

    return results


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_us_index_quotes() -> list[dict]:
    """获取美股三大指数最新行情（隔夜数据）

    Returns:
        [{"name": "道琼斯", "price": 48501.27, "pct_change": -0.83, ...}, ...]
    """
    symbols = ",".join(US_INDICES.keys())
    r = requests.get(f"https://qt.gtimg.cn/q={symbols}", timeout=10)
    r.raise_for_status()
    return _parse_tencent_global(r.text, US_INDICES, market="us")


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_hk_index_quotes() -> list[dict]:
    """获取港股主要指数最新行情

    Returns:
        [{"name": "恒生指数", "price": 25131.08, "pct_change": -2.47, ...}, ...]
    """
    symbols = ",".join(HK_INDICES.keys())
    r = requests.get(f"https://qt.gtimg.cn/q={symbols}", timeout=10)
    r.raise_for_status()
    return _parse_tencent_global(r.text, HK_INDICES, market="hk")


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_a50_quote() -> dict | None:
    """获取富时中国A50期货最新行情（新浪期货接口）

    Returns:
        {"name": "富时中国A50", "price": 14640.1, "pct_change": 0.86, ...} 或 None
    """
    r = requests.get(
        "https://hq.sinajs.cn/list=hf_CHA50CFD",
        headers={"Referer": "https://finance.sina.com.cn"},
        timeout=10,
    )
    r.raise_for_status()
    # 解析: var hq_str_hf_CHA50CFD="val1,val2,...";
    text = r.text.strip()
    if not text or '=""' in text:
        return None
    start = text.index('"') + 1
    end = text.rindex('"')
    fields = text[start:end].split(",")
    if len(fields) < 13:
        return None

    def sf(idx):
        try:
            return float(fields[idx])
        except (ValueError, IndexError):
            return None

    price = sf(0)
    prev_close = sf(7)
    if price is None or prev_close is None or prev_close == 0:
        return None

    pct_change = round((price - prev_close) / prev_close * 100, 2)
    change_amt = round(price - prev_close, 2)
    date_str = fields[12].strip() if len(fields) > 12 else ""
    time_str = fields[6].strip() if len(fields) > 6 else ""
    display_time = f"{date_str} {time_str}" if date_str and time_str else ""

    return {
        "name": "富时中国A50",
        "price": price,
        "pct_change": pct_change,
        "change_amt": change_amt,
        "open": sf(8),
        "high": sf(4),
        "low": sf(5),
        "prev_close": prev_close,
        "time": display_time,
    }


def fetch_global_index_quotes() -> dict:
    """获取全球主要指数（美股 + 港股 + A50），graceful 失败

    Returns:
        {"us_indices": [...], "hk_indices": [...], "a50": {...}} 或部分/空 dict
    """
    result = {}

    try:
        us = fetch_us_index_quotes()
        if us:
            result["us_indices"] = us
    except Exception:
        pass

    try:
        hk = fetch_hk_index_quotes()
        if hk:
            result["hk_indices"] = hk
    except Exception:
        pass

    try:
        a50 = fetch_a50_quote()
        if a50:
            result["a50"] = a50
    except Exception:
        pass

    return result
