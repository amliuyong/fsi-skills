"""A 股交易日历模块 — 判断当前是否为交易时间。"""

import json
import os
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_BJ = ZoneInfo("Asia/Shanghai")
from fsi.config import FSI_DIR as _DATA_DIR
_CACHE_FILE = _DATA_DIR / "trading_calendar.json"
_CACHE_MAX_DAYS = 7


def _load_cache() -> set[str] | None:
    """从本地缓存加载交易日集合，过期或不存在返回 None。"""
    if not _CACHE_FILE.exists():
        return None
    try:
        data = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        updated = date.fromisoformat(data["updated"])
        today = datetime.now(tz=_BJ).date()
        if (today - updated).days > _CACHE_MAX_DAYS:
            return None
        return set(data.get("days", []))
    except Exception:
        return None


def _fetch_and_cache() -> set[str] | None:
    """调用 AKShare 拉取交易日历并缓存，失败返回 None。"""
    try:
        import akshare as ak

        df = ak.tool_trade_date_hist_sina()
        if df is None or df.empty:
            return None
        # 只保留当年数据，减小缓存体积
        year = datetime.now(tz=_BJ).year
        col = df.columns[0]  # 通常是 "trade_date"
        days = []
        for v in df[col]:
            d = str(v)[:10]  # "YYYY-MM-DD"
            if d.startswith(str(year)):
                days.append(d)
        if not days:
            return None
        today_str = datetime.now(tz=_BJ).strftime("%Y-%m-%d")
        cache = {"updated": today_str, "days": sorted(days)}
        os.makedirs(_CACHE_FILE.parent, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
        return set(days)
    except Exception:
        return None


def _get_trading_days() -> set[str] | None:
    """获取交易日集合（优先缓存，miss 则拉取）。"""
    days = _load_cache()
    if days is not None:
        return days
    return _fetch_and_cache()


def is_trading_day(d: date | None = None) -> bool:
    """判断指定日期是否为 A 股交易日。

    缓存失效且 AKShare 调用失败时 fallback: weekday < 5。
    """
    if d is None:
        d = datetime.now(tz=_BJ).date()
    days = _get_trading_days()
    if days is not None:
        return d.isoformat() in days
    # fallback: 非周末即视为交易日
    return d.weekday() < 5


def is_trading_time() -> bool:
    """判断当前北京时间是否在 A 股交易时段。

    交易日 + 时间窗口 09:30-11:30 或 13:00-14:57。
    避开集合竞价阶段（9:15-9:30 虚拟盘口不可靠，14:57-15:00 尾盘集合竞价）。
    """
    now = datetime.now(tz=_BJ)
    if not is_trading_day(now.date()):
        return False
    t = now.hour * 60 + now.minute  # 分钟数
    # 09:30 ~ 11:30
    if 9 * 60 + 30 <= t <= 11 * 60 + 30:
        return True
    # 13:00 ~ 14:57
    if 13 * 60 <= t <= 14 * 60 + 57:
        return True
    return False
