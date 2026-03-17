"""市场热点新闻采集 — 东财 7x24 快讯 + 财新数据通 + 百度经济日历

不继承 BaseFetcher（无需 DB 写入/sync_log），直接用 tenacity 重试。
数据不持久化，每次分析时鲜活获取。
"""

from datetime import date

from fsi.config import today_bj

import akshare as ak
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_breaking_news(limit: int = 30) -> list[dict]:
    """获取东方财富 7x24 实时快讯

    Returns:
        [{"发布时间": "2026-03-05 10:43:28", "标题": "...", "摘要": "..."}, ...]
    """
    df = ak.stock_info_global_em()
    if df.empty:
        return []

    records = []
    for _, row in df.head(limit).iterrows():
        records.append({
            "发布时间": str(row.get("发布时间", "")),
            "标题": str(row.get("标题", "")),
            "摘要": str(row.get("摘要", "")),
        })

    return records


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_caixin_news(limit: int = 20, tags: list[str] | None = None) -> list[dict]:
    """获取财新网数据通市场新闻（市场动态、今日热点、华尔街原声等）

    Returns:
        [{"标签": "市场动态", "摘要": "..."}, ...]
    """
    df = ak.stock_news_main_cx()
    if df.empty:
        return []

    # 只保留标签和摘要，减少 token 消耗
    records = []
    for _, row in df.head(limit * 2).iterrows():
        tag = str(row.get("tag", row.get("标签", "")))
        summary = str(row.get("summary", row.get("摘要", row.get("内容", ""))))

        if tags and tag not in tags:
            continue

        records.append({"标签": tag, "摘要": summary})
        if len(records) >= limit:
            break

    return records


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_economic_calendar(target_date: date | None = None) -> list[dict]:
    """获取百度经济日历（GDP/CPI/PMI 等经济数据发布）

    Returns:
        [{"时间": "", "地区": "", "事件": "", "重要性": "", ...}, ...]
    """
    if target_date is None:
        target_date = today_bj()

    date_str = target_date.strftime("%Y%m%d")
    df = ak.news_economic_baidu(date=date_str)
    if df.empty:
        return []

    records = []
    for _, row in df.iterrows():
        entry = {}
        for col in df.columns:
            val = row[col]
            if val is not None and str(val).strip():
                entry[col] = str(val).strip()
        if entry:
            records.append(entry)

    return records
