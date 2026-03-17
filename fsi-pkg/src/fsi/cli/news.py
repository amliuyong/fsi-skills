"""个股新闻（东方财富 / 新浪 feed）"""

import click
import akshare as ak
import pandas as pd

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import NEWS_SYSTEM_PROMPT, build_user_message
from fsi.output.formatter import output


def _fetch_news_akshare(code: str, limit: int = 20) -> list[dict]:
    """从东方财富获取个股新闻"""
    df = ak.stock_news_em(symbol=code)
    items = []
    for _, row in df.head(limit).iterrows():
        items.append({
            "标题": row.get("新闻标题", ""),
            "时间": row.get("发布时间", ""),
            "来源": row.get("文章来源", ""),
            "内容": row.get("新闻内容", ""),
            "链接": row.get("新闻链接", ""),
        })
    return items


def _fetch_news_sina(code: str, limit: int = 20) -> list[dict]:
    """从新浪 feed API 获取个股新闻"""
    import requests
    from datetime import datetime
    from zoneinfo import ZoneInfo
    _tz_bj = ZoneInfo("Asia/Shanghai")
    url = "https://feed.mix.sina.com.cn/api/roll/get"
    params = {"pageid": "153", "lid": "2516", "k": code, "num": limit, "page": 1}
    resp = requests.get(url, params=params, timeout=10,
                        headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    data = resp.json()
    result = data.get("result", {})
    raw_items = result.get("data", [])
    items = []
    for item in raw_items[:limit]:
        ts = item.get("ctime", "")
        try:
            # Unix timestamp → 北京时间（避免服务器 UTC 时区导致偏差）
            time_str = datetime.fromtimestamp(int(ts), tz=_tz_bj).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
        except (ValueError, OSError):
            time_str = str(ts)
        items.append({
            "标题": item.get("title", ""),
            "时间": time_str,
            "来源": item.get("media_name", item.get("author", "")),
            "内容": item.get("summary", item.get("intro", "")),
            "链接": item.get("url", ""),
        })
    return items


def fetch_stock_news(code: str, limit: int = 20) -> list[dict]:
    """获取个股新闻，按 api_health.json 配置选源。供其他模块复用。"""
    from fsi.health import get_source
    source = get_source("stock_news")

    if source == "sina":
        fetchers = [("sina", _fetch_news_sina), ("akshare", _fetch_news_akshare)]
    else:
        fetchers = [("akshare", _fetch_news_akshare), ("sina", _fetch_news_sina)]

    for name, fn in fetchers:
        try:
            items = fn(code, limit)
            if items:
                return items
        except Exception:
            pass
    return []


def save_news_to_db(conn, code: str, news_items: list[dict]) -> int:
    """将新闻存入 DuckDB，返回新增条数。基于 (code, url) 去重。"""
    if not news_items:
        return 0
    rows = []
    for item in news_items:
        url = item.get("链接", "")
        if not url:
            continue
        rows.append({
            "code": code,
            "url": url,
            "title": item.get("标题", ""),
            "content": item.get("内容", ""),
            "source": item.get("来源", ""),
            "pub_time": item.get("时间", None),
        })
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    conn.execute("INSERT OR REPLACE INTO stock_news SELECT *, current_timestamp FROM df")
    return len(df)


@click.command("news")
@click.argument("codes", nargs=-1, required=True)
@click.option("--limit", "-n", default=20, show_default=True, help="每只股票返回条数")
@click.pass_context
def news_cmd(ctx, codes, limit):
    """获取个股新闻（支持多只: fsi news 000001 600519）"""
    from fsi.db.connection import get_connection

    fmt = ctx.obj.get("fmt", "json")
    conn = get_connection(ctx.obj.get("db"))
    results = []
    for code in codes:
        items = fetch_stock_news(code, limit)
        if items:
            saved = save_news_to_db(conn, code, items)
            click.echo(f"{code} 获取 {len(items)} 条新闻，入库 {saved} 条", err=True)
            results.append({"code": code, "count": len(items), "news": items})
        else:
            results.append({"code": code, "error": "获取失败"})

    # AI 情感分析
    ai_data = {"stocks": results}
    click.echo("正在生成 AI 分析...", err=True)
    try:
        ai_text = call_bedrock(NEWS_SYSTEM_PROMPT, build_user_message(ai_data))
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        ai_text = None

    if len(results) == 1:
        data = results[0]
        if ai_text:
            data["ai_analysis"] = ai_text
        if fmt == "table" and "news" in data:
            rows = [{"标题": n["标题"], "时间": n["时间"], "来源": n["来源"]} for n in data["news"]]
            output(rows, fmt, title=f"个股新闻 {data['code']}")
        else:
            output(data, fmt)
    else:
        if ai_text:
            for r in results:
                r["ai_analysis"] = ai_text
        if fmt == "table":
            for data in results:
                if "news" in data:
                    rows = [{"标题": n["标题"], "时间": n["时间"], "来源": n["来源"]} for n in data["news"]]
                    output(rows, fmt, title=f"个股新闻 {data['code']}")
                else:
                    output(data, fmt)
        else:
            output(results, fmt)
