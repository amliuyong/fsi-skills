"""monitor 子命令 — 每日买卖信号监控"""

import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import pandas as pd

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import MONITOR_SYSTEM_PROMPT, build_user_message
from fsi.config import FSI_DIR, today_bj
from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.indicators.scoring import compute_scores
from fsi.indicators.swing import detect_swings, summarize_swings
from fsi.output.formatter import DecimalEncoder, output

# 默认 zigzag 阈值
_DEFAULT_THRESHOLD = {"stock": 5.0, "etf": 3.0, "index": 3.0}
_MONITOR_CONFIG = FSI_DIR / "monitor.json"
_MONITOR_LOG = FSI_DIR / "monitor_log.jsonl"
_REPORT_DIR = FSI_DIR / "reports"


@click.command("monitor")
@click.argument("codes", nargs=-1)
@click.option("--days", "-d", default=120, help="分析天数（默认 120）")
@click.option("--no-ai", is_flag=True, help="跳过 AI 分析（仅输出算法评分）")
@click.pass_context
def monitor_cmd(ctx, codes, days, no_ai):
    """每日买卖信号监控（技术分析 + 波浪理论）"""
    fmt = ctx.obj["fmt"]
    db_path = ctx.obj["db"]

    # 加载 watchlist
    watchlist = _load_watchlist(codes)
    if not watchlist:
        click.echo("无监控标的。用法: fsi monitor 600519 510300 或配置 data/monitor.json", err=True)
        return

    conn = get_connection(db_path)
    engine = IndicatorEngine()
    results = []

    for item in watchlist:
        code = item["code"]
        asset_type = item.get("type") or _detect_type(conn, code)
        threshold = item.get("threshold", _DEFAULT_THRESHOLD.get(asset_type, 5.0))
        table = _type_to_table(asset_type)

        click.echo(f"分析 {code} ({asset_type})...", err=True)
        r = _analyze_one(conn, engine, table, code, days, threshold,
                         asset_type, db_path)
        if r:
            results.append(r)

    if not results:
        click.echo("所有标的分析失败", err=True)
        return

    # 获取市场宏观上下文
    click.echo("获取市场上下文...", err=True)
    from fsi.market_context import fetch_market_context, get_a_share_index_history
    market_ctx = fetch_market_context(
        include_news=True,
        include_calendar=True,
        include_qvix=True,
        include_global_indices=True,
        verbose=ctx.obj.get("verbose", False),
    )
    a_index_hist = get_a_share_index_history(days=5)
    if a_index_hist:
        market_ctx["a_share_index_history"] = a_index_hist

    # 汇总所有结果后调 AI
    ai_text = None
    if not no_ai:
        click.echo("正在生成 AI 综合研判...", err=True)
        try:
            ai_input = {"items": results}
            ai_input.update(market_ctx)
            ai_text = call_bedrock(MONITOR_SYSTEM_PROMPT, build_user_message(ai_input))
        except Exception as e:
            click.echo(f"AI 分析失败: {e}", err=True)

    # 写入信号日志
    _append_log(results)

    today = today_bj()
    final = {"date": today.isoformat(), "monitor": results}
    if market_ctx:
        final["market_context"] = market_ctx
    if ai_text:
        final["ai_analysis"] = ai_text

    # 保存完整报告到文件
    _save_report(final, today)

    output(final, fmt, title="每日信号监控")


def _analyze_one(conn, engine, table, code, days, threshold,
                 asset_type, db_path) -> dict | None:
    """分析单个标的，返回结果 dict。"""
    multiplier = 6
    today = today_bj()
    cutoff = (today - timedelta(days=int(days * multiplier))).isoformat()

    df = conn.execute(
        f"SELECT * FROM {table} WHERE code = ? AND trade_date >= ? ORDER BY trade_date",
        [code, cutoff],
    ).fetchdf()

    if df.empty:
        click.echo(f"  无 {code} 数据，跳过", err=True)
        return None

    # 名称
    name = ""
    name_col = asset_type in ("index", "etf")
    if asset_type == "stock":
        info = conn.execute(
            "SELECT name FROM stock_list WHERE code = ?", [code]
        ).fetchone()
        if info:
            name = info[0]
    elif name_col and "name" in df.columns:
        name = df["name"].iloc[0] if not df["name"].isna().all() else ""

    # 计算指标
    df = engine.calculate(df)
    df_window = df.tail(days).reset_index(drop=True)

    if len(df_window) < 10:
        click.echo(f"  {code} 数据不足，跳过", err=True)
        return None

    # 转折点检测
    swings = detect_swings(df_window, pct_threshold=threshold)
    segments = summarize_swings(swings)

    # 评分
    scores = compute_scores(df_window, swings)

    latest = df_window.iloc[-1]

    # 支撑位 / 压力位
    close = float(latest["close"])
    support, resistance = _calc_levels(latest, close, swings)

    # 波浪位置描述
    wave_desc = _describe_wave_position(swings, close)

    result = {
        "code": code,
        "name": name,
        "type": asset_type,
        "date": today.isoformat(),
        "close": _dec(latest.get("close")),
        "pct_change": _dec(latest.get("pct_change")),
        "signal": scores["signal"],
        "score": scores["total"],
        "scores": {
            "trend": scores["trend"],
            "momentum": scores["momentum"],
            "overbought": scores["overbought"],
            "volume": scores["volume"],
            "wave": scores["wave"],
        },
        "key_levels": {
            "support": support,
            "resistance": resistance,
        },
        "wave_position": wave_desc,
        "swing_points": swings,
        "swing_segments": segments,
        "indicators": engine.get_latest_indicators(df_window),
        "tech_signals": engine.detect_signals(df_window),
    }

    # 注入信号历史 streak
    streak_info = _get_signal_streak(code)
    if streak_info["streak"] > 0:
        result["signal_streak"] = {
            "prev_signal": streak_info["signal"],
            "days": streak_info["streak"],
        }

    signal_icon = _signal_icon(scores["signal"])
    streak_str = f" streak={streak_info['streak']}d" if streak_info["streak"] > 1 else ""
    click.echo(f"  {signal_icon} {code} {name}: {scores['signal']} "
               f"(score={scores['total']:.2f}{streak_str})", err=True)

    return result


def _calc_levels(latest, close, swings: list[dict] | None = None) -> tuple[list, list]:
    """根据均线、布林带和 swing 转折点计算支撑位/压力位。"""
    support = []
    resistance = []
    for key in ["ma20", "ma60", "ma120"]:
        v = _f(latest.get(key))
        if v is not None:
            if v < close:
                support.append(round(v, 2))
            else:
                resistance.append(round(v, 2))
    boll_upper = _f(latest.get("boll_upper"))
    boll_lower = _f(latest.get("boll_lower"))
    if boll_upper is not None:
        resistance.append(round(boll_upper, 2))
    if boll_lower is not None:
        support.append(round(boll_lower, 2))
    # swing 高低点作为支撑/压力
    if swings:
        for s in swings[-4:]:
            price = round(s["price"], 2)
            if s["type"] == "high" and price > close:
                resistance.append(price)
            elif s["type"] == "low" and price < close:
                support.append(price)
    # 去重并排序
    support = sorted(set(support), reverse=True)
    resistance = sorted(set(resistance))
    return support, resistance


def _describe_wave_position(swings, current_price) -> str:
    """用自然语言描述当前波浪位置。"""
    if len(swings) < 2:
        return "转折点不足，无法判断"

    last = swings[-1]
    prev = swings[-2]

    # 低点趋势
    lows = [s for s in swings if s["type"] == "low"]
    highs = [s for s in swings if s["type"] == "high"]

    parts = []
    if last["type"] == "high":
        drop = (last["price"] - current_price) / last["price"] * 100
        parts.append(f"最近高点 {last['price']:.2f}({last['date']})")
        parts.append(f"已回落 {drop:.1f}%")
    else:
        bounce = (current_price - last["price"]) / last["price"] * 100
        parts.append(f"最近低点 {last['price']:.2f}({last['date']})")
        parts.append(f"已反弹 {bounce:.1f}%")

    if len(lows) >= 2:
        if lows[-1]["price"] > lows[-2]["price"]:
            parts.append("底部抬高")
        else:
            parts.append("底部降低")

    if len(highs) >= 2:
        if highs[-1]["price"] > highs[-2]["price"]:
            parts.append("顶部抬高")
        else:
            parts.append("顶部降低")

    return "，".join(parts)


def _signal_icon(signal: str) -> str:
    icons = {"BUY": "[BUY]", "SELL": "[SELL]", "BULLISH": "[+]",
             "BEARISH": "[-]", "NEUTRAL": "[=]"}
    return icons.get(signal, "[?]")


def _load_watchlist(codes: tuple) -> list[dict]:
    """加载 watchlist：CLI 参数直接用，否则 monitor.json + portfolio.json 合并去重。"""
    if codes:
        return [{"code": c} for c in codes]

    items = []
    seen = set()

    # 读 monitor.json
    if _MONITOR_CONFIG.exists():
        try:
            with open(_MONITOR_CONFIG, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for item in data:
                    items.append(item)
                    seen.add(item["code"])
        except (json.JSONDecodeError, KeyError):
            pass

    # 合并 portfolio.json（去重）
    from fsi.portfolio import load_portfolio
    positions = load_portfolio()
    for p in positions:
        if p["code"] not in seen:
            items.append({"code": p["code"]})
            seen.add(p["code"])

    return items


def _detect_type(conn, code: str) -> str:
    """自动检测代码类型：先查 stock_list，再查 index/etf 表。"""
    r = conn.execute("SELECT 1 FROM stock_list WHERE code = ? LIMIT 1", [code]).fetchone()
    if r:
        return "stock"
    r = conn.execute("SELECT 1 FROM index_daily WHERE code = ? LIMIT 1", [code]).fetchone()
    if r:
        return "index"
    r = conn.execute("SELECT 1 FROM etf_daily WHERE code = ? LIMIT 1", [code]).fetchone()
    if r:
        return "etf"
    return "stock"  # 兜底


def _type_to_table(t: str) -> str:
    return {"stock": "stock_daily", "index": "index_daily", "etf": "etf_daily"}.get(t, "stock_daily")


def _save_report(final: dict, today):
    """保存完整监控报告到 data/reports/{date}-monitor.json"""
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = _REPORT_DIR / f"{today.isoformat()}-monitor.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(final, cls=DecimalEncoder, ensure_ascii=False, indent=2, fp=f)
        click.echo(f"报告已保存: {path}", err=True)
    except OSError as e:
        click.echo(f"报告保存失败: {e}", err=True)


def _append_log(results: list[dict]):
    """追加信号到日志文件（JSONL 格式），同日同 code 自动覆盖。

    策略：只读取尾部最近 N 行做去重判断，避免全量读取。
    若今日 (date, code) 已存在于尾部则先截掉旧行再追加，否则直接 append。
    """
    new_entries = []
    for r in results:
        new_entries.append({
            "date": r["date"],
            "code": r["code"],
            "name": r["name"],
            "type": r["type"],
            "close": r["close"],
            "signal": r["signal"],
            "score": r["score"],
            "scores": r["scores"],
        })
    new_keys = {(e["date"], e["code"]) for e in new_entries}

    try:
        # 读尾部最近 200 行判断是否有重复（日均 ~8 标的，200 行覆盖 25 天）
        tail_lines: list[str] = []
        if _MONITOR_LOG.exists():
            with open(_MONITOR_LOG, "r", encoding="utf-8") as f:
                tail_lines = f.readlines()[-200:]

        has_dup = False
        for line in tail_lines:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
                if (e.get("date"), e.get("code")) in new_keys:
                    has_dup = True
                    break
            except json.JSONDecodeError:
                continue

        if has_dup:
            # 有重复才做全量读取-过滤-重写（极少触发：同日重跑）
            existing = []
            with open(_MONITOR_LOG, "r", encoding="utf-8") as f:
                for raw in f:
                    raw = raw.strip()
                    if raw:
                        try:
                            existing.append(json.loads(raw))
                        except json.JSONDecodeError:
                            continue
            kept = [e for e in existing if (e.get("date"), e.get("code")) not in new_keys]
            kept.extend(new_entries)
            with open(_MONITOR_LOG, "w", encoding="utf-8") as f:
                for entry in kept:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        else:
            # 无重复，直接追加（常规路径，零额外 I/O）
            with open(_MONITOR_LOG, "a", encoding="utf-8") as f:
                for entry in new_entries:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _get_signal_streak(code: str) -> dict:
    """读取历史日志，返回该 code 的连续信号天数和方向。

    返回: {"signal": "BULLISH", "streak": 3, "history": ["BULLISH","BULLISH","BULLISH"]}
    """
    if not _MONITOR_LOG.exists():
        return {"signal": "", "streak": 0, "history": []}
    try:
        entries = []
        with open(_MONITOR_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                    if e.get("code") == code:
                        entries.append(e)
                except json.JSONDecodeError:
                    continue
        if not entries:
            return {"signal": "", "streak": 0, "history": []}
        # 按日期降序，取最近的记录（每天只取最后一条）
        seen_dates = {}
        for e in entries:
            d = e.get("date", "")
            seen_dates[d] = e  # 同日多次运行取最后一次
        daily = sorted(seen_dates.values(), key=lambda x: x["date"], reverse=True)
        # 计算连续相同信号天数
        last_signal = daily[0]["signal"]
        streak = 0
        history = []
        for d in daily:
            if d["signal"] == last_signal:
                streak += 1
                history.append(d["signal"])
            else:
                break
        return {"signal": last_signal, "streak": streak, "history": history}
    except OSError:
        return {"signal": "", "streak": 0, "history": []}


def _dec(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, Decimal):
        return float(val)
    return round(float(val), 4)


def _f(val) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        if pd.isna(v):
            return None
        return v
    except (ValueError, TypeError):
        return None
