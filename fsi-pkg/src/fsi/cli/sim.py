"""sim 子命令 — 模拟盘管理（支持多 profile）"""

from datetime import datetime
from zoneinfo import ZoneInfo

import click

from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine
from fsi.indicators.scoring import compute_scores
from fsi.indicators.swing import detect_swings
from fsi.output.formatter import output
from fsi.sim import (
    PRESETS,
    calc_buy_price,
    calc_buy_shares,
    calc_dynamic_weights,
    calc_sell_price,
    execute_buy,
    execute_sell,
    fetch_quote,
    get_target_weight,
    init_portfolio,
    list_profiles,
    load_portfolio,
    normalize_target,
    portfolio_summary,
    save_portfolio,
)

import json
from pathlib import Path
from fsi.config import FSI_DIR

_CHECK_LOG_DIR = FSI_DIR / "sim_logs"

_BJ = ZoneInfo("Asia/Shanghai")
_DEFAULT_THRESHOLD = {"stock": 5.0, "etf": 3.0, "index": 3.0}

# 高相关资产组定义（同组总买入占可用现金上限 60%）
_ASSET_GROUPS = {
    "broad_a": {"510300", "510500", "159949", "588000"},  # A股宽基 ETF
    "overseas": {"513500", "513100"},                       # 海外 ETF
}
_GROUP_MAX_RATIO = 0.60  # 同组最大占比

# ETF vs 个股差异化参数
_ASSET_PARAMS = {
    "etf": {
        "underweight_ratio": 0.9,   # 低配阈值：持仓 < 目标×0.9 才买入（ETF 更积极补仓）
        "overweight_ratio": 1.2,    # 超配阈值：持仓 > 目标×1.2 就减仓（ETF 更积极再平衡）
        "signal_confirm_days": 1,   # 确认天数：ETF 1 天即可行动（波动小，信号更可靠）
    },
    "stock": {
        "underweight_ratio": 0.8,
        "overweight_ratio": 1.3,
        "signal_confirm_days": 2,   # 个股需连续 2 天确认
    },
    "index": {
        "underweight_ratio": 0.9,
        "overweight_ratio": 1.2,
        "signal_confirm_days": 1,
    },
}


@click.group()
@click.pass_context
def sim(ctx):
    """模拟盘命令（支持多 profile）"""
    pass


@sim.command("init")
@click.argument("profile", default="default")
@click.option("--capital", default=1_000_000, type=float, help="初始资金（默认 100 万）")
@click.option("--preset", type=click.Choice(list(PRESETS.keys())), default=None,
              help="预设模板（不指定则按 profile 名自动匹配）")
@click.pass_context
def sim_init(ctx, profile, capital, preset):
    """初始化模拟盘（可指定 profile 名称）"""
    p = load_portfolio(profile)
    if p:
        click.echo(f"模拟盘 [{profile}] 已存在！如需重置，请先删除 data/sim_{profile}.json", err=True)
        return

    # preset 未指定时，用 profile 名匹配预设，匹配不到兜底 conservative
    if not preset:
        preset = profile if profile in PRESETS else "conservative"

    p = init_portfolio(profile, capital, preset=preset)
    preset_label = PRESETS[preset]["label"]
    click.echo(f"模拟盘 [{profile}] 已初始化: {capital:,.0f} 元 ({preset_label})", err=True)
    click.echo("标的配置:", err=True)
    for t in p["config"]["targets"]:
        t = normalize_target(t)
        strategy = t.get("strategy", "trend")
        tag = f" [{strategy}]" if strategy == "hold" else ""
        click.echo(f"  {t['code']} {t['name']}{tag} "
                   f"权重 {t['min_weight']*100:.0f}%~{t['max_weight']*100:.0f}%", err=True)

    output(p, ctx.obj["fmt"], title=f"模拟盘初始化 [{profile}]")


@sim.command("check")
@click.argument("profile", default=None, required=False)
@click.option("--no-ai", "no_ai", is_flag=True, help="禁用 AI 决策，使用规则引擎")
@click.pass_context
def sim_check(ctx, profile, no_ai):
    """检查信号并执行交易（不指定 profile 则检查所有）

    默认使用 AI 决策，--no-ai 回退到规则引擎。
    """
    use_ai = not no_ai
    profiles = [profile] if profile else list_profiles()
    if not profiles:
        click.echo("无模拟盘，请先 fsi sim init [profile]", err=True)
        return

    all_results = []
    for pf in profiles:
        result = _check_one_profile(ctx, pf, use_ai=use_ai)
        if result:
            all_results.append(result)

    if len(all_results) == 1:
        output(all_results[0], ctx.obj["fmt"], title=f"模拟盘检查 [{profiles[0]}]")
    else:
        output(all_results, ctx.obj["fmt"], title="模拟盘检查（全部）")


@sim.command("status")
@click.argument("profile", default=None, required=False)
@click.pass_context
def sim_status(ctx, profile):
    """查看持仓和盈亏（不指定 profile 则显示所有）"""
    profiles = [profile] if profile else list_profiles()
    if not profiles:
        click.echo("无模拟盘，请先 fsi sim init [profile]", err=True)
        return

    all_summaries = []
    for pf in profiles:
        p = load_portfolio(pf)
        if not p:
            continue

        quotes = {}
        for code in p["positions"]:
            q = fetch_quote(code)
            if q:
                quotes[code] = q

        summary = portfolio_summary(p, quotes)
        summary["profile"] = pf
        summary["preset"] = p["config"].get("preset", "")
        summary["created"] = p["config"]["created"]

        preset_label = PRESETS.get(summary["preset"], {}).get("label", "")
        click.echo(f"\n=== [{pf}] {preset_label} ===", err=True)
        click.echo(f"总资产: {summary['total_assets']:,.2f}", err=True)
        click.echo(f"总盈亏: {summary['total_pnl']:,.2f} ({summary['total_pnl_pct']:.2f}%)", err=True)
        click.echo(f"现金: {summary['cash']:,.2f} ({summary['cash_pct']:.1f}%)", err=True)
        if summary["positions"]:
            click.echo("持仓:", err=True)
            for pos in summary["positions"]:
                icon = "+" if pos["pnl"] >= 0 else ""
                click.echo(f"  {pos['code']} {pos['name']}: {pos['shares']}股 "
                           f"成本{pos['avg_cost']:.3f} 现价{pos['current_price']:.3f} "
                           f"{icon}{pos['pnl']:,.2f} ({icon}{pos['pnl_pct']:.2f}%) "
                           f"仓位{pos['weight']:.1f}%", err=True)

        all_summaries.append(summary)

    if len(all_summaries) == 1:
        output(all_summaries[0], ctx.obj["fmt"], title=f"模拟盘状态 [{profiles[0]}]")
    else:
        output(all_summaries, ctx.obj["fmt"], title="模拟盘状态（全部）")


@sim.command("log")
@click.argument("profile", default=None, required=False)
@click.option("--last", "-n", default=20, help="显示最近 N 条（默认 20）")
@click.pass_context
def sim_log(ctx, profile, last):
    """查看交易记录（不指定 profile 则显示所有）"""
    profiles = [profile] if profile else list_profiles()
    if not profiles:
        click.echo("无模拟盘", err=True)
        return

    all_trades = []
    for pf in profiles:
        p = load_portfolio(pf)
        if not p:
            continue
        trades = p.get("trades", [])
        if not trades:
            continue

        recent = trades[-last:]
        preset_label = PRESETS.get(p["config"].get("preset", ""), {}).get("label", "")
        click.echo(f"\n=== [{pf}] {preset_label} — 最近 {len(recent)} 条 ===", err=True)
        for t in recent:
            icon = "买入" if t["action"] == "BUY" else "卖出"
            click.echo(f"  {t['date']} {t['time']} {icon} {t['code']} {t['name']} "
                       f"{t['shares']}股 @ {t['price']} = {t['amount']:,.2f}", err=True)
        all_trades.append({"profile": pf, "trades": recent, "total": len(trades)})

    if len(all_trades) == 1:
        output(all_trades[0], ctx.obj["fmt"], title=f"交易记录 [{profiles[0]}]")
    else:
        output(all_trades, ctx.obj["fmt"], title="交易记录（全部）")


@sim.command("list")
@click.pass_context
def sim_list(ctx):
    """列出所有模拟盘"""
    profiles = list_profiles()
    if not profiles:
        click.echo("无模拟盘，请先 fsi sim init [profile]", err=True)
        return

    items = []
    for pf in profiles:
        p = load_portfolio(pf)
        if not p:
            continue
        preset = p["config"].get("preset", "")
        label = PRESETS.get(preset, {}).get("label", "")
        items.append({
            "profile": pf,
            "preset": f"{preset} ({label})" if label else preset,
            "capital": p["config"]["initial_capital"],
            "cash": p["cash"],
            "positions": len(p["positions"]),
            "trades": len(p["trades"]),
            "created": p["config"]["created"],
        })

    click.echo("模拟盘列表:", err=True)
    for item in items:
        click.echo(f"  [{item['profile']}] {item['preset']} "
                   f"初始{item['capital']:,.0f} 现金{item['cash']:,.0f} "
                   f"{item['positions']}只持仓 {item['trades']}笔交易 "
                   f"创建于{item['created']}", err=True)

    output(items, ctx.obj["fmt"], title="模拟盘列表")


# ── 内部函数 ──────────────────────────────────────────────────

def _check_one_profile(ctx, profile: str, *, use_ai: bool = False) -> dict | None:
    """检查单个 profile 的信号并执行交易。"""
    p = load_portfolio(profile)
    if not p:
        click.echo(f"模拟盘 [{profile}] 不存在", err=True)
        return None

    now = datetime.now(tz=_BJ)
    check_time = now.strftime("%H:%M")
    today = now.strftime("%Y-%m-%d")

    preset_label = PRESETS.get(p["config"].get("preset", ""), {}).get("label", "")
    click.echo(f"\n=== [{profile}] {preset_label} — {today} {check_time} ===", err=True)
    click.echo(f"可用现金: {p['cash']:,.2f}", err=True)

    targets = p["config"]["targets"]
    db_path = ctx.obj["db"]
    conn = get_connection(db_path)
    engine = IndicatorEngine()

    executed_trades = []
    # T+1: 追踪今日每只股票买入的股数（今日买入的不可卖，之前持仓可卖）
    today_bought: dict[str, int] = {}  # code → 今日已买入股数
    today_sold: set[str] = set()       # 今日已卖出的 code（同一天不重复卖）
    for t in p["trades"]:
        if t["date"] == today:
            if t["action"] == "BUY":
                today_bought[t["code"]] = today_bought.get(t["code"], 0) + t["shares"]
            elif t["action"] == "SELL":
                today_sold.add(t["code"])

    # 批量获取所有标的 + 所有持仓的实时报价
    all_codes = {t["code"] for t in targets} | set(p["positions"].keys())
    quotes_cache: dict[str, dict] = {}
    for c in all_codes:
        q = fetch_quote(c)
        if q and q.get("price"):
            quotes_cache[c] = q

    # 用实时价格计算总资产（报价失败的用 avg_cost 兜底）
    total_assets = p["cash"]
    for c, ps in p["positions"].items():
        if c in quotes_cache:
            total_assets += ps["shares"] * quotes_cache[c]["price"]
        else:
            total_assets += ps["shares"] * ps["avg_cost"]

    # ── 止损检查 ─────────────────────────────────────────────
    initial_capital = p["config"]["initial_capital"]
    preset_name = p["config"].get("preset", "conservative")
    preset_cfg = PRESETS.get(preset_name, PRESETS["conservative"])
    # 优先读 profile config 自定义值，fallback 到预设模板
    stop_loss_pct = p["config"].get("stop_loss_pct", preset_cfg.get("stop_loss_pct", -8.0))
    max_drawdown_pct = p["config"].get("max_drawdown_pct", preset_cfg.get("max_drawdown_pct", -12.0))

    # 组合最大回撤检查（从净值高点算起，非初始本金）
    portfolio_hwm = p.get("portfolio_hwm", initial_capital)
    if total_assets > portfolio_hwm:
        portfolio_hwm = total_assets
        p["portfolio_hwm"] = round(portfolio_hwm, 2)
    drawdown_pct = (total_assets - portfolio_hwm) / portfolio_hwm * 100 if portfolio_hwm > 0 else 0

    # 回撤恢复机制：之前触发过减半，但资产已回升到高水位 95% → 解除限制
    was_halved = p.get("drawdown_halved", False)
    if was_halved and total_assets >= portfolio_hwm * 0.95:
        p["drawdown_halved"] = False
        was_halved = False
        click.echo(f"  ✓ 组合资产回升至高水位 95%（{total_assets:,.0f}/{portfolio_hwm:,.0f}），"
                   f"回撤限制已解除", err=True)

    drawdown_triggered = drawdown_pct <= max_drawdown_pct and not was_halved

    if drawdown_triggered:
        p["drawdown_halved"] = True
        click.echo(f"  ⚠ 组合从高点回撤 {drawdown_pct:.1f}%（高点{portfolio_hwm:,.0f}）"
                   f"触及止损线 {max_drawdown_pct}%，全部持仓减半!", err=True)

    # 回撤期间（已减半但未恢复）暂停买入
    in_drawdown_mode = p.get("drawdown_halved", False)

    # code → asset_type 映射（targets 优先，兜底 stock）
    code_type_map: dict[str, str] = {t["code"]: t["type"] for t in targets}
    # code → strategy 映射（默认 trend）
    code_strategy_map: dict[str, str] = {t["code"]: t.get("strategy", "trend") for t in targets}

    # Trailing Stop 差异化阈值（放宽后，给趋势行情更多空间）
    _TS_PARAMS = {
        "etf":   {"profit_trigger": 16, "drawdown_trigger": 8},
        "index": {"profit_trigger": 16, "drawdown_trigger": 8},
        "stock": {"profit_trigger": 25, "drawdown_trigger": 12},
    }

    # 个股止损 + Trailing Stop 集合
    stop_loss_codes: set[str] = set()
    trailing_stop_codes: set[str] = set()
    for code, pos in p["positions"].items():
        if code not in quotes_cache:
            continue
        cur_price = quotes_cache[code]["price"]
        avg_cost = pos["avg_cost"]
        if avg_cost <= 0:
            continue
        pnl_pct = (cur_price - avg_cost) / avg_cost * 100

        # 个股硬止损（ETF/指数止损线额外放宽 4 个百分点）
        atype = code_type_map.get(code, "stock")
        effective_sl = stop_loss_pct - 4.0 if atype in ("etf", "index") else stop_loss_pct
        if pnl_pct <= effective_sl:
            stop_loss_codes.add(code)
            click.echo(f"  ⚠ {code} {pos.get('name','')} 亏损 {pnl_pct:.1f}% "
                       f"触及止损线 {effective_sl}%", err=True)
            continue

        # Trailing Stop：更新 high_watermark，按标的类型差异化触发
        # hold 策略不执行 Trailing Stop（只买不卖，除非硬止损）
        atype = code_type_map.get(code, "stock")
        ts = _TS_PARAMS.get(atype, _TS_PARAMS["stock"])
        hw = pos.get("high_watermark", avg_cost)
        if cur_price > hw:
            hw = cur_price
            pos["high_watermark"] = round(hw, 4)
        if code_strategy_map.get(code, "trend") == "hold":
            continue
        profit_from_cost = (hw - avg_cost) / avg_cost * 100
        drawdown_from_hw = (hw - cur_price) / hw * 100
        if profit_from_cost > ts["profit_trigger"] and drawdown_from_hw > ts["drawdown_trigger"]:
            trailing_stop_codes.add(code)
            click.echo(f"  ⚠ {code} {pos.get('name','')} Trailing Stop: "
                       f"最高盈利{profit_from_cost:.0f}%，从高点回撤{drawdown_from_hw:.1f}% "
                       f"(阈值: 盈利>{ts['profit_trigger']}%+回撤>{ts['drawdown_trigger']}%)", err=True)

    # ── 大盘过滤器：沪深300均线状态 → 买入力度系数 ──────────
    market_discount = _calc_market_discount(conn, engine)
    if market_discount < 1.0:
        click.echo(f"  大盘偏弱，买入力度 ×{market_discount:.0%}", err=True)

    # ── QVIX 恐慌指数过滤 ──────────────────────────────────
    qvix_discount = _calc_qvix_discount()
    if qvix_discount < 1.0:
        click.echo(f"  QVIX 偏高，买入力度 ×{qvix_discount:.0%}", err=True)
    market_discount *= qvix_discount  # 叠加

    # ── 主力资金流向参考 ──────────────────────────────────
    fund_discount = _calc_fund_flow_discount()
    if fund_discount < 1.0:
        click.echo(f"  主力资金流出，买入力度 ×{fund_discount:.0%}", err=True)
    elif fund_discount > 1.0:
        click.echo(f"  主力资金流入，买入力度 ×{fund_discount:.0%}", err=True)
    market_discount *= fund_discount  # 叠加
    market_discount = max(market_discount, 0.4)  # 下限 0.4，防止底部完全停止买入

    # ── 第一轮：扫描所有标的信号 + 评分 ─────────────────────────
    buy_intents = []   # [(target, quote, scores, want_amount)]
    sell_intents = []  # [(target, quote, scores, sell_shares)]
    signal_snapshots = []  # 每个标的的决策快照（用于 check log）
    scores_map = {}    # code → scores dict
    signal_map = {}    # code → signal str
    snapshot_map = {}  # code → snapshot dict（快速查找用）

    from fsi.trading_calendar import is_trading_day

    for target in targets:
        code = target["code"]
        name = target["name"]
        asset_type = target["type"]
        threshold = _DEFAULT_THRESHOLD.get(asset_type, 5.0)
        ap = _ASSET_PARAMS.get(asset_type, _ASSET_PARAMS["stock"])

        # 获取实时报价（从缓存取）
        quote = quotes_cache.get(code)
        if not quote:
            click.echo(f"\n{code} {name}: 获取报价失败，跳过", err=True)
            continue

        price = quote["price"]
        click.echo(f"\n{code} {name}: 现价 {price}", err=True)

        # 算法评分
        table = {"stock": "stock_daily", "etf": "etf_daily", "index": "index_daily"}[asset_type]
        scores = _quick_score(conn, engine, table, code, threshold,
                              asset_type=asset_type)
        if not scores:
            click.echo(f"  评分失败（数据不足），跳过", err=True)
            continue

        signal = scores["signal"]
        score = scores["total"]
        scores_map[code] = scores
        signal_map[code] = signal

        # 信号确认：更新连续信号计数，要求连续 >= 2 天同方向才行动
        sig_hist = p.setdefault("signal_history", {})
        direction = "buy" if signal in ("BUY", "BULLISH") else \
                    "sell" if signal in ("SELL", "BEARISH") else "neutral"
        prev = sig_hist.get(code, {})
        prev_score = prev.get("score", 0)
        if is_trading_day():
            if prev.get("direction") == direction and prev.get("date") != today:
                streak = prev.get("streak", 1) + 1
            elif prev.get("date") == today and prev.get("direction") == direction:
                streak = prev.get("streak", 1)  # 同一天同方向不重复计数
            elif prev.get("date") == today:
                streak = 1  # 同日方向翻转，重置
            else:
                streak = 1
            sig_hist[code] = {"direction": direction, "streak": streak,
                              "date": today, "score": round(score, 2)}
        else:
            streak = prev.get("streak", 1) if prev.get("direction") == direction else 1

        confirmed = streak >= ap["signal_confirm_days"] or direction == "neutral"

        # 信号强度衰减检测：同方向但 score 下降 > 0.3 → 标记衰减
        score_weakening = False
        if direction == "buy" and prev.get("direction") == "buy" and prev_score > 0:
            score_drop = prev_score - score
            if score_drop > 0.3:
                score_weakening = True

        streak_tag = f" [连续{streak}天]" if direction != "neutral" else ""
        if not confirmed:
            streak_tag += " 待确认"
        if score_weakening:
            streak_tag += f" ⚠衰减({prev_score:.2f}→{score:.2f})"
        click.echo(f"  信号: {signal} (score={score:.2f}){streak_tag}", err=True)

        pos = p["positions"].get(code)
        holding_shares = pos["shares"] if pos else 0
        holding_value = holding_shares * price if holding_shares > 0 else 0
        sellable_shares = max(0, holding_shares - today_bought.get(code, 0))

        # 记录决策快照
        snap = {
            "code": code,
            "name": name,
            "type": asset_type,
            "price": price,
            "score": scores["total"],
            "scores": scores,
            "signal": signal,
            "confirmed": confirmed,
            "streak": streak,
            "score_weakening": score_weakening,
            "holding_shares": holding_shares,
            "holding_value": round(holding_value, 2),
            "stop_loss": code in stop_loss_codes,
            "trailing_stop": code in trailing_stop_codes,
            "in_cooldown": p.get("cooldown", {}).get(code, "") >= today,
        }
        signal_snapshots.append(snap)
        snapshot_map[code] = snap

    # ── 动态权重计算 ─────────────────────────────────────────
    cash_reserve = PRESETS.get(preset_name, {}).get("cash_reserve", 0.05)
    verbose = ctx.obj.get("verbose", False)
    dw_result = calc_dynamic_weights(
        targets, scores_map, signal_map,
        cash_reserve=cash_reserve, verbose=True,
    )
    dynamic_weights = dw_result["weights"]

    click.echo(f"\n--- 动态权重 ---", err=True)
    for d in dw_result.get("debug", []):
        tag = f" [{d['strategy']}]" if d["strategy"] == "hold" else ""
        click.echo(f"  {d['code']} {d['name']}{tag}: "
                   f"配置分={d['allocation_score']:+.2f} "
                   f"权重={d['dynamic_weight']*100:.1f}% "
                   f"(范围 {d['min_weight']*100:.0f}%~{d['max_weight']*100:.0f}%)",
                   err=True)

    # 更新 snapshot 中的 target_weight 和 target_amount
    for snap in signal_snapshots:
        code = snap["code"]
        w = dynamic_weights.get(code, 0)
        snap["target_weight"] = round(w, 4)
        snap["target_amount"] = round(total_assets * w, 2)

    # ── 预计算 hold 标的的 MA60（网格加仓用）────────────────
    ma60_cache: dict[str, float | None] = {}
    for target in targets:
        if target.get("strategy") != "hold":
            continue
        code = target["code"]
        table = {"stock": "stock_daily", "etf": "etf_daily", "index": "index_daily"}[target["type"]]
        ma60_cache[code] = _calc_ma60(conn, table, code)
        if ma60_cache[code] is not None:
            click.echo(f"  {code} {target['name']} MA60={ma60_cache[code]:.3f}", err=True)

    # ── 第二轮：止损 + 买卖意图收集 ──────────────────────────
    for target in targets:
        code = target["code"]
        name = target["name"]
        asset_type = target["type"]
        weight = dynamic_weights.get(code, 0)
        ap = _ASSET_PARAMS.get(asset_type, _ASSET_PARAMS["stock"])

        quote = quotes_cache.get(code)
        snap = snapshot_map.get(code)
        if not quote or not snap:
            continue

        price = quote["price"]
        scores = snap["scores"]
        signal = snap["signal"]
        score = snap["score"]
        confirmed = snap["confirmed"]
        score_weakening = snap.get("score_weakening", False)

        pos = p["positions"].get(code)
        holding_shares = pos["shares"] if pos else 0
        holding_value = holding_shares * price if holding_shares > 0 else 0
        target_amount = total_assets * weight
        sellable_shares = max(0, holding_shares - today_bought.get(code, 0))

        already_bought_today = code in today_bought
        already_sold_today = code in today_sold

        # 冷却期检查
        cooldown = p.setdefault("cooldown", {})
        in_cooldown = cooldown.get(code, "") >= today

        # 止损优先：个股止损 / Trailing Stop / 组合回撤 → 强制卖出
        is_hold = target.get("strategy") == "hold"
        force_sell = (code in stop_loss_codes) or (code in trailing_stop_codes and not is_hold) or drawdown_triggered
        if force_sell and sellable_shares > 0 and not already_sold_today:
            if code in stop_loss_codes:
                sell_shares = sellable_shares
                reason = "个股止损"
            elif code in trailing_stop_codes:
                sell_shares = (sellable_shares // 2 // 100) * 100
                if sell_shares <= 0:
                    sell_shares = sellable_shares
                reason = "Trailing Stop"
            else:
                sell_shares = (sellable_shares // 2 // 100) * 100
                if sell_shares <= 0:
                    sell_shares = sellable_shares
                reason = "组合回撤止损"
            sell_intents.append((target, quote, scores, sell_shares))
            click.echo(f"  → {reason}：计划卖出 {sell_shares}股", err=True)

        elif not use_ai:
            # ── 规则引擎意图收集（使用动态权重）──
            if is_hold:
                # 网格加仓：MA60 偏离度决定加仓倍数
                ma60 = ma60_cache.get(code)
                grid_mult, deviation = _calc_grid_multiplier(price, ma60)

                # 反弹减仓：价格高于 MA60 超 8% 且持仓显著超配 → 减仓
                max_weight = target.get("max_weight", weight)
                max_hold_value = total_assets * max_weight * 1.1
                if deviation is not None and deviation > 0.08 \
                        and holding_value > max_hold_value \
                        and sellable_shares > 0 and not already_sold_today:
                    trim_target = total_assets * max_weight
                    trim_amount = holding_value - trim_target
                    trim_shares = int(trim_amount / price)
                    trim_shares = (trim_shares // 100) * 100
                    if trim_shares > 0:
                        trim_shares = min(trim_shares, sellable_shares)
                        sell_intents.append((target, quote, scores, trim_shares))
                        click.echo(f"  [hold] → 反弹减仓 {trim_shares}股"
                                   f"（偏离MA60 {deviation:+.1%}，"
                                   f"持仓{holding_value:,.0f} > 上限{max_hold_value:,.0f}）",
                                   err=True)

                elif holding_value < target_amount * ap["underweight_ratio"] \
                        and not already_bought_today and not in_drawdown_mode \
                        and not in_cooldown \
                        and signal not in ("SELL",):
                    score_factor = 0.6
                    if scores.get("overbought", 0) >= 0.5:
                        score_factor *= 1.5
                        click.echo(f"  [hold] 逢跌加仓（超卖信号），额度×1.5", err=True)
                    gap = target_amount - holding_value
                    want = gap * score_factor * grid_mult
                    if want > 0:
                        buy_intents.append((target, quote, scores, want))
                        grid_tag = f" 网格×{grid_mult:.1f}" if grid_mult > 1.0 else ""
                        dev_tag = f" 偏离MA60={deviation:+.1%}" if deviation is not None else ""
                        click.echo(f"  [hold] → 底仓补仓 {want:,.0f}元"
                                   f"（缺口{gap:,.0f}，权重{weight*100:.1f}%"
                                   f"，系数{score_factor:.1f}"
                                   f"{grid_tag}{dev_tag}）", err=True)
                    else:
                        click.echo(f"  [hold] 持有 {holding_shares}股，已达目标", err=True)
                else:
                    reason_parts = []
                    if signal == "SELL":
                        reason_parts.append("SELL信号暂停买入")
                    if already_bought_today:
                        reason_parts.append("今日已买入")
                    if in_cooldown:
                        reason_parts.append(f"冷却期至{cooldown[code]}")
                    reason_str = f"（{'，'.join(reason_parts)}）" if reason_parts else ""
                    click.echo(f"  [hold] 持有 {holding_shares}股，长持{reason_str}", err=True)

            elif signal in ("BUY", "BULLISH") and confirmed \
                    and holding_value < target_amount * ap["underweight_ratio"] \
                    and not already_bought_today and not in_drawdown_mode \
                    and not in_cooldown:
                if score >= 0.8:
                    score_factor = 1.0
                else:
                    score_factor = 0.5 + (score - 0.2) / (0.8 - 0.2) * 0.3
                    score_factor = max(0.5, min(0.8, score_factor))
                if score_weakening:
                    score_factor *= 0.5
                want = (target_amount - holding_value) * market_discount * score_factor
                if want > 0:
                    buy_intents.append((target, quote, scores, want))
                    click.echo(f"  → 计划买入 {want:,.0f}元（动态权重{weight*100:.1f}%）", err=True)
                else:
                    click.echo(f"  持有 {holding_shares}股，观望", err=True)

            elif signal in ("SELL", "BEARISH") and confirmed and sellable_shares > 0 and not already_sold_today:
                sell_shares = _calc_sell_shares(sellable_shares, scores, pos, price)
                sell_intents.append((target, quote, scores, sell_shares))
                click.echo(f"  → 计划卖出 {sell_shares}股", err=True)

            # 超配再平衡：当 allocation_score <= 0 且有持仓 → 减仓（trend 标的）
            elif not is_hold and scores_map.get(code) is not None:
                from fsi.sim import calc_allocation_score
                a_score = calc_allocation_score(normalize_target(target), scores, signal)
                if a_score <= 0 and sellable_shares > 0 and not already_sold_today:
                    # 配置分为负，trend 标的应减仓
                    sell_shares = _calc_sell_shares(sellable_shares, scores, pos, price)
                    sell_intents.append((target, quote, scores, sell_shares))
                    click.echo(f"  → 配置分{a_score:+.2f}，减仓 {sell_shares}股", err=True)
                elif holding_value > target_amount * ap["overweight_ratio"] and sellable_shares > 0 and not already_sold_today:
                    excess = holding_value - target_amount
                    excess_shares = int(excess / price)
                    excess_shares = (excess_shares // 100) * 100
                    if excess_shares > 0:
                        excess_shares = min(excess_shares, sellable_shares)
                        sell_intents.append((target, quote, scores, excess_shares))
                        click.echo(f"  → 超配再平衡：减仓 {excess_shares}股 "
                                   f"(当前{holding_value/total_assets*100:.0f}% > "
                                   f"动态目标{weight*100:.1f}%)", err=True)
                    else:
                        click.echo(f"  持有 {holding_shares}股，观望", err=True)
                else:
                    if holding_shares > 0:
                        click.echo(f"  持有 {holding_shares}股，观望", err=True)
                    else:
                        click.echo(f"  未持仓，观望", err=True)

            else:
                if holding_shares > 0:
                    reason = ""
                    if already_bought_today and signal in ("SELL", "BEARISH"):
                        reason = "（今日买入T+1不可卖）"
                    elif already_sold_today:
                        reason = "（今日已卖出）"
                    elif in_drawdown_mode:
                        reason = "（组合回撤止损中，暂停买入）"
                    elif in_cooldown:
                        reason = f"（冷却期至{cooldown[code]}）"
                    click.echo(f"  持有 {holding_shares}股，观望{reason}", err=True)
                else:
                    click.echo(f"  未持仓，观望", err=True)

    # ── AI 决策层（use_ai=True 时替代规则意图）──────────────
    ai_result = None
    if use_ai:
        click.echo(f"\n--- AI 决策模式 ---", err=True)
        ai_result = _ai_decide(
            signal_snapshots, p, market_discount, qvix_discount, fund_discount,
            dynamic_weights=dynamic_weights,
        )
        if ai_result:
            click.echo(f"  市场判断: {ai_result.get('market_view', '')}", err=True)
            _apply_ai_decisions(
                ai_result, targets, quotes_cache, p,
                total_assets, market_discount,
                today_bought, today_sold, in_drawdown_mode,
                buy_intents, sell_intents,
                dynamic_weights=dynamic_weights,
                ma60_cache=ma60_cache,
            )
        else:
            click.echo("  ⚠ AI 决策失败，回退到规则引擎", err=True)
            # 回退：用规则引擎重跑意图收集
            _apply_rule_fallback(
                targets, quotes_cache, p, signal_snapshots,
                total_assets, market_discount, in_drawdown_mode,
                today_bought, today_sold,
                buy_intents, sell_intents,
                dynamic_weights=dynamic_weights,
                ma60_cache=ma60_cache,
            )

    # ── 最低仓位 30% 温和补仓 ───────────────────────────────
    # 当总仓位 < 30% 且无标的处于 SELL 信号、非空头市场时，允许小额补仓
    if not in_drawdown_mode and market_discount >= 0.7:
        # 计算当前总仓位比例
        total_holding_value = sum(
            (p["positions"].get(t["code"], {}).get("shares", 0) *
             quotes_cache.get(t["code"], {}).get("price", 0))
            for t in targets
        )
        position_ratio = total_holding_value / total_assets if total_assets > 0 else 0

        # 检查是否有标的处于 SELL 信号
        has_sell_signal = any(
            s.get("signal") == "SELL" for s in signal_snapshots
        )

        if position_ratio < 0.30 and not has_sell_signal:
            click.echo(f"\n  仓位 {position_ratio:.0%} < 30%，触发最低仓位补仓", err=True)
            max_topup = total_assets * 0.05  # 每次最多补总资产 5%
            for target in targets:
                code = target["code"]
                if code in today_bought or code in today_sold:
                    continue
                cooldown = p.get("cooldown", {})
                if cooldown.get(code, "") >= today:
                    continue
                quote = quotes_cache.get(code)
                if not quote:
                    continue
                price = quote["price"]
                holding_value = (p["positions"].get(code, {}).get("shares", 0) * price)
                target_amount = total_assets * dynamic_weights.get(code, 0)
                if holding_value >= target_amount:
                    continue
                gap = target_amount - holding_value
                want = min(gap * market_discount * 0.5, max_topup)  # 保守补仓：缺口×50%×折扣
                if want > 0:
                    # 检查 buy_intents 中是否已有该标的
                    already_in = any(t["code"] == code for t, _, _, _ in buy_intents)
                    if not already_in:
                        buy_intents.append((target, quote, {"total": 0, "signal": "MIN_POS"}, want))
                        click.echo(f"  → 最低仓位补仓 {code} {target['name']} {want:,.0f}元", err=True)

    # ── 非 targets 持仓的止损处理 ──────────────────────────
    target_codes = {t["code"] for t in targets}
    for code in (stop_loss_codes | trailing_stop_codes) - target_codes:
        pos = p["positions"].get(code)
        if not pos or code not in quotes_cache:
            continue
        sellable = max(0, pos["shares"] - today_bought.get(code, 0))
        if sellable <= 0 or code in today_sold:
            continue
        quote = quotes_cache[code]
        reason = "个股止损" if code in stop_loss_codes else "Trailing Stop"
        # 构造虚拟 target 用于卖出执行
        pseudo_target = {"code": code, "name": pos.get("name", ""), "type": "stock", "weight": 0}
        sell_intents.append((pseudo_target, quote, {"total": 0, "signal": "SELL"}, sellable))
        click.echo(f"\n{code} {pos.get('name','')}: 非 targets 持仓 {reason}，计划清仓 {sellable}股", err=True)

    # ── 第二轮：先执行卖出（回收现金），再按比例分配买入 ────────
    from fsi.trading_calendar import is_trading_time

    if not is_trading_time():
        click.echo(f"\n--- 非交易时间，跳过交易执行（信号已记录）---", err=True)
    else:
        click.echo(f"\n--- 执行交易 ---", err=True)

        # 卖出
        from datetime import timedelta
        for target, quote, scores, sell_shares in sell_intents:
            code, name = target["code"], target["name"]
            sell_price = calc_sell_price(quote)
            if sell_shares > 0 and sell_price > 0:
                trade = execute_sell(p, code, sell_price, sell_shares,
                                    name=name, check_time=check_time,
                                    profile=profile,
                                    asset_type=target.get("type", "stock"))
                if trade:
                    today_sold.add(code)
                    executed_trades.append(trade)
                    click.echo(f"  SELL {code} {name} {sell_shares}股 @ {sell_price} "
                               f"= {trade['amount']:,.2f}元 (费用{trade['fee']:.2f})", err=True)
                    # 止损清仓后设置冷却期（大盘强5天，弱10天）
                    if code in stop_loss_codes and code not in p["positions"]:
                        cooldown_days = 5 if market_discount >= 1.0 else 10
                        cooldown_end = (now + timedelta(days=cooldown_days)).strftime("%Y-%m-%d")
                        p.setdefault("cooldown", {})[code] = cooldown_end
                        click.echo(f"    冷却期{cooldown_days}天至 {cooldown_end}"
                                   f"（大盘{'强势' if cooldown_days == 5 else '偏弱'}）", err=True)
                    # TS 半仓卖出后重置 high_watermark（防止剩余仓位反复触发 TS）
                    if code in trailing_stop_codes and code in p["positions"]:
                        p["positions"][code]["high_watermark"] = round(sell_price, 4)

        # 买入：按需求金额比例分配可用现金（留 5% 备用金）
        # 同组标的总分配上限 = 可用现金 × _GROUP_MAX_RATIO
        if buy_intents:
            available_cash = p["cash"] * 0.95
            total_want = sum(want for _, _, _, want in buy_intents)

            # 计算各资产组的原始分配总额，超限时按比例缩减
            group_alloc = {}  # group_name → total allocated
            for target, _, _, want in buy_intents:
                code = target["code"]
                for gname, gcodes in _ASSET_GROUPS.items():
                    if code in gcodes:
                        group_alloc.setdefault(gname, 0)
                        raw = available_cash * (want / total_want) if total_want > 0 else 0
                        group_alloc[gname] += raw

            # 计算各组缩减比例
            group_scale = {}
            group_cap = available_cash * _GROUP_MAX_RATIO
            for gname, g_total in group_alloc.items():
                if g_total > group_cap:
                    group_scale[gname] = group_cap / g_total
                    click.echo(f"  同组限额: {gname} 分配{g_total:,.0f}元 > "
                               f"上限{group_cap:,.0f}元，缩减至 {group_cap/g_total:.0%}", err=True)

            for target, quote, scores, want in buy_intents:
                code, name = target["code"], target["name"]
                # 按需求比例分配
                alloc = available_cash * (want / total_want) if total_want > 0 else 0
                # 同组限额缩减
                for gname, gcodes in _ASSET_GROUPS.items():
                    if code in gcodes and gname in group_scale:
                        alloc *= group_scale[gname]
                        break
                buy_amount = min(want, alloc)
                if buy_amount > 0:
                    buy_price = calc_buy_price(quote)
                    atype = target.get("type", "stock")
                    shares = calc_buy_shares(buy_price, buy_amount, asset_type=atype)
                    if shares > 0:
                        trade = execute_buy(p, code, buy_price, shares,
                                           name=name, check_time=check_time,
                                           profile=profile, asset_type=atype)
                        if trade:
                            today_bought[code] = today_bought.get(code, 0) + shares
                            executed_trades.append(trade)
                            click.echo(f"  BUY  {code} {name} {shares}股 @ {buy_price} "
                                       f"= {trade['amount']:,.2f}元 (费用{trade['fee']:.2f})", err=True)

        click.echo(f"\n本次执行 {len(executed_trades)} 笔交易，剩余现金: {p['cash']:,.2f}", err=True)

    # 保存信号历史（即使没有交易也要持久化）
    save_portfolio(p, profile)

    # 保存完整 check log（追加到 JSONL）
    check_log_entry = {
        "profile": profile,
        "mode": "ai" if use_ai else "rule",
        "date": today,
        "time": check_time,
        "total_assets": round(total_assets, 2),
        "cash": p["cash"],
        "portfolio_hwm": p.get("portfolio_hwm"),
        "drawdown_pct": round(drawdown_pct, 2),
        "in_drawdown_mode": p.get("drawdown_halved", False),
        "market_factors": {
            "market_discount": round(market_discount, 4),
            "qvix_discount": qvix_discount,
            "fund_discount": fund_discount,
        },
        "dynamic_weights": {code: round(w, 4) for code, w in dynamic_weights.items()},
        "signals": signal_snapshots,
        "buy_intents": [
            {"code": t["code"], "name": t["name"], "want": round(w, 2)}
            for t, _, _, w in buy_intents
        ],
        "sell_intents": [
            {"code": t["code"], "name": t["name"], "shares": s}
            for t, _, _, s in sell_intents
        ],
        "executed_trades": executed_trades,
        "positions_after": {
            code: {"shares": pos["shares"], "avg_cost": pos["avg_cost"]}
            for code, pos in p["positions"].items()
        },
        **({"ai_decisions": ai_result.get("decisions"),
            "ai_market_view": ai_result.get("market_view")}
           if ai_result else {}),
    }
    _save_check_log(profile, check_log_entry)

    return {
        "profile": profile,
        "check_time": f"{today} {check_time}",
        "trades": executed_trades,
        "cash": p["cash"],
        "positions": p["positions"],
    }


def _save_check_log(profile: str, entry: dict):
    """追加 check 决策日志到 data/sim_logs/{profile}_check.jsonl（带文件锁）"""
    import fcntl
    _CHECK_LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = _CHECK_LOG_DIR / f"{profile}_check.jsonl"
    try:
        line = json.dumps(entry, ensure_ascii=False, default=str) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(line)
            f.flush()
            fcntl.flock(f, fcntl.LOCK_UN)
    except OSError:
        pass


def _ai_decide(signal_snapshots: list, portfolio: dict,
               market_discount: float, qvix_discount: float,
               fund_discount: float,
               dynamic_weights: dict | None = None) -> dict | None:
    """调用 AI 生成交易决策，返回解析后的 dict 或 None（失败时）。"""
    from fsi.ai.bedrock import call_bedrock
    from fsi.ai.prompts import SIM_DECISION_PROMPT, build_user_message
    from fsi.market_context import fetch_market_context
    import re

    # 组装 context
    initial_capital = portfolio["config"]["initial_capital"]
    total_assets = portfolio["cash"]
    for pos in portfolio["positions"].values():
        total_assets += pos["shares"] * pos["avg_cost"]  # 近似值，够用

    context = {
        "portfolio": {
            "profile": portfolio["config"].get("preset", ""),
            "cash": portfolio["cash"],
            "total_assets": round(total_assets, 2),
            "initial_capital": initial_capital,
            "drawdown_pct": round(
                (total_assets - portfolio.get("portfolio_hwm", initial_capital))
                / max(portfolio.get("portfolio_hwm", initial_capital), 1) * 100, 2),
            "in_drawdown_mode": portfolio.get("drawdown_halved", False),
        },
        "market_factors": {
            "market_discount": round(market_discount, 4),
            "qvix_discount": round(qvix_discount, 4),
            "fund_discount": round(fund_discount, 4),
        },
        "targets": signal_snapshots,
        "dynamic_weights": {code: round(w * 100, 1)
                            for code, w in (dynamic_weights or {}).items()},
        "recent_trades": portfolio.get("trades", [])[-10:],
    }

    # 获取市场上下文
    try:
        market_ctx = fetch_market_context(
            include_news=True, include_calendar=True,
            include_qvix=True, include_global_indices=True,
        )
        if market_ctx:
            context["market_context"] = market_ctx
    except Exception:
        pass

    # 调用 AI
    try:
        response = call_bedrock(SIM_DECISION_PROMPT, build_user_message(context))
    except Exception as e:
        click.echo(f"  AI 调用异常: {e}", err=True)
        return None

    # 从响应中提取 JSON（支持 ```json 包裹）
    try:
        m = re.search(r"```json\s*(.*?)\s*```", response, re.DOTALL)
        json_str = m.group(1) if m else response
        result = json.loads(json_str)
        if "decisions" not in result:
            click.echo(f"  AI 返回缺少 decisions 字段", err=True)
            return None
        return result
    except (json.JSONDecodeError, AttributeError) as e:
        click.echo(f"  AI 返回 JSON 解析失败: {e}", err=True)
        return None


def _apply_ai_decisions(ai_result: dict, targets: list, quotes_cache: dict,
                        portfolio: dict, total_assets: float,
                        market_discount: float,
                        today_bought: dict, today_sold: set,
                        in_drawdown_mode: bool,
                        buy_intents: list, sell_intents: list,
                        dynamic_weights: dict | None = None,
                        ma60_cache: dict | None = None):
    """将 AI 的 decisions 映射为 buy_intents / sell_intents。"""
    # 建立 code → target 映射
    target_map = {t["code"]: t for t in targets}

    for d in ai_result.get("decisions", []):
        code = d.get("code", "")
        action = d.get("action", "hold")
        intensity = max(0.0, min(1.0, float(d.get("intensity", 0))))
        reason = d.get("reason", "")

        target = target_map.get(code)
        if not target:
            continue
        quote = quotes_cache.get(code)
        if not quote:
            continue

        price = quote["price"]
        pos = portfolio["positions"].get(code)
        holding_shares = pos["shares"] if pos else 0
        holding_value = holding_shares * price if holding_shares > 0 else 0
        weight = (dynamic_weights or {}).get(code, get_target_weight(target))
        target_amount = total_assets * weight
        sellable_shares = max(0, holding_shares - today_bought.get(code, 0))

        cooldown = portfolio.get("cooldown", {})
        now = datetime.now(tz=_BJ)
        today_str = now.strftime("%Y-%m-%d")
        in_cooldown = cooldown.get(code, "") >= today_str

        is_hold = target.get("strategy") == "hold"

        if action == "buy" and intensity > 0:
            if code in today_bought or in_drawdown_mode or in_cooldown:
                click.echo(f"  AI→{code} buy 被护栏阻止"
                           f"{'（今日已买）' if code in today_bought else ''}"
                           f"{'（回撤模式）' if in_drawdown_mode else ''}"
                           f"{'（冷却期）' if in_cooldown else ''}", err=True)
                continue
            # hold 策略：网格加仓 + 不受 market_discount 影响 + 每日上限
            discount = 1.0 if is_hold else market_discount
            want = (target_amount - holding_value) * intensity * discount
            if is_hold:
                grid_mult, deviation = _calc_grid_multiplier(
                    price, (ma60_cache or {}).get(code))
                max_per_day = total_assets * 0.03 * grid_mult
                want = min(want, max_per_day)
            if want > 0:
                buy_intents.append((target, quote, {"total": 0, "signal": "AI_BUY"}, want))
                tag = " [hold]" if is_hold else ""
                grid_info = ""
                if is_hold and grid_mult > 1.0:
                    grid_info = f" 网格×{grid_mult:.1f}"
                click.echo(f"  AI→{code}{tag} 买入 {want:,.0f}元 (intensity={intensity:.1f}){grid_info} {reason}", err=True)

        elif action == "sell" and intensity > 0:
            # hold 策略：一般忽略 AI 的 sell，但反弹减仓除外
            if is_hold:
                # 反弹减仓：偏离 MA60 > 8% 且显著超配时允许减仓
                ma60 = (ma60_cache or {}).get(code)
                _, deviation = _calc_grid_multiplier(price, ma60)
                max_weight = target.get("max_weight", weight)
                max_hold_value = total_assets * max_weight * 1.1
                if deviation is not None and deviation > 0.08 \
                        and holding_value > max_hold_value \
                        and sellable_shares > 0 and code not in today_sold:
                    trim_target = total_assets * max_weight
                    trim_amount = holding_value - trim_target
                    trim_shares = int(trim_amount / price)
                    trim_shares = (trim_shares // 100) * 100
                    if trim_shares > 0:
                        trim_shares = min(trim_shares, sellable_shares)
                        sell_intents.append((target, quote, {"total": 0, "signal": "AI_SELL"}, trim_shares))
                        click.echo(f"  AI→{code} [hold] 反弹减仓 {trim_shares}股"
                                   f"（偏离MA60 {deviation:+.1%}）", err=True)
                        continue
                click.echo(f"  AI→{code} [hold] sell 被忽略（底仓长持策略）", err=True)
                continue
            if code in today_sold or sellable_shares <= 0:
                click.echo(f"  AI→{code} sell 被护栏阻止"
                           f"{'（今日已卖）' if code in today_sold else ''}"
                           f"{'（无可卖）' if sellable_shares <= 0 else ''}", err=True)
                continue
            sell_shares = int(sellable_shares * intensity)
            sell_shares = (sell_shares // 100) * 100
            if sell_shares <= 0:
                sell_shares = sellable_shares  # 不足一手卖全部
            sell_intents.append((target, quote, {"total": 0, "signal": "AI_SELL"}, sell_shares))
            click.echo(f"  AI→{code} 卖出 {sell_shares}股 (intensity={intensity:.1f}) {reason}", err=True)

        else:
            click.echo(f"  AI→{code} 持有 {reason}", err=True)


def _apply_rule_fallback(targets: list, quotes_cache: dict, portfolio: dict,
                         signal_snapshots: list, total_assets: float,
                         market_discount: float, in_drawdown_mode: bool,
                         today_bought: dict, today_sold: set,
                         buy_intents: list, sell_intents: list,
                         dynamic_weights: dict | None = None,
                         ma60_cache: dict | None = None):
    """AI 失败时回退到规则引擎的意图收集（精简版）。"""
    snapshot_map = {s["code"]: s for s in signal_snapshots}

    for target in targets:
        code = target["code"]
        asset_type = target["type"]
        weight = (dynamic_weights or {}).get(code, get_target_weight(target))
        ap = _ASSET_PARAMS.get(asset_type, _ASSET_PARAMS["stock"])

        quote = quotes_cache.get(code)
        if not quote:
            continue

        snap = snapshot_map.get(code)
        if not snap:
            continue

        # 已被止损强制卖出的跳过
        if snap.get("stop_loss") or snap.get("trailing_stop"):
            continue

        price = quote["price"]
        signal = snap["signal"]
        score = snap["score"]
        confirmed = snap["confirmed"]
        score_weakening = snap.get("score_weakening", False)

        pos = portfolio["positions"].get(code)
        holding_shares = pos["shares"] if pos else 0
        holding_value = holding_shares * price if holding_shares > 0 else 0
        target_amount = total_assets * weight
        sellable_shares = max(0, holding_shares - today_bought.get(code, 0))

        cooldown = portfolio.get("cooldown", {})
        now = datetime.now(tz=_BJ)
        today_str = now.strftime("%Y-%m-%d")
        in_cooldown = cooldown.get(code, "") >= today_str

        is_hold = target.get("strategy") == "hold"

        if is_hold:
            # 网格加仓
            grid_mult, deviation = _calc_grid_multiplier(
                price, (ma60_cache or {}).get(code))

            # 反弹减仓：偏离 MA60 > 8% 且显著超配
            max_weight = target.get("max_weight", weight)
            max_hold_value = total_assets * max_weight * 1.1
            already_sold = code in today_sold
            if deviation is not None and deviation > 0.08 \
                    and holding_value > max_hold_value \
                    and sellable_shares > 0 and not already_sold:
                trim_target = total_assets * max_weight
                trim_amount = holding_value - trim_target
                trim_shares = int(trim_amount / price)
                trim_shares = (trim_shares // 100) * 100
                if trim_shares > 0:
                    trim_shares = min(trim_shares, sellable_shares)
                    sell_intents.append((target, quote, snap.get("scores", {}), trim_shares))
                    click.echo(f"  规则回退→{code} [hold] 反弹减仓 {trim_shares}股", err=True)

            elif holding_value < target_amount * ap["underweight_ratio"] \
                    and code not in today_bought and not in_drawdown_mode \
                    and not in_cooldown and signal not in ("SELL",):
                score_factor = 0.6
                if snap.get("scores", {}).get("overbought", 0) >= 0.5:
                    score_factor *= 1.5
                want = (target_amount - holding_value) * score_factor * grid_mult
                if want > 0:
                    buy_intents.append((target, quote, snap.get("scores", {}), want))
                    grid_tag = f" 网格×{grid_mult:.1f}" if grid_mult > 1.0 else ""
                    click.echo(f"  规则回退→{code} [hold] 底仓补仓 {want:,.0f}元{grid_tag}", err=True)

        elif signal in ("BUY", "BULLISH") and confirmed \
                and holding_value < target_amount * ap["underweight_ratio"] \
                and code not in today_bought and not in_drawdown_mode \
                and not in_cooldown:
            if score >= 0.8:
                score_factor = 1.0
            else:
                score_factor = 0.5 + (score - 0.2) / (0.8 - 0.2) * 0.3
                score_factor = max(0.5, min(0.8, score_factor))
            if score_weakening:
                score_factor *= 0.5
            want = (target_amount - holding_value) * market_discount * score_factor
            if want > 0:
                buy_intents.append((target, quote, snap.get("scores", {}), want))
                click.echo(f"  规则回退→{code} 买入 {want:,.0f}元", err=True)

        elif signal in ("SELL", "BEARISH") and confirmed \
                and sellable_shares > 0 and code not in today_sold:
            sell_shares = _calc_sell_shares(sellable_shares, snap.get("scores", {}), pos, price)
            sell_intents.append((target, quote, snap.get("scores", {}), sell_shares))
            click.echo(f"  规则回退→{code} 卖出 {sell_shares}股", err=True)


def _calc_qvix_discount() -> float:
    """根据 QVIX（50ETF 波动率指数）计算买入力度系数。

    返回:
        1.0 — QVIX < 25 或获取失败（正常买入）
        0.7 — QVIX 25~30（市场紧张）
        0.5 — QVIX > 30（恐慌情绪）
    """
    try:
        from fsi.fetcher.qvix import fetch_qvix_daily
        records = fetch_qvix_daily(days=3)
        if not records:
            return 1.0
        # 取最新一条的收盘价
        latest = records[-1]
        qvix = latest.get("close") or latest.get("qvix")
        if qvix is None:
            return 1.0
        qvix = float(qvix)
        if qvix > 30:
            return 0.5
        elif qvix > 25:
            return 0.7
        return 1.0
    except Exception:
        return 1.0


def _calc_fund_flow_discount() -> float:
    """根据近 3 日大盘主力资金净流入计算买入力度系数。

    返回:
        1.1 — 连续净流入（积极信号）
        1.0 — 数据不足或中性
        0.8 — 连续净流出（偏保守）
    """
    try:
        from fsi.fetcher.capital_flow import fetch_market_fund_flow
        records = fetch_market_fund_flow(days=5)
        if not records or len(records) < 3:
            return 1.0
        # 取最近 3 天
        recent = records[-3:]
        # 提取主力净流入字段（东方财富格式）
        net_flows = []
        for r in recent:
            nf = r.get("主力净流入-净额")
            if nf is None:
                nf = r.get("主力净流入")
            if nf is None:
                nf = r.get("主力净流入-净占比")
            if nf is not None:
                net_flows.append(float(nf))
        if len(net_flows) < 3:
            return 1.0
        # 全部为负 → 主力撤退
        if all(nf < 0 for nf in net_flows):
            return 0.8
        # 全部为正 → 主力进场
        if all(nf > 0 for nf in net_flows):
            return 1.1
        return 1.0
    except Exception:
        return 1.0


def _calc_sell_shares(sellable: int, scores: dict, pos: dict | None, price: float) -> int:
    """根据信号强度和持仓盈亏计算卖出股数。

    score 映射（保守策略，边界信号轻仓试探）：
      ≤ -0.8 → 100% 清仓
      -0.8 ~ -0.3 → 15%~80% 线性
    公式: ratio = 0.15 + (|score| - 0.3) / 0.5 × 0.65
    亏损仓位上浮 20%（加速止损），盈利仓位下浮 10%（多留利润）。
    """
    if sellable <= 0:
        return 0

    total = scores.get("total", 0)
    abs_score = abs(total)

    # 线性映射: |score| 0.5 → 卖 15%, |score| 0.8 → 卖 80%, > 0.8 → 100%
    if abs_score >= 0.8:
        ratio = 1.0
    else:
        ratio = 0.15 + (abs_score - 0.5) / (0.8 - 0.5) * 0.65
        ratio = max(0.15, min(0.80, ratio))

    # 持仓盈亏调整
    if pos and pos.get("avg_cost", 0) > 0:
        pnl_pct = (price - pos["avg_cost"]) / pos["avg_cost"] * 100
        if pnl_pct < -3:
            ratio = min(1.0, ratio + 0.2)   # 亏损仓位加速卖出
        elif pnl_pct > 0:
            ratio *= 0.5                     # 盈利持仓卖出比例减半（减少无效卖出）
            ratio = max(0.10, ratio)         # 最低保留 10%

    sell_shares = int(sellable * ratio)
    # 整手（100股）取整
    sell_shares = (sell_shares // 100) * 100
    # 不足一手时卖全部
    if sell_shares <= 0:
        sell_shares = sellable
    return sell_shares


def _calc_market_discount(conn, engine) -> float:
    """根据多指数均线状态加权计算买入力度系数。

    同时看沪深300（权重0.4）、中证500（0.3）、创业板指（0.3），
    各指数独立判定后加权平均。

    返回:
        1.0 — 大盘多头或数据不足（正常买入）
        0.5 — 大盘空头排列（买入金额减半）
        0.7 — 大盘弱势但非完全空头
    """
    import pandas as pd
    from datetime import timedelta

    indices = [
        ("000300", 0.4),  # 沪深300
        ("000905", 0.3),  # 中证500
        ("399006", 0.3),  # 创业板指
    ]
    today_bj = datetime.now(tz=_BJ).date()
    cutoff = (today_bj - timedelta(days=120)).isoformat()

    def _safe_float(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            v = float(val)
            return None if pd.isna(v) else v
        except (ValueError, TypeError):
            return None

    def _single_index_discount(code: str) -> float | None:
        try:
            df = conn.execute(
                "SELECT * FROM index_daily WHERE code = ? AND trade_date >= ? "
                "ORDER BY trade_date",
                [code, cutoff],
            ).fetchdf()
        except Exception:
            return None
        if df.empty or len(df) < 30:
            return None
        df = engine.calculate(df)
        curr = df.iloc[-1]
        ma5 = _safe_float(curr.get("ma5"))
        ma10 = _safe_float(curr.get("ma10"))
        ma20 = _safe_float(curr.get("ma20"))
        if ma5 is None or ma10 is None or ma20 is None:
            return None
        if ma5 < ma10 < ma20:
            return 0.5
        close = _safe_float(curr.get("close"))
        if close is not None and close < ma20:
            return 0.7
        return 1.0

    weighted_sum = 0.0
    total_weight = 0.0
    for code, w in indices:
        d = _single_index_discount(code)
        if d is not None:
            weighted_sum += d * w
            total_weight += w

    if total_weight == 0:
        return 1.0
    return round(weighted_sum / total_weight, 2)


def _calc_ma60(conn, table: str, code: str) -> float | None:
    """从 DB 获取标的最近 60 个交易日的收盘均价（MA60）。

    数据不足 60 日时用可用天数的均线代替。返回 None 表示数据不足（<10日）。
    """
    import pandas as pd

    df = conn.execute(
        f"SELECT close FROM {table} WHERE code = ? ORDER BY trade_date DESC LIMIT 60",
        [code],
    ).fetchdf()
    if df.empty or len(df) < 10:
        return None
    closes = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(closes) < 10:
        return None
    return float(closes.mean())


def _calc_grid_multiplier(price: float, ma60: float | None) -> tuple[float, float | None]:
    """根据当前价格相对 MA60 的偏离度计算网格加仓倍数。

    返回 (grid_multiplier, deviation)。ma60 为 None 时返回 (1.0, None)。
    """
    if ma60 is None or ma60 <= 0:
        return 1.0, None
    deviation = (price - ma60) / ma60
    if deviation < -0.10:
        return 3.0, deviation   # 极度低估
    elif deviation < -0.06:
        return 2.0, deviation   # 明显低估
    elif deviation < -0.03:
        return 1.5, deviation   # 轻度低估
    else:
        return 1.0, deviation   # 正常


def _quick_score(conn, engine, table, code, threshold,
                 asset_type: str = "stock") -> dict | None:
    """快速评分：取数据 → 计算指标 → zigzag → 打分。

    评分窗口按标的类型自适应：ETF/指数 150 日，个股 90 日。
    """
    from datetime import timedelta

    # 自适应评分窗口
    score_window = 150 if asset_type in ("etf", "index") else 90

    today_bj = datetime.now(tz=_BJ).date()
    cutoff = (today_bj - timedelta(days=720)).isoformat()

    df = conn.execute(
        f"SELECT * FROM {table} WHERE code = ? AND trade_date >= ? ORDER BY trade_date",
        [code, cutoff],
    ).fetchdf()

    if df.empty or len(df) < 20:
        return None

    df = engine.calculate(df)
    df = df.tail(score_window).reset_index(drop=True)

    if len(df) < 10:
        return None

    swings = detect_swings(df, pct_threshold=threshold)
    return compute_scores(df, swings)
