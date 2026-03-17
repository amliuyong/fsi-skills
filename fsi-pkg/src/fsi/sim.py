"""模拟盘核心引擎 — 持仓状态管理、交易执行、报价获取"""

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fsi.cli.quote import _parse_tencent_quote
from fsi.config import FSI_DIR

_SIM_DIR = FSI_DIR
_BJ = ZoneInfo("Asia/Shanghai")

# ── 预设模板 ─────────────────────────────────────────────────

PRESETS = {
    "conservative": {
        "label": "保守型",
        "stop_loss_pct": -8.0,       # 个股止损线 -8%
        "max_drawdown_pct": -12.0,   # 组合最大回撤 -12%
        "cash_reserve": 0.05,        # 现金保留比例 5%
        "targets": [
            {"code": "512890", "type": "etf", "name": "红利ETF", "min_weight": 0.10, "max_weight": 0.35, "strategy": "hold"},
            {"code": "513500", "type": "etf", "name": "标普500ETF", "min_weight": 0.10, "max_weight": 0.35, "strategy": "hold"},
            {"code": "510300", "type": "etf", "name": "沪深300ETF", "min_weight": 0, "max_weight": 0.25},
            {"code": "510500", "type": "etf", "name": "中证500ETF", "min_weight": 0, "max_weight": 0.20},
            {"code": "159928", "type": "etf", "name": "消费ETF", "min_weight": 0, "max_weight": 0.15},
        ],
    },
    "balanced": {
        "label": "均衡型",
        "stop_loss_pct": -10.0,
        "max_drawdown_pct": -15.0,
        "cash_reserve": 0.05,
        "targets": [
            {"code": "510300", "type": "etf", "name": "沪深300ETF", "min_weight": 0, "max_weight": 0.25},
            {"code": "510500", "type": "etf", "name": "中证500ETF", "min_weight": 0, "max_weight": 0.20},
            {"code": "512890", "type": "etf", "name": "红利ETF", "min_weight": 0.05, "max_weight": 0.30, "strategy": "hold"},
            {"code": "513500", "type": "etf", "name": "标普500ETF", "min_weight": 0.05, "max_weight": 0.30, "strategy": "hold"},
            {"code": "159928", "type": "etf", "name": "消费ETF", "min_weight": 0, "max_weight": 0.15},
            {"code": "159949", "type": "etf", "name": "创业板50ETF", "min_weight": 0, "max_weight": 0.20},
        ],
    },
    "aggressive": {
        "label": "激进型",
        "stop_loss_pct": -12.0,
        "max_drawdown_pct": -20.0,
        "cash_reserve": 0.05,
        "targets": [
            {"code": "510300", "type": "etf", "name": "沪深300ETF", "min_weight": 0, "max_weight": 0.20},
            {"code": "510500", "type": "etf", "name": "中证500ETF", "min_weight": 0, "max_weight": 0.25},
            {"code": "159949", "type": "etf", "name": "创业板50ETF", "min_weight": 0, "max_weight": 0.25},
            {"code": "513100", "type": "etf", "name": "纳指ETF", "min_weight": 0.05, "max_weight": 0.30, "strategy": "hold"},
            {"code": "588000", "type": "etf", "name": "科创50ETF", "min_weight": 0, "max_weight": 0.25},
            {"code": "159928", "type": "etf", "name": "消费ETF", "min_weight": 0, "max_weight": 0.15},
        ],
    },
}

DEFAULT_TARGETS = PRESETS["conservative"]["targets"]


# ── 向后兼容：旧格式 weight → min_weight/max_weight ─────────────

def normalize_target(target: dict) -> dict:
    """将旧格式 weight 转换为 min_weight/max_weight 格式。"""
    if "max_weight" in target:
        return target
    # 旧格式：weight → max_weight，min_weight 默认 0
    w = target.get("weight", 0)
    t = dict(target)
    t["max_weight"] = w
    t["min_weight"] = 0
    t.pop("weight", None)
    return t


def get_target_weight(target: dict) -> float:
    """获取标的的参考权重（用于向后兼容，取 max_weight 或旧 weight）。"""
    if "max_weight" in target:
        return target["max_weight"]
    return target.get("weight", 0)


# ── 动态权重计算 ──────────────────────────────────────────────

def calc_allocation_score(target: dict, scores: dict | None, signal: str) -> float:
    """计算单个标的的配置分（allocation_score）。

    hold 标的：基础分 1.0，趋势向上 +0.5，超卖 +0.3
    trend 标的：基础分 0，根据信号和趋势加减分
    """
    strategy = target.get("strategy", "trend")
    if strategy == "hold":
        base = 1.0
        if scores:
            # 趋势向上（trend 维度 > 0）
            trend_score = scores.get("trend", 0)
            if trend_score > 0:
                base += 0.5
            # 超卖（overbought 维度 >= 0.5，逆向指标，高值=超卖）
            ob_score = scores.get("overbought", 0)
            if ob_score >= 0.5:
                base += 0.3
        return base
    else:
        # trend 标的
        base = 0.0
        if signal == "BUY":
            base += 1.5
        elif signal == "BULLISH":
            base += 1.0
        elif signal == "NEUTRAL":
            base += 0.0
        elif signal == "BEARISH":
            base -= 0.5
        elif signal == "SELL":
            base -= 1.0
        # 趋势加成
        if scores:
            trend_score = scores.get("trend", 0)
            if trend_score > 0:
                base += 0.3
        return base


def calc_dynamic_weights(targets: list, scores_map: dict, signal_map: dict,
                         cash_reserve: float = 0.05,
                         verbose: bool = False) -> dict:
    """计算所有标的的动态目标权重。

    Args:
        targets: 标的列表（含 min_weight, max_weight, strategy）
        scores_map: code → scores dict（评分引擎输出）
        signal_map: code → signal str（BUY/BULLISH/NEUTRAL/BEARISH/SELL）
        cash_reserve: 现金保留比例（默认 5%）
        verbose: 是否返回调试信息

    Returns:
        dict with keys:
            weights: {code: float} — 各标的动态目标权重（总和 = 1 - cash_reserve）
            debug: list of dicts — 每只标的的打分和权重明细（verbose=True 时）
    """
    investable = 1.0 - cash_reserve  # 可投资比例

    # Step 1: 计算每只标的的 allocation_score
    alloc_scores = {}
    debug_info = []
    for t in targets:
        t = normalize_target(t)
        code = t["code"]
        scores = scores_map.get(code)
        signal = signal_map.get(code, "NEUTRAL")
        a_score = calc_allocation_score(t, scores, signal)
        alloc_scores[code] = a_score
        debug_info.append({
            "code": code,
            "name": t["name"],
            "strategy": t.get("strategy", "trend"),
            "signal": signal,
            "allocation_score": round(a_score, 2),
            "min_weight": t.get("min_weight", 0),
            "max_weight": t.get("max_weight", 0),
        })

    # Step 2: 归一化为权重（只对 score > 0 的标的分配）
    # 建 code → target 映射（归一化后的）
    target_map = {normalize_target(t)["code"]: normalize_target(t) for t in targets}
    positive_codes = [c for c, s in alloc_scores.items() if s > 0]
    positive_sum = sum(alloc_scores[c] for c in positive_codes)

    raw_weights = {}
    for code in alloc_scores:
        t = target_map[code]
        min_w = t.get("min_weight", 0)
        if code in positive_codes and positive_sum > 0:
            raw_weights[code] = alloc_scores[code] / positive_sum * investable
        else:
            # score <= 0，给 min_weight（hold 标的始终 > 0 不会走到这里）
            raw_weights[code] = min_w

    # Step 3: clamp 到 [min_weight, max_weight]
    clamped = {}
    for code, rw in raw_weights.items():
        t = target_map[code]
        min_w = t.get("min_weight", 0)
        max_w = t.get("max_weight", 0)
        clamped[code] = max(min_w, min(max_w, rw))

    # Step 4: 归一化确保总和 = investable
    total = sum(clamped.values())
    weights = {}
    if total > 0:
        scale = investable / total
        for code, w in clamped.items():
            t = target_map[code]
            min_w = t.get("min_weight", 0)
            max_w = t.get("max_weight", 0)
            # 缩放后再 clamp，迭代收敛
            weights[code] = max(min_w, min(max_w, w * scale))
    else:
        # 全部 score <= 0，按 min_weight 分配
        for code in clamped:
            t = target_map[code]
            weights[code] = t.get("min_weight", 0)

    # 填充 debug 信息
    for d in debug_info:
        d["dynamic_weight"] = round(weights.get(d["code"], 0), 4)

    result = {"weights": weights}
    if verbose:
        result["debug"] = debug_info
    return result


# ── 路径与持久化 ──────────────────────────────────────────────

def _sim_path(profile: str) -> Path:
    return _SIM_DIR / f"sim_{profile}.json"


def list_profiles() -> list[str]:
    """列出所有已存在的 profile 名称。"""
    _SIM_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(
        p.stem.removeprefix("sim_")
        for p in _SIM_DIR.glob("sim_*.json")
    )


def init_portfolio(profile: str, capital: float = 1_000_000,
                   preset: str | None = None) -> dict:
    """初始化模拟盘，返回 portfolio dict。"""
    targets = PRESETS.get(preset or "conservative", PRESETS["conservative"])["targets"]
    now = datetime.now(tz=_BJ)
    portfolio = {
        "config": {
            "profile": profile,
            "preset": preset or "conservative",
            "initial_capital": capital,
            "created": now.strftime("%Y-%m-%d %H:%M"),
            "targets": targets,
        },
        "cash": capital,
        "positions": {},
        "trades": [],
        "signal_history": {},  # code → {"direction": "buy"|"sell"|"neutral", "streak": N, "date": "YYYY-MM-DD"}
        "cooldown": {},        # code → "YYYY-MM-DD"（冷却截止日期）
    }
    save_portfolio(portfolio, profile)
    return portfolio


def load_portfolio(profile: str = "default") -> dict | None:
    """加载模拟盘状态。不存在返回 None。"""
    path = _sim_path(profile)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_portfolio(portfolio: dict, profile: str = "default"):
    """保存模拟盘状态。"""
    path = _sim_path(profile)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(portfolio, f, ensure_ascii=False, indent=2)


def fetch_quote(code: str) -> dict | None:
    """获取实时报价（含盘口）。"""
    return _parse_tencent_quote(code)


def calc_buy_price(quote: dict) -> float:
    """计算买入价：卖一价 + 0.001（确保成交）。"""
    ask = quote.get("ask1", {}).get("price")
    price = quote.get("price")
    base = ask if ask and ask > 0 else price
    if not base or base <= 0:
        return 0
    return round(base + 0.001, 3)


def calc_sell_price(quote: dict) -> float:
    """计算卖出价：买一价 - 0.001（确保成交）。"""
    bid = quote.get("bid1", {}).get("price")
    price = quote.get("price")
    base = bid if bid and bid > 0 else price
    if not base or base <= 0:
        return 0
    return round(base - 0.001, 3)


# ── 交易成本 ───────────────────────────────────────────────────

# A 股交易费率
_COMMISSION_RATE = 0.00025   # 佣金 0.025%（券商普遍费率）
_COMMISSION_MIN = 5.0        # 最低佣金 5 元
_STAMP_TAX_RATE = 0.0005     # 印花税 0.05%（仅卖出）
_TRANSFER_FEE_RATE = 0.00001 # 过户费 0.001%


def calc_buy_shares(price: float, amount: float, min_lot: int = 100,
                    asset_type: str = "stock") -> int:
    """计算可买入股数（整手，向下取整，预留交易成本）。"""
    if price <= 0:
        return 0
    # 预留买入交易成本：佣金(min 5元) + 过户费(ETF 免)
    est_fee = max(amount * _COMMISSION_RATE, _COMMISSION_MIN)
    if asset_type == "stock":
        est_fee += amount * _TRANSFER_FEE_RATE
    effective_amount = amount - est_fee
    if effective_amount <= 0:
        return 0
    shares = int(effective_amount / price)
    return (shares // min_lot) * min_lot


def calc_trade_cost(amount: float, action: str, asset_type: str = "stock") -> float:
    """计算交易成本：佣金 + 印花税(仅股票卖出) + 过户费(仅股票)。

    ETF 免征印花税和过户费。
    """
    # 佣金（买卖双向，ETF/股票均收）
    commission = max(amount * _COMMISSION_RATE, _COMMISSION_MIN)
    if asset_type == "stock":
        # 过户费（买卖双向，仅股票）
        transfer_fee = amount * _TRANSFER_FEE_RATE
        # 印花税（仅股票卖出）
        stamp_tax = amount * _STAMP_TAX_RATE if action == "SELL" else 0
    else:
        transfer_fee = 0
        stamp_tax = 0
    return round(commission + stamp_tax + transfer_fee, 2)


def execute_buy(portfolio: dict, code: str, price: float, shares: int,
                name: str = "", check_time: str = "",
                profile: str = "default",
                asset_type: str = "stock") -> dict | None:
    """执行模拟买入，返回交易记录（含交易成本）。"""
    if shares <= 0 or price <= 0:
        return None
    cost = round(price * shares, 2)
    fee = calc_trade_cost(cost, "BUY", asset_type)
    total_cost = round(cost + fee, 2)
    if total_cost > portfolio["cash"]:
        return None

    portfolio["cash"] = round(portfolio["cash"] - total_cost, 2)

    pos = portfolio["positions"].get(code, {"shares": 0, "avg_cost": 0, "name": name})
    old_shares = pos["shares"]
    old_cost = pos["avg_cost"] * old_shares
    new_shares = old_shares + shares
    # avg_cost 含交易成本，反映真实持仓成本
    pos["avg_cost"] = round((old_cost + total_cost) / new_shares, 4) if new_shares > 0 else 0
    pos["shares"] = new_shares
    pos["name"] = name or pos.get("name", "")
    # 初始化或保留 high_watermark（用于 Trailing Stop）
    if "high_watermark" not in pos:
        pos["high_watermark"] = round(price, 4)
    elif price > pos["high_watermark"]:
        pos["high_watermark"] = round(price, 4)
    portfolio["positions"][code] = pos

    now = datetime.now(tz=_BJ)
    trade = {
        "date": now.strftime("%Y-%m-%d"),
        "time": check_time or now.strftime("%H:%M"),
        "code": code,
        "name": name,
        "action": "BUY",
        "price": price,
        "shares": shares,
        "amount": cost,
        "fee": fee,
        "cash_after": portfolio["cash"],
    }
    portfolio["trades"].append(trade)
    save_portfolio(portfolio, profile)
    return trade


def execute_sell(portfolio: dict, code: str, price: float, shares: int,
                 name: str = "", check_time: str = "",
                 profile: str = "default",
                 asset_type: str = "stock") -> dict | None:
    """执行模拟卖出，返回交易记录（含交易成本）。"""
    pos = portfolio["positions"].get(code)
    if not pos or pos["shares"] <= 0 or shares <= 0 or price <= 0:
        return None

    shares = min(shares, pos["shares"])
    gross = round(price * shares, 2)
    fee = calc_trade_cost(gross, "SELL", asset_type)
    net_proceeds = round(gross - fee, 2)
    portfolio["cash"] = round(portfolio["cash"] + net_proceeds, 2)

    pos["shares"] -= shares
    if pos["shares"] <= 0:
        del portfolio["positions"][code]
    else:
        portfolio["positions"][code] = pos

    now = datetime.now(tz=_BJ)
    trade = {
        "date": now.strftime("%Y-%m-%d"),
        "time": check_time or now.strftime("%H:%M"),
        "code": code,
        "name": name or (pos.get("name", "") if pos else ""),
        "action": "SELL",
        "price": price,
        "shares": shares,
        "amount": gross,
        "fee": fee,
        "cash_after": portfolio["cash"],
    }
    portfolio["trades"].append(trade)
    save_portfolio(portfolio, profile)
    return trade


def portfolio_summary(portfolio: dict, quotes: dict[str, dict]) -> dict:
    """计算持仓概况：市值、盈亏、收益率。"""
    initial = portfolio["config"]["initial_capital"]
    cash = portfolio["cash"]
    total_market_value = 0
    total_cost = 0
    position_details = []

    for code, pos in portfolio["positions"].items():
        q = quotes.get(code)
        current_price = q["price"] if q and q.get("price") else pos["avg_cost"]
        mv = round(current_price * pos["shares"], 2)
        cost_val = round(pos["avg_cost"] * pos["shares"], 2)
        pnl = round(mv - cost_val, 2)
        pnl_pct = round(pnl / cost_val * 100, 2) if cost_val > 0 else 0

        total_market_value += mv
        total_cost += cost_val

        position_details.append({
            "code": code,
            "name": pos.get("name", ""),
            "shares": pos["shares"],
            "avg_cost": pos["avg_cost"],
            "current_price": current_price,
            "market_value": mv,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "weight": 0,  # 计算后填充
        })

    total_assets = round(cash + total_market_value, 2)
    total_pnl = round(total_assets - initial, 2)
    total_pnl_pct = round(total_pnl / initial * 100, 2) if initial > 0 else 0

    # 计算各持仓权重
    for pd_ in position_details:
        pd_["weight"] = round(pd_["market_value"] / total_assets * 100, 2) if total_assets > 0 else 0

    return {
        "initial_capital": initial,
        "cash": cash,
        "total_market_value": round(total_market_value, 2),
        "total_assets": total_assets,
        "total_pnl": total_pnl,
        "total_pnl_pct": total_pnl_pct,
        "cash_pct": round(cash / total_assets * 100, 2) if total_assets > 0 else 100,
        "positions": position_details,
        "trade_count": len(portfolio["trades"]),
    }
