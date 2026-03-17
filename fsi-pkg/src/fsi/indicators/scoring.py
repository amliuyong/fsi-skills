"""多维度技术评分引擎 — 用于 monitor 命令的买卖信号评估"""

import pandas as pd


# 信号阈值
SIGNAL_THRESHOLDS = {
    "BUY": 0.8,
    "BULLISH": 0.2,
    "NEUTRAL_HIGH": 0.2,
    "NEUTRAL_LOW": -0.5,
    "BEARISH": -0.5,
    "SELL": -0.8,
}


def compute_scores(df: pd.DataFrame, swings: list[dict]) -> dict:
    """计算 5 维技术评分，返回各维度分数和加权总分。

    参数:
        df: 含指标的 DataFrame（已计算 MA/MACD/KDJ/RSI/BOLL，按日期排序）
        swings: detect_swings() 返回的转折点列表

    返回:
        {
            "trend": float, "momentum": float, "overbought": float,
            "volume": float, "wave": float,
            "total": float, "signal": str
        }
    """
    if len(df) < 5:
        return _empty_scores()

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    trend = _score_trend(curr)
    momentum = _score_momentum(df, curr, prev)
    overbought = _score_overbought(curr)
    volume = _score_volume(df, curr)
    wave = _score_wave(swings, float(curr["close"]))

    # 加权汇总
    weights = {
        "trend": 0.25,
        "momentum": 0.20,
        "overbought": 0.20,
        "volume": 0.15,
        "wave": 0.20,
    }
    total = (
        trend * weights["trend"]
        + momentum * weights["momentum"]
        + overbought * weights["overbought"]
        + volume * weights["volume"]
        + wave * weights["wave"]
    )

    signal = _classify_signal(total)

    return {
        "trend": round(trend, 2),
        "momentum": round(momentum, 2),
        "overbought": round(overbought, 2),
        "volume": round(volume, 2),
        "wave": round(wave, 2),
        "total": round(total, 2),
        "signal": signal,
    }


def _score_trend(curr) -> float:
    """趋势评分：均线排列 + 价格与关键均线关系。"""
    score = 0.0
    close = _f(curr.get("close"))
    ma5 = _f(curr.get("ma5"))
    ma10 = _f(curr.get("ma10"))
    ma20 = _f(curr.get("ma20"))
    ma60 = _f(curr.get("ma60"))

    if not all(v is not None for v in [close, ma5, ma10, ma20]):
        return 0.0

    # 短期均线排列
    if ma5 > ma10 > ma20:
        score += 1.0
    elif ma5 < ma10 < ma20:
        score -= 1.0

    # 中期均线排列
    if ma60 is not None:
        if ma10 > ma20 > ma60:
            score += 0.5
        elif ma10 < ma20 < ma60:
            score -= 0.5

    # 价格与 MA20 关系（关键分水岭）
    if close > ma20:
        score += 0.5
    else:
        score -= 0.5

    return _clamp(score, -2, 2)


def _score_momentum(df, curr, prev) -> float:
    """动量评分：MACD 方向 + 交叉信号 + 持续性 + 背离检测。"""
    score = 0.0
    dif = _f(curr.get("dif"))
    dea = _f(curr.get("dea"))
    hist = _f(curr.get("macd_hist"))
    prev_dif = _f(prev.get("dif"))
    prev_dea = _f(prev.get("dea"))
    prev_hist = _f(prev.get("macd_hist"))

    if dif is None or dea is None:
        return 0.0

    # DIF vs DEA
    if dif > dea:
        score += 0.5
    else:
        score -= 0.5

    # 今日交叉
    if prev_dif is not None and prev_dea is not None:
        if prev_dif <= prev_dea and dif > dea:
            score += 1.0  # 金叉
        elif prev_dif >= prev_dea and dif < dea:
            score -= 1.0  # 死叉

    # 柱状图方向
    if hist is not None and prev_hist is not None:
        if hist > prev_hist:
            score += 0.5  # 柱转强（放大正或收缩负）
        elif hist < prev_hist:
            score -= 0.5

    # 动量持续性：DIF 在 DEA 之上/下连续天数（最多看10天）
    if len(df) >= 5 and "dif" in df.columns and "dea" in df.columns:
        recent = df.tail(10)
        streak = 0
        above = dif > dea
        for i in range(len(recent) - 1, -1, -1):
            r_dif = _f(recent.iloc[i].get("dif"))
            r_dea = _f(recent.iloc[i].get("dea"))
            if r_dif is None or r_dea is None:
                break
            if above and r_dif > r_dea:
                streak += 1
            elif not above and r_dif < r_dea:
                streak += 1
            else:
                break
        # 持续5天以上给额外 ±0.3
        if streak >= 5:
            score += 0.3 if above else -0.3

    # MACD 背离检测（近20日内的价格低点/高点 vs DIF 低点/高点）
    score += _detect_divergence(df)

    return _clamp(score, -2, 2)


def _detect_divergence(df) -> float:
    """检测 MACD 背离：价格创新低但 DIF 底部抬高（底背离）= +1，反之顶背离 = -1。

    简化版：比较近20日内的两个局部低点/高点。
    """
    if len(df) < 20 or "dif" not in df.columns:
        return 0.0

    recent = df.tail(20)
    closes = pd.to_numeric(recent["close"], errors="coerce")
    difs = pd.to_numeric(recent["dif"], errors="coerce")

    if closes.isna().any() or difs.isna().any():
        return 0.0

    # 找局部低点（比前后都低的点）
    lows = []
    highs = []
    for i in range(1, len(closes) - 1):
        if closes.iloc[i] <= closes.iloc[i - 1] and closes.iloc[i] <= closes.iloc[i + 1]:
            lows.append(i)
        if closes.iloc[i] >= closes.iloc[i - 1] and closes.iloc[i] >= closes.iloc[i + 1]:
            highs.append(i)

    # 底背离：最近两个低点，价格新低但 DIF 底部抬高
    if len(lows) >= 2:
        i1, i2 = lows[-2], lows[-1]
        if closes.iloc[i2] < closes.iloc[i1] and difs.iloc[i2] > difs.iloc[i1]:
            return 1.0  # 底背离，看多

    # 顶背离：最近两个高点，价格新高但 DIF 顶部降低
    if len(highs) >= 2:
        i1, i2 = highs[-2], highs[-1]
        if closes.iloc[i2] > closes.iloc[i1] and difs.iloc[i2] < difs.iloc[i1]:
            return -1.0  # 顶背离，看空

    return 0.0


def _score_overbought(curr) -> float:
    """超买超卖评分（逆向）：超卖=正分（买入机会），超买=负分（卖出信号）。"""
    score = 0.0
    j = _f(curr.get("j"))
    rsi6 = _f(curr.get("rsi6"))
    close = _f(curr.get("close"))
    boll_upper = _f(curr.get("boll_upper"))
    boll_lower = _f(curr.get("boll_lower"))

    # KDJ J 值
    if j is not None:
        if j < 0:
            score += 1.5
        elif j < 20:
            score += 1.0
        elif j > 100:
            score -= 1.5
        elif j > 80:
            score -= 1.0

    # RSI6
    if rsi6 is not None:
        if rsi6 < 20:
            score += 0.5
        elif rsi6 < 30:
            score += 0.3
        elif rsi6 > 80:
            score -= 0.5
        elif rsi6 > 70:
            score -= 0.3

    # 布林带：价格触及上下轨
    if close is not None:
        if boll_lower is not None and close < boll_lower:
            score += 0.5  # 跌破下轨，超卖
        if boll_upper is not None and close > boll_upper:
            score -= 0.5  # 突破上轨，超买

    return _clamp(score, -2, 2)


def _score_volume(df, curr) -> float:
    """量能评分：量价配合。"""
    score = 0.0

    # 使用 volume 列
    if "volume" not in df.columns:
        return 0.0

    vol = pd.to_numeric(df["volume"], errors="coerce")
    if vol.isna().all() or len(vol) < 20:
        return 0.0

    avg_20 = vol.tail(20).mean()
    avg_5 = vol.tail(5).mean()
    if avg_20 == 0:
        return 0.0

    vol_ratio = avg_5 / avg_20

    # 用近3日累计涨跌判方向（替代单日pct_change，降低噪声）
    pct_col = pd.to_numeric(df["pct_change"], errors="coerce") if "pct_change" in df.columns else None
    if pct_col is not None and len(pct_col) >= 3:
        pct = float(pct_col.tail(3).sum())
    else:
        pct = _f(curr.get("pct_change")) or 0.0

    # 量价配合
    if pct > 0:
        if vol_ratio > 1.5:
            score = 2.0   # 放量上涨
        elif vol_ratio > 1.1:
            score = 1.0
        elif vol_ratio < 0.7:
            score = -0.5  # 缩量上涨（可疑）
    elif pct < 0:
        if vol_ratio > 1.5:
            score = -2.0  # 放量下跌
        elif vol_ratio > 1.1:
            score = -1.0
        elif vol_ratio < 0.7:
            score = 0.5   # 缩量下跌（抛压减弱）
    else:
        # 放量平盘：量能消耗但无方向，多空分歧信号
        if vol_ratio > 1.5:
            score = -0.5
        elif vol_ratio > 1.1:
            score = -0.3

    return _clamp(score, -2, 2)


def _score_wave(swings: list[dict], current_price: float) -> float:
    """波浪位置评分：基于转折点判断当前所处波浪阶段。"""
    if len(swings) < 3:
        return 0.0

    score = 0.0
    last = swings[-1]

    # 最近转折点类型 + 当前价格位置
    if last["type"] == "low":
        # 最后确认的是低点 → 价格在低点之上 = 可能开始上涨
        bounce_pct = (current_price - last["price"]) / last["price"] * 100
        if bounce_pct > 3:
            score += 0.5  # 已经反弹
        elif bounce_pct < -2:
            score -= 0.5  # 创新低，还在下跌
    else:
        # 最后确认的是高点 → 价格在高点之下 = 可能开始下跌
        drop_pct = (last["price"] - current_price) / last["price"] * 100
        if drop_pct > 3:
            score -= 0.5
        elif drop_pct < -2:
            score += 0.5  # 再创新高

    # 低点趋势：低点抬高=多头，低点降低=空头
    lows = [s for s in swings if s["type"] == "low"]
    if len(lows) >= 2:
        if lows[-1]["price"] > lows[-2]["price"]:
            score += 0.7  # 底部抬高
        else:
            score -= 0.7  # 底部降低

    # 高点趋势：高点抬高=多头，高点降低=空头
    highs = [s for s in swings if s["type"] == "high"]
    if len(highs) >= 2:
        if highs[-1]["price"] > highs[-2]["price"]:
            score += 0.5
        else:
            score -= 0.5

    return _clamp(score, -2, 2)


def _classify_signal(total: float) -> str:
    """根据加权总分判定信号。"""
    if total >= SIGNAL_THRESHOLDS["BUY"]:
        return "BUY"
    elif total >= SIGNAL_THRESHOLDS["BULLISH"]:
        return "BULLISH"
    elif total <= SIGNAL_THRESHOLDS["SELL"]:
        return "SELL"
    elif total <= SIGNAL_THRESHOLDS["BEARISH"]:
        return "BEARISH"
    else:
        return "NEUTRAL"


def _f(val) -> float | None:
    """安全转 float。"""
    if val is None:
        return None
    try:
        v = float(val)
        if pd.isna(v):
            return None
        return v
    except (ValueError, TypeError):
        return None


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _empty_scores() -> dict:
    return {
        "trend": 0, "momentum": 0, "overbought": 0,
        "volume": 0, "wave": 0,
        "total": 0, "signal": "NEUTRAL",
    }
