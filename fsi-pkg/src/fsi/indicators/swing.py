"""Zigzag 转折点检测 — 用于波浪分析"""

import pandas as pd


def detect_swings(df: pd.DataFrame, pct_threshold: float = 5.0) -> list[dict]:
    """基于百分比阈值的经典 zigzag 算法检测转折点（swing high / swing low）。

    采用状态机方式：交替寻找高点和低点，确保结果严格高低交替。
    当从已确认的低点反弹 >= threshold% 时确认该低点；
    当从已确认的高点回落 >= threshold% 时确认该高点。

    参数:
        df: 含 trade_date, high, low, close 的 DataFrame（已按日期排序）
        pct_threshold: 最小反转幅度百分比（默认 5%），过滤噪声

    返回:
        转折点列表，每个元素: {seq, date, price, type: "high"|"low", index}
    """
    if len(df) < 3:
        return []

    highs = pd.to_numeric(df["high"], errors="coerce").values
    lows = pd.to_numeric(df["low"], errors="coerce").values
    dates = df["trade_date"].values
    threshold = pct_threshold / 100.0

    points = []  # 确认的转折点: (index, price, type)

    # 初始化：用前几根 K 线确定初始方向
    first_high_idx = 0
    first_low_idx = 0
    for i in range(len(df)):
        if highs[i] > highs[first_high_idx]:
            first_high_idx = i
        if lows[i] < lows[first_low_idx]:
            first_low_idx = i
        # 一旦出现足够幅度的波动就停止初始化
        if i > 0:
            up_move = (highs[first_high_idx] - lows[first_low_idx]) / lows[first_low_idx]
            if up_move >= threshold:
                break

    # 确定初始搜索方向
    if first_low_idx < first_high_idx:
        # 先有低点后有高点 → 先确认低点，开始找高点
        looking_for = "high"
        candidate_low = (first_low_idx, float(lows[first_low_idx]))
        candidate_high = (first_high_idx, float(highs[first_high_idx]))
        points.append(candidate_low + ("low",))
        start_i = first_high_idx + 1
    else:
        # 先有高点后有低点 → 先确认高点，开始找低点
        looking_for = "low"
        candidate_high = (first_high_idx, float(highs[first_high_idx]))
        candidate_low = (first_low_idx, float(lows[first_low_idx]))
        points.append(candidate_high + ("high",))
        start_i = first_low_idx + 1

    # 候选点：当前正在追踪的尚未确认的极值
    cand_high_idx, cand_high_price = candidate_high
    cand_low_idx, cand_low_price = candidate_low

    for i in range(start_i, len(df)):
        h = float(highs[i])
        lo = float(lows[i])

        if looking_for == "high":
            # 追踪更高的高点
            if h > cand_high_price:
                cand_high_idx = i
                cand_high_price = h
            # 检查是否从候选高点回落足够幅度 → 确认高点
            if cand_high_price > 0 and (cand_high_price - lo) / cand_high_price >= threshold:
                points.append((cand_high_idx, cand_high_price, "high"))
                # 切换到寻找低点
                looking_for = "low"
                cand_low_idx = i
                cand_low_price = lo

        else:  # looking_for == "low"
            # 追踪更低的低点
            if lo < cand_low_price:
                cand_low_idx = i
                cand_low_price = lo
            # 检查是否从候选低点反弹足够幅度 → 确认低点
            if cand_low_price > 0 and (h - cand_low_price) / cand_low_price >= threshold:
                points.append((cand_low_idx, cand_low_price, "low"))
                # 切换到寻找高点
                looking_for = "high"
                cand_high_idx = i
                cand_high_price = h

    # 转为输出格式
    result = []
    for i, (idx, price, tp) in enumerate(points):
        result.append({
            "seq": i + 1,
            "date": _date_str(dates[idx]),
            "price": round(price, 4),
            "type": tp,
            "index": idx,
        })

    return result


def summarize_swings(swings: list[dict]) -> list[dict]:
    """生成相邻转折点之间的波段摘要（幅度、天数）。"""
    if len(swings) < 2:
        return []

    segments = []
    for i in range(1, len(swings)):
        prev, curr = swings[i - 1], swings[i]
        pct = (curr["price"] - prev["price"]) / prev["price"] * 100
        segments.append({
            "from_seq": prev["seq"],
            "to_seq": curr["seq"],
            "from_date": prev["date"],
            "to_date": curr["date"],
            "from_price": prev["price"],
            "to_price": curr["price"],
            "change_pct": round(pct, 2),
            "direction": "up" if pct > 0 else "down",
        })
    return segments


def _date_str(val) -> str:
    if hasattr(val, "isoformat"):
        return val.isoformat()[:10]
    return str(val)[:10]
