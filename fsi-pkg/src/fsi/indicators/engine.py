"""指标计算引擎 - 编排所有技术指标"""

import pandas as pd

from fsi.indicators.moving_average import add_moving_averages
from fsi.indicators.macd import add_macd
from fsi.indicators.kdj import add_kdj
from fsi.indicators.rsi import add_rsi
from fsi.indicators.bollinger import add_bollinger
from fsi.indicators.volume import add_volume_indicators
from fsi.indicators.bias import add_bias


class IndicatorEngine:
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or len(df) < 2:
            return df
        df = df.sort_values("trade_date").reset_index(drop=True)
        df = add_moving_averages(df)
        df = add_macd(df)
        df = add_kdj(df)
        df = add_rsi(df)
        df = add_bollinger(df)
        df = add_volume_indicators(df)
        df = add_bias(df)
        return df

    def get_latest_indicators(self, df: pd.DataFrame) -> dict:
        """提取最新一行的指标值"""
        if df.empty:
            return {}
        row = df.iloc[-1]
        result = {}

        # MA
        for p in [5, 10, 20, 60, 120, 250]:
            key = f"ma{p}"
            if key in row and pd.notna(row[key]):
                result[key] = round(float(row[key]), 4)

        # MACD
        if "dif" in row:
            result["macd"] = {
                "dif": _r(row.get("dif")),
                "dea": _r(row.get("dea")),
                "histogram": _r(row.get("macd_hist")),
            }

        # KDJ
        if "k" in row:
            result["kdj"] = {
                "k": _r(row.get("k")),
                "d": _r(row.get("d")),
                "j": _r(row.get("j")),
            }

        # RSI
        rsi = {}
        for p in [6, 12, 24]:
            key = f"rsi{p}"
            if key in row and pd.notna(row[key]):
                rsi[key] = _r(row[key])
        if rsi:
            result["rsi"] = rsi

        # Bollinger
        if "boll_mid" in row:
            result["bollinger"] = {
                "upper": _r(row.get("boll_upper")),
                "mid": _r(row.get("boll_mid")),
                "lower": _r(row.get("boll_lower")),
            }

        # BIAS
        bias = {}
        for p in [6, 12, 24]:
            key = f"bias{p}"
            if key in row and pd.notna(row[key]):
                bias[key] = _r(row[key])
        if bias:
            result["bias"] = bias

        return result

    def detect_signals(self, df: pd.DataFrame) -> list[str]:
        """检测技术信号"""
        if len(df) < 2:
            return []

        signals = []
        curr = df.iloc[-1]
        prev = df.iloc[-2]

        # MA 金叉/死叉
        for short, long in [(5, 10), (5, 20), (10, 20)]:
            sk, lk = f"ma{short}", f"ma{long}"
            if sk in curr and lk in curr and pd.notna(curr[sk]) and pd.notna(curr[lk]):
                if pd.notna(prev[sk]) and pd.notna(prev[lk]):
                    if prev[sk] <= prev[lk] and curr[sk] > curr[lk]:
                        signals.append(f"MA{short} 上穿 MA{long}（短期金叉）")
                    elif prev[sk] >= prev[lk] and curr[sk] < curr[lk]:
                        signals.append(f"MA{short} 下穿 MA{long}（短期死叉）")

        # MACD 金叉/死叉
        if "dif" in curr and "dea" in curr:
            if pd.notna(curr["dif"]) and pd.notna(prev["dif"]):
                if prev["dif"] <= prev["dea"] and curr["dif"] > curr["dea"]:
                    signals.append("MACD 金叉（DIF 上穿 DEA）")
                elif prev["dif"] >= prev["dea"] and curr["dif"] < curr["dea"]:
                    signals.append("MACD 死叉（DIF 下穿 DEA）")

        # MACD 柱状图转正/转负
        if "macd_hist" in curr and pd.notna(curr["macd_hist"]) and pd.notna(prev.get("macd_hist")):
            if prev["macd_hist"] <= 0 and curr["macd_hist"] > 0:
                signals.append("MACD 柱状图由负转正")
            elif prev["macd_hist"] >= 0 and curr["macd_hist"] < 0:
                signals.append("MACD 柱状图由正转负")

        # KDJ
        if "k" in curr and "d" in curr:
            if pd.notna(curr["k"]) and pd.notna(prev.get("k")):
                if prev["k"] <= prev["d"] and curr["k"] > curr["d"]:
                    signals.append("KDJ 金叉（K 上穿 D）")
                elif prev["k"] >= prev["d"] and curr["k"] < curr["d"]:
                    signals.append("KDJ 死叉（K 下穿 D）")
            if pd.notna(curr.get("j")):
                if curr["j"] > 100:
                    signals.append("KDJ J值 > 100（超买区间）")
                elif curr["j"] < 0:
                    signals.append("KDJ J值 < 0（超卖区间）")
                elif curr["j"] > 80:
                    signals.append("KDJ J值 > 80（短期偏强）")
                elif curr["j"] < 20:
                    signals.append("KDJ J值 < 20（短期偏弱）")

        # RSI
        if "rsi6" in curr and pd.notna(curr["rsi6"]):
            rsi6 = float(curr["rsi6"])
            if rsi6 > 80:
                signals.append(f"RSI6 = {rsi6:.1f}（超买）")
            elif rsi6 < 20:
                signals.append(f"RSI6 = {rsi6:.1f}（超卖）")

        # 布林带突破
        if "boll_upper" in curr and pd.notna(curr["boll_upper"]):
            close = float(curr["close"])
            if close > float(curr["boll_upper"]):
                signals.append("收盘价突破布林带上轨")
            elif close < float(curr["boll_lower"]):
                signals.append("收盘价跌破布林带下轨")

        # 均线突破
        for p in [20, 60]:
            mk = f"ma{p}"
            if mk in curr and pd.notna(curr[mk]):
                close_c = float(curr["close"])
                close_p = float(prev["close"])
                ma_c = float(curr[mk])
                ma_p = float(prev[mk]) if pd.notna(prev[mk]) else ma_c
                if close_p <= ma_p and close_c > ma_c:
                    signals.append(f"股价突破 MA{p} 均线")
                elif close_p >= ma_p and close_c < ma_c:
                    signals.append(f"股价跌破 MA{p} 均线")

        return signals


def _r(val, ndigits: int = 4) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return round(float(val), ndigits)
