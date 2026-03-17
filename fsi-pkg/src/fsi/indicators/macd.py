"""MACD 指标（中国惯例：柱状图 = 2*(DIF-DEA)）"""

import pandas as pd


def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    close = df["close"].astype(float)

    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()

    df["dif"] = ema_fast - ema_slow
    df["dea"] = df["dif"].ewm(span=signal, adjust=False).mean()
    # 中国惯例：MACD 柱状图 = 2 * (DIF - DEA)
    df["macd_hist"] = 2 * (df["dif"] - df["dea"])

    return df
