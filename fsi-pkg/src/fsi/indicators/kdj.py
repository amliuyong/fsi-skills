"""KDJ 指标（匹配通达信算法，初始值 K=D=50）"""

import pandas as pd
import numpy as np


def add_kdj(df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    lowest_low = low.rolling(window=n, min_periods=1).min()
    highest_high = high.rolling(window=n, min_periods=1).max()

    rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
    rsv = rsv.fillna(50)

    # 通达信算法：K = 2/3 * 前K + 1/3 * RSV，初始 K=D=50
    k_values = np.zeros(len(df))
    d_values = np.zeros(len(df))
    k_values[0] = 50.0
    d_values[0] = 50.0

    rsv_arr = rsv.values
    for i in range(1, len(df)):
        k_values[i] = (m1 - 1) / m1 * k_values[i - 1] + 1 / m1 * rsv_arr[i]
        d_values[i] = (m2 - 1) / m2 * d_values[i - 1] + 1 / m2 * k_values[i]

    df["k"] = k_values
    df["d"] = d_values
    df["j"] = 3 * df["k"] - 2 * df["d"]

    return df
