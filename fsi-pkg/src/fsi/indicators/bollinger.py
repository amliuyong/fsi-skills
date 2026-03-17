"""布林带指标 BOLL(20, 2)"""

import pandas as pd


def add_bollinger(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
    close = df["close"].astype(float)

    df["boll_mid"] = close.rolling(window=period, min_periods=period).mean()
    rolling_std = close.rolling(window=period, min_periods=period).std()

    df["boll_upper"] = df["boll_mid"] + std_dev * rolling_std
    df["boll_lower"] = df["boll_mid"] - std_dev * rolling_std

    return df
