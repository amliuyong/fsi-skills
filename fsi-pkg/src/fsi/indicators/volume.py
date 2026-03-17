"""成交量指标：OBV、成交量均线"""

import pandas as pd
import numpy as np


def add_volume_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)

    # OBV
    direction = np.sign(close.diff()).fillna(0)
    df["obv"] = (direction * volume).cumsum()

    # 成交量均线
    df["vol_ma5"] = volume.rolling(window=5, min_periods=1).mean()
    df["vol_ma10"] = volume.rolling(window=10, min_periods=1).mean()

    return df
