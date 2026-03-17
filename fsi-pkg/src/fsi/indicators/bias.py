"""乖离率 BIAS(6, 12, 24)"""

import pandas as pd

BIAS_PERIODS = [6, 12, 24]


def add_bias(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)

    for p in BIAS_PERIODS:
        ma = close.rolling(window=p, min_periods=p).mean()
        df[f"bias{p}"] = (close - ma) / ma * 100

    return df
