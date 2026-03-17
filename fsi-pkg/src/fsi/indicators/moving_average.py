"""均线指标 MA5/10/20/60/120/250"""

import pandas as pd

MA_PERIODS = [5, 10, 20, 60, 120, 250]


def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    for p in MA_PERIODS:
        df[f"ma{p}"] = df["close"].rolling(window=p, min_periods=p).mean()
    return df
