"""RSI 指标 (6, 12, 24)"""

import pandas as pd

RSI_PERIODS = [6, 12, 24]


def add_rsi(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    delta = close.diff()

    for p in RSI_PERIODS:
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)

        avg_gain = gain.ewm(alpha=1 / p, min_periods=p, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / p, min_periods=p, adjust=False).mean()

        rs = avg_gain / avg_loss
        df[f"rsi{p}"] = 100 - (100 / (1 + rs))

    return df
