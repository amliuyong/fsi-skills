"""大盘指数数据采集 - 使用新浪财经数据源"""

from datetime import timedelta

from fsi.config import today_bj

import akshare as ak
import pandas as pd
from tqdm import tqdm

from fsi.config import MAJOR_INDICES
from fsi.fetcher.base import BaseFetcher


def _index_symbol(code: str) -> str:
    """将纯数字代码转为新浪格式 (sh000001 / sz399001)"""
    if code.startswith("399"):
        return f"sz{code}"
    return f"sh{code}"


class IndexFetcher(BaseFetcher):

    def fetch_daily(self, code: str, start_date: str | None = None,
                    end_date: str | None = None, full: bool = False) -> int:
        name = MAJOR_INDICES.get(code, "")

        if not full and start_date is None:
            last = self.get_last_sync_date("index_daily", code)
            if last:
                row_count = self.conn.execute(
                    "SELECT count(*) FROM index_daily WHERE code = ?", [code]
                ).fetchone()[0]
                if row_count > 0:
                    start_date = (last + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    self.log(f"指数 {code} sync_log 存在但数据为空，执行全量拉取")

        if start_date is None:
            start_date = "19900101"
        if end_date is None:
            end_date = today_bj().strftime("%Y%m%d")

        start_date = start_date.replace("-", "")
        end_date = end_date.replace("-", "")

        symbol = _index_symbol(code)
        self.log(f"获取指数 {code}({name}) 日线 ({start_date} ~ {end_date})...")

        try:
            df = self.fetch_with_retry(
                ak.stock_zh_index_daily,
                symbol=symbol,
            )
        except Exception as e:
            print(f"获取指数 {code} 失败: {e}")
            return 0

        if df is None or df.empty:
            print(f"指数 {code} 无新数据")
            return 0

        # 新浪源返回全量数据，需按日期过滤
        df["date"] = pd.to_datetime(df["date"])
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]

        if df.empty:
            print(f"指数 {code} 在 {start_date}~{end_date} 无数据")
            return 0

        df = df.rename(columns={"date": "trade_date"})
        df["trade_date"] = df["trade_date"].dt.date
        df["code"] = code
        df["name"] = name

        # 计算涨跌幅
        df = df.sort_values("trade_date").reset_index(drop=True)
        prev_close = df["close"].shift(1)
        df["pct_change"] = ((df["close"] - prev_close) / prev_close * 100).round(4)

        # 新浪指数源没有 amount 列
        if "amount" not in df.columns:
            df["amount"] = None

        schema_cols = ["code", "name", "trade_date", "open", "close", "high", "low",
                       "volume", "amount", "pct_change"]
        for col in schema_cols:
            if col not in df.columns:
                df[col] = None
        df = df[schema_cols]

        rows = self.save_to_db(df, "index_daily", code)
        if rows > 0:
            last_date = df["trade_date"].max()
            self.update_sync_log("index_daily", code, last_date, rows)

        self.log(f"指数 {code} 写入 {rows} 条记录")
        return rows

    def fetch_all(self, full: bool = False) -> int:
        total = 0
        for code in tqdm(MAJOR_INDICES, desc="获取大盘指数"):
            try:
                total += self.fetch_daily(code, full=full)
            except Exception as e:
                self.log(f"指数 {code} 失败: {e}")
                continue
        return total
