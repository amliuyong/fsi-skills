"""场内基金(ETF)数据采集 - 使用新浪财经数据源"""

from datetime import timedelta

from fsi.config import today_bj

import akshare as ak
import pandas as pd

from fsi.fetcher.base import BaseFetcher


def _etf_symbol(code: str) -> str:
    """将纯数字代码转为新浪格式 (sh510300 / sz159919)"""
    if code.startswith("1"):
        return f"sz{code}"
    return f"sh{code}"


class ETFFetcher(BaseFetcher):

    def get_etf_list(self) -> list[dict]:
        self.log("获取 ETF 列表...")
        df = self.fetch_with_retry(ak.fund_etf_category_sina, symbol="ETF基金")
        df = df.rename(columns={"代码": "raw_code", "名称": "name"})
        if "raw_code" not in df.columns or "name" not in df.columns:
            return []
        # 新浪返回的代码带交易所前缀(sz159998)，去掉前缀
        df["code"] = df["raw_code"].str.replace(r"^[a-z]{2}", "", regex=True)
        df = df.sort_values("code").reset_index(drop=True)
        return df[["code", "name"]].to_dict("records")

    def fetch_daily(self, code: str, name: str = "", start_date: str | None = None,
                    end_date: str | None = None, full: bool = False) -> int:
        if not full and start_date is None:
            last = self.get_last_sync_date("etf_daily", code)
            if last:
                row_count = self.conn.execute(
                    "SELECT count(*) FROM etf_daily WHERE code = ?", [code]
                ).fetchone()[0]
                if row_count > 0:
                    start_date = (last + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    self.log(f"ETF {code} sync_log 存在但数据为空，执行全量拉取")

        if start_date is None:
            start_date = "19900101"
        if end_date is None:
            end_date = today_bj().strftime("%Y%m%d")

        start_date = start_date.replace("-", "")
        end_date = end_date.replace("-", "")

        symbol = _etf_symbol(code)
        self.log(f"获取 ETF {code}({name}) 日线 ({start_date} ~ {end_date})...")

        try:
            df = self.fetch_with_retry(
                ak.fund_etf_hist_sina,
                symbol=symbol,
            )
        except Exception as e:
            print(f"获取 ETF {code} 失败: {e}")
            return 0

        if df is None or df.empty:
            print(f"ETF {code} 无新数据")
            return 0

        # 新浪源返回全量数据，需按日期过滤
        df["date"] = pd.to_datetime(df["date"])
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]

        if df.empty:
            self.log(f"ETF {code} 在 {start_date}~{end_date} 无数据")
            return 0

        df = df.rename(columns={"date": "trade_date"})
        df["trade_date"] = df["trade_date"].dt.date
        df["code"] = code
        df["name"] = name

        # 计算涨跌幅
        df = df.sort_values("trade_date").reset_index(drop=True)
        prev_close = df["close"].shift(1)
        df["pct_change"] = ((df["close"] - prev_close) / prev_close * 100).round(4)

        # 新浪 ETF 源没有 turnover 列
        if "turnover" not in df.columns:
            df["turnover"] = None

        schema_cols = ["code", "name", "trade_date", "open", "close", "high", "low",
                       "volume", "amount", "pct_change", "turnover"]
        for col in schema_cols:
            if col not in df.columns:
                df[col] = None
        df = df[schema_cols]

        rows = self.save_to_db(df, "etf_daily", code)
        if rows > 0:
            last_date = df["trade_date"].max()
            self.update_sync_log("etf_daily", code, last_date, rows)

        self.log(f"ETF {code} 写入 {rows} 条记录")
        return rows

    def fetch_all(self, codes: list[dict] | None = None, full: bool = False,
                  batch_size: int | None = None, skip: int = 0) -> int:
        if codes is None:
            codes = self.get_etf_list()

        if not codes:
            print("ETF 列表为空")
            return 0

        # 跳过前 skip 只
        if skip > 0:
            codes = codes[skip:]
        # 只取 batch_size 只
        if batch_size is not None:
            codes = codes[:batch_size]

        if not codes:
            print("经过 skip/batch-size 过滤后无 ETF 需要处理")
            return 0

        total_codes = len(codes)
        total_rows = 0
        skipped = 0

        for i, item in enumerate(codes):
            code = item["code"] if isinstance(item, dict) else item
            name = item.get("name", "") if isinstance(item, dict) else ""

            # 增量同步：如果最近 1 天内已同步过，跳过
            if not full:
                last = self.get_last_sync_date("etf_daily", code)
                if last and (today_bj() - last).days <= 1:
                    skipped += 1
                    continue

            try:
                total_rows += self.fetch_daily(code, name=name, full=full)
            except Exception as e:
                print(f"ETF {code} 失败: {e}")
                continue

            # 每 50 只打印一次进度
            done = i + 1
            if done % 50 == 0 or done == total_codes:
                print(f"进度: {done}/{total_codes}，已写入 {total_rows} 条，跳过 {skipped} 只（近期已同步）")

        print(f"ETF 同步完成: 共 {total_codes} 只，写入 {total_rows} 条，跳过 {skipped} 只")
        return total_rows
