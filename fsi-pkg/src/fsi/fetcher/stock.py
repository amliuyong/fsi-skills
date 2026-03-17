"""个股数据采集 - 使用新浪财经数据源"""

from datetime import timedelta

from fsi.config import today_bj

import akshare as ak
import pandas as pd
from tqdm import tqdm

from fsi.fetcher.base import BaseFetcher


def _stock_symbol(code: str) -> str:
    """将纯数字代码转为新浪格式 (sz000001 / sh600000 / bj430047)"""
    if code.startswith(("6",)):
        return f"sh{code}"
    elif code.startswith(("0", "3")):
        return f"sz{code}"
    elif code.startswith(("4", "8")):
        return f"bj{code}"
    return f"sz{code}"


class StockFetcher(BaseFetcher):

    def fetch_stock_list(self) -> int:
        self.log("正在获取 A 股股票列表...")
        df = self.fetch_with_retry(ak.stock_info_a_code_name)

        # stock_info_a_code_name 返回 code, name 两列
        keep_cols = ["code", "name"]
        df = df[[c for c in keep_cols if c in df.columns]]

        def get_exchange(code: str) -> str:
            if code.startswith(("6",)):
                return "SH"
            elif code.startswith(("0", "3")):
                return "SZ"
            elif code.startswith(("4", "8")):
                return "BJ"
            return "UNKNOWN"

        df["exchange"] = df["code"].apply(get_exchange)
        df["is_st"] = df["name"].str.contains("ST", case=False, na=False)
        df["industry"] = None
        df["list_date"] = None
        df["updated_at"] = pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None)

        df = df[["code", "name", "exchange", "industry", "list_date", "is_st", "updated_at"]]

        self.conn.execute("DELETE FROM stock_list")
        self.conn.execute("INSERT INTO stock_list SELECT * FROM df")
        self.log(f"已更新 {len(df)} 只股票信息")
        return len(df)

    def _resolve_date_range(self, code: str, start_date: str | None,
                            end_date: str | None, full: bool) -> tuple[str, str]:
        """解析增量日期范围，返回 (start_date, end_date) 格式 YYYYMMDD"""
        if not full and start_date is None:
            last = self.get_last_sync_date("stock_daily", code)
            if last:
                row_count = self.conn.execute(
                    "SELECT count(*) FROM stock_daily WHERE code = ?", [code]
                ).fetchone()[0]
                if row_count > 0:
                    start_date = (last + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    self.log(f"{code} sync_log 存在但数据为空，执行全量拉取")

        if start_date is None:
            start_date = "19700101"
        if end_date is None:
            end_date = today_bj().strftime("%Y%m%d")

        return start_date.replace("-", ""), end_date.replace("-", "")

    def _fetch_daily_akshare(self, code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        """通过 AKShare（新浪源）获取单股日线，返回 FSI schema DataFrame"""
        symbol = _stock_symbol(code)
        self.log(f"获取 {code}({symbol}) 日线数据 [AKShare] ({start_date} ~ {end_date})...")

        df = self.fetch_with_retry(
            ak.stock_zh_a_daily,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )

        if df is None or df.empty:
            return None

        self.log(f"{code} AKShare 获取到 {len(df)} 行")

        # 新浪源返回列: date, open, high, low, close, volume, amount, outstanding_share, turnover
        df = df.rename(columns={"date": "trade_date"})
        df["code"] = code
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

        df = df.sort_values("trade_date").reset_index(drop=True)
        prev_close = df["close"].shift(1)
        df["pct_change"] = ((df["close"] - prev_close) / prev_close * 100).round(4)
        df["change_amt"] = (df["close"] - prev_close).round(4)
        df["amplitude"] = ((df["high"] - df["low"]) / prev_close * 100).round(4)

        # turnover: 新浪返回的是比率（如 0.006303），转换为百分比（0.63）
        if "turnover" in df.columns:
            df["turnover"] = (df["turnover"] * 100).round(4)
        else:
            df["turnover"] = None

        return df

    def _save_daily_df(self, code: str, df: pd.DataFrame) -> int:
        """将日线 DataFrame 写入 DB，返回写入行数"""
        schema_cols = ["code", "trade_date", "open", "close", "high", "low",
                       "volume", "amount", "amplitude", "pct_change", "change_amt", "turnover"]

        df["code"] = code
        for col in schema_cols:
            if col not in df.columns:
                df[col] = None
        df = df[schema_cols]

        try:
            rows = self.save_to_db(df, "stock_daily", code)
        except Exception as e:
            print(f"{code} 写入数据库失败: {e}")
            return 0

        if rows > 0:
            last_date = df["trade_date"].max()
            self.update_sync_log("stock_daily", code, last_date, rows)

        self.log(f"{code} 写入 {rows} 条记录")
        return rows

    def fetch_daily(self, code: str, start_date: str | None = None,
                    end_date: str | None = None, full: bool = False) -> int:
        start_date, end_date = self._resolve_date_range(code, start_date, end_date, full)

        from fsi.health import get_source
        source = get_source("stock_daily")

        if source == "yahoo":
            # 健康检查配置指定优先 Yahoo
            return self._fetch_daily_with_fallback(
                code, start_date, end_date,
                primary="yahoo", fallback="akshare"
            )
        else:
            # 默认优先 AKShare（有完整的 amount/turnover）
            return self._fetch_daily_with_fallback(
                code, start_date, end_date,
                primary="akshare", fallback="yahoo"
            )

    def _fetch_daily_with_fallback(self, code: str, start_date: str, end_date: str,
                                   primary: str, fallback: str) -> int:
        """按指定顺序尝试获取日线数据"""
        fetchers = {
            "akshare": self._try_akshare_daily,
            "yahoo": self._try_yahoo_daily,
        }
        for source_name in [primary, fallback]:
            fn = fetchers.get(source_name)
            if fn is None:
                continue
            result = fn(code, start_date, end_date)
            if result is not None:
                return result
        print(f"获取 {code} 失败（所有数据源均不可用）")
        return 0

    def _try_akshare_daily(self, code: str, start_date: str, end_date: str) -> int | None:
        """尝试 AKShare 获取，成功返回行数，失败返回 None"""
        try:
            df = self._fetch_daily_akshare(code, start_date, end_date)
            if df is not None and not df.empty:
                return self._save_daily_df(code, df)
            if df is not None:
                print(f"{code} 无新数据（{start_date} ~ {end_date}）")
                return 0
        except Exception as e:
            self.log(f"{code} AKShare 失败: {e}")
        return None

    def _try_yahoo_daily(self, code: str, start_date: str, end_date: str) -> int | None:
        """尝试 Yahoo 获取，成功返回行数，失败返回 None"""
        try:
            from fsi.fetcher.yahoo import fetch_stock_daily
            df = fetch_stock_daily(code, start_date, end_date)
            if df is not None and not df.empty:
                self.log(f"{code} Yahoo 获取到 {len(df)} 行")
                return self._save_daily_df(code, df)
        except Exception as e:
            self.log(f"{code} Yahoo 失败: {e}")
        return None

    def fetch_all_daily(self, codes: list[str] | None = None, full: bool = False) -> int:
        if codes is None:
            result = self.conn.execute("SELECT code FROM stock_list ORDER BY code").fetchall()
            codes = [r[0] for r in result]

        if not codes:
            print("股票列表为空，请先执行 fsi fetch list")
            return 0

        # 分离北交所（Yahoo 不支持）和其他股票
        yahoo_codes = [c for c in codes if not c.startswith(("4", "8"))]
        bj_codes = [c for c in codes if c.startswith(("4", "8"))]

        total = 0

        # Yahoo 批量下载（快速）
        if yahoo_codes:
            try:
                total += self._fetch_batch_yahoo(yahoo_codes, full)
            except Exception as e:
                self.log(f"Yahoo 批量下载失败: {e}，回退逐个 AKShare 获取")
                for code in tqdm(yahoo_codes, desc="获取个股日线 [AKShare]"):
                    try:
                        total += self.fetch_daily(code, full=full)
                    except Exception as ex:
                        print(f"{code} 失败: {ex}")
                        continue

        # 北交所逐个通过 AKShare 获取
        if bj_codes:
            for code in tqdm(bj_codes, desc="获取北交所日线 [AKShare]"):
                try:
                    total += self.fetch_daily(code, full=full)
                except Exception as e:
                    print(f"{code} 失败: {e}")
                    continue

        return total

    def _fetch_batch_yahoo(self, codes: list[str], full: bool) -> int:
        """使用 Yahoo Finance 批量下载，返回总写入行数"""
        from fsi.fetcher.yahoo import fetch_stocks_daily_batch

        # 确定起始日期：取所有股票最早的 sync 日期
        if full:
            start_date = "19700101"
        else:
            # 使用最保守的策略：取最早的 last_sync_date，确保不遗漏
            earliest = None
            for code in codes:
                last = self.get_last_sync_date("stock_daily", code)
                if last is None:
                    # 有股票从未同步过，需要全量
                    earliest = None
                    break
                if earliest is None or last < earliest:
                    earliest = last
            if earliest:
                start_date = (earliest + timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = "19700101"

        end_date = today_bj().strftime("%Y%m%d")
        self.log(f"Yahoo 批量下载 {len(codes)} 只股票 ({start_date} ~ {end_date})...")

        batch_result = fetch_stocks_daily_batch(codes, start_date, end_date)
        self.log(f"Yahoo 返回 {len(batch_result)} 只股票数据")

        total = 0
        for code, df in tqdm(batch_result.items(), desc="写入 Yahoo 数据"):
            try:
                total += self._save_daily_df(code, df)
            except Exception as e:
                print(f"{code} 写入失败: {e}")

        # 对 Yahoo 未返回数据的股票，逐个通过 AKShare 获取
        missing_codes = [c for c in codes if c not in batch_result]
        if missing_codes:
            self.log(f"{len(missing_codes)} 只股票 Yahoo 无数据，回退 AKShare...")
            for code in tqdm(missing_codes, desc="补充获取 [AKShare]"):
                try:
                    total += self.fetch_daily(code, full=full)
                except Exception as e:
                    print(f"{code} 失败: {e}")

        return total

    def fetch_all(self, codes: list[str] | None = None, full: bool = False,
                  batch_size: int | None = None, skip: int = 0) -> int:
        if codes is None:
            result = self.conn.execute("SELECT code FROM stock_list ORDER BY code").fetchall()
            codes = [r[0] for r in result]

        if not codes:
            print("股票列表为空，请先执行 fsi fetch list")
            return 0

        # 按 code 排序确保稳定性
        codes = sorted(codes)

        # 跳过前 skip 只
        if skip > 0:
            codes = codes[skip:]
        # 只取 batch_size 只
        if batch_size is not None:
            codes = codes[:batch_size]

        if not codes:
            print("经过 skip/batch-size 过滤后无股票需要处理")
            return 0

        total_codes = len(codes)
        total_rows = 0
        skipped = 0

        for i, code in enumerate(codes):
            # 增量同步：如果最近 1 天内已同步过，跳过
            if not full:
                last = self.get_last_sync_date("stock_daily", code)
                if last and (today_bj() - last).days <= 1:
                    skipped += 1
                    continue

            try:
                total_rows += self.fetch_daily(code, full=full)
            except Exception as e:
                print(f"{code} 失败: {e}")
                continue

            # 每 50 只打印一次进度
            done = i + 1
            if done % 50 == 0 or done == total_codes:
                print(f"进度: {done}/{total_codes}，已写入 {total_rows} 条，跳过 {skipped} 只（近期已同步）")

        print(f"股票同步完成: 共 {total_codes} 只，写入 {total_rows} 条，跳过 {skipped} 只")
        return total_rows

    def get_stock_codes(self) -> list[str]:
        result = self.conn.execute("SELECT code FROM stock_list ORDER BY code").fetchall()
        return [r[0] for r in result]
