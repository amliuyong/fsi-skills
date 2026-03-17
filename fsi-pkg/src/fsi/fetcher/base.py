"""BaseFetcher - 重试、限速、列名映射、增量更新"""

import time
from abc import ABC, abstractmethod
from datetime import date

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from fsi.config import RATE_LIMIT_SECONDS, MAX_RETRIES
from fsi.db.connection import get_connection


class BaseFetcher(ABC):
    COLUMN_MAP: dict[str, str] = {}

    # 每写入 FLUSH_EVERY 条记录后自动 flush 到主 DB（仅 temp DB 模式生效）
    FLUSH_EVERY = 500

    def __init__(self, db_path: str | None = None, verbose: bool = False,
                 flush_fn: callable = None):
        self.db_path = db_path
        self.verbose = verbose
        self._last_request_time = 0.0
        self._flush_fn = flush_fn
        self._write_count = 0

    @property
    def conn(self):
        return get_connection(self.db_path)

    def _rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_request_time = time.time()

    def fetch_with_retry(self, func, **kwargs) -> pd.DataFrame:
        self._rate_limit()

        @retry(
            stop=stop_after_attempt(MAX_RETRIES),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            retry=retry_if_exception_type((ConnectionError, TimeoutError, Exception)),
            reraise=True,
        )
        def _call():
            return func(**kwargs)

        return _call()

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.COLUMN_MAP:
            df = df.rename(columns=self.COLUMN_MAP)
        return df

    def get_last_sync_date(self, table_name: str, code: str) -> date | None:
        result = self.conn.execute(
            "SELECT last_date FROM sync_log WHERE table_name = ? AND code = ? "
            "ORDER BY synced_at DESC LIMIT 1",
            [table_name, code],
        ).fetchone()
        return result[0] if result else None

    def save_to_db(self, df: pd.DataFrame, table_name: str, code: str | None = None) -> int:
        if df.empty:
            return 0
        self.conn.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df")
        rows = len(df)
        self._write_count += 1
        if self._flush_fn and self._write_count % self.FLUSH_EVERY == 0:
            self.log(f"已写入 {self._write_count} 批，flush 到主 DB...")
            try:
                self._flush_fn()
            except Exception as e:
                self.log(f"flush 失败（不影响继续）: {e}")
        return rows

    def update_sync_log(self, table_name: str, code: str, last_date: date, rows: int):
        self.conn.execute(
            "INSERT INTO sync_log (table_name, code, last_date, rows_synced) VALUES (?, ?, ?, ?)",
            [table_name, code, last_date, rows],
        )

    def log(self, msg: str):
        if self.verbose:
            print(msg)
