"""DuckDB 连接管理"""

import os

import duckdb

from fsi.config import DB_PATH, ensure_data_dir
from fsi.db.schema import init_schema


_connection = None


def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    global _connection
    if _connection is not None and db_path is None:
        return _connection
    ensure_data_dir()
    path = db_path or str(DB_PATH)
    conn = duckdb.connect(path)
    init_schema(conn)
    if db_path is None:
        _connection = conn
    return conn


def close_connection():
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None


def get_temp_connection(db_path: str | None = None) -> tuple[duckdb.DuckDBPyConnection, str]:
    """创建临时 DB 连接，返回 (conn, temp_path)。

    临时文件放在正式 DB 同目录，以 PID 命名避免冲突。
    """
    ensure_data_dir()
    main_path = db_path or str(DB_PATH)
    parent = os.path.dirname(main_path)
    temp_path = os.path.join(parent, f".tmp_fetch_{os.getpid()}.duckdb")
    conn = duckdb.connect(temp_path)
    init_schema(conn)
    return conn, temp_path


def copy_stock_list(main_db_path: str | None, temp_conn: duckdb.DuckDBPyConnection):
    """从主 DB 拷贝 stock_list 到临时 DB，支持 fetch_all 按码列表运行。"""
    path = main_db_path or str(DB_PATH)
    if not os.path.exists(path):
        return
    main_conn = duckdb.connect(path, read_only=True)
    try:
        df = main_conn.execute("SELECT * FROM stock_list").fetchdf()
    except duckdb.CatalogException:
        return
    finally:
        main_conn.close()
    if not df.empty:
        temp_conn.execute("INSERT INTO stock_list SELECT * FROM df")


def copy_sync_log(main_db_path: str | None, temp_conn: duckdb.DuckDBPyConnection):
    """从主 DB 拷贝 sync_log 到临时 DB，支持增量判断。"""
    path = main_db_path or str(DB_PATH)
    if not os.path.exists(path):
        return
    main_conn = duckdb.connect(path, read_only=True)
    try:
        df = main_conn.execute("SELECT * FROM sync_log").fetchdf()
    except duckdb.CatalogException:
        # 主 DB 还没有 sync_log 表
        return
    finally:
        main_conn.close()
    if not df.empty:
        # 不复制 id 列，由 temp DB 的序列自动生成，避免主键冲突
        temp_conn.execute(
            "INSERT INTO sync_log (table_name, code, last_date, rows_synced, synced_at) "
            "SELECT table_name, code, last_date, rows_synced, synced_at FROM df"
        )


def import_from_temp(temp_path: str, main_db_path: str | None = None, cleanup: bool = True,
                     retries: int = 5, retry_delay: float = 10.0):
    """从临时 DB 批量导入主 DB。

    Args:
        temp_path: 临时 DB 文件路径
        main_db_path: 主 DB 文件路径
        cleanup: 导入后是否删除临时文件（False 用于中间 flush）
        retries: DuckDB 锁冲突时的重试次数
        retry_delay: 每次重试间隔秒数
    """
    import time as _time

    path = main_db_path or str(DB_PATH)

    for attempt in range(1, retries + 1):
        try:
            main = get_connection(path)
            break
        except duckdb.IOException as e:
            if "lock" in str(e).lower() and attempt < retries:
                print(f"[import_from_temp] 主库被锁，{retry_delay}s 后重试 ({attempt}/{retries})...")
                _time.sleep(retry_delay)
                continue
            raise

    main.execute(f"ATTACH '{temp_path}' AS tmp (READ_ONLY)")

    tables = ["stock_daily", "index_daily", "etf_daily",
              "stock_news", "stock_finance", "sync_log"]
    try:
        for table in tables:
            count = main.execute(f"SELECT count(*) FROM tmp.{table}").fetchone()[0]
            if count > 0:
                main.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM tmp.{table}")

        # stock_list 特殊处理：如果临时 DB 有数据则全量替换
        sl_count = main.execute("SELECT count(*) FROM tmp.stock_list").fetchone()[0]
        if sl_count > 0:
            main.execute("DELETE FROM stock_list")
            main.execute("INSERT INTO stock_list SELECT * FROM tmp.stock_list")
    finally:
        main.execute("DETACH tmp")

    if cleanup:
        _remove_temp(temp_path)


def _remove_temp(temp_path: str):
    """删除临时 DB 文件"""
    for suffix in ("", ".wal"):
        f = temp_path + suffix if suffix else temp_path
        if os.path.exists(f):
            os.remove(f)


def cleanup_orphaned_temps(db_path: str | None = None):
    """清理孤儿临时 DB 文件（对应 PID 已不存在的）"""
    import glob

    path = db_path or str(DB_PATH)
    parent = os.path.dirname(path)
    pattern = os.path.join(parent, ".tmp_fetch_*.duckdb")
    removed = 0

    for f in glob.glob(pattern):
        basename = os.path.basename(f)
        # 提取 PID: .tmp_fetch_{PID}.duckdb
        try:
            pid_str = basename.replace(".tmp_fetch_", "").replace(".duckdb", "")
            pid = int(pid_str)
        except ValueError:
            continue

        # 当前进程的 temp 不清理
        if pid == os.getpid():
            continue

        # 检查 PID 是否还存活
        try:
            os.kill(pid, 0)
            # 进程存活，跳过
            continue
        except OSError:
            # 进程不存在，可以清理
            pass

        _remove_temp(f)
        removed += 1

    return removed
