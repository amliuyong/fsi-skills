"""DuckDB DDL 定义与增量迁移"""


# 基础表定义（CREATE IF NOT EXISTS，安全幂等）
BASE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS stock_list (
        code        VARCHAR PRIMARY KEY,
        name        VARCHAR,
        exchange    VARCHAR,
        industry    VARCHAR,
        list_date   DATE,
        is_st       BOOLEAN DEFAULT FALSE,
        updated_at  TIMESTAMP DEFAULT current_timestamp
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_daily (
        code        VARCHAR NOT NULL,
        trade_date  DATE NOT NULL,
        open        DECIMAL(12,4),
        close       DECIMAL(12,4),
        high        DECIMAL(12,4),
        low         DECIMAL(12,4),
        volume      BIGINT,
        amount      DECIMAL(18,2),
        amplitude   DECIMAL(12,4),
        pct_change  DECIMAL(12,4),
        change_amt  DECIMAL(12,4),
        turnover    DECIMAL(12,4),
        PRIMARY KEY (code, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS index_daily (
        code        VARCHAR NOT NULL,
        name        VARCHAR,
        trade_date  DATE NOT NULL,
        open        DECIMAL(12,4),
        close       DECIMAL(12,4),
        high        DECIMAL(12,4),
        low         DECIMAL(12,4),
        volume      BIGINT,
        amount      DECIMAL(18,2),
        pct_change  DECIMAL(12,4),
        PRIMARY KEY (code, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS etf_daily (
        code        VARCHAR NOT NULL,
        name        VARCHAR,
        trade_date  DATE NOT NULL,
        open        DECIMAL(12,4),
        close       DECIMAL(12,4),
        high        DECIMAL(12,4),
        low         DECIMAL(12,4),
        volume      BIGINT,
        amount      DECIMAL(18,2),
        pct_change  DECIMAL(12,4),
        turnover    DECIMAL(12,4),
        PRIMARY KEY (code, trade_date)
    )
    """,
    """
    CREATE SEQUENCE IF NOT EXISTS sync_log_id_seq START 1
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_log (
        id          INTEGER PRIMARY KEY DEFAULT nextval('sync_log_id_seq'),
        table_name  VARCHAR NOT NULL,
        code        VARCHAR,
        last_date   DATE NOT NULL,
        rows_synced INTEGER,
        synced_at   TIMESTAMP DEFAULT current_timestamp
    )
    """,
]


SCHEMA_VERSION = 6


# 增量迁移：键为目标版本号，值为该版本需要执行的 SQL 列表
# 每次升级只执行 current_version < version <= SCHEMA_VERSION 的迁移
MIGRATIONS = {
    4: [
        """
        CREATE TABLE IF NOT EXISTS stock_news (
            code        VARCHAR NOT NULL,
            url         VARCHAR NOT NULL,
            title       VARCHAR,
            content     VARCHAR,
            source      VARCHAR,
            pub_time    TIMESTAMP,
            fetched_at  TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (code, url)
        )
        """,
    ],
    5: [
        """
        CREATE TABLE IF NOT EXISTS stock_finance (
            code            VARCHAR NOT NULL,
            report_date     DATE NOT NULL,
            report_name     VARCHAR,
            eps             DECIMAL(12,4),
            bps             DECIMAL(12,4),
            revenue         DECIMAL(18,2),
            net_profit      DECIMAL(18,2),
            net_profit_ded  DECIMAL(18,2),
            revenue_yoy     DECIMAL(12,4),
            net_profit_yoy  DECIMAL(12,4),
            roe             DECIMAL(12,4),
            gross_margin    DECIMAL(12,4),
            net_margin      DECIMAL(12,4),
            debt_ratio      DECIMAL(12,4),
            ocf_per_share   DECIMAL(12,4),
            fetched_at      TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (code, report_date)
        )
        """,
    ],
    6: [
        """
        CREATE TABLE IF NOT EXISTS market_news (
            title       VARCHAR PRIMARY KEY,
            summary     VARCHAR,
            source      VARCHAR,
            pub_time    TIMESTAMP,
            tags        VARCHAR,
            fetched_at  TIMESTAMP DEFAULT current_timestamp
        )
        """,
    ],
}


def init_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_meta (
            key   VARCHAR PRIMARY KEY,
            value VARCHAR
        )
    """)
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'version'"
    ).fetchone()
    current_version = int(row[0]) if row else 0

    # 首次初始化（空库）：创建基础表
    if current_version == 0:
        for ddl in BASE_DDL:
            conn.execute(ddl)

    # 增量迁移：只执行尚未应用的版本
    for version in sorted(MIGRATIONS.keys()):
        if current_version < version <= SCHEMA_VERSION:
            for sql in MIGRATIONS[version]:
                conn.execute(sql)

    conn.execute(
        "INSERT OR REPLACE INTO schema_meta VALUES ('version', ?)",
        [str(SCHEMA_VERSION)],
    )
