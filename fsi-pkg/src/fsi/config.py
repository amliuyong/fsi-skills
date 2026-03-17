"""全局配置"""

import os
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# 北京时区
TZ_BJ = ZoneInfo("Asia/Shanghai")


def today_bj() -> date:
    """获取北京时间当前日期（服务器跑 UTC，直接 date.today() 会差 8 小时）"""
    return datetime.now(tz=TZ_BJ).date()

FSI_HOME = Path(os.environ.get("FSI_HOME", Path.home() / ".fsi"))
FSI_DIR = FSI_HOME / "data"
DB_PATH = FSI_DIR / "market_data.duckdb"

# 默认参数
DEFAULT_DAYS = 60
DEFAULT_FORMAT = "json"

# 主要指数列表
MAJOR_INDICES = {
    "000001": "上证指数",
    "399001": "深证成指",
    "399006": "创业板指",
    "000016": "上证50",
    "000300": "沪深300",
    "000905": "中证500",
}

# 限速配置
RATE_LIMIT_SECONDS = 1.0
MAX_RETRIES = 3

def ensure_data_dir():
    FSI_DIR.mkdir(parents=True, exist_ok=True)


def load_dotenv():
    """从项目根目录 .env 加载环境变量（不覆盖已有值）"""
    import os
    env_path = FSI_HOME / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            if key and key not in os.environ:
                os.environ[key] = val
