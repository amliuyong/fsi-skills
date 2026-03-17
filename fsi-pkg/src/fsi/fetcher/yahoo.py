"""Yahoo Finance 数据源适配层

不继承 BaseFetcher（纯数据获取，DB 操作仍在 StockFetcher）。
提供单股/批量日线获取和财报数据获取，输出 FSI schema 兼容格式。

Yahoo Finance 特征:
- A 股 ticker: 000001.SZ (深交所), 600519.SS (上交所)
- 北交所 (BJ) 不支持
- 缺失 amount (成交额) 和 turnover (换手率)
- 前复权: auto_adjust=True (默认)
"""

from datetime import datetime

import pandas as pd
import yfinance as yf


def to_yahoo_symbol(code: str) -> str | None:
    """6 位代码 → Yahoo ticker，北交所返回 None

    000001 → 000001.SZ, 600519 → 600519.SS, 430047 → None
    """
    if code.startswith("6"):
        return f"{code}.SS"
    elif code.startswith(("0", "3")):
        return f"{code}.SZ"
    # 北交所 (4xx, 8xx) Yahoo 不支持
    return None


def from_yahoo_symbol(symbol: str) -> str:
    """Yahoo ticker → 6 位代码

    000001.SZ → 000001
    """
    return symbol.split(".")[0]


def _parse_date(d: str) -> str:
    """统一日期格式: YYYYMMDD 或 YYYY-MM-DD → YYYY-MM-DD"""
    d = d.replace("-", "")
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def fetch_stock_daily(code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    """获取单股日线数据，返回 FSI stock_daily schema 兼容的 DataFrame

    列: trade_date, open, close, high, low, volume, amount, pct_change, change_amt, amplitude, turnover
    amount 使用 VWAP 近似估算，turnover 为 None
    """
    symbol = to_yahoo_symbol(code)
    if symbol is None:
        return None

    start = _parse_date(start_date)
    end = _parse_date(end_date)

    # yfinance end 是 exclusive，需要加一天
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    from datetime import timedelta
    end_inclusive = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    ticker = yf.Ticker(symbol)
    df = ticker.history(start=start, end=end_inclusive, auto_adjust=True)

    if df is None or df.empty:
        return None

    return _convert_to_fsi_schema(df)


def fetch_stocks_daily_batch(
    codes: list[str],
    start_date: str,
    end_date: str,
    batch_size: int = 500,
) -> dict[str, pd.DataFrame]:
    """批量下载多股日线数据，返回 {code: DataFrame} 字典

    分批处理（每批 batch_size 只），自动跳过北交所股票。
    """
    # 过滤掉北交所
    symbol_map = {}  # yahoo_symbol → code
    for code in codes:
        sym = to_yahoo_symbol(code)
        if sym is not None:
            symbol_map[sym] = code

    if not symbol_map:
        return {}

    start = _parse_date(start_date)
    end = _parse_date(end_date)
    from datetime import timedelta
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    end_inclusive = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    symbols = list(symbol_map.keys())
    result = {}

    # 分批下载
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        try:
            data = yf.download(
                batch,
                start=start,
                end=end_inclusive,
                auto_adjust=True,
                group_by="ticker",
                threads=True,
                progress=False,
            )
        except Exception:
            # 批量失败时逐个下载
            for sym in batch:
                code = symbol_map[sym]
                try:
                    df = fetch_stock_daily(code, start_date, end_date)
                    if df is not None and not df.empty:
                        result[code] = df
                except Exception:
                    continue
            continue

        if data is None or data.empty:
            continue

        # 解析批量下载结果
        if len(batch) == 1:
            # 单股时 yf.download 返回普通 DataFrame（无 MultiIndex）
            sym = batch[0]
            code = symbol_map[sym]
            df = _convert_to_fsi_schema(data)
            if df is not None and not df.empty:
                result[code] = df
        else:
            # 多股时返回 MultiIndex columns: (ticker, field)
            for sym in batch:
                code = symbol_map[sym]
                try:
                    stock_df = data[sym].dropna(how="all")
                    if stock_df.empty:
                        continue
                    df = _convert_to_fsi_schema(stock_df)
                    if df is not None and not df.empty:
                        result[code] = df
                except (KeyError, Exception):
                    continue

    return result


def _convert_to_fsi_schema(df: pd.DataFrame) -> pd.DataFrame | None:
    """将 yfinance DataFrame 转换为 FSI stock_daily schema 格式

    yfinance 列: Open, High, Low, Close, Volume
    FSI 列: trade_date, open, close, high, low, volume, amount, pct_change, change_amt, amplitude, turnover
    """
    if df is None or df.empty:
        return None

    out = pd.DataFrame()

    # trade_date: 先转北京时间再提取日期（避免 UTC 日期偏差）
    idx = pd.to_datetime(df.index)
    if idx.tz is not None:
        idx = idx.tz_convert("Asia/Shanghai")
    out["trade_date"] = idx.date

    out["open"] = df["Open"].values
    out["high"] = df["High"].values
    out["low"] = df["Low"].values
    out["close"] = df["Close"].values
    out["volume"] = df["Volume"].values

    # amount: VWAP 近似 = (open + high + low + close) / 4 * volume
    out["amount"] = ((out["open"] + out["high"] + out["low"] + out["close"]) / 4 * out["volume"]).round(2)

    # pct_change, change_amt, amplitude: 从 OHLCV 计算
    out = out.sort_values("trade_date").reset_index(drop=True)
    prev_close = out["close"].shift(1)
    out["pct_change"] = ((out["close"] - prev_close) / prev_close * 100).round(4)
    out["change_amt"] = (out["close"] - prev_close).round(4)
    out["amplitude"] = ((out["high"] - out["low"]) / prev_close * 100).round(4)

    # turnover: Yahoo 无总股本数据，设为 None
    out["turnover"] = None

    return out


def fetch_stock_finance(code: str, limit: int = 8) -> list[dict] | None:
    """从 Yahoo Finance 获取财报数据，返回 FSI stock_finance DB 格式字典列表

    使用 .quarterly_income_stmt + .quarterly_balance_sheet + .info 映射到 DB 字段。
    """
    symbol = to_yahoo_symbol(code)
    if symbol is None:
        return None

    ticker = yf.Ticker(symbol)

    try:
        info = ticker.info or {}
    except Exception:
        info = {}

    try:
        income_stmt = ticker.quarterly_income_stmt
    except Exception:
        income_stmt = pd.DataFrame()

    try:
        balance_sheet = ticker.quarterly_balance_sheet
    except Exception:
        balance_sheet = pd.DataFrame()

    if income_stmt.empty:
        return None

    rows = []
    # income_stmt 列名是 Timestamp（报告期），行名是项目
    periods = list(income_stmt.columns[:limit])

    for period in periods:
        report_date = period.strftime("%Y-%m-%d") if hasattr(period, "strftime") else str(period)[:10]

        item = {"code": code, "report_date": report_date}

        # report_name: 从日期生成中文报告期名
        item["report_name"] = _make_report_name(period)

        # 从 income_stmt 获取
        item["revenue"] = _safe_get(income_stmt, "Total Revenue", period)
        item["net_profit"] = _safe_get(income_stmt, "Net Income", period)
        item["net_profit_ded"] = None  # 扣非净利润是 A 股特有概念

        # 从 info 获取（TTM 数据，所有期共用）
        item["eps"] = _safe_float(info.get("trailingEps"))
        item["bps"] = _safe_float(info.get("bookValue"))
        item["roe"] = _pct(info.get("returnOnEquity"))
        item["gross_margin"] = _pct(info.get("grossMargins"))
        item["net_margin"] = _pct(info.get("profitMargins"))
        item["ocf_per_share"] = _calc_ocf_per_share(info)

        # 从 balance_sheet 计算资产负债率
        item["debt_ratio"] = _calc_debt_ratio(balance_sheet, period)

        # 同比增长率（需要找到去年同期数据）
        item["revenue_yoy"] = _calc_yoy(income_stmt, "Total Revenue", period)
        item["net_profit_yoy"] = _calc_yoy(income_stmt, "Net Income", period)

        rows.append(item)

    return rows if rows else None


def _safe_get(df: pd.DataFrame, row_name: str, col) -> float | None:
    """安全获取 DataFrame 中的值"""
    try:
        val = df.loc[row_name, col]
        if pd.notna(val):
            return round(float(val), 2)
    except (KeyError, TypeError, ValueError):
        pass
    return None


def _safe_float(val) -> float | None:
    """安全转换为 float"""
    if val is None:
        return None
    try:
        f = float(val)
        if pd.notna(f):
            return round(f, 4)
    except (TypeError, ValueError):
        pass
    return None


def _pct(val) -> float | None:
    """将小数比率转为百分比 (0.15 → 15.0)"""
    if val is None:
        return None
    try:
        f = float(val)
        if pd.notna(f):
            return round(f * 100, 4)
    except (TypeError, ValueError):
        pass
    return None


def _calc_ocf_per_share(info: dict) -> float | None:
    """计算每股经营现金流"""
    ocf = info.get("operatingCashflow")
    shares = info.get("sharesOutstanding")
    if ocf is not None and shares is not None and shares > 0:
        try:
            return round(float(ocf) / float(shares), 4)
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return None


def _calc_debt_ratio(balance_sheet: pd.DataFrame, period) -> float | None:
    """从资产负债表计算资产负债率 = 总负债 / 总资产 × 100"""
    if balance_sheet.empty:
        return None
    try:
        total_assets = balance_sheet.loc["Total Assets", period]
        total_liab = balance_sheet.loc["Total Liabilities Net Minority Interest", period]
        if pd.notna(total_assets) and pd.notna(total_liab) and total_assets > 0:
            return round(float(total_liab) / float(total_assets) * 100, 4)
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        pass
    return None


def _calc_yoy(income_stmt: pd.DataFrame, row_name: str, period) -> float | None:
    """计算同比增长率：找同一季度的去年数据"""
    try:
        current = income_stmt.loc[row_name, period]
        if pd.isna(current):
            return None

        # 找去年同期（约 4 个季度前的列）
        cols = list(income_stmt.columns)
        idx = cols.index(period)
        if idx + 4 < len(cols):
            prev_period = cols[idx + 4]
            prev = income_stmt.loc[row_name, prev_period]
            if pd.notna(prev) and prev != 0:
                return round((float(current) - float(prev)) / abs(float(prev)) * 100, 4)
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        pass
    return None


def _make_report_name(period) -> str:
    """从 Timestamp 生成中文报告期名称，如 '2025三季报'"""
    try:
        month = period.month
        year = period.year
        if month <= 3:
            return f"{year}一季报"
        elif month <= 6:
            return f"{year}中报"
        elif month <= 9:
            return f"{year}三季报"
        else:
            return f"{year}年报"
    except (AttributeError, TypeError):
        return str(period)[:10]
