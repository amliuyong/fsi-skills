"""单只股票实时行情（腾讯财经接口）"""

import click
import requests

from fsi.output.formatter import output


def _parse_tencent_quote(code: str) -> dict | None:
    """从腾讯财经获取实时行情"""
    # 沪市: 6开头(个股), 5开头(ETF/基金)
    # 深市: 0/3开头(个股), 1开头(ETF/基金)
    if code.startswith(("6", "5")):
        prefix = "sh"
    elif code.startswith(("0", "3", "1")):
        prefix = "sz"
    else:
        prefix = "bj"
    symbol = f"{prefix}{code}"
    try:
        r = requests.get(f"https://qt.gtimg.cn/q={symbol}", timeout=10)
        r.raise_for_status()
    except Exception:
        return None

    parts = r.text.split("~")
    if len(parts) < 50:
        return None

    def safe_float(idx):
        try:
            return float(parts[idx])
        except (ValueError, IndexError):
            return None

    return {
        "code": code,
        "name": parts[1],
        "price": safe_float(3),
        "pct_change": safe_float(32),
        "change_amt": safe_float(31),
        "open": safe_float(5),
        "high": safe_float(33),
        "low": safe_float(34),
        "prev_close": safe_float(4),
        "volume": int(safe_float(6) or 0) * 100,  # 腾讯返回手，转股
        "amount": safe_float(37),
        "turnover": safe_float(38),
        "pe": safe_float(39),
        "pb": safe_float(46),
        "market_cap": safe_float(45),
        "time": parts[30] if len(parts) > 30 else None,
        "bid1": {"price": safe_float(9), "volume": int(safe_float(10) or 0) * 100},
        "ask1": {"price": safe_float(19), "volume": int(safe_float(20) or 0) * 100},
    }


@click.command("quote")
@click.argument("codes", nargs=-1, required=True)
@click.pass_context
def quote_cmd(ctx, codes):
    """获取实时行情（支持多只: fsi quote 000001 600519）"""
    fmt = ctx.obj.get("fmt", "json")
    results = []
    for code in codes:
        data = _parse_tencent_quote(code)
        if data:
            results.append(data)
        else:
            results.append({"code": code, "error": "获取失败"})

    if len(results) == 1:
        output(results[0], fmt)
    else:
        output(results, fmt)
