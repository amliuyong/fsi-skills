"""chart intraday 子命令 — 分时走势图"""

import matplotlib
matplotlib.use("Agg")

from pathlib import Path

import click
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

from fsi.config import FSI_DIR, MAJOR_INDICES
from fsi.cli.chart import _setup_cjk_font
from fsi.fetcher.intraday import fetch_intraday


# ── 代码类型自动识别 ──────────────────────────────────────────

def _resolve_code_and_type(code: str) -> tuple[str, str, str]:
    """根据代码自动判断 asset_type 和名称。

    Returns: (code, asset_type, name)
    """
    if code in MAJOR_INDICES:
        return code, "index", MAJOR_INDICES[code]
    # ETF：5 开头（沪）或 1 开头（深）
    if code.startswith("5") or code.startswith("1"):
        return code, "etf", ""
    return code, "stock", ""


def _fetch_prev_close(code: str, asset_type: str) -> float | None:
    """获取昨收价。"""
    try:
        if asset_type == "index":
            from fsi.portfolio import fetch_index_quote
            q = fetch_index_quote(code)
        else:
            from fsi.cli.quote import _parse_tencent_quote
            q = _parse_tencent_quote(code)
        if q and q.get("prev_close"):
            return float(q["prev_close"])
    except Exception:
        pass
    return None


def _fetch_name(code: str, asset_type: str) -> str:
    """获取证券名称。"""
    try:
        if asset_type == "index":
            from fsi.portfolio import fetch_index_quote
            q = fetch_index_quote(code)
        else:
            from fsi.cli.quote import _parse_tencent_quote
            q = _parse_tencent_quote(code)
        if q and q.get("name"):
            return q["name"]
    except Exception:
        pass
    return ""


# ── 绘图 ─────────────────────────────────────────────────────

def generate_intraday_chart(
    df: pd.DataFrame,
    code: str,
    name: str = "",
    prev_close: float | None = None,
    output_dir: str | None = None,
    asset_type: str = "",
) -> str | None:
    """生成分时走势图 PNG，返回文件路径。

    df 列：时间, 收盘, 成交量, 成交额, 均价
    """
    if df.empty:
        return None

    font_name = _setup_cjk_font()

    # 解析时间
    df = df.copy()
    df["dt"] = pd.to_datetime(df["时间"])
    trade_date = df["dt"].iloc[-1].strftime("%Y-%m-%d")

    # 过滤午休时段（11:31-12:59），闭合上下午
    hm = df["dt"].dt.hour * 100 + df["dt"].dt.minute
    df = df[~((hm > 1130) & (hm < 1300))].reset_index(drop=True)

    prices = df["收盘"].values.astype(float)
    avg_prices = df["均价"].values.astype(float)
    volumes = df["成交量"].values.astype(float)
    times = df["dt"].values
    # 用整数索引做 X 轴（午休闭合），时间仅用于刻度标签
    x = np.arange(len(prices))
    time_labels = pd.to_datetime(times).strftime("%H:%M")

    # 昨收 fallback：用第一根的收盘价
    if prev_close is None or prev_close <= 0:
        prev_close = float(prices[0])

    # 涨跌幅序列
    pct_changes = (prices - prev_close) / prev_close * 100

    # 成交量颜色：对比前一根，涨红跌绿
    vol_colors = []
    for i in range(len(prices)):
        if i == 0:
            vol_colors.append("#CC0000" if prices[i] >= prev_close else "#00CC00")
        else:
            vol_colors.append("#CC0000" if prices[i] >= prices[i - 1] else "#00CC00")

    # ── 创建图表 ──
    fig, (ax_price, ax_vol) = plt.subplots(
        2, 1, figsize=(14, 8),
        gridspec_kw={"height_ratios": [3, 1], "hspace": 0.05},
        sharex=True,
    )
    fig.patch.set_facecolor("white")

    # ── 上方：价格面板 ──
    ax_price.plot(x, prices, color="#1E90FF", linewidth=1.2, label="价格")
    ax_price.plot(x, avg_prices, color="#FF8C00", linewidth=1.0,
                  linestyle="--", alpha=0.8, label="均价")

    # 价格与昨收之间的填充
    ax_price.fill_between(
        x, prices, prev_close,
        where=(prices >= prev_close),
        color="#FFCCCC", alpha=0.3, interpolate=True,
    )
    ax_price.fill_between(
        x, prices, prev_close,
        where=(prices < prev_close),
        color="#CCFFCC", alpha=0.3, interpolate=True,
    )

    # 昨收参考线
    ax_price.axhline(y=prev_close, color="#888888", linewidth=0.8,
                     linestyle="--", alpha=0.6)
    ax_price.annotate(
        f"{prev_close:.2f}", xy=(0, prev_close),
        xycoords=("axes fraction", "data"),
        fontsize=8, color="#888888", va="center", ha="right",
        xytext=(-4, 0), textcoords="offset points",
    )

    # Y 轴范围：对称
    max_dev = max(abs(prices.max() - prev_close), abs(prices.min() - prev_close))
    if max_dev < prev_close * 0.001:
        max_dev = prev_close * 0.01
    y_margin = max_dev * 1.15
    ax_price.set_ylim(prev_close - y_margin, prev_close + y_margin)

    # 右侧涨跌幅 Y 轴
    ax_pct = ax_price.twinx()
    pct_margin = y_margin / prev_close * 100
    ax_pct.set_ylim(-pct_margin, pct_margin)
    ax_pct.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:+.2f}%"))
    ax_pct.tick_params(axis="y", labelsize=9, colors="#666666")

    ax_price.tick_params(axis="y", labelsize=9)
    ax_price.grid(True, linestyle="--", alpha=0.3)
    ax_price.legend(loc="upper left", fontsize=9, framealpha=0.8)

    # ── 下方：成交量面板 ──
    ax_vol.bar(x, volumes, width=0.8, color=vol_colors, alpha=0.7)
    ax_vol.tick_params(axis="y", labelsize=8)
    ax_vol.grid(True, linestyle="--", alpha=0.3)

    # 成交量 Y 轴格式化（万手/手）
    max_vol = volumes.max() if len(volumes) > 0 else 1
    if max_vol >= 10000:
        ax_vol.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda v, _: f"{v/10000:.0f}万"))
    ax_vol.set_ylabel("成交量", fontsize=9)

    # ── X 轴：用索引，手动标注时间刻度 ──
    # 只取整点刻度（避免 11:30/13:00 午休交界重叠）
    tick_positions = []
    tick_labels_str = []
    for i, lbl in enumerate(time_labels):
        if lbl.endswith(":00"):
            tick_positions.append(i)
            tick_labels_str.append(lbl)
    ax_vol.set_xticks(tick_positions)
    ax_vol.set_xticklabels(tick_labels_str, fontsize=9)
    ax_vol.set_xlim(-1, len(x))

    # 午休分隔线
    for i in range(1, len(time_labels)):
        if time_labels[i - 1] <= "11:30" and time_labels[i] >= "13:00":
            ax_price.axvline(x=i - 0.5, color="#CCCCCC", linewidth=0.8,
                             linestyle=":", alpha=0.6)
            ax_vol.axvline(x=i - 0.5, color="#CCCCCC", linewidth=0.8,
                           linestyle=":", alpha=0.6)
            break

    # ── 最高/最低点标注 ──
    hi_idx = int(np.argmax(prices))
    lo_idx = int(np.argmin(prices))
    hi_price = prices[hi_idx]
    lo_price = prices[lo_idx]
    hi_pct = (hi_price - prev_close) / prev_close * 100
    lo_pct = (lo_price - prev_close) / prev_close * 100
    hi_sign = "+" if hi_pct >= 0 else ""
    lo_sign = "+" if lo_pct >= 0 else ""

    # 最高点：标注在点上方
    ax_price.plot(hi_idx, hi_price, "v", color="#CC0000", markersize=5)
    ax_price.annotate(
        f"{hi_price:.2f} ({hi_sign}{hi_pct:.2f}%)",
        xy=(hi_idx, hi_price), fontsize=7.5, color="#CC0000",
        va="bottom", ha="center",
        xytext=(0, 8), textcoords="offset points",
    )
    ax_price.axhline(y=hi_price, color="#CC0000", linewidth=0.4,
                     linestyle=":", alpha=0.35)

    # 最低点：标注在点下方
    ax_price.plot(lo_idx, lo_price, "^", color="#00CC00", markersize=5)
    ax_price.annotate(
        f"{lo_price:.2f} ({lo_sign}{lo_pct:.2f}%)",
        xy=(lo_idx, lo_price), fontsize=7.5, color="#00CC00",
        va="top", ha="center",
        xytext=(0, -8), textcoords="offset points",
    )
    ax_price.axhline(y=lo_price, color="#00CC00", linewidth=0.4,
                     linestyle=":", alpha=0.35)

    # ── 当前价格标注 ──
    latest_price = prices[-1]
    change = latest_price - prev_close
    change_pct = (change / prev_close) * 100
    sign = "+" if change >= 0 else ""
    price_color = "#CC0000" if change >= 0 else "#00CC00"

    # 现价水平线 + 末端圆点
    ax_price.axhline(y=latest_price, color=price_color, linewidth=0.6,
                     linestyle="--", alpha=0.5)
    ax_price.plot(x[-1], latest_price, "o", color=price_color, markersize=4)

    # 右侧现价 + 涨跌幅标签（带背景色块）
    ax_price.annotate(
        f" {latest_price:.2f}  {sign}{change_pct:.2f}% ",
        xy=(1, latest_price),
        xycoords=("axes fraction", "data"),
        fontsize=8, color="white", fontweight="bold",
        va="center", ha="left",
        xytext=(4, 0), textcoords="offset points",
        bbox=dict(boxstyle="round,pad=0.2", facecolor=price_color, alpha=0.9),
    )

    # ── 标题 ──
    title_left = f"{code} {name}" if name else code
    title_right = f"{latest_price:.2f}  {sign}{change:.2f} ({sign}{change_pct:.2f}%)"

    ax_price.set_title(title_left, loc="left", fontsize=13, fontweight="bold")
    ax_price.set_title(title_right, loc="right", fontsize=12, color=price_color)
    ax_price.set_title(trade_date, loc="center", fontsize=10, color="#888888")

    # ── 保存 ──
    if output_dir:
        out_path = Path(output_dir)
    else:
        out_path = FSI_DIR / "charts"
    out_path.mkdir(parents=True, exist_ok=True)

    tag = f"_{asset_type}" if asset_type else ""
    filename = f"intraday_{code}{tag}_{trade_date}.png"
    filepath = out_path / filename
    fig.savefig(str(filepath), dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    return str(filepath)


# ── Click 命令 ────────────────────────────────────────────────

@click.command("intraday")
@click.argument("code")
@click.option("--type", "-t", "asset_type_opt", default=None,
              type=click.Choice(["stock", "index", "etf"]),
              help="强制指定类型（覆盖自动识别）")
@click.option("--output", "-o", default=None, help="输出目录")
@click.pass_context
def chart_intraday(ctx, code, asset_type_opt, output):
    """生成分时走势图（自动识别股票/指数/ETF）"""
    verbose = ctx.obj.get("verbose", False)

    if asset_type_opt:
        asset_type, name = asset_type_opt, ""
    else:
        code, asset_type, name = _resolve_code_and_type(code)

    # 获取分时数据
    df, meta = fetch_intraday(code, asset_type, verbose=verbose)
    if df is None or df.empty:
        click.echo(f"无法获取 {code} 分时数据", err=True)
        return

    # 优先用 meta 中的昨收价和名称（腾讯 API 一次返回）
    prev_close = meta.get("prev_close") or _fetch_prev_close(code, asset_type)
    if not name:
        name = meta.get("name") or _fetch_name(code, asset_type)

    path = generate_intraday_chart(df, code, name, prev_close, output, asset_type)
    if path:
        if meta.get("cache_file"):
            click.echo(meta["cache_file"])
        click.echo(path)
    else:
        click.echo("分时图生成失败", err=True)
