"""chart 子命令 — K 线图生成"""

import matplotlib
matplotlib.use("Agg")  # 无头后端，必须在 mplfinance 之前

from datetime import timedelta
from pathlib import Path

import click
import matplotlib.font_manager as fm
import matplotlib.patheffects as pe
import mplfinance as mpf
import pandas as pd

from fsi.config import FSI_DIR, today_bj
from fsi.db.connection import get_connection
from fsi.indicators.engine import IndicatorEngine


# ── 中文字体配置 ──────────────────────────────────────────────

def _setup_cjk_font() -> str | None:
    """检测并配置 CJK 字体，返回字体名或 None。"""
    # matplotlib 可能未自动索引 .ttc/.otf 字体，手动尝试加载
    font_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/misans/MiSans-Regular.ttf",
        "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
    ]
    for fp in font_paths:
        if Path(fp).exists():
            fm.fontManager.addfont(fp)

    candidates = [
        "Noto Sans CJK SC",
        "MiSans",
        "Droid Sans Fallback",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    for name in candidates:
        if name in available:
            matplotlib.rcParams["font.sans-serif"] = [name]
            matplotlib.rcParams["font.family"] = "sans-serif"
            matplotlib.rcParams["axes.unicode_minus"] = False
            return name
    return None


# ── Click 命令组 ──────────────────────────────────────────────

@click.group()
@click.pass_context
def chart(ctx):
    """K 线图表命令"""
    pass


@chart.command("stock")
@click.argument("code")
@click.option("--days", "-d", default=60, help="显示天数")
@click.option("--start", default=None, help="开始日期 (YYYY-MM-DD)")
@click.option("--end", default=None, help="结束日期 (YYYY-MM-DD)")
@click.option("--output", "-o", default=None, help="输出目录")
@click.pass_context
def chart_stock(ctx, code, days, start, end, output):
    """生成个股 K 线图"""
    path = generate_kline_chart("stock_daily", code, days, output,
                                db_path=ctx.obj["db"], start=start, end=end)
    if path:
        click.echo(str(path))


@chart.command("index")
@click.argument("code")
@click.option("--days", "-d", default=60, help="显示天数")
@click.option("--output", "-o", default=None, help="输出目录")
@click.pass_context
def chart_index(ctx, code, days, output):
    """生成指数 K 线图"""
    path = generate_kline_chart("index_daily", code, days, output,
                                db_path=ctx.obj["db"], name_col=True)
    if path:
        click.echo(str(path))


@chart.command("etf")
@click.argument("code")
@click.option("--days", "-d", default=60, help="显示天数")
@click.option("--output", "-o", default=None, help="输出目录")
@click.pass_context
def chart_etf(ctx, code, days, output):
    """生成 ETF K 线图"""
    path = generate_kline_chart("etf_daily", code, days, output,
                                db_path=ctx.obj["db"], name_col=True)
    if path:
        click.echo(str(path))


# ── 核心绘图逻辑 ─────────────────────────────────────────────

def generate_kline_chart(table: str, code: str, days: int,
                         output_dir: str | None = None,
                         db_path: str | None = None,
                         name_col: bool = False,
                         filename: str | None = None,
                         start: str | None = None,
                         end: str | None = None,
                         swing_points: list[dict] | None = None) -> str | None:
    """生成 K 线图 PNG，返回文件路径。失败返回 None。

    swing_points: 转折点列表 [{index, price, type: "high"|"low", seq, date}, ...]
                  传入时在主图上标注转折点和连线。
    """
    conn = get_connection(db_path)

    # 构造查询条件
    if start and end:
        start_q = start.replace("-", "")
        end_q = end.replace("-", "")
        where = "code = ? AND trade_date >= ? AND trade_date <= ?"
        params = [code, start_q, end_q]
    else:
        multiplier = 3
        cutoff = (today_bj() - timedelta(days=int(days * multiplier))).isoformat()
        where = "code = ? AND trade_date >= ?"
        params = [code, cutoff]

    df = conn.execute(
        f"SELECT * FROM {table} WHERE {where} ORDER BY trade_date", params
    ).fetchdf()

    if df.empty:
        click.echo(f"无 {code} 数据，请先 fetch", err=True)
        return None

    # 解析名称
    name = ""
    if table == "stock_daily":
        info = conn.execute(
            "SELECT name FROM stock_list WHERE code = ?", [code]
        ).fetchone()
        if info:
            name = info[0]
    elif name_col and "name" in df.columns:
        name = df["name"].iloc[0] if not df["name"].isna().all() else ""

    # 计算指标（MA + MACD）
    engine = IndicatorEngine()
    df = engine.calculate(df)

    # 截取显示窗口
    if not (start and end):
        df = df.tail(days).reset_index(drop=True)

    if df.empty:
        click.echo(f"{code} 数据不足", err=True)
        return None

    # ── 准备 mplfinance DataFrame ──
    df_chart = df.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    })
    df_chart["trade_date"] = pd.to_datetime(df_chart["trade_date"])
    df_chart.set_index("trade_date", inplace=True)

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df_chart.columns:
            df_chart[col] = pd.to_numeric(df_chart[col], errors="coerce")

    # ── 构建 addplot ──
    addplots = []

    # MA 均线（主图叠加）
    ma_colors = {
        "ma5": "#FF6600",   # 橙
        "ma10": "#0066FF",  # 蓝
        "ma20": "#CC00CC",  # 紫
        "ma60": "#00CC00",  # 绿
    }
    for col, color in ma_colors.items():
        if col in df_chart.columns:
            series = pd.to_numeric(df_chart[col], errors="coerce")
            if series.notna().any():
                addplots.append(mpf.make_addplot(
                    series, panel=0, color=color, width=1.0,
                ))

    # MACD 副图（panel 2）
    has_macd = all(c in df_chart.columns for c in ("dif", "dea", "macd_hist"))
    if has_macd:
        dif = pd.to_numeric(df_chart["dif"], errors="coerce")
        dea = pd.to_numeric(df_chart["dea"], errors="coerce")
        hist = pd.to_numeric(df_chart["macd_hist"], errors="coerce")

        # DIF / DEA 线
        addplots.append(mpf.make_addplot(
            dif, panel=2, color="#FF6600", width=1.0, ylabel="MACD",
        ))
        addplots.append(mpf.make_addplot(
            dea, panel=2, color="#0066FF", width=1.0,
        ))

        # 柱状图：正红负绿
        hist_pos = hist.where(hist >= 0, 0)
        hist_neg = hist.where(hist < 0, 0)
        addplots.append(mpf.make_addplot(
            hist_pos, panel=2, type="bar", color="#CC0000", width=0.7,
        ))
        addplots.append(mpf.make_addplot(
            hist_neg, panel=2, type="bar", color="#00CC00", width=0.7,
        ))

    # ── 样式：红涨绿跌（A 股惯例）──
    font_name = _setup_cjk_font()
    rc_kwargs = {"axes.unicode_minus": False}
    if font_name:
        rc_kwargs["font.family"] = font_name

    mc = mpf.make_marketcolors(
        up="#CC0000", down="#00CC00",
        edge="inherit", wick="inherit",
        volume="inherit", ohlc="inherit",
    )
    mpf_style = mpf.make_mpf_style(
        marketcolors=mc,
        figcolor="white",
        gridcolor="#E0E0E0",
        gridstyle="--",
        rc=rc_kwargs,
    )

    title = f"{code} {name}" if name else code
    panel_ratios = (3, 1, 1.2) if has_macd else (3, 1)

    fig, axes = mpf.plot(
        df_chart,
        type="candle",
        style=mpf_style,
        volume=True,
        addplot=addplots if addplots else None,
        panel_ratios=panel_ratios,
        figsize=(14, 10),
        tight_layout=True,
        returnfig=True,
    )
    axes[0].set_title(title, loc="left", fontsize=13, fontweight="bold")

    # ── 标注最新价格 ──
    latest = df_chart.iloc[-1]
    latest_close = float(latest["Close"])
    if len(df_chart) >= 2:
        prev_close = float(df_chart.iloc[-2]["Close"])
        change = latest_close - prev_close
        change_pct = (change / prev_close) * 100
        sign = "+" if change >= 0 else ""
        price_color = "#CC0000" if change >= 0 else "#00CC00"
        price_text = f"{latest_close:.2f}  {sign}{change:.2f} ({sign}{change_pct:.2f}%)"
    else:
        price_color = "#333333"
        price_text = f"{latest_close:.2f}"

    axes[0].set_title(price_text, loc="right", fontsize=12, color=price_color)
    axes[0].axhline(y=latest_close, color=price_color, linewidth=0.8,
                    linestyle="--", alpha=0.5)

    # ── 标注波浪转折点 ──
    if swing_points:
        _annotate_swings(fig, axes, df_chart, swing_points)

    # ── 保存 PNG ──
    if output_dir:
        out_path = Path(output_dir)
    else:
        out_path = FSI_DIR / "charts"
    out_path.mkdir(parents=True, exist_ok=True)

    if not filename:
        kind = table.replace("_daily", "")      # stock / index / etf
        filename = f"{kind}_{code}_{today_bj().isoformat()}.png"
    filepath = out_path / filename
    fig.savefig(str(filepath), dpi=150, bbox_inches="tight", facecolor="white")
    matplotlib.pyplot.close(fig)

    return str(filepath)


def _annotate_swings(fig, axes, df_chart, swing_points: list[dict]):
    """在主图上标注波浪转折点：标记 + 序号 + zigzag 连线。"""
    ax = axes[0]  # 主图 axes

    # 收集有效的转折点坐标（x 为 DataFrame 行号位置，y 为价格）
    xs, ys = [], []
    for sp in swing_points:
        idx = sp["index"]
        if idx < 0 or idx >= len(df_chart):
            continue

        x = idx
        price = sp["price"]
        is_high = sp["type"] == "high"
        seq = sp.get("seq", "")

        # 高点：红色倒三角在上方；低点：绿色三角在下方
        marker = "v" if is_high else "^"
        color = "#FF0000" if is_high else "#00CC00"

        ax.plot(x, price, marker=marker, color=color,
                markersize=13, markeredgecolor="black", markeredgewidth=0.8,
                zorder=10)
        ax.annotate(
            str(seq), xy=(x, price),
            xytext=(0, 16 if is_high else -16),
            textcoords="offset points",
            fontsize=14, fontweight="bold", color=color,
            ha="center", va="bottom" if is_high else "top",
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=color, linewidth=0.8, alpha=0.9),
            path_effects=[pe.withStroke(linewidth=2, foreground="white"), pe.Normal()],
            zorder=11,
        )

        xs.append(x)
        ys.append(price)

    # zigzag 连线（蓝色实线，醒目）
    if len(xs) >= 2:
        ax.plot(xs, ys, color="#1E90FF", linewidth=1.8,
                linestyle="-", alpha=0.85, zorder=5)
