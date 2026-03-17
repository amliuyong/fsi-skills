"""美股分析命令 - 包装 stock-analysis skill"""

import os
import subprocess
import sys

import click


@click.command("us")
@click.argument("tickers", nargs=-1)
@click.option("--output", type=click.Choice(["json", "text"]), default=None,
              help="输出格式（默认跟随全局 --format）")
@click.option("--portfolio", "-p", type=str, default=None,
              help="分析组合")
@click.option("--period", type=click.Choice(["daily", "weekly", "monthly", "quarterly", "yearly"]),
              default=None, help="周期报告")
@click.pass_context
def us_cmd(ctx, tickers, output, portfolio, period):
    """美股/加密货币分析（基于 Yahoo Finance）"""
    if not tickers and not portfolio:
        raise click.UsageError("请提供 ticker 或使用 --portfolio 指定组合")

    # 定位 skill 脚本
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    script_path = os.path.join(project_root, ".agents", "skills", "stock-analysis", "scripts", "analyze_stock.py")
    if not os.path.exists(script_path):
        raise click.ClickException(f"未找到分析脚本: {script_path}")

    # 构建命令
    cmd = ["uv", "run", script_path]
    cmd.extend(tickers)

    # 输出格式：未指定时跟随全局 --format
    if output is None:
        fmt = ctx.obj.get("fmt", "json") if ctx.obj else "json"
        output = "json" if fmt == "json" else "text"
    cmd.extend(["--output", output])

    if portfolio:
        cmd.extend(["--portfolio", portfolio])
    if period:
        cmd.extend(["--period", period])

    result = subprocess.run(cmd)
    sys.exit(result.returncode)
