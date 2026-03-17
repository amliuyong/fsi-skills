"""fsi pick — 东方财富妙想智能选股"""

import asyncio
import csv
from pathlib import Path

import click

from fsi.config import load_dotenv
from fsi.output.formatter import output


def _read_csv(csv_path: str) -> list[dict]:
    """读取 CSV 并返回 dict 列表"""
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


@click.command("pick")
@click.argument("query", nargs=-1, required=True)
@click.option("--type", "select_type",
              type=click.Choice(["A股", "港股", "美股", "基金", "ETF", "可转债", "板块"]),
              default="A股", help="证券类型（默认 A股）")
@click.option("--no-ai", is_flag=True, help="跳过 AI 分析")
@click.pass_context
def pick_cmd(ctx, query, select_type, no_ai):
    """智能选股筛选（自然语言）

    示例：
      fsi pick 股价大于100元的半导体股票
      fsi pick --type ETF 近一年收益率前10
      fsi pick --type 可转债 双低策略
    """
    query_str = " ".join(query)
    fmt = ctx.obj["fmt"]
    verbose = ctx.obj.get("verbose", False)

    load_dotenv()

    click.echo(f"选股: [{select_type}] {query_str} ...", err=True)

    try:
        from mx_skills.stockpick import query_stock_pick

        data = asyncio.run(query_stock_pick(
            query=query_str,
            select_type=select_type,
            output_dir=Path("workspace/MX_StockPick"),
        ))
    except Exception as e:
        output({"error": f"选股失败: {e}"}, fmt)
        return

    if data.get("error"):
        output({"error": data["error"], "raw_preview": data.get("raw_preview", "")}, fmt)
        return

    # 读取 CSV 结果
    rows = []
    if data.get("csv_path"):
        try:
            rows = _read_csv(data["csv_path"])
        except Exception as e:
            click.echo(f"读取 CSV 失败: {e}", err=True)

    result = {
        "report_type": "智能选股",
        "query": query_str,
        "select_type": select_type,
        "row_count": data.get("row_count", 0),
        "data": rows,
        "csv_path": data.get("csv_path"),
    }

    if verbose:
        click.echo(f"筛选到 {data.get('row_count', 0)} 条结果", err=True)
        if data.get("csv_path"):
            click.echo(f"CSV: {data['csv_path']}", err=True)

    # AI 分析
    if not no_ai and rows:
        click.echo("正在生成 AI 分析...", err=True)
        try:
            from fsi.ai.bedrock import call_bedrock
            from fsi.ai.prompts import build_user_message

            system_prompt = (
                "你是专业的金融分析师。请基于以下选股筛选结果，提供分析：\n"
                "1. 筛选结果概览\n"
                "2. 值得关注的标的（Top 3-5）及理由\n"
                "3. 风险提示\n"
                "用简洁中文回答。"
            )
            ai_text = call_bedrock(system_prompt, build_user_message(result))
            result["ai_analysis"] = ai_text
        except Exception as e:
            click.echo(f"AI 分析失败: {e}", err=True)
            result["ai_analysis"] = None

    output(result, fmt, title="智能选股")
