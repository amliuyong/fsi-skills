"""fsi search — 东方财富妙想财经资讯搜索"""

import asyncio
from pathlib import Path

import click

from fsi.config import load_dotenv
from fsi.output.formatter import output


@click.command("search")
@click.argument("query", nargs=-1, required=True)
@click.option("--no-ai", is_flag=True, help="跳过 AI 分析")
@click.pass_context
def search_cmd(ctx, query, no_ai):
    """财经资讯搜索（新闻/公告/研报）

    示例：fsi search 宁德时代最新研报
    """
    query_str = " ".join(query)
    fmt = ctx.obj["fmt"]
    verbose = ctx.obj.get("verbose", False)

    load_dotenv()

    click.echo(f"搜索: {query_str} ...", err=True)

    try:
        from mx_skills.finsearch import query_financial_news

        data = asyncio.run(query_financial_news(
            query=query_str,
            output_dir=Path("workspace/MX_FinSearch"),
            save_to_file=True,
        ))
    except Exception as e:
        output({"error": f"搜索失败: {e}"}, fmt)
        return

    if data.get("error"):
        output({"error": data["error"]}, fmt)
        return

    result = {
        "report_type": "财经资讯搜索",
        "query": query_str,
        "content": data.get("content", ""),
        "output_path": data.get("output_path"),
    }

    if verbose and data.get("output_path"):
        click.echo(f"结果已保存: {data['output_path']}", err=True)

    # AI 分析
    if not no_ai and result.get("content"):
        click.echo("正在生成 AI 分析...", err=True)
        try:
            from fsi.ai.bedrock import call_bedrock

            system_prompt = (
                "你是专业的金融分析师。请基于以下财经资讯搜索结果，提供结构化分析：\n"
                "1. 核心要点提炼（3-5条）\n"
                "2. 市场情绪判断（看多/看空/中性）\n"
                "3. 潜在影响和关注点\n"
                "用简洁中文回答，标注信息来源。"
            )
            ai_text = call_bedrock(system_prompt, result["content"])
            result["ai_analysis"] = ai_text
        except Exception as e:
            click.echo(f"AI 分析失败: {e}", err=True)
            result["ai_analysis"] = None

    output(result, fmt, title="财经资讯搜索")
