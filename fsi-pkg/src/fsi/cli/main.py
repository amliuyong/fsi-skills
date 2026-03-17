"""Click 根命令组 + 全局选项"""

import click

from fsi.cli.help import help_cmd
from fsi.cli.fetch import fetch
from fsi.cli.query import query
from fsi.cli.screen import screen
from fsi.cli.compare import compare
from fsi.cli.report import report
from fsi.cli.quote import quote_cmd
from fsi.cli.us import us_cmd
from fsi.cli.news import news_cmd
from fsi.cli.finance import finance_cmd
from fsi.cli.mid import mid_cmd
from fsi.cli.pos import pos_cmd
from fsi.cli.eod import eod_cmd
from fsi.cli.hot import hot_cmd
from fsi.cli.digest import digest_cmd
from fsi.cli.flow import flow_cmd
from fsi.cli.am import am_cmd
from fsi.cli.now import now_cmd
from fsi.cli.pm import pm_cmd
from fsi.cli.am_post import am_post_cmd
from fsi.cli.now_post import now_post_cmd
from fsi.cli.pm_post import pm_post_cmd
from fsi.cli.doc_post import doc_post_cmd
from fsi.cli.pm_video import pm_video_cmd
from fsi.cli.doc_video import doc_video_cmd
from fsi.cli.chart import chart
from fsi.cli.intraday_chart import chart_intraday
from fsi.cli.wave import wave
from fsi.cli.monitor import monitor_cmd
from fsi.cli.sim import sim
from fsi.cli.check import check_cmd
from fsi.cli.search import search_cmd
from fsi.cli.pick import pick_cmd


@click.group()
@click.option("--db", default=None, help="DuckDB 文件路径（默认 ~/.fsi/market_data.duckdb）")
@click.option("--format", "fmt", type=click.Choice(["json", "table"]), default="json", help="输出格式")
@click.option("--verbose", "-v", is_flag=True, help="详细输出")
@click.option("--no-proxy", is_flag=True, help="跳过代理配置（调试用）")
@click.pass_context
def cli(ctx, db, fmt, verbose, no_proxy):
    """FSI - A股数据抓取与分析 CLI 工具"""
    ctx.ensure_object(dict)
    ctx.obj["db"] = db
    ctx.obj["fmt"] = fmt
    ctx.obj["verbose"] = verbose

    # 自动加载代理配置
    from fsi.proxy import init_proxy
    proxy_url = init_proxy(no_proxy=no_proxy)
    if proxy_url and verbose:
        click.echo(f"已启用代理: {proxy_url}", err=True)


cli.add_command(help_cmd)
cli.add_command(fetch)
cli.add_command(query)
cli.add_command(screen)
cli.add_command(compare)
cli.add_command(report)
cli.add_command(quote_cmd)
cli.add_command(us_cmd)
cli.add_command(news_cmd)
cli.add_command(finance_cmd)
cli.add_command(mid_cmd)
cli.add_command(pos_cmd)
cli.add_command(eod_cmd)
cli.add_command(hot_cmd)
cli.add_command(digest_cmd)
cli.add_command(flow_cmd)
cli.add_command(am_cmd)
cli.add_command(now_cmd)
cli.add_command(pm_cmd)
cli.add_command(am_post_cmd)
cli.add_command(now_post_cmd)
cli.add_command(pm_post_cmd)
cli.add_command(doc_post_cmd)
cli.add_command(pm_video_cmd)
cli.add_command(doc_video_cmd)
cli.add_command(chart)
chart.add_command(chart_intraday)
cli.add_command(wave)
cli.add_command(monitor_cmd)
cli.add_command(sim)
cli.add_command(check_cmd)
cli.add_command(search_cmd)
cli.add_command(pick_cmd)
