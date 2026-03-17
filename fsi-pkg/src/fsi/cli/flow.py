"""fsi flow — 资金面分析 + QVIX 恐慌指数 + AI 解读"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import click

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import FLOW_SYSTEM_PROMPT, build_user_message
from fsi.output.formatter import output


@click.command("flow")
@click.pass_context
def flow_cmd(ctx):
    """资金流向 + QVIX 恐慌指数 + AI 解读"""
    fmt = ctx.obj["fmt"]
    verbose = ctx.obj.get("verbose", False)

    result = {"report_type": "资金面分析"}

    # 所有数据源并行获取
    click.echo("获取资金面数据（并行）...", err=True)

    def _fetch_index_quotes():
        from fsi.portfolio import fetch_all_index_quotes
        return "index_quotes", fetch_all_index_quotes()

    def _fetch_global_indices():
        from fsi.fetcher.global_index import fetch_global_index_quotes
        return "global_indices", fetch_global_index_quotes()

    def _fetch_hsgt_summary():
        from fsi.fetcher.capital_flow import fetch_hsgt_summary
        return "hsgt_summary", fetch_hsgt_summary()

    def _fetch_northbound_hist():
        from fsi.fetcher.capital_flow import fetch_northbound_hist
        return "northbound_hist", fetch_northbound_hist(days=10)

    def _fetch_market_fund_flow():
        from fsi.fetcher.capital_flow import fetch_market_fund_flow
        return "market_fund_flow", fetch_market_fund_flow(days=10)

    def _fetch_sector_fund_flow():
        from fsi.fetcher.capital_flow import fetch_sector_fund_flow
        return "sector_fund_flow", fetch_sector_fund_flow(top=15)

    def _fetch_qvix_daily():
        from fsi.fetcher.qvix import fetch_qvix_daily
        return "qvix_daily", fetch_qvix_daily(days=20)

    def _fetch_qvix_intraday():
        from fsi.fetcher.qvix import fetch_qvix_intraday
        return "qvix_intraday", fetch_qvix_intraday()

    def _fetch_market_news():
        from fsi.fetcher.market_news import fetch_caixin_news
        return "market_hot_news", fetch_caixin_news(limit=20)

    tasks = [
        _fetch_index_quotes,
        _fetch_global_indices,
        _fetch_hsgt_summary,
        _fetch_northbound_hist,
        _fetch_market_fund_flow,
        _fetch_sector_fund_flow,
        _fetch_qvix_daily,
        _fetch_qvix_intraday,
        _fetch_market_news,
    ]

    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {pool.submit(fn): fn.__name__ for fn in tasks}
        for future in as_completed(futures):
            fn_name = futures[future]
            try:
                key, data = future.result()
                if data:
                    if key == "global_indices":
                        result.update(data)
                        if verbose:
                            us_n = len(data.get("us_indices", []))
                            hk_n = len(data.get("hk_indices", []))
                            click.echo(f"  ✓ 外盘指数: 美股 {us_n} + 港股 {hk_n}", err=True)
                    else:
                        result[key] = data
                        if verbose:
                            n = len(data) if isinstance(data, list) else ""
                            click.echo(f"  ✓ {key}: {n} 条", err=True)
            except Exception as e:
                if verbose:
                    click.echo(f"  ✗ {fn_name}: {e}", err=True)

    # 校验至少有部分数据可用
    data_keys = [
        "hsgt_summary", "northbound_hist", "market_fund_flow",
        "sector_fund_flow", "qvix_daily",
    ]
    if not any(k in result for k in data_keys):
        output({"error": "资金面数据获取失败，请检查网络连接"}, fmt)
        return

    # AI 解读
    click.echo("正在生成 AI 资金面解读...", err=True)
    try:
        ai_text = call_bedrock(FLOW_SYSTEM_PROMPT, build_user_message(result))
        result["ai_analysis"] = ai_text
    except Exception as e:
        click.echo(f"AI 分析失败: {e}", err=True)
        result["ai_analysis"] = None

    output(result, fmt, title="资金面分析")
