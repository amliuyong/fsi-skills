"""fsi help — 列出所有命令及典型用法"""

import click

HELP_TEXT = """\
FSI — A股数据抓取与分析 CLI 工具

\033[1m全局选项\033[0m
  --db PATH          DuckDB 文件路径（默认 ~/.fsi/market_data.duckdb）
  --format json|table 输出格式（默认 json）
  --verbose, -v      详细输出
  --no-proxy         跳过代理配置

\033[1m━━━ 数据采集 ━━━\033[0m
  fsi fetch list                        拉取 A 股列表
  fsi fetch stock 000001                拉取单只股票日线
  fsi fetch stock_all 600938 601398     单股全量（日线+新闻+财报）
  fsi fetch stocks --batch-size 500     批量同步股票
  fsi fetch etfs --batch-size 500       批量同步 ETF
  fsi fetch news 000001                 拉取个股新闻入库
  fsi fetch finance 000001              拉取财务数据入库

\033[1m━━━ 查询分析 ━━━\033[0m
  fsi query stock 000001 --days 20 --indicators   股票查询 + 技术指标 + AI
  fsi query index 000001 --days 20 --indicators   指数查询 + AI
  fsi query etf 510300 --days 20 --indicators     ETF 查询 + AI
  fsi screen --rsi-below 30 --min-turnover 2      多条件选股 + AI
  fsi compare 000001 600519                       多股对比
  fsi report 000001                               综合分析报告（财报+新闻）

\033[1m━━━ 实时行情 ━━━\033[0m
  fsi quote 000001 600519               实时行情（支持多只）
  fsi news 000001                       个股新闻 + AI 情绪分析
  fsi finance 000001                    财务指标 + AI 解读
  fsi hot                               突发新闻 + 市场热点 + 日历 + AI
  fsi digest                            近 N 天新闻回顾 + AI（默认 3天/50条）
  fsi digest --days 7 --limit 100       自定义天数和条数
  fsi flow                              资金流向 + QVIX + 外盘 + AI

\033[1m━━━ 盘前/盘中/盘后 ━━━\033[0m
  fsi am                                盘前速览 — 隔夜外盘 + 今日预判
  fsi now                               盘中速报 — 实时指数 + 资金 + 行业
  fsi pm                                盘后复盘 — 全日总结 + 资金 + 展望

\033[1m━━━ 图表 & 波浪 ━━━\033[0m
  fsi chart stock 000001 --days 60      K 线图 → PNG
  fsi chart index 000001 --days 90      指数 K 线图
  fsi chart etf 510300 --days 60        ETF K 线图
  fsi chart intraday 000001            分时走势图（自动识别类型）
  fsi chart intraday 000001 -t stock   强制为股票（平安银行）
  fsi wave stock 000001                 波浪理论分析（默认 120天/5%）
  fsi wave index 000001                 指数波浪分析（默认 120天/3%）
  fsi wave stock 600519 -d 250 -t 8    自定义天数和摆幅阈值

\033[1m━━━ 工具 ━━━\033[0m
  fsi check-network                     API 健康检测，生成 api_health.json
"""


@click.command("help")
def help_cmd():
    """列出所有命令及典型用法"""
    click.echo(HELP_TEXT)
