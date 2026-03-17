"""fsi help — 列出所有命令及典型用法"""

import click

HELP_TEXT = """\
FSI — A股数据抓取与分析 CLI 工具

\033[1m全局选项\033[0m
  --db PATH          DuckDB 文件路径（默认 data/market_data.duckdb）
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

\033[1m━━━ 行情资讯 ━━━\033[0m
  fsi quote 000001 600519               实时行情（支持多只）
  fsi news 000001                       个股新闻 + AI 情绪分析
  fsi finance 000001                    财务指标 + AI 解读
  fsi hot                               突发新闻 + 市场热点 + 日历 + AI
  fsi digest                            近 N 天新闻回顾 + AI（默认 3天/50条）
  fsi digest --days 7 --limit 100       自定义天数和条数
  fsi flow                              资金流向 + QVIX + 外盘 + AI
  fsi us AAPL                           美股/加密货币分析

\033[1m━━━ 盘前/盘中/盘后（公开，无持仓）━━━\033[0m
  fsi am                                盘前速览 — 隔夜外盘 + 今日预判
  fsi now                               盘中速报 — 实时指数 + 资金 + 行业
  fsi pm                                盘后复盘 — 全日总结 + 资金 + 展望

\033[1m━━━ 文章生成（三步 AI：draft→review→refine）━━━\033[0m
  fsi am_post                            盘前文章 → data/broadcasts/{date}-am_post.txt
  fsi now_post                           盘中文章 → {date}-now-{HHMM}_post.txt（可多次）
  fsi pm_post                            盘后文章 → data/broadcasts/{date}-pm_post.txt
  fsi pm_post --duration 5               指定 5 分钟阅读时长
  fsi am_post --force                    强制重新生成
  fsi doc_post -i data/research/xxx.md   研究文档 → 阅读版文章
  fsi doc_post -i xxx.md --tts           研究文档 → TTS 版文章（语音播报）

\033[1m━━━ 持仓分析（需 portfolio.json）━━━\033[0m
  fsi mid                               盘中解读 — 大盘 + 持仓快照 + AI
  fsi pos                               持仓分析 — 逐股深度 + AI 评级
  fsi eod                               盘后回顾 — 最全面复盘 + AI

\033[1m━━━ 图表 & 波浪 ━━━\033[0m
  fsi chart stock 000001 --days 60      K 线图 → PNG
  fsi chart index 000001 --days 90      指数 K 线图
  fsi chart etf 510300 --days 60        ETF K 线图
  fsi chart intraday 000001            分时走势图（自动识别类型）
  fsi chart intraday 000001 -t stock   强制为股票（平安银行）
  fsi chart intraday 600519 -o /tmp    自定义输出目录
  fsi wave stock 000001                 波浪理论分析（默认 120天/5%）
  fsi wave index 000001                 指数波浪分析（默认 120天/3%）
  fsi wave stock 600519 -d 250 -t 8    自定义天数和摆幅阈值

\033[1m━━━ 信号监控 ━━━\033[0m
  fsi monitor 600519 510300             每日信号监控（技术+波浪+AI）
  fsi monitor                           监控 monitor.json + portfolio 合并
  fsi monitor 600519 --no-ai            仅算法打分，跳过 AI

\033[1m━━━ 模拟盘 ━━━\033[0m
  fsi sim init conservative             初始化模拟组合（保守/均衡/激进）
  fsi sim init aggressive --capital 500000  激进组合，50万本金
  fsi sim check                         检查所有组合：行情+打分+自动交易
  fsi sim check conservative            检查单个组合
  fsi sim status                        所有组合盈亏摘要
  fsi sim log                           交易记录
  fsi sim list                          列出所有组合

\033[1m━━━ 视频生成 ━━━\033[0m
  fsi pm_video                          盘后数据 → AI → JSON → Remotion 视频
  fsi pm_video --engine b2v             使用 b2v 管线
  fsi pm_video --engine news            使用 news 风格管线
  fsi pm_video --force                  强制重新生成
  fsi doc_video -i data/research/xxx.md  研究文档 → TTS 文稿 → 视频
  fsi doc_video -i xxx.md --step script  仅生成 TTS 文稿

\033[1m━━━ 工具 ━━━\033[0m
  fsi check-network                     API 健康检测，生成 api_health.json

\033[1m━━━ 数据同步脚本 ━━━\033[0m
  bash scripts/sync.sh daily            每日增量同步
  bash scripts/sync.sh full             全量同步
"""


@click.command("help")
def help_cmd():
    """列出所有命令及典型用法"""
    click.echo(HELP_TEXT)
