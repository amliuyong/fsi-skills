"""fetch 子命令组"""

import click


def _handle_fetch_error(e: Exception) -> None:
    """统一处理 fetch 异常，输出简洁错误信息"""
    msg = str(e)
    if "RemoteDisconnected" in msg or "ConnectionError" in msg or "Connection aborted" in msg:
        click.echo("错误: 无法连接数据服务器。可能原因：", err=True)
        click.echo("  1. 当前网络无法访问数据源（海外服务器需配置代理）", err=True)
        click.echo("  2. 请求频率过快被限流", err=True)
        click.echo("  3. 数据接口暂时不可用", err=True)
        click.echo("提示: 运行 `fsi check-network` 检测各数据源可用性并自动选源", err=True)
        click.echo("      如需代理，配置 proxy-config.json 或设置 HTTP_PROXY 环境变量", err=True)
    elif "Timeout" in msg or "timed out" in msg:
        click.echo("错误: 请求超时，请检查网络或稍后重试", err=True)
        click.echo("提示: 运行 `fsi check-network` 检测各数据源可用性", err=True)
    else:
        click.echo(f"错误: {msg}", err=True)


def _do_orphan_cleanup(db):
    """启动时清理孤儿临时 DB 文件"""
    from fsi.db.connection import cleanup_orphaned_temps
    removed = cleanup_orphaned_temps(db)
    if removed:
        click.echo(f"已清理 {removed} 个孤儿临时文件", err=True)


def _make_flush_fn(temp_path, db):
    """创建 flush 回调：将 temp DB 中间数据导入主 DB"""
    def flush():
        from fsi.db.connection import import_from_temp
        click.echo("正在 flush 到主数据库...", err=True)
        import_from_temp(temp_path, db, cleanup=False)
    return flush


@click.group()
@click.pass_context
def fetch(ctx):
    """数据采集命令"""
    pass


@fetch.command("list")
@click.pass_context
def fetch_list(ctx):
    """更新 A 股股票列表"""
    from fsi.fetcher.stock import StockFetcher
    f = StockFetcher(db_path=ctx.obj["db"], verbose=ctx.obj["verbose"])
    try:
        count = f.fetch_stock_list()
        click.echo(f"已更新 {count} 只股票")
    except Exception as e:
        _handle_fetch_error(e)
        raise SystemExit(1)


@fetch.command("stock")
@click.argument("code")
@click.option("--start", default=None, help="开始日期 YYYYMMDD")
@click.option("--end", default=None, help="结束日期 YYYYMMDD")
@click.option("--full", is_flag=True, help="全量拉取（忽略增量）")
@click.pass_context
def fetch_stock(ctx, code, start, end, full):
    """获取单只股票日线数据"""
    from fsi.fetcher.stock import StockFetcher
    f = StockFetcher(db_path=ctx.obj["db"], verbose=ctx.obj["verbose"])
    try:
        rows = f.fetch_daily(code, start_date=start, end_date=end, full=full)
        click.echo(f"{code} 写入 {rows} 条记录")
    except Exception as e:
        _handle_fetch_error(e)
        raise SystemExit(1)


@fetch.command("stocks")
@click.option("--full", is_flag=True, help="全量拉取")
@click.option("--codes", default=None, help="逗号分隔的股票代码列表")
@click.option("--batch-size", type=int, default=None, help="每批处理的股票数量（如 500 只跑前 500 只）")
@click.option("--skip", type=int, default=0, help="跳过前 N 只股票（配合 --batch-size 分段运行）")
@click.pass_context
def fetch_stocks(ctx, full, codes, batch_size, skip):
    """批量获取个股日线数据（使用临时 DB，不阻塞查询）"""
    from fsi.fetcher.stock import StockFetcher
    from fsi.db.connection import get_temp_connection, copy_sync_log, copy_stock_list, import_from_temp

    db = ctx.obj["db"]
    _do_orphan_cleanup(db)
    temp_conn, temp_path = get_temp_connection(db)
    copy_sync_log(db, temp_conn)
    # 无显式 codes 时，从主库拷贝 stock_list 供 fetch_all 使用
    if codes is None:
        copy_stock_list(db, temp_conn)
    temp_conn.close()

    flush_fn = _make_flush_fn(temp_path, db)
    f = StockFetcher(db_path=temp_path, verbose=ctx.obj["verbose"], flush_fn=flush_fn)
    code_list = codes.split(",") if codes else None
    try:
        if batch_size is not None or skip > 0:
            # 使用支持 batch_size/skip 的 fetch_all 方法
            total = f.fetch_all(codes=code_list, full=full, batch_size=batch_size, skip=skip)
        else:
            total = f.fetch_all_daily(codes=code_list, full=full)
    except Exception as e:
        click.echo("中断，正在保存已获取的数据...", err=True)
        import_from_temp(temp_path, db)
        _handle_fetch_error(e)
        raise SystemExit(1)

    click.echo("正在导入数据到主数据库...", err=True)
    import_from_temp(temp_path, db)
    click.echo(f"共写入 {total} 条记录")


@fetch.command("indices")
@click.option("--full", is_flag=True, help="全量拉取")
@click.pass_context
def fetch_indices(ctx, full):
    """获取大盘指数日线数据（使用临时 DB，不阻塞查询）"""
    from fsi.fetcher.index import IndexFetcher
    from fsi.db.connection import get_temp_connection, copy_sync_log, import_from_temp

    db = ctx.obj["db"]
    _do_orphan_cleanup(db)
    temp_conn, temp_path = get_temp_connection(db)
    copy_sync_log(db, temp_conn)
    temp_conn.close()

    flush_fn = _make_flush_fn(temp_path, db)
    f = IndexFetcher(db_path=temp_path, verbose=ctx.obj["verbose"], flush_fn=flush_fn)
    try:
        total = f.fetch_all(full=full)
    except Exception as e:
        click.echo("中断，正在保存已获取的数据...", err=True)
        import_from_temp(temp_path, db)
        _handle_fetch_error(e)
        raise SystemExit(1)

    click.echo("正在导入数据到主数据库...", err=True)
    import_from_temp(temp_path, db)
    click.echo(f"指数共写入 {total} 条记录")


@fetch.command("etfs")
@click.option("--full", is_flag=True, help="全量拉取")
@click.option("--batch-size", type=int, default=None, help="每批处理的 ETF 数量（如 500 只跑前 500 只）")
@click.option("--skip", type=int, default=0, help="跳过前 N 只 ETF（配合 --batch-size 分段运行）")
@click.pass_context
def fetch_etfs(ctx, full, batch_size, skip):
    """获取场内基金日线数据（使用临时 DB，不阻塞查询）"""
    from fsi.fetcher.etf import ETFFetcher
    from fsi.db.connection import get_temp_connection, copy_sync_log, import_from_temp

    db = ctx.obj["db"]
    _do_orphan_cleanup(db)
    temp_conn, temp_path = get_temp_connection(db)
    copy_sync_log(db, temp_conn)
    temp_conn.close()

    flush_fn = _make_flush_fn(temp_path, db)
    f = ETFFetcher(db_path=temp_path, verbose=ctx.obj["verbose"], flush_fn=flush_fn)
    try:
        total = f.fetch_all(full=full, batch_size=batch_size, skip=skip)
    except Exception as e:
        click.echo("中断，正在保存已获取的数据...", err=True)
        import_from_temp(temp_path, db)
        _handle_fetch_error(e)
        raise SystemExit(1)

    click.echo("正在导入数据到主数据库...", err=True)
    import_from_temp(temp_path, db)
    click.echo(f"ETF 共写入 {total} 条记录")


@fetch.command("news")
@click.argument("codes", nargs=-1, required=True)
@click.pass_context
def fetch_news(ctx, codes):
    """抓取个股新闻并入库（支持多只: fsi fetch news 000001 600519）"""
    from fsi.cli.news import fetch_stock_news, save_news_to_db
    from fsi.db.connection import get_connection

    conn = get_connection(ctx.obj["db"])
    total = 0
    for code in codes:
        try:
            items = fetch_stock_news(code, limit=100)
            saved = save_news_to_db(conn, code, items)
            click.echo(f"{code} 入库 {saved} 条新闻")
            total += saved
        except Exception as e:
            _handle_fetch_error(e)
    click.echo(f"共入库 {total} 条新闻")


@fetch.command("finance")
@click.argument("codes", nargs=-1, required=True)
@click.pass_context
def fetch_finance(ctx, codes):
    """抓取个股财报数据并入库（支持多只: fsi fetch finance 000001 600519）"""
    from fsi.cli.finance import _fetch_from_api, save_finance_to_db
    from fsi.db.connection import get_connection

    conn = get_connection(ctx.obj["db"])
    total = 0
    for code in codes:
        try:
            rows = _fetch_from_api(code, limit=8)
            saved = save_finance_to_db(conn, code, rows)
            click.echo(f"{code} 入库 {saved} 期财报")
            total += saved
        except Exception as e:
            _handle_fetch_error(e)
    click.echo(f"共入库 {total} 期财报")


@fetch.command("stock_all")
@click.argument("codes", nargs=-1, required=True)
@click.option("--full", is_flag=True, help="全量拉取日线")
@click.pass_context
def fetch_stock_all(ctx, codes, full):
    """一键拉取个股日线+新闻+财报（使用临时 DB，不阻塞查询）"""
    from fsi.fetcher.stock import StockFetcher
    from fsi.cli.news import fetch_stock_news, save_news_to_db
    from fsi.cli.finance import _fetch_from_api, save_finance_to_db
    from fsi.db.connection import get_temp_connection, copy_sync_log, import_from_temp

    db = ctx.obj["db"]
    _do_orphan_cleanup(db)
    temp_conn, temp_path = get_temp_connection(db)
    copy_sync_log(db, temp_conn)

    flush_fn = _make_flush_fn(temp_path, db)
    sf = StockFetcher(db_path=temp_path, verbose=ctx.obj["verbose"], flush_fn=flush_fn)

    for code in codes:
        click.echo(f"=== {code} ===", err=True)
        # 日线
        try:
            rows = sf.fetch_daily(code, full=full)
            click.echo(f"  日线: {rows} 条", err=True)
        except Exception as e:
            _handle_fetch_error(e)
        # 新闻
        try:
            items = fetch_stock_news(code, limit=100)
            saved = save_news_to_db(temp_conn, code, items)
            click.echo(f"  新闻: 获取 {len(items)} 条，入库 {saved} 条", err=True)
        except Exception as e:
            _handle_fetch_error(e)
        # 财报
        try:
            fin_rows = _fetch_from_api(code, limit=8)
            saved = save_finance_to_db(temp_conn, code, fin_rows)
            click.echo(f"  财报: 入库 {saved} 期", err=True)
        except Exception as e:
            _handle_fetch_error(e)

    temp_conn.close()
    click.echo("正在导入数据到主数据库...", err=True)
    import_from_temp(temp_path, db)
    click.echo("完成", err=True)


@fetch.command("all")
@click.option("--full", is_flag=True, help="全量拉取")
@click.pass_context
def fetch_all(ctx, full):
    """获取全部数据（股票列表 + 个股日线 + 指数 + ETF，使用临时 DB）"""
    from fsi.fetcher.stock import StockFetcher
    from fsi.fetcher.index import IndexFetcher
    from fsi.fetcher.etf import ETFFetcher
    from fsi.db.connection import get_temp_connection, copy_sync_log, import_from_temp

    db = ctx.obj["db"]
    verbose = ctx.obj["verbose"]

    _do_orphan_cleanup(db)
    temp_conn, temp_path = get_temp_connection(db)
    copy_sync_log(db, temp_conn)
    temp_conn.close()

    flush_fn = _make_flush_fn(temp_path, db)

    try:
        sf = StockFetcher(db_path=temp_path, verbose=verbose, flush_fn=flush_fn)
        click.echo("=== 更新股票列表 ===")
        sf.fetch_stock_list()

        click.echo("=== 获取大盘指数 ===")
        IndexFetcher(db_path=temp_path, verbose=verbose, flush_fn=flush_fn).fetch_all(full=full)

        click.echo("=== 获取个股日线 ===")
        sf.fetch_all_daily(full=full)

        click.echo("=== 获取 ETF ===")
        ETFFetcher(db_path=temp_path, verbose=verbose, flush_fn=flush_fn).fetch_all(full=full)
    except Exception as e:
        click.echo("中断，正在保存已获取的数据...", err=True)
        import_from_temp(temp_path, db)
        _handle_fetch_error(e)
        raise SystemExit(1)

    click.echo("正在导入数据到主数据库...", err=True)
    import_from_temp(temp_path, db)
    click.echo("全部数据获取完成")
