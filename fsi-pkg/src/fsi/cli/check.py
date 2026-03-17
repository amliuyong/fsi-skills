"""fsi check-network — API 健康检查命令"""

import click


@click.command("check-network")
@click.option("--quiet", "-q", is_flag=True, help="静默模式（仅写入配置，不输出表格）")
@click.pass_context
def check_cmd(ctx, quiet):
    """检测所有数据源可用性，生成 api_health.json"""
    from fsi.health import check_all, save_health, TASK_SOURCES

    verbose = ctx.obj.get("verbose", False)

    if not quiet:
        click.echo("正在检测数据源可用性...", err=True)

    results = check_all(verbose=verbose)
    path = save_health(results)

    if quiet:
        # 静默模式：仅输出摘要
        total = len(results["sources"])
        ok_count = sum(
            1 for t in results["sources"].values()
            if any(v.get("ok") for k, v in t.items() if k != "preferred")
        )
        click.echo(f"检测完成: {ok_count}/{total} 项有可用源 → {path}")
        return

    # 表格输出
    click.echo("")
    click.echo(f"{'数据任务':<20} {'源':<10} {'状态':<6} {'延迟(ms)':<10} {'错误'}")
    click.echo("─" * 75)

    for task in TASK_SOURCES:
        task_info = results["sources"].get(task, {})
        preferred = task_info.get("preferred", "")
        first = True
        for source_name in TASK_SOURCES[task]:
            info = task_info.get(source_name, {})
            ok = info.get("ok", False)
            ms = info.get("ms", 0)
            error = info.get("error", "")
            status = "✓" if ok else "✗"
            pref_mark = " ★" if source_name == preferred else ""
            task_label = task if first else ""
            click.echo(f"{task_label:<20} {source_name + pref_mark:<10} {status:<6} {ms:<10} {error}")
            first = False

    click.echo("─" * 75)
    click.echo(f"★ = preferred（自动选择最快可用源）")
    click.echo(f"配置已写入: {path}")
    click.echo(f"检测时间: {results['last_check']}")
