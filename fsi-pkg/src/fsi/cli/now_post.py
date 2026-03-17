"""fsi now_post — 盘中口播稿生成（draft → review → refine）"""

import click

from fsi.broadcast import (
    generate_broadcast,
    get_output_path,
    save_broadcast,
)
from fsi.cli.now import collect_now_data


@click.command("now_post")
@click.option("--duration", "-d", default=3, type=int, help="目标时长（分钟），默认 3")
@click.pass_context
def now_post_cmd(ctx, duration):
    """盘中口播稿 — draft → review → refine 三步 AI 生成（带时间戳，可多次运行）"""
    verbose = ctx.obj.get("verbose", False)

    # now 带时间戳，每次运行生成新文件
    output_path = get_output_path("now")

    # 采集数据
    data = collect_now_data(verbose=verbose)
    if data is None:
        click.echo("盘中数据获取失败，请检查网络连接", err=True)
        raise SystemExit(1)

    # 三步生成
    click.echo("生成盘中口播稿...", err=True)
    script, stats = generate_broadcast(data, "now", duration=duration, verbose=verbose)

    # 保存
    save_broadcast(script, output_path)
    click.echo(
        f"已保存: {output_path} "
        f"({stats['total_chars']}字 / {stats['topic_count']} topics / "
        f"~{stats['estimated_minutes']}分钟)",
        err=True,
    )

    # 输出口播稿到 stdout
    click.echo(script)
