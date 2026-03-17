"""fsi pm_post — 盘后口播稿生成（draft → review → refine）"""

import click

from fsi.broadcast import (
    generate_broadcast,
    get_output_path,
    save_broadcast,
)
from fsi.cli.pm import collect_pm_data


@click.command("pm_post")
@click.option("--duration", "-d", default=3, type=int, help="目标时长（分钟），默认 3")
@click.option("--force", is_flag=True, help="强制重新生成（覆盖已有文件）")
@click.pass_context
def pm_post_cmd(ctx, duration, force):
    """盘后口播稿 — draft → review → refine 三步 AI 生成"""
    verbose = ctx.obj.get("verbose", False)

    output_path = get_output_path("pm")

    # 幂等：已存在则跳过
    if not force and output_path.exists():
        click.echo(f"已存在: {output_path} (用 --force 重新生成)", err=True)
        click.echo(output_path.read_text(encoding="utf-8"))
        return

    # 采集数据
    data = collect_pm_data(db_path=ctx.obj.get("db"), verbose=verbose)
    if data is None:
        click.echo("盘后数据获取失败，请检查网络连接", err=True)
        raise SystemExit(1)

    # 三步生成
    click.echo("生成盘后口播稿...", err=True)
    script, stats = generate_broadcast(data, "pm", duration=duration, verbose=verbose)

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
