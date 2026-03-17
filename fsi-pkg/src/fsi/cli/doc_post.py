"""fsi doc_post — 研究文档转口播稿（draft → review → refine）"""

from pathlib import Path

import click

from fsi.broadcast import (
    generate_doc_broadcast,
    get_doc_output_path,
    save_broadcast,
)


@click.command("doc_post")
@click.option("--input", "-i", "input_path", required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="输入 markdown 文件路径")
@click.option("--duration", "-d", default=3, type=int,
              help="目标阅读时长（分钟），默认 3")
@click.option("--output", "-o", "output_path", default=None,
              type=click.Path(dir_okay=False),
              help="自定义输出路径（默认从文件名推导）")
@click.option("--force", is_flag=True, help="强制重新生成（覆盖已有文件）")
@click.option("--tts", is_flag=True, help="TTS 模式：禁止股票代码和视觉符号，适配语音播报")
@click.pass_context
def doc_post_cmd(ctx, input_path, duration, output_path, force, tts):
    """研究文档转口播稿 — draft → review → refine 三步 AI 生成"""
    verbose = ctx.obj.get("verbose", False)

    input_file = Path(input_path)
    out = Path(output_path) if output_path else get_doc_output_path(input_file)

    # 幂等：已存在则跳过
    if not force and out.exists():
        click.echo(f"已存在: {out} (用 --force 重新生成)", err=True)
        click.echo(out.read_text(encoding="utf-8"))
        return

    # 读取研究文档
    content = input_file.read_text(encoding="utf-8")
    if not content.strip():
        click.echo(f"文件为空: {input_file}", err=True)
        raise SystemExit(1)

    # 从文件名提取标题（去掉日期前缀和扩展名）
    title = input_file.stem
    # 去掉常见日期前缀 "2026-03-12_" 或 "2026-03-12-"
    import re
    title = re.sub(r"^\d{4}-\d{2}-\d{2}[_-]", "", title)
    # 下划线转空格
    title = title.replace("_", " ")

    # 三步生成
    mode = "TTS口播稿" if tts else "阅读版文章"
    click.echo(f"生成专题{mode}: {title}...", err=True)
    script, stats = generate_doc_broadcast(
        content, title, duration=duration, verbose=verbose, tts=tts,
    )

    # 保存
    save_broadcast(script, out)
    click.echo(
        f"已保存: {out} "
        f"({stats['total_chars']}字 / {stats['topic_count']} topics / "
        f"~{stats['estimated_minutes']}分钟)",
        err=True,
    )

    # 输出口播稿到 stdout
    click.echo(script)
