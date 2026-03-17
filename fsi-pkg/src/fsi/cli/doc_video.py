"""fsi doc_video — 研究文档 → TTS 文稿 → 视频生成一键流水线"""

import subprocess
from pathlib import Path

import click

# video-books 项目根目录（视频生成脚本运行的 cwd）
_VIDEO_BOOKS_ROOT = Path("/home/ubuntu/my/video-books")
# gen_blog_video.sh 脚本路径
_SCRIPT = _VIDEO_BOOKS_ROOT / "books" / "FSI_POST" / "gen_blog_video.sh"


@click.command("doc_video")
@click.option(
    "--input", "-i", "input_path",
    required=True,
    type=click.Path(exists=True),
    help="研究文档 markdown 路径",
)
@click.option("--duration", "-d", default=3, type=int, help="目标时长（分钟），默认 3")
@click.option(
    "--step",
    type=click.Choice(["all", "script", "video", "images", "audio", "render", "cover"]),
    default="all",
    help="执行步骤: all=全部, script=仅TTS文稿, images/audio/render/cover=单步",
)
@click.option("--force", is_flag=True, help="强制重新生成（覆盖已有文件）")
@click.pass_context
def doc_video_cmd(ctx, input_path, duration, step, force):
    """研究文档 → TTS 文稿 → 图片/音频/视频渲染"""
    if not _SCRIPT.exists():
        click.echo(f"脚本不存在: {_SCRIPT}", err=True)
        ctx.exit(1)
        return

    # 取绝对路径传给脚本
    abs_input = str(Path(input_path).resolve())

    cmd = [
        "bash", str(_SCRIPT),
        "--input", abs_input,
        "--duration", str(duration),
        "--step", step,
    ]
    if force:
        cmd.append("--force")

    click.echo(f"调用视频生成: {' '.join(cmd)}", err=True)
    try:
        subprocess.run(cmd, cwd=str(_VIDEO_BOOKS_ROOT), check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"视频生成失败 (exit {e.returncode})", err=True)
        ctx.exit(e.returncode)
        return
    except FileNotFoundError:
        click.echo("bash 不可用，跳过视频生成", err=True)
