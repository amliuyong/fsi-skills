"""fsi pm_video — 盘后复盘数据收集 + 视频生成一键流水线"""

import json
import subprocess
from fsi.config import today_bj
from pathlib import Path

import click

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import PM_SYSTEM_PROMPT, build_user_message
from fsi.cli.chart import generate_kline_chart
from fsi.cli.pm import collect_pm_data
from fsi.config import MAJOR_INDICES
from fsi.output.formatter import DecimalEncoder

# video-books 项目根目录（视频生成脚本运行的 cwd）
_VIDEO_BOOKS_ROOT = Path("/home/ubuntu/my/video-books")
# FSI_POST 目录（通过 symlink 或直接路径）
_FSI_POST_ROOT = _VIDEO_BOOKS_ROOT / "books" / "FSI_POST"


@click.command("pm_video")
@click.option(
    "--engine",
    type=click.Choice(["remotion", "b2v", "news"]),
    default="remotion",
    help="视频引擎: remotion(默认), b2v(broadcast2video), news(news风格)",
)
@click.option("--duration", default=3, type=int, help="视频时长（分钟），默认 3")
@click.option("--force", is_flag=True, help="强制重新生成（覆盖已有文件）")
@click.option(
    "--step",
    type=click.Choice(["all", "script", "video", "images", "audio", "render", "cover"]),
    default="all",
    help="执行步骤: all=全部, script=仅口播稿, video=仅视频（b2v/news 引擎支持更多步骤）",
)
@click.option("--no-ai", is_flag=True, help="跳过 AI 分析（仅保存原始数据）")
@click.pass_context
def pm_video_cmd(ctx, engine, duration, force, step, no_ai):
    """盘后复盘 → JSON 保存 → 口播稿 + 视频生成"""
    fmt = ctx.obj["fmt"]
    verbose = ctx.obj.get("verbose", False)

    # 确定 JSON 输出路径
    today = today_bj()
    date_dir = today.strftime("%Y%m%d")
    date_iso = today.isoformat()

    out_dir = _FSI_POST_ROOT / date_dir
    json_path = out_dir / f"{date_iso}-PM.json"

    # JSON 已存在且非 --force：跳过数据收集和 AI，直接加载
    skipped_data = False
    if json_path.exists() and not force:
        click.echo(f"JSON 已存在: {json_path}，跳过数据收集", err=True)
        with open(json_path, "r", encoding="utf-8") as f:
            result = json.load(f)
        skipped_data = True
    else:
        # 1. 收集盘后数据
        result = collect_pm_data(db_path=ctx.obj["db"], verbose=verbose)
        if result is None:
            click.echo("盘后数据获取失败，请检查网络连接", err=True)
            ctx.exit(1)
            return

        # 2. AI 分析（可选跳过）
        if not no_ai:
            click.echo("正在生成 AI 盘后分析...", err=True)
            try:
                ai_text = call_bedrock(PM_SYSTEM_PROMPT, build_user_message(result))
                result["ai_analysis"] = ai_text
            except Exception as e:
                click.echo(f"AI 分析失败: {e}", err=True)
                result["ai_analysis"] = None

        # 3. 保存 JSON
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, cls=DecimalEncoder, ensure_ascii=False, indent=2, fp=f)
        click.echo(f"已保存: {json_path}", err=True)

    # 3.5 生成主要指数 K 线图（60 日）
    _generate_index_charts(out_dir, ctx.obj["db"], skipped_data and not force)

    # 4. 调用视频生成脚本（在 video-books 项目根目录运行）
    _ENGINE_SCRIPTS = {
        "remotion": "gen_remotion_video.sh",
        "b2v": "gen_b2v_video.sh",
        "news": "gen_news_video.sh",
    }
    script_name = _ENGINE_SCRIPTS[engine]
    script_path = _FSI_POST_ROOT / script_name
    if not script_path.exists():
        click.echo(f"{script_name} 不存在: {script_path}，跳过视频生成", err=True)
        _output_result(result, fmt)
        return

    # 脚本接受相对于 video-books 项目根目录的路径
    json_rel = json_path.relative_to(_VIDEO_BOOKS_ROOT)

    cmd = [
        "bash", str(script_path),
        "--input", str(json_rel),
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

    _output_result(result, fmt)


def _generate_index_charts(out_dir: Path, db_path: str | None, skip_existing: bool):
    """生成主要指数 K 线图到 out_dir。skip_existing=True 时跳过已存在的文件。"""
    click.echo("正在生成指数 K 线图...", err=True)
    for idx_code, idx_name in MAJOR_INDICES.items():
        fname = f"kline_{idx_code}_{idx_name}.png"
        filepath = out_dir / fname
        if skip_existing and filepath.exists():
            click.echo(f"  已存在: {filepath}", err=True)
            continue
        path = generate_kline_chart(
            "index_daily", idx_code, 60, str(out_dir),
            db_path=db_path, name_col=True, filename=fname,
        )
        if path:
            click.echo(f"  {path}", err=True)


def _output_result(result, fmt):
    """输出最终结果"""
    from fsi.output.formatter import output
    output(result, fmt, title="盘后复盘 (视频)")
