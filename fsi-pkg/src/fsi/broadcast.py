"""口播稿生成核心模块 — 三步 AI 流水线（draft → review → refine）"""

import re
from datetime import datetime
from pathlib import Path

import click

from fsi.ai.bedrock import call_bedrock
from fsi.ai.prompts import (
    BROADCAST_CONTENT_FRAMEWORKS,
    BROADCAST_DRAFT_SYSTEM_PROMPT,
    BROADCAST_REFINE_SYSTEM_PROMPT,
    BROADCAST_REFINE_TTS_ADDENDUM,
    BROADCAST_REVIEW_SYSTEM_PROMPT,
    BROADCAST_TITLE_PREFIXES,
)
from fsi.config import FSI_DIR, TZ_BJ

# 308 中文字 ≈ 1 分钟音频
CHARS_PER_MINUTE = 308

BROADCAST_DIR = FSI_DIR / "broadcasts"


def extract_market_summary(data: dict) -> str:
    """从数据 dict 中提取关键市场数据，构建结构化摘要供 AI 使用。
    支持 am/now/pm 三种数据格式。
    """
    sections = []

    # 1. 报告类型和日期
    report_type = data.get("report_type", "市场分析")
    # 优先使用显式注入的 report_date，其次从 index_quotes 时间戳解析，最后用 fund_flow
    today_date = data.get("report_date", "")
    if not today_date:
        index_quotes = data.get("index_quotes", [])
        if index_quotes and index_quotes[0].get("time"):
            ts = str(index_quotes[0]["time"])
            if len(ts) >= 8:
                today_date = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"
    if not today_date:
        fund_flow = data.get("market_fund_flow", [])
        today_date = fund_flow[-1]["日期"] if fund_flow else ""
    else:
        fund_flow = data.get("market_fund_flow", [])
    if today_date:
        sections.append(f"## 报告类型：{report_type}  日期：{today_date}")
    else:
        sections.append(f"## 报告类型：{report_type}")

    # 2. A 股指数行情（index_quotes）
    index_quotes = data.get("index_quotes", [])
    if index_quotes:
        sections.append("\n## A股指数行情")
        for idx in index_quotes:
            name = idx.get("name", idx.get("指数名称", ""))
            price = idx.get("price", idx.get("最新价", ""))
            pct = idx.get("pct_change", idx.get("涨跌幅", ""))
            amount = idx.get("amount", 0)
            if isinstance(pct, (int, float)):
                line = f"- {name}: {price} ({pct:+.2f}%)"
            else:
                line = f"- {name}: {price} ({pct})"
            if amount:
                line += f", 成交额{amount / 1e4:.0f}亿"
            sections.append(line)

    # 3. A 股指数历史（a_share_indices — am 格式）
    a_share_indices = data.get("a_share_indices", [])
    if a_share_indices:
        sections.append("\n## A股指数最近收盘（上一交易日）")
        for idx in a_share_indices:
            name = idx.get("name", "")
            close = idx.get("last_close", idx.get("close", ""))
            pct = idx.get("pct_change", "")
            date_str = idx.get("last_date", idx.get("date", ""))
            if isinstance(pct, (int, float)):
                sections.append(f"- {name}: {close} ({pct:+.2f}%) [{date_str}]")
            else:
                sections.append(f"- {name}: {close} ({pct}) [{date_str}]")
            # 近 5 日走势
            recent = idx.get("recent", [])
            if recent:
                points = [
                    f"{r['date']}: {r['close']}({r['pct_change']:+.2f}%)"
                    for r in recent[:5]
                ]
                sections.append(f"  近5日: {' → '.join(points)}")

    # 4. 今日资金流向
    if fund_flow:
        today_flow = fund_flow[-1]
        sections.append("\n## 今日资金流向")
        主力净额 = today_flow.get("主力净流入-净额", 0)
        主力占比 = today_flow.get("主力净流入-净占比", 0)
        超大单净额 = today_flow.get("超大单净流入-净额", 0)
        小单净额 = today_flow.get("小单净流入-净额", 0)
        sections.append(f"- 主力净流入: {主力净额 / 1e8:.1f}亿 (占比{主力占比}%)")
        sections.append(f"- 超大单净流入: {超大单净额 / 1e8:.1f}亿")
        sections.append(f"- 小单净流入: {小单净额 / 1e8:.1f}亿")

        if len(fund_flow) >= 3:
            sections.append("\n## 近3日资金流向对比")
            for item in fund_flow[-3:]:
                净额 = item.get("主力净流入-净额", 0)
                sections.append(
                    f"- {item['日期']}: 上证{item['上证-收盘价']}"
                    f"({item['上证-涨跌幅']:+.2f}%), 主力净流{净额 / 1e8:.1f}亿"
                )

    # 5. 行业资金流向
    sector_flow = data.get("sector_fund_flow", [])
    if sector_flow:
        sections.append("\n## 行业资金净流入 TOP15")
        for s in sector_flow[:15]:
            sections.append(
                f"- {s['行业']}: 涨跌{s['涨跌幅(%)']}%, 净流入{s['净流入(亿)']}亿, "
                f"领涨股{s['领涨股']}({s['领涨股涨跌(%)']:+.2f}%)"
            )

    # 6. 美股
    us_indices = data.get("us_indices", [])
    if us_indices:
        sections.append("\n## 隔夜美股")
        for idx in us_indices:
            sections.append(
                f"- {idx['name']}: {idx['price']} ({idx['pct_change']:+.2f}%)"
            )

    # 7. 港股
    hk_indices = data.get("hk_indices", [])
    if hk_indices:
        sections.append("\n## 港股")
        for idx in hk_indices:
            sections.append(
                f"- {idx['name']}: {idx['price']} ({idx['pct_change']:+.2f}%)"
            )

    # 8. 富时中国A50
    a50 = data.get("a50", {})
    if a50:
        sections.append("\n## 富时中国A50期货")
        name = a50.get("name", "富时中国A50")
        price = a50.get("price", "")
        pct = a50.get("pct_change", "")
        if isinstance(pct, (int, float)):
            sections.append(f"- {name}: {price} ({pct:+.2f}%)")
        else:
            sections.append(f"- {name}: {price} ({pct})")

    # 9. 沪深港通（仅保留南向，北向资金数据已停止披露）
    hsgt = data.get("hsgt_summary", [])
    if hsgt:
        south_only = [item for item in hsgt if item.get("资金方向", "") != "北向"]
        if south_only:
            sections.append("\n## 沪深港通（南向资金）")
            for item in south_only:
                sections.append(
                    f"- {item['板块']}({item['资金方向']}): 净买额{item['成交净买额']}亿, "
                    f"上涨{item.get('上涨数', 0)} 下跌{item.get('下跌数', 0)}"
                )

    # 10. 重要经济日历
    calendar = data.get("economic_calendar", [])
    important_events = [e for e in calendar if int(e.get("重要性", 0)) >= 2]
    if important_events:
        sections.append("\n## 重要经济数据")
        for e in important_events[:10]:
            sections.append(
                f"- [{e['地区']}] {e['事件']}: "
                f"公布{e['公布']}, 预期{e['预期']}, 前值{e['前值']}"
            )

    # 11. 热点新闻
    hot_news = data.get("market_hot_news", [])
    if hot_news:
        sections.append("\n## 市场热点新闻")
        for n in hot_news:
            sections.append(f"- [{n['标签']}] {n['摘要']}")

    # 12. 5 日指数走势（pm 格式，dict keyed by index code）
    index_hist = data.get("index_history_5d", {})
    code_name_map = {
        q.get("code", ""): q.get("name", "") for q in index_quotes
    }
    if isinstance(index_hist, dict) and index_hist:
        sections.append("\n## 近5日指数走势")
        for code, hist_list in index_hist.items():
            name = code_name_map.get(code, code)
            if isinstance(hist_list, list) and hist_list:
                points = [
                    f"{h.get('date', '')}: {h.get('close', '')}"
                    f"({(h.get('pct_change') or 0):+.2f}%)"
                    for h in hist_list[-5:]
                ]
                sections.append(f"- {name}: {' -> '.join(points)}")

    # 13. A 股指数历史（now 格式 — a_share_index_history，字段同 a_share_indices）
    a_idx_hist = data.get("a_share_index_history", [])
    if a_idx_hist:
        sections.append("\n## A股指数历史")
        for idx in a_idx_hist:
            name = idx.get("name", "")
            close = idx.get("last_close", idx.get("close", ""))
            pct = idx.get("pct_change", "")
            date_str = idx.get("last_date", idx.get("date", ""))
            if isinstance(pct, (int, float)):
                sections.append(f"- {name}: {close} ({pct:+.2f}%) [{date_str}]")
            else:
                sections.append(f"- {name}: {close} ({pct}) [{date_str}]")

    # 14. QVIX 波动率指数（daily）
    qvix = data.get("qvix_daily", [])
    if qvix:
        sections.append("\n## QVIX波动率指数（近期走势）")
        for item in qvix[-5:]:
            sections.append(
                f"- {item['date']}: 收{item['close']} "
                f"(高{item['high']} 低{item['low']})"
            )
        latest = qvix[-1]["close"]
        if len(qvix) >= 5:
            avg5 = sum(q["close"] for q in qvix[-5:]) / 5
            sections.append(f"- 最新: {latest}, 5日均值: {avg5:.2f}")

    # 15. QVIX 盘中（now 格式）
    qvix_intraday = data.get("qvix_intraday", [])
    if qvix_intraday:
        sections.append("\n## QVIX盘中走势")
        # 取最近几条和最新
        sample = qvix_intraday[-5:] if len(qvix_intraday) > 5 else qvix_intraday
        for item in sample:
            sections.append(
                f"- {item.get('time', '')}: {item.get('qvix', item.get('close', item.get('price', '')))}"
            )

    # 16. QVIX 近期（mid/eod 格式）
    qvix_recent = data.get("qvix_recent", [])
    if qvix_recent:
        sections.append("\n## QVIX近期走势")
        for item in qvix_recent[-5:]:
            sections.append(
                f"- {item.get('date', '')}: {item.get('close', '')}"
            )

    return "\n".join(sections)


def _postprocess(script: str) -> str:
    """后处理：清理 AI 输出中可能的多余前缀/后缀。"""
    # 去掉开头可能的 markdown 代码块标记
    script = script.strip()
    if script.startswith("```"):
        script = re.sub(r"^```\w*\n?", "", script)
    if script.endswith("```"):
        script = script[:-3].rstrip()
    return script.strip()


def _compute_stats(script: str) -> dict:
    """统计文章总字数、topic 数量、预估阅读时长。"""
    # 总字数（排除空行）
    total_chars = sum(len(line) for line in script.splitlines() if line.strip())
    # topic 数量：匹配 "1. " "2. " 或 "第一，" "第二，" 等行首编号
    topic_count = len(re.findall(
        r"^(?:\d+\.\s|第[一二三四五六七八九十]+[，,])",
        script, re.MULTILINE,
    ))
    estimated_min = total_chars / CHARS_PER_MINUTE if CHARS_PER_MINUTE else 0
    return {
        "total_chars": total_chars,
        "topic_count": topic_count,
        "estimated_minutes": round(estimated_min, 1),
    }


def generate_broadcast(data: dict, report_type: str, duration: int = 3,
                       verbose: bool = False) -> tuple[str, dict]:
    """三步 AI 流水线生成市场分析文章。

    Args:
        data: 市场数据 dict（collect_am_data / collect_now_data / collect_pm_data 返回值）
        report_type: "am" / "now" / "pm"
        duration: 目标阅读时长（分钟）
        verbose: 是否输出详细信息

    Returns:
        (final_text, stats_dict)
    """
    # 构建市场摘要
    market_summary = extract_market_summary(data)
    if verbose:
        click.echo(f"  摘要: {len(market_summary)} 字", err=True)

    # 目标字数
    target_chars = duration * CHARS_PER_MINUTE
    min_chars = int(target_chars * 0.9)
    max_chars = int(target_chars * 1.15)
    hard_limit = int(target_chars * 1.5)

    # 时间戳（含星期几，帮助 AI 正确引用"昨天""上周五"等）
    _weekdays = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    _now = datetime.now(TZ_BJ)
    beijing_now = _now.strftime("%Y年%m月%d日 %H:%M") + f" {_weekdays[_now.weekday()]}"

    # 内容框架 + 标题前缀
    content_framework = BROADCAST_CONTENT_FRAMEWORKS.get(report_type, "")
    title_prefix = BROADCAST_TITLE_PREFIXES.get(report_type, "市场分析")

    # Step 1: Draft
    click.echo("  [1/3] 起草文章...", err=True)
    draft_system = BROADCAST_DRAFT_SYSTEM_PROMPT.format(
        content_framework=content_framework,
        title_prefix=title_prefix,
    )
    user_prompt = (
        f"请根据以下A股市场数据生成分析文章。\n\n"
        f"当前北京时间: {beijing_now}\n\n"
        f"## 要求\n"
        f"- 目标阅读时长: {duration} 分钟\n"
        f"- 正文总字数控制在 {min_chars}-{max_chars} 字之间，【硬性上限 {hard_limit} 字，超出必须删减】\n"
        f"- topic 数量: 4-6 个\n"
        f"- 直接输出文章，不要任何额外前言或说明\n\n"
        f"## 市场数据\n\n{market_summary}"
    )
    draft = call_bedrock(draft_system, user_prompt)
    if verbose:
        draft_stats = _compute_stats(draft)
        click.echo(
            f"    草稿: {draft_stats['total_chars']}字 / "
            f"{draft_stats['topic_count']} topics",
            err=True,
        )

    # Step 2: Review
    click.echo("  [2/3] 审阅草稿...", err=True)
    review_input = (
        f"## 字数要求\n目标 {min_chars}-{max_chars} 字，硬性上限 {hard_limit} 字\n\n"
        f"## 原始市场数据\n\n{market_summary}\n\n"
        f"## 文章草稿\n\n{draft}"
    )
    review = call_bedrock(BROADCAST_REVIEW_SYSTEM_PROMPT, review_input)
    if verbose:
        click.echo(f"    审阅意见: {len(review)} 字", err=True)

    # Step 3: Refine
    click.echo("  [3/3] 精修文章...", err=True)
    refine_input = (
        f"## 原始市场数据\n\n{market_summary}\n\n"
        f"## 文章草稿\n\n{draft}\n\n"
        f"## 审阅意见\n\n{review}"
    )
    refined = call_bedrock(BROADCAST_REFINE_SYSTEM_PROMPT, refine_input)

    # 后处理
    final_text = _postprocess(refined)
    stats = _compute_stats(final_text)

    return final_text, stats


def generate_doc_broadcast(content: str, title: str, duration: int = 3,
                           verbose: bool = False,
                           tts: bool = False) -> tuple[str, dict]:
    """三步 AI 流水线：将研究文档转化为社交媒体风格文章。

    Args:
        content: 研究文档 markdown 内容
        title: 文档标题（用于标题行）
        duration: 目标阅读时长（分钟）
        verbose: 是否输出详细信息
        tts: TTS 模式 — 禁止股票代码和视觉符号，适配语音播报

    Returns:
        (final_text, stats_dict)
    """
    # 目标字数
    target_chars = duration * CHARS_PER_MINUTE
    min_chars = int(target_chars * 0.9)
    max_chars = int(target_chars * 1.15)
    hard_limit = int(target_chars * 1.5)

    # 时间戳
    _weekdays = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    _now = datetime.now(TZ_BJ)
    beijing_now = _now.strftime("%Y年%m月%d日 %H:%M") + f" {_weekdays[_now.weekday()]}"

    # 内容框架 + 标题前缀：TTS 模式用 doc_tts，阅读模式用 doc
    framework_key = "doc_tts" if tts else "doc"
    content_framework = BROADCAST_CONTENT_FRAMEWORKS.get(framework_key, "")
    title_prefix = BROADCAST_TITLE_PREFIXES.get("doc", "深度专题")

    # 标的信息要求：TTS 模式只保留名称，阅读模式保留代码和名称
    if tts:
        data_note = "- 只保留标的名称，禁止出现任何股票代码数字"
    else:
        data_note = "- 保留文档中的关键数据、标的代码和名称"

    # Step 1: Draft
    mode_label = "TTS" if tts else "阅读"
    click.echo(f"  [1/3] 起草文章（{mode_label}版）...", err=True)
    draft_system = BROADCAST_DRAFT_SYSTEM_PROMPT.format(
        content_framework=content_framework,
        title_prefix=title_prefix,
    )
    # TTS 模式：将格式规则中的阿拉伯数字编号替换为中文序数词
    if tts:
        draft_system = draft_system.replace(
            "编号 topic 用 `1. ` `2. ` 等阿拉伯数字 + 英文句点 + 空格",
            "编号 topic 用中文序数词（第一，第二，第三，第四）",
        ).replace(
            "1. Topic 标题：简短有力",
            "第一，Topic 标题：简短有力",
        ).replace(
            "2. Topic 标题",
            "第二，Topic 标题",
        )
    user_prompt = (
        f"请根据以下研究文档生成社交媒体风格的分析文章。\n\n"
        f"当前北京时间: {beijing_now}\n"
        f"文档标题: {title}\n\n"
        f"## 要求\n"
        f"- 目标阅读时长: {duration} 分钟\n"
        f"- 正文总字数控制在 {min_chars}-{max_chars} 字之间，【硬性上限 {hard_limit} 字，超出必须删减】\n"
        f"- topic 数量: 3-5 个\n"
        f"- 从研究文档中提炼核心观点，不要照搬原文\n"
        f"{data_note}\n"
        f"- 直接输出文章，不要任何额外前言或说明\n\n"
        f"## 研究文档原文\n\n{content}"
    )
    draft = call_bedrock(draft_system, user_prompt)
    if verbose:
        draft_stats = _compute_stats(draft)
        click.echo(
            f"    草稿: {draft_stats['total_chars']}字 / "
            f"{draft_stats['topic_count']} topics",
            err=True,
        )

    # Step 2: Review
    click.echo("  [2/3] 审阅草稿...", err=True)
    review_input = (
        f"## 字数要求\n目标 {min_chars}-{max_chars} 字，硬性上限 {hard_limit} 字\n\n"
        f"## 原始研究文档\n\n{content}\n\n"
        f"## 文章草稿\n\n{draft}"
    )
    review = call_bedrock(BROADCAST_REVIEW_SYSTEM_PROMPT, review_input)
    if verbose:
        click.echo(f"    审阅意见: {len(review)} 字", err=True)

    # Step 3: Refine — TTS 模式附加 TTS 规则
    click.echo("  [3/3] 精修文章...", err=True)
    refine_system = BROADCAST_REFINE_SYSTEM_PROMPT
    if tts:
        # 在 "直接输出" 之前插入 TTS 附加规则
        refine_system = refine_system.replace(
            "\n\n直接输出完整的精修后文章",
            BROADCAST_REFINE_TTS_ADDENDUM + "\n\n直接输出完整的精修后文章",
        )
    refine_input = (
        f"## 原始研究文档\n\n{content}\n\n"
        f"## 文章草稿\n\n{draft}\n\n"
        f"## 审阅意见\n\n{review}"
    )
    refined = call_bedrock(refine_system, refine_input)

    # 后处理
    final_text = _postprocess(refined)

    # TTS 模式：去掉【类型｜日期】标题行（视频中已有视觉展示，TTS 朗读不自然）
    if tts:
        final_text = re.sub(r"^【[^】]+】[^\n]*\n*", "", final_text)

    stats = _compute_stats(final_text)

    return final_text, stats


def get_doc_output_path(input_path: Path) -> Path:
    """从输入文件名推导输出路径。

    例: data/research/2026-03-12_电力芯片产业链分析.md
      → data/broadcasts/2026-03-12-电力芯片产业链分析_post.txt
    """
    stem = input_path.stem  # e.g. "2026-03-12_电力芯片产业链分析"
    # 将第一个下划线替换为连字符，保持与其他 broadcast 文件名风格一致
    name = stem.replace("_", "-", 1)
    return BROADCAST_DIR / f"{name}_post.txt"


def get_output_path(report_type: str) -> Path:
    """生成输出文件路径：data/broadcasts/{date}-{type}_post.txt
    now 类型带时间戳（盘中可多次运行）：{date}-now-{HHMM}_post.txt
    """
    from fsi.config import TZ_BJ, today_bj
    date_str = today_bj().isoformat()
    if report_type == "now":
        ts = datetime.now(TZ_BJ).strftime("%H%M")
        return BROADCAST_DIR / f"{date_str}-now-{ts}_post.txt"
    type_map = {"am": "am", "pm": "pm"}
    suffix = type_map.get(report_type, report_type)
    return BROADCAST_DIR / f"{date_str}-{suffix}_post.txt"


def save_broadcast(script: str, output_path: Path) -> Path:
    """保存口播稿到文件。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(script, encoding="utf-8")
    return output_path
