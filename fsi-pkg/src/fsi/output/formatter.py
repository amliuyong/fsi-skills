"""JSON / Table 输出格式化"""

import json
from datetime import date, datetime
from decimal import Decimal

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def output_json(data: dict | list, indent: int = 2):
    print(json.dumps(data, cls=DecimalEncoder, ensure_ascii=False, indent=indent))


def output_table(rows: list[dict], title: str = ""):
    if not rows:
        print("无数据")
        return

    console = Console()
    table = Table(title=title, show_lines=False)

    cols = list(rows[0].keys())
    for col in cols:
        table.add_column(col, justify="right" if col != "name" else "left")

    for row in rows:
        table.add_row(*[_fmt(row.get(c)) for c in cols])

    console.print(table)


def output(data, fmt: str = "json", title: str = ""):
    if fmt == "table":
        # table 模式下渲染 AI 分析 markdown
        ai_text = None
        if isinstance(data, dict):
            ai_text = data.get("ai_analysis")
            if "data" in data:
                output_table(data["data"], title=title)
            else:
                output_table([data], title=title)
        elif isinstance(data, list):
            output_table(data, title=title)
        else:
            output_json(data)
        if ai_text and isinstance(ai_text, str):
            Console().print(Panel(Markdown(ai_text), title="AI 分析", border_style="blue"))
    else:
        output_json(data)


def _fmt(val) -> str:
    if val is None:
        return "-"
    if isinstance(val, float):
        return f"{val:.4f}" if abs(val) < 1 else f"{val:.2f}"
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    return str(val)
