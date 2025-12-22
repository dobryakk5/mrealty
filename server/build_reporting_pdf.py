#!/usr/bin/env python3
"""Создаёт PDF-версию отчётного описания из Markdown."""

from __future__ import annotations

import argparse
import html
import sys
from pathlib import Path

import markdown
from bs4 import BeautifulSoup, NavigableString, Tag

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SERVER_DIR) in sys.path:
    sys.path.remove(str(SERVER_DIR))

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, Preformatted, SimpleDocTemplate

import server.reportlab as reportlab_module  # noqa: E402

DEFAULT_INPUT = SERVER_DIR / "reporting.md"
DEFAULT_OUTPUT = SERVER_DIR / "Аналитика (оценка вторички).pdf"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Генерирует PDF из server/reporting.md")
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=DEFAULT_INPUT,
        help="Путь к Markdown-файлу с документацией.",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=DEFAULT_OUTPUT, help="Куда положить готовый PDF."
    )
    return parser.parse_args()


def _ensure_fonts() -> tuple[str, str]:
    reportlab_module._ensure_fonts()
    return reportlab_module._FONT_REGULAR_NAME, reportlab_module._FONT_BOLD_NAME


def _make_styles() -> dict[str, ParagraphStyle]:
    regular_font, bold_font = _ensure_fonts()
    base = getSampleStyleSheet()
    return {
        "h1": ParagraphStyle(
            "reporting:h1",
            parent=base["Heading1"],
            fontName=bold_font,
            fontSize=20,
            leading=26,
            spaceAfter=6,
            spaceBefore=8,
        ),
        "h2": ParagraphStyle(
            "reporting:h2",
            parent=base["Heading2"],
            fontName=bold_font,
            fontSize=16,
            leading=22,
            spaceAfter=4,
            spaceBefore=6,
        ),
        "h3": ParagraphStyle(
            "reporting:h3",
            parent=base["Heading3"],
            fontName=bold_font,
            fontSize=13,
            leading=18,
            spaceAfter=3,
            spaceBefore=4,
        ),
        "body": ParagraphStyle(
            "reporting:body",
            parent=base["BodyText"],
            fontName=regular_font,
            fontSize=10.5,
            leading=14,
            spaceAfter=4,
            spaceBefore=0,
        ),
        "list_item": ParagraphStyle(
            "reporting:list",
            parent=base["BodyText"],
            fontName=regular_font,
            fontSize=10.5,
            leading=13,
            leftIndent=3 * mm,
            spaceAfter=2,
            spaceBefore=0,
        ),
        "quote": ParagraphStyle(
            "reporting:quote",
            parent=base["BodyText"],
            fontName=regular_font,
            fontSize=10.5,
            leading=14,
            textColor=colors.HexColor("#444444"),
            leftIndent=4 * mm,
            italic=True,
            spaceAfter=4,
        ),
        "code": ParagraphStyle(
            "reporting:code",
            parent=base["Code"],
            fontName="Courier",
            fontSize=8.5,
            leading=10,
            leftIndent=4 * mm,
            rightIndent=4 * mm,
            spaceBefore=4,
            spaceAfter=6,
            backColor=colors.HexColor("#f6f6f6"),
            borderColor=colors.HexColor("#d9d9d9"),
            borderWidth=0.5,
            borderPadding=4,
        ),
    }


def _render_inline(node: Tag | NavigableString) -> str:
    if isinstance(node, NavigableString):
        text = str(node).replace("\n", " ")
        return html.escape(text)
    if not isinstance(node, Tag):
        return ""
    name = node.name.lower()
    if name in {"strong", "b"}:
        inner = "".join(_render_inline(child) for child in node.children)
        return f"<b>{inner}</b>"
    if name in {"em", "i"}:
        inner = "".join(_render_inline(child) for child in node.children)
        return f"<i>{inner}</i>"
    if name == "code":
        return f'<font face="Courier">{html.escape(node.get_text())}</font>'
    if name == "a":
        inner = "".join(_render_inline(child) for child in node.children)
        href = node.get("href")
        if href:
            return f'{inner} <font size="8">({html.escape(href)})</font>'
        return inner
    if name in {"ul", "ol"}:
        return ""
    if name == "br":
        return "<br/>"
    return "".join(_render_inline(child) for child in node.children)


def _render_list(
    tag: Tag,
    story: list,
    styles: dict[str, ParagraphStyle],
    ordered: bool = False,
    depth: int = 0,
) -> None:
    count = 0
    for li in tag.find_all("li", recursive=False):
        count += 1
        text = _render_inline(li).strip()
        if not text:
            continue
        prefix = f"{count}. " if ordered else "• "
        indent = styles["list_item"].leftIndent + depth * (4 * mm)
        item_style = ParagraphStyle(
            f"reporting:list-depth-{depth}-{count}",
            parent=styles["list_item"],
            leftIndent=indent,
        )
        story.append(Paragraph(prefix + text, item_style))
        for nested in li.find_all(("ul", "ol"), recursive=False):
            _render_list(
                nested,
                story,
                styles,
                ordered=nested.name.lower() == "ol",
                depth=depth + 1,
            )


def _collect_story(html_root: Tag, styles: dict[str, ParagraphStyle]) -> list:
    story: list = []
    for node in html_root.children:
        if isinstance(node, NavigableString) and not node.strip():
            continue
        if not isinstance(node, Tag):
            continue
        name = node.name.lower()
        if name in {"h1", "h2", "h3"}:
            text = _render_inline(node).strip()
            if text:
                story.append(Paragraph(text, styles.get(name, styles["h3"])))
            continue
        if name == "p":
            text = _render_inline(node).strip()
            if text:
                story.append(Paragraph(text, styles["body"]))
            continue
        if name in {"ul", "ol"}:
            _render_list(node, story, styles, ordered=name == "ol")
            continue
        if name == "pre":
            code_text = node.get_text()
            story.append(Preformatted(code_text.rstrip("\n"), styles["code"]))
            continue
        if name == "blockquote":
            text = _render_inline(node).strip()
            if text:
                story.append(Paragraph(text, styles["quote"]))
            continue
        text = _render_inline(node).strip()
        if text:
            story.append(Paragraph(text, styles["body"]))
    return story


def build_pdf(input_path: Path, output_path: Path) -> None:
    markdown_text = input_path.read_text(encoding="utf-8")
    html_body = markdown.markdown(markdown_text, extensions=["fenced_code", "tables"])
    soup = BeautifulSoup(html_body, "html.parser")
    styles = _make_styles()
    story = _collect_story(soup, styles)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=16 * mm,
        rightMargin=16 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title="Аналитика (оценка вторички)",
    )
    doc.build(story)


def main() -> None:
    args = _parse_args()
    input_path = args.input.expanduser().resolve()
    output_path = args.output.expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f"Файл {input_path} не найден.")
    build_pdf(input_path, output_path)
    print(f"PDF подготовлен: {output_path}")


if __name__ == "__main__":
    main()
