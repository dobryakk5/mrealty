from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import psycopg2
from psycopg2.extras import RealDictCursor

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# При загрузке через spec_from_file_location нет пакета server.*, поэтому пробуем оба варианта.
try:
    from .formatting import fmt_money as _fmt_money, fmt_num as _fmt_num  # type: ignore
    from .report_ai_commentary import ReportAICommentary  # type: ignore
except Exception:  # pragma: no cover - fallback for direct import
    from server.formatting import fmt_money as _fmt_money, fmt_num as _fmt_num  # type: ignore
    from server.report_ai_commentary import ReportAICommentary  # type: ignore


def fetch_latest_report_json(
    dsn: str,
    tg_user_id: int,
    house_id: int,
    floor: int,
    rooms: int,
    radius_m: int = 1500,
    report_date: Optional[str] = None,  # 'YYYY-MM-DD' или None = сегодня/последний
) -> Dict[str, Any]:
    """
    Забирает report_json из users.flat_reports.
    Если report_date=None — берёт самый свежий по updated_at.
    """
    sql = """
        SELECT report_json
        FROM users.flat_reports
        WHERE tg_user_id = %(tg_user_id)s
          AND house_id   = %(house_id)s
          AND floor      = %(floor)s
          AND rooms      = %(rooms)s
          AND radius_m   = %(radius_m)s
          AND (%(report_date)s::date IS NULL OR report_date = %(report_date)s::date)
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
    """
    with psycopg2.connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            sql,
            dict(
                tg_user_id=tg_user_id,
                house_id=house_id,
                floor=floor,
                rooms=rooms,
                radius_m=radius_m,
                report_date=report_date,
            ),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("Отчёт не найден в users.flat_reports по заданным ключам.")
        return row["report_json"]



# Настройка шрифтов ---------------------------------------------------------
_FONT_INITIALIZED = False
_FONT_REGULAR_NAME = "Helvetica"
_FONT_BOLD_NAME = "Helvetica-Bold"
_FONT_CUSTOM_REG = "ReportSans"
_FONT_CUSTOM_BOLD = "ReportSansBold"
_FONT_FALLBACK_PAIRS = [
    ("Inter-V.ttf", "Inter-V.ttf"),
    ("Inter-Regular.ttf", "Inter-Bold.ttf"),
    ("Inter-Regular.otf", "Inter-Bold.otf"),
    ("DejaVuSans.ttf", "DejaVuSans-Bold.ttf"),
    ("ArialUnicode.ttf", "ArialUnicodeBold.ttf"),
]


def _font_candidates(filename: str) -> Sequence[str]:
    """Возвращает пути, где может лежать шрифт."""
    repo_dir = Path(__file__).resolve().parent
    env_path = os.getenv("REPORT_FONT_PATH")
    candidates = [
        env_path,
        repo_dir / filename,
        repo_dir / "fonts" / filename,
        repo_dir / "Inter" / filename,
        Path("/usr/share/fonts/truetype/dejavu") / filename,
        Path("/usr/local/share/fonts") / filename,
        Path("/Library/Fonts") / filename,
        Path("/System/Library/Fonts/Supplemental") / filename,
    ]
    extra = os.getenv("REPORT_FONT_EXTRA_PATHS")
    if extra:
        for chunk in extra.split(":"):
            candidates.append(Path(chunk) / filename)
    return [str(Path(path).expanduser()) for path in candidates if path]


def _try_register_font(font_name: str, filenames: Sequence[str]) -> bool:
    """Пытается зарегистрировать шрифт по списку кандидатов."""
    for candidate in filenames:
        path = Path(candidate)
        if not path.exists():
            continue
        try:
            if font_name not in pdfmetrics.getRegisteredFontNames():
                pdfmetrics.registerFont(TTFont(font_name, str(path)))
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ Не удалось зарегистрировать шрифт {path}: {exc}")
    return False


def _ensure_fonts() -> None:
    """Включает шрифты с поддержкой кириллицы (если доступны)."""
    global _FONT_INITIALIZED, _FONT_REGULAR_NAME, _FONT_BOLD_NAME
    if _FONT_INITIALIZED:
        return

    env_reg = os.getenv("REPORT_FONT_REGULAR")
    env_bold = os.getenv("REPORT_FONT_BOLD")
    custom_font_used = False
    if env_reg and _try_register_font(_FONT_CUSTOM_REG, [env_reg]):
        _FONT_REGULAR_NAME = _FONT_CUSTOM_REG
        custom_font_used = True
    if env_bold and _try_register_font(_FONT_CUSTOM_BOLD, [env_bold]):
        _FONT_BOLD_NAME = _FONT_CUSTOM_BOLD
    elif custom_font_used:
        _FONT_BOLD_NAME = _FONT_REGULAR_NAME

    if _FONT_REGULAR_NAME == "Helvetica":
        for regular_file, bold_file in _FONT_FALLBACK_PAIRS:
            if _try_register_font(_FONT_CUSTOM_REG, _font_candidates(regular_file)):
                _FONT_REGULAR_NAME = _FONT_CUSTOM_REG
                if _try_register_font(_FONT_CUSTOM_BOLD, _font_candidates(bold_file)):
                    _FONT_BOLD_NAME = _FONT_CUSTOM_BOLD
                else:
                    _FONT_BOLD_NAME = _FONT_CUSTOM_REG
                break

    _FONT_INITIALIZED = True


def build_flat_report_pdf(
    report_json: Dict[str, Any] | str,
    output_pdf_path: str,
    title: str = "Отчет по квартире",
) -> str:
    """
    Превращает report_json (как dict или json-строка) в PDF и возвращает путь к файлу.
    """
    _ensure_fonts()

    if isinstance(report_json, str):
        report = json.loads(report_json)
    else:
        report = report_json

    def _fmt_text(value: Any) -> str:
        if value is None or value == "":
            return "—"
        if isinstance(value, bool):
            return "да" if value else "нет"
        if isinstance(value, (list, tuple)):
            joined = ", ".join(str(v) for v in value if v is not None)
            return joined or "—"
        return str(value)

    def _fmt_floor_pair(floor: Any, total: Any) -> str:
        if floor is None and total is None:
            return "—"
        if total is None:
            return _fmt_num(floor, 0)
        if floor is None:
            return f"—/{_fmt_num(total, 0)}"
        return f"{_fmt_num(floor, 0)}/{_fmt_num(total, 0)}"

    def _short_url(value: Any, limit: int = 40) -> str:
        if not value:
            return "—"
        text = str(value)
        if len(text) <= limit:
            return text
        return text[: limit - 1] + "…"

    styles = getSampleStyleSheet()
    style_h = styles["Heading1"]
    style_h2 = styles["Heading2"]
    style_p = styles["BodyText"]
    style_h.fontName = _FONT_BOLD_NAME
    style_h2.fontName = _FONT_BOLD_NAME
    style_p.fontName = _FONT_REGULAR_NAME

    meta = report.get("meta", {}) or {}
    subject = report.get("subject", {}) or {}
    market = report.get("market", {}) or {}
    top = report.get("top_competitors", []) or []

    key = meta.get("key", {}) or {}
    gen_at = meta.get("generated_at")
    if gen_at:
        # на всякий случай — оставляем как строку
        gen_at_str = str(gen_at)
    else:
        gen_at_str = datetime.now().isoformat(timespec="seconds")

    doc = SimpleDocTemplate(
        output_pdf_path,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title=title,
    )

    story = []
    story.append(Paragraph(title, style_h))
    story.append(Spacer(1, 6 * mm))

    story.append(
        Paragraph(
            f"<b>Ключ квартиры:</b> house_id={key.get('house_id','—')}, этаж={key.get('floor','—')}, комнаты={key.get('rooms','—')}",
            style_p,
        )
    )
    story.append(Paragraph(f"<b>Радиус анализа:</b> {meta.get('radius_m', '—')} м", style_p))
    story.append(Paragraph(f"<b>Сформирован:</b> {gen_at_str}", style_p))
    story.append(Spacer(1, 6 * mm))

    # 1) Объект
    story.append(Paragraph("1. Объект оценки", style_h2))
    subj_table = Table(
        [
            ["Площадь, м²", _fmt_num(subject.get("area_m2"), 2)],
            ["Цена, ₽", _fmt_money(subject.get("price"))],
            ["Цена за м², ₽/м²", _fmt_num(subject.get("ppm"), 0)],
        ],
        colWidths=[60 * mm, 110 * mm],
        hAlign="LEFT",
    )
    subj_table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("FONTNAME", (0, 0), (-1, -1), _FONT_REGULAR_NAME),
                ("FONTNAME", (0, 0), (-1, 0), _FONT_BOLD_NAME),
            ]
        )
    )
    ai_commentary = ReportAICommentary()

    story.append(subj_table)
    story.append(Spacer(1, 3 * mm))
    story.append(
        Paragraph(
            f"<b>Комментарий:</b> {ai_commentary.subject_commentary(subject, meta)}",
            style_p,
        )
    )
    story.append(Spacer(1, 6 * mm))

    # 2) Рынок
    story.append(Paragraph("2. Рынок и позиционирование", style_h2))
    suggested = market.get("suggested_price") or {}
    market_table = Table(
        [
            ["Конкурентов учтено", str(market.get("competitors_count", "—"))],
            ["P25 (₽/м²)", _fmt_num(market.get("ppm_p25"), 0)],
            ["P50 (₽/м²)", _fmt_num(market.get("ppm_p50"), 0)],
            ["P75 (₽/м²)", _fmt_num(market.get("ppm_p75"), 0)],
            ["Позиция (0..1)", _fmt_num(market.get("position01"), 3)],
            ["Вердикт", str(market.get("verdict", "—"))],
            ["Рекоменд. цена P25, ₽", _fmt_money(suggested.get("p25"))],
            ["Рекоменд. цена P50, ₽", _fmt_money(suggested.get("p50"))],
            ["Рекоменд. цена P75, ₽", _fmt_money(suggested.get("p75"))],
        ],
        colWidths=[60 * mm, 110 * mm],
        hAlign="LEFT",
    )
    market_table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("FONTNAME", (0, 0), (-1, -1), _FONT_REGULAR_NAME),
                ("FONTNAME", (0, 0), (-1, 0), _FONT_BOLD_NAME),
            ]
        )
    )
    story.append(market_table)
    story.append(Spacer(1, 3 * mm))
    story.append(
        Paragraph(
            f"<b>Комментарий:</b> {ai_commentary.market_commentary(market)}",
            style_p,
        )
    )
    story.append(Spacer(1, 6 * mm))

    # 3) Топ конкурентов
    story.append(Paragraph("3. Топ конкурентов (выборка)", style_h2))
    if not top:
        story.append(Paragraph("Нет данных по конкурентам (проверь наличие актуальных объявлений).", style_p))
    else:
        rows = [["dist_m", "price", "area_m2", "ppm", "url"]]
        for item in top[:50]:
            rows.append(
                [
                    _fmt_num(item.get("dist_m"), 0),
                    _fmt_money(item.get("price")),
                    _fmt_num(item.get("area_m2"), 2),
                    _fmt_num(item.get("ppm"), 0),
                    (item.get("url") or "")[:70],
                ]
            )
        t = Table(rows, colWidths=[18 * mm, 26 * mm, 22 * mm, 22 * mm, 82 * mm], hAlign="LEFT")
        t.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("FONTNAME", (0, 0), (-1, -1), _FONT_REGULAR_NAME),
                    ("FONTNAME", (0, 0), (-1, 0), _FONT_BOLD_NAME),
                ]
            )
        )
        story.append(t)

    story.append(Spacer(1, 3 * mm))
    story.append(
        Paragraph(
            f"<b>Комментарий:</b> {ai_commentary.competitors_commentary(top, subject)}",
            style_p,
        )
    )

    top_for_matrix = top[:5]
    if top_for_matrix:
        story.append(PageBreak())
        story.append(Paragraph("4. Сравнение 5 конкурентов", style_h2))
        story.append(
            Paragraph(
                "Детализация ключевых параметров по 5 ближайшим аналогам (колонки) и оцениваемому лоту.",
                style_p,
            )
        )
        story.append(Spacer(1, 3 * mm))

        columns = [("Оцениваемый лот", subject)] + [
            (f"Аналог {idx + 1}", item) for idx, item in enumerate(top_for_matrix)
        ]

        field_rows: Sequence[tuple[str, Any]] = [
            ("Цена, ₽", lambda item: _fmt_money(item.get("price"))),
            ("Цена за м², ₽/м²", lambda item: _fmt_num(item.get("ppm"), 0)),
            ("Площадь, м²", lambda item: _fmt_num(item.get("area_m2"), 2)),
            ("Кухня, м²", lambda item: _fmt_num(item.get("kitchen_area"), 2)),
            ("Жилая, м²", lambda item: _fmt_num(item.get("living_area"), 2)),
            ("Комнат", lambda item: _fmt_num(item.get("rooms"), 0)),
            ("Этаж/этажность", lambda item: _fmt_floor_pair(item.get("floor"), item.get("total_floors"))),
            ("Год постройки", lambda item: _fmt_num(item.get("construction_year"), 0)),
            ("Высота потолка, м", lambda item: _fmt_num(item.get("ceiling_height"), 2)),
            ("Отделка", lambda item: _fmt_text(item.get("renovation"))),
            ("Балкон/лоджия", lambda item: _fmt_text(item.get("balcony"))),
            ("Мебель", lambda item: _fmt_text(item.get("furniture"))),
            ("Тип дома", lambda item: _fmt_text(item.get("house_type"))),
            ("Расстояние до объекта, м", lambda item: _fmt_num(item.get("dist_m"), 0)),
            ("Метро", lambda item: _fmt_text(item.get("metro_station"))),
            ("До метро, мин", lambda item: _fmt_num(item.get("metro_time"), 0)),
            ("URL", lambda item: _short_url(item.get("url"))),
        ]

        header = ["Параметр"] + [title for title, _ in columns]
        table_data = [header]
        for label, getter in field_rows:
            row = [label]
            for _, data in columns:
                try:
                    row.append(getter(data))
                except Exception:
                    row.append("—")
            table_data.append(row)

        table_width = doc.pagesize[0] - doc.leftMargin - doc.rightMargin
        label_width = 48 * mm
        value_columns = len(columns)
        remaining_width = max(table_width - label_width, 60 * mm)
        value_width = remaining_width / max(value_columns, 1)
        col_widths = [label_width] + [value_width] * value_columns

        detailed_table = Table(table_data, colWidths=col_widths, hAlign="LEFT", repeatRows=1)
        detailed_table.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("FONTNAME", (0, 0), (-1, -1), _FONT_REGULAR_NAME),
                    ("FONTNAME", (0, 0), (-1, 0), _FONT_BOLD_NAME),
                ]
            )
        )
        story.append(detailed_table)

    doc.build(story)
    return output_pdf_path
