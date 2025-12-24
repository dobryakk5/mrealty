from __future__ import annotations

import html
import json
import os
import re
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

    inter_dir = Path(__file__).resolve().parent / "Inter"
    inter_regular = inter_dir / "Inter-Regular.ttf"
    inter_var = inter_dir / "Inter-VariableFont_opsz,wght.ttf"
    if inter_regular.exists():
        try:
            pdfmetrics.registerFont(TTFont("Inter", str(inter_regular)))
            if inter_var.exists():
                pdfmetrics.registerFont(TTFont("Inter-Bold", str(inter_var)))
                _FONT_BOLD_NAME = "Inter-Bold"
            else:
                _FONT_BOLD_NAME = "Inter"
            _FONT_REGULAR_NAME = "Inter"
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ Не удалось загрузить Inter: {exc}")

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


def _fmt_floor_pair(floor: Any, total: Any) -> str:
    if floor is None and total is None:
        return "—"
    if total is None:
        return _fmt_num(floor, 0)
    if floor is None:
        return f"—/{_fmt_num(total, 0)}"
    return f"{_fmt_num(floor, 0)}/{_fmt_num(total, 0)}"


def _md_to_reportlab(text: Any) -> str:
    """
    Преобразует простую Markdown-разметку (**bold**, *italic*) в формат,
    понятный reportlab Paragraph (HTML-подмножество).
    """
    if text is None:
        return "—"
    escaped = html.escape(str(text), quote=False)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(r"__(.+?)__", r"<b>\1</b>", escaped)
    escaped = re.sub(r"(?<!\*)\*(?!\*)([^*]+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", escaped)
    escaped = re.sub(r"(?<!_)_(?!_)([^_]+?)(?<!_)_(?!_)", r"<i>\1</i>", escaped)
    return escaped.replace("\n", "<br/>")


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

    def _short_url(value: Any, limit: int = 40) -> str:
        if not value:
            return "—"
        text = str(value)
        if len(text) <= limit:
            return text
        return text[: limit - 1] + "…"

    def _safe_ratio(diff: float, base: float) -> float:
        if base is None or base == 0:
            return 0.0
        try:
            return abs(diff) / abs(base)
        except Exception:
            return 0.0

    def _to_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except Exception:
            return None

    def _format_signed_num(value: Any, digits: int = 0) -> str:
        num = _to_float(value)
        if num is None:
            return "—"
        prefix = "+" if num > 0 else ""
        formatted = _fmt_num(num, digits)
        if num > 0 and not formatted.startswith("+"):
            return prefix + formatted
        return formatted

    def _fmt_year(value: Any) -> str:
        num = _to_float(value)
        if num is None:
            return "—"
        try:
            return str(int(round(num)))
        except Exception:
            return str(value)

    def _score_house_type(value: Any) -> int:
        text = (str(value) or "").lower()
        if "кирп" in text:
            return 4
        if "монолит" in text:
            return 3
        if "блоч" in text:
            return 2
        if "панел" in text:
            return 1
        return 0

    def _score_renovation(value: Any) -> int:
        text = (str(value) or "").lower()
        if "евро" in text:
            return 2
        if "космет" in text:
            return 1
        return 0

    def _score_balcony(value: Any) -> int:
        if value is None:
            return 0
        text = str(value).lower()
        if not text.strip():
            return 0
        if "нет" in text or "без" in text:
            return 0
        return 1

    def _numeric_diff(base: Any, val: Any, digits: int = 0) -> str:
        base_num = _to_float(base)
        val_num = _to_float(val)
        if base_num is None or val_num is None:
            return "—"
        return _format_signed_num(val_num - base_num, digits)

    def _numeric_diff_percent(base: Any, val: Any, digits: int = 1) -> str:
        base_num = _to_float(base)
        val_num = _to_float(val)
        if base_num is None or val_num is None or base_num == 0:
            return "—"
        diff_pct = (val_num - base_num) / base_num * 100
        return _format_signed_num(diff_pct, digits) + "%"

    def _score_generic(direction: str, base: Any, val: Any) -> Optional[float]:
        base_num = _to_float(base)
        val_num = _to_float(val)
        if base_num is None or val_num is None:
            return None
        if direction == "higher_better":
            return val_num - base_num
        if direction == "lower_better":
            return base_num - val_num
        return None

    def _score_enum(score_fn, base: Any, val: Any) -> Optional[float]:
        if base is None or val is None:
            return None
        try:
            return float(score_fn(val) - score_fn(base))
        except Exception:
            return None

    def _color_for_score(score: Optional[float]):
        if score is None:
            return None
        if score < 0:
            return colors.red
        if score > 0:
            return colors.green
        return None

    def _format_enum_diff(score: Optional[float]) -> str:
        if score is None:
            return "—"
        if score > 0:
            return "лучше"
        if score < 0:
            return "хуже"
        return "равно"

    def _fmt_thousands(value: Any) -> str:
        num = _to_float(value)
        if num is None:
            return "—"
        return f"{_fmt_num(num / 1000, 0)} тыс"

    def _history_events(entries: Any) -> list[tuple[str, float]]:
        events: list[tuple[str, float]] = []
        if not entries:
            return events
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            ts = entry.get("ts") or entry.get("updated")
            if ts is None:
                continue
            price = _to_float(entry.get("price"))
            if price is None:
                continue
            ts_raw = str(ts)
            date_label = ts_raw[:10]
            events.append((date_label, price))
        events.sort(key=lambda x: x[0])
        return events

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
    price_history = report.get("price_history", {}) or {}
    subject_history = price_history.get("subject") or []
    competitors_history = price_history.get("competitors") or []
    numbered_top = list(enumerate(top, start=1))
    top_display = numbered_top[:7]
    history_map: Dict[tuple[Any, Any, Any], Any] = {
        (item.get("house_id"), item.get("floor"), item.get("rooms")): item.get("history") or []
        for item in competitors_history
        if isinstance(item, dict)
    }

    key = meta.get("key", {}) or {}
    subject_key = (key.get("house_id"), key.get("floor"), key.get("rooms"))
    history_map[subject_key] = subject_history
    gen_at = meta.get("generated_at")
    if gen_at:
        # на всякий случай — оставляем как строку
        gen_at_str = str(gen_at)
    else:
        gen_at_str = datetime.now().isoformat(timespec="seconds")

    def _append_current_price(h_map: Dict[tuple[Any, Any, Any], Any], k: tuple[Any, Any, Any], price: Any) -> None:
        if price is None:
            return
        ts_entry = {"ts": gen_at_str, "price": price}
        if k in h_map and isinstance(h_map[k], list):
            h_map[k].append(ts_entry)
        else:
            h_map[k] = [ts_entry]

    _append_current_price(history_map, subject_key, subject.get("price"))
    for _, item in top_display:
        h_key = (item.get("house_id"), item.get("floor"), item.get("rooms"))
        _append_current_price(history_map, h_key, item.get("price"))

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

    story.append(Paragraph(f"<b>Радиус анализа:</b> {meta.get('radius_m', '—')} м", style_p))
    story.append(Paragraph(f"<b>Сформирован:</b> {gen_at_str}", style_p))
    story.append(Spacer(1, 6 * mm))

    # 1) Объект
    story.append(Paragraph("1. Объект оценки", style_h2))
    rooms_val = subject.get("rooms")
    room_label = f"{_fmt_num(rooms_val, 0)}к квартира" if rooms_val is not None else "—"
    address = subject.get("address") or meta.get("address") or "—"
    subj_table = Table(
        [
            ["Адрес", address],
            ["Объект", room_label],
            ["Площадь, м²", _fmt_num(subject.get("area_m2"), 2)],
            ["Цена, ₽", _fmt_money(subject.get("price"))],
            ["Цена за м², ₽/м²", _fmt_num(subject.get("ppm"), 0)],
            ["URL", _short_url(subject.get("url"))],
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
    story.append(Spacer(1, 6 * mm))

    # 2) Топ конкурентов
    story.append(Paragraph("2. Топ конкурентов (выборка)", style_h2))
    if not top:
        story.append(Paragraph("Нет данных по конкурентам (проверь наличие актуальных объявлений).", style_p))
    else:
        rows = [["№", "dist_m", "price", "area_m2", "ppm", "url"]]
        for idx, item in top_display:
            rows.append(
                [
                    str(idx),
                    _fmt_num(item.get("dist_m"), 0),
                    _fmt_money(item.get("price")),
                    _fmt_num(item.get("area_m2"), 2),
                    _fmt_num(item.get("ppm"), 0),
                    (item.get("url") or "")[:70],
                ]
            )
        t = Table(rows, colWidths=[10 * mm, 18 * mm, 26 * mm, 22 * mm, 22 * mm, 76 * mm], hAlign="LEFT")
        t.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("FONTNAME", (0, 0), (-1, -1), _FONT_REGULAR_NAME),
                    ("FONTNAME", (0, 0), (-1, 0), _FONT_BOLD_NAME),
                    ("WORDWRAP", (0, 0), (-1, -1), "CJK"),
                ]
            )
        )
        story.append(t)

    story.append(Spacer(1, 6 * mm))

    top_for_matrix = top_display
    if top_for_matrix:
        story.append(PageBreak())
        story.append(Paragraph("3. Сравнение 7 конкурентов", style_h2))
        story.append(
            Paragraph(
                "Детализация ключевых параметров по 7 ближайшим аналогам (колонки) и оцениваемому лоту.",
                style_p,
            )
        )
        story.append(Spacer(1, 3 * mm))

        columns = [("Оцениваемый лот", subject)] + [(f"Аналог {num}", item) for num, item in top_for_matrix]
        subject_data = columns[0][1]
        analogs = columns[1:]

        field_specs: Sequence[Dict[str, Any]] = [
            {"label": "Цена, ₽", "key": "price", "fmt": _fmt_money, "direction": "lower_better", "digits": 0},
            {"label": "Цена за м², ₽/м²", "key": "ppm", "fmt": lambda v: _fmt_num(v, 0), "direction": "lower_better", "digits": 0},
            {"label": "Площадь, м²", "key": "area_m2", "fmt": lambda v: _fmt_num(v, 2), "direction": "higher_better", "digits": 2},
            {"label": "Кухня, м²", "key": "kitchen_area", "fmt": lambda v: _fmt_num(v, 2), "direction": "higher_better", "digits": 2},
            {"label": "Жилая, м²", "key": "living_area", "fmt": lambda v: _fmt_num(v, 2), "direction": "higher_better", "digits": 2},
            {"label": "Комнат", "key": "rooms", "fmt": lambda v: _fmt_num(v, 0), "direction": None, "digits": 0},
            {"label": "Этаж", "key": "floor", "fmt": lambda v: _fmt_num(v, 0), "direction": None, "digits": 0},
            {"label": "Этажность", "key": "total_floors", "fmt": lambda v: _fmt_num(v, 0), "direction": None, "digits": 0},
            {"label": "Год постройки", "key": "construction_year", "fmt": _fmt_year, "direction": "higher_better", "digits": 0},
            {"label": "Высота потолка, м", "key": "ceiling_height", "fmt": lambda v: _fmt_num(v, 2), "direction": "higher_better", "digits": 2},
            {"label": "Отделка", "key": "renovation", "fmt": _fmt_text, "direction": None, "score_fn": _score_renovation, "value_color": True},
            {"label": "Балкон/лоджия", "key": "balcony", "fmt": _fmt_text, "direction": "enum", "score_fn": _score_balcony},
            {"label": "Мебель", "key": "furniture", "fmt": _fmt_text, "direction": None},
            {"label": "Тип дома", "key": "house_type", "fmt": _fmt_text, "direction": None, "score_fn": _score_house_type, "value_color": True},
            {"label": "Расстояние до объекта, м", "key": "dist_m", "fmt": lambda v: _fmt_num(v, 0), "direction": "lower_better", "digits": 0},
            {"label": "Метро", "key": "metro_station", "fmt": _fmt_text, "direction": None},
            {"label": "До метро, мин", "key": "metro_time", "fmt": lambda v: _fmt_num(v, 0), "direction": "lower_better", "digits": 0},
        ]

        header = ["Параметр"] + [title for title, _ in columns]
        table_data = [header]

        table_width = doc.pagesize[0] - doc.leftMargin - doc.rightMargin
        table_font_size = 7
        table_para_style = styles["BodyText"].clone("TableText")
        table_para_style.fontName = _FONT_REGULAR_NAME
        table_para_style.fontSize = table_font_size
        table_para_style.leading = table_font_size + 1
        table_para_style_bold = styles["BodyText"].clone("TableTextBold")
        table_para_style_bold.fontName = _FONT_REGULAR_NAME
        table_para_style_bold.fontSize = table_font_size + 1
        table_para_style_bold.leading = table_para_style_bold.fontSize + 1
        colored_styles_cache: Dict[tuple[str, str], Any] = {}

        def _style_with_color(base_style, color):
            if not color:
                return base_style
            key = (base_style.name, color.hexval())
            if key not in colored_styles_cache:
                colored_style = base_style.clone(f"{base_style.name}_{color.hexval()}")
                colored_style.textColor = color
                colored_styles_cache[key] = colored_style
            return colored_styles_cache[key]
        labels = [header[0]] + [spec["label"] for spec in field_specs]
        max_label_width_pt = max(
            pdfmetrics.stringWidth(text, _FONT_REGULAR_NAME, table_font_size) for text in labels
        )
        label_width = max_label_width_pt + 6 * mm  # small padding
        value_columns = len(columns)
        min_value_width = 18 * mm
        max_label_width = table_width - value_columns * min_value_width
        if max_label_width > 0:
            label_width = min(label_width, max_label_width)
        else:
            label_width = table_width * 0.35
        available_width = max(table_width - label_width, 10 * mm)
        value_width = available_width / max(value_columns, 1)
        total_min_values = min_value_width * value_columns
        if value_width < min_value_width and total_min_values <= table_width - label_width:
            value_width = min_value_width
        col_widths = [label_width] + [value_width] * value_columns

        style_cmds = [
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("FONTSIZE", (0, 0), (-1, -1), table_font_size),
            ("FONTNAME", (0, 0), (-1, -1), _FONT_REGULAR_NAME),
            ("FONTNAME", (0, 0), (-1, 0), _FONT_BOLD_NAME),
            ("WORDWRAP", (0, 0), (-1, -1), "CJK"),
        ]

        value_rows: list[list[Any]] = []
        diff_rows: list[Dict[str, Any]] = []
        floor_row_pending: Optional[int] = None
        value_row_indices: list[tuple[Dict[str, Any], int]] = []

        for spec in field_specs:
            value_row = [spec["label"]]
            for _, data in columns:
                cell_value = spec["fmt"](data.get(spec["key"]))
                if spec["key"] == "metro_station":
                    cell_value = Paragraph(cell_value, table_para_style)
                value_row.append(cell_value)
            value_rows.append(value_row)

            if spec["key"] == "floor":
                floor_row_pending = len(table_data) + len(value_rows) - 1  # row index after adding value rows later

            direction = spec.get("direction")
            if direction:
                diff_row = [f"Δ {spec['label']}", "—"]
                base_val = subject_data.get(spec["key"])
                scores: list[Optional[float]] = []
                texts: list[str] = []

                for _, data in analogs:
                    val = data.get(spec["key"])
                    score: Optional[float]
                    if direction == "enum":
                        score = _score_enum(spec["score_fn"], base_val, val)
                        texts.append(_format_enum_diff(score))
                    else:
                        score = _score_generic(direction, base_val, val)
                        texts.append(_numeric_diff_percent(base_val, val, 1))
                    scores.append(score)

                diff_row.extend(texts)
                diff_rows.append({"row": diff_row, "scores": scores})
            value_row_indices.append((spec, len(table_data) + len(value_rows) - 1))

        table_data.extend(value_rows)

        if floor_row_pending is not None:
            for col_idx, (_, data) in enumerate(columns, start=1):
                floor_val = data.get("floor")
                total_floors = data.get("total_floors")
                if floor_val is not None and total_floors is not None and (floor_val == 1 or floor_val == total_floors):
                    style_cmds.append(("TEXTCOLOR", (col_idx, floor_row_pending), (col_idx, floor_row_pending), colors.red))

        for spec, row_idx in value_row_indices:
            if spec.get("value_color") and spec.get("score_fn"):
                base_val = subject_data.get(spec["key"])
                for col_idx, (_, data) in enumerate(columns, start=1):
                    score = _score_enum(spec["score_fn"], base_val, data.get(spec["key"]))
                    color = _color_for_score(score)
                    if color:
                        style_cmds.append(("TEXTCOLOR", (col_idx, row_idx), (col_idx, row_idx), color))

        if diff_rows:
            diff_start_idx = len(table_data)
            style_cmds.append(("LINEABOVE", (0, diff_start_idx), (-1, diff_start_idx), 0.4, colors.lightgrey))
            for offset, diff in enumerate(diff_rows):
                row_idx = diff_start_idx + offset
                row = diff["row"]
                scores = diff["scores"]
                present_scores = [s for s in scores if s is not None]
                best_positive = max((s for s in present_scores if s > 0), default=None)
                worst_negative = min((s for s in present_scores if s < 0), default=None)
                row_cells = list(row)
                for idx, score in enumerate(scores, start=2):
                    if score is None:
                        continue
                    is_best = best_positive is not None and score == best_positive
                    is_worst = worst_negative is not None and score == worst_negative
                    color = colors.green if score > 0 else colors.red if score < 0 else None
                    if color:
                        style_cmds.append(("TEXTCOLOR", (idx, row_idx), (idx, row_idx), color))
                    if color or is_best or is_worst:
                        base_style = table_para_style_bold if (is_best or is_worst) else table_para_style
                        row_cells[idx] = Paragraph(f"{row_cells[idx]}", _style_with_color(base_style, color))
                table_data.append(row_cells)

        detailed_table = Table(table_data, colWidths=col_widths, hAlign="LEFT", repeatRows=1)
        detailed_table.setStyle(TableStyle(style_cmds))
        story.append(detailed_table)
    # 4) История цен
    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("4. История цен", style_h2))

    history_sources: list[tuple[str, list[tuple[str, float]]]] = []
    subject_events = _history_events(history_map.get(subject_key, []))
    history_sources.append(("Оцениваемый лот", subject_events))
    for num, item in top_display:
        h_key = (item.get("house_id"), item.get("floor"), item.get("rooms"))
        history_sources.append((f"Аналог {num}", _history_events(history_map.get(h_key, []))))

    all_dates: list[str] = []
    seen_dates = set()
    for _, events in history_sources:
        for date_label, _ in events:
            if date_label not in seen_dates:
                seen_dates.add(date_label)
                all_dates.append(date_label)
    all_dates.sort()

    history_table_data: list[list[Any]] = []
    history_price_rows: list[list[Optional[float]]] = []
    header = ["Объект/цена(т.р.)"] + all_dates
    history_table_data.append(header)
    for label, events in history_sources:
        price_map = {date: price for date, price in events}
        row: list[Any] = [label]
        price_row: list[Optional[float]] = []
        for date_label in all_dates:
            price = price_map.get(date_label)
            row.append(_fmt_num((price / 1000) if price is not None else None, 0))
            price_row.append(price)
        history_table_data.append(row)
        history_price_rows.append(price_row)

    hist_table_width = doc.pagesize[0] - doc.leftMargin - doc.rightMargin
    hist_label_width = 36 * mm
    value_cols = max(len(all_dates), 1)
    remaining = max(hist_table_width - hist_label_width, value_cols * 12 * mm)
    value_width = remaining / value_cols if value_cols else remaining
    history_table = Table(
        history_table_data,
        colWidths=[hist_label_width] + [value_width] * value_cols,
        hAlign="LEFT",
        repeatRows=1,
    )
    hist_style_cmds = [
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("FONTNAME", (0, 0), (-1, -1), _FONT_REGULAR_NAME),
        ("FONTNAME", (0, 0), (-1, 0), _FONT_BOLD_NAME),
        ("WORDWRAP", (0, 0), (-1, -1), "CJK"),
    ]

    history_table.setStyle(TableStyle(hist_style_cmds))
    story.append(history_table)
    story.append(Spacer(1, 3 * mm))
    history_text = _md_to_reportlab(ai_commentary.history_commentary(history_sources))
    story.append(
        Paragraph(
            f"<b>Комментарий:</b> {history_text}",
            style_p,
        )
    )

    # 5) Рынок и позиционирование
    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("5. Рынок и позиционирование", style_h2))
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

    verdict_text = str(market.get("verdict", "—"))
    rec_summary_parts = []
    for label, key in (("P25", "p25"), ("P50", "p50"), ("P75", "p75")):
        price_val = suggested.get(key)
        rec_summary_parts.append(f"{label}: {_fmt_money(price_val)} ₽")
    rec_summary = "; ".join(rec_summary_parts)
    verdict_text_md = _md_to_reportlab(verdict_text)
    rec_summary_md = _md_to_reportlab(rec_summary)
    story.append(
        Paragraph(
            f"<b>Вердикт:</b> {verdict_text_md}. <b>Рекомендованные цены:</b> {rec_summary_md}.",
            style_p,
        )
    )
    story.append(Spacer(1, 3 * mm))
    market_commentary = _md_to_reportlab(ai_commentary.market_commentary(market))
    story.append(
        Paragraph(
            f"<b>Комментарий:</b> {market_commentary}",
            style_p,
        )
    )

    doc.build(story)
    return output_pdf_path
