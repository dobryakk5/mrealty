"""Утилита для генерации PDF отчёта напрямую из БД."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SERVER_DIR) in sys.path:
    sys.path.remove(str(SERVER_DIR))

from server.reportlab import build_flat_report_pdf, fetch_latest_report_json  # noqa: E402


def _get_dsn() -> str:
    load_dotenv(os.getenv("REPORT_ENV_FILE", Path(REPO_ROOT) / ".env"))
    dsn = os.getenv("FLAT_REPORTS_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("Не задан FLAT_REPORTS_DSN / DATABASE_URL")
    return dsn


def _run_report_procs(
    dsn: str,
    tg_user_id: int,
    house_id: int,
    floor: int,
    rooms: int,
    radius_m: int,
    analogs_area_ratio: float,
    analogs_floor_delta: int,
    analogs_days_limit: int,
) -> None:
    """Вызывает SQL-функции, которые собирают report_json в users.flat_reports."""
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT users.build_flat_report(%s, %s, %s, %s, %s);",
            (tg_user_id, house_id, floor, rooms, radius_m),
        )
        cur.fetchone()
        cur.execute(
            "SELECT users.build_flat_report_analogs(%s, %s, %s, %s, %s, %s, %s, %s);",
            (
                tg_user_id,
                house_id,
                floor,
                rooms,
                radius_m,
                analogs_area_ratio,
                analogs_floor_delta,
                analogs_days_limit,
            ),
        )
        cur.fetchone()


def main() -> None:
    # Бейз-значения — подставь свои при необходимости
    tg_user_id = 123456789
    house_id = 96993
    floor = 13
    rooms = 2
    radius_m = 500
    report_date = None
    regenerate = True
    analogs_area_ratio = 0.15
    analogs_floor_delta = 4
    analogs_days_limit = 300
    output_path = Path("flat_report.pdf")

    dsn = _get_dsn()

    if regenerate:
        _run_report_procs(
            dsn,
            tg_user_id,
            house_id,
            floor,
            rooms,
            radius_m,
            analogs_area_ratio,
            analogs_floor_delta,
            analogs_days_limit,
        )

    params = dict(
        tg_user_id=tg_user_id,
        house_id=house_id,
        floor=floor,
        rooms=rooms,
        radius_m=radius_m,
        report_date=report_date,
    )

    report_json = fetch_latest_report_json(dsn=dsn, **params)
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path = build_flat_report_pdf(report_json, str(output_path))
    print(f"PDF готов: {pdf_path}")


if __name__ == "__main__":
    main()
