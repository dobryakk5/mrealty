"""Отладочный скрипт: вызывает SQL-процедуры и печатает report_json."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_env() -> str:
    env_file = os.getenv("REPORT_ENV_FILE") or (REPO_ROOT / ".env")
    if Path(env_file).exists():
        load_dotenv(env_file)
    dsn = os.getenv("FLAT_REPORTS_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("Не задана переменная FLAT_REPORTS_DSN / DATABASE_URL")
    return dsn


def main() -> None:
    # Базовые параметры (подставьте свои при необходимости)
    tg_user_id = 123456789
    house_id = 96993
    floor = 13
    rooms = 2
    radius_m = 1500
    area_pct = 0.15
    floor_delta = 2
    limit_days = 30

    dsn = _load_env()

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
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
                    area_pct,
                    floor_delta,
                    limit_days,
                ),
            )
            cur.fetchone()

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT report_json
                FROM users.flat_reports
                WHERE tg_user_id = %s
                  AND house_id = %s
                  AND floor = %s
                  AND rooms = %s
                  AND radius_m = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (tg_user_id, house_id, floor, rooms, radius_m),
            )
            row = cur.fetchone()
            if not row:
                print("Отчёт не найден")
                return

    print(json.dumps(row["report_json"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
