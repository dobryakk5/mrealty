"""Выводит определения SQL-функций build_flat_report* из БД."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_dsn() -> str:
    env_file = os.getenv("REPORT_ENV_FILE") or (REPO_ROOT / ".env")
    if Path(env_file).exists():
        load_dotenv(env_file)
    dsn = os.getenv("FLAT_REPORTS_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("Не задана переменная FLAT_REPORTS_DSN / DATABASE_URL")
    return dsn


def main() -> None:
    dsn = _load_dsn()

    sql = """
        SELECT
            n.nspname AS schema,
            p.proname AS name,
            pg_get_functiondef(p.oid) AS definition
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname IN ('build_flat_report', 'build_flat_report_analogs')
        ORDER BY n.nspname, p.proname;
    """

    with psycopg2.connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        print("Функции build_flat_report* не найдены")
        return

    for row in rows:
        print(f"-- {row['schema']}.{row['name']}")
        print(row["definition"])
        print()


if __name__ == "__main__":
    main()
