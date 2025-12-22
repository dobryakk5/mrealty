import argparse
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Tuple

import asyncpg
import pandas as pd


def load_rows(path: str, sheet: str | int | None, rows: int) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet if sheet is not None else 0, nrows=rows)


def parse_table_name(table: str) -> Tuple[str, str]:
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = "public", table
    return schema, name


async def fetch_column_types(conn: asyncpg.Connection, table: str) -> Dict[str, str]:
    schema, name = parse_table_name(table)
    rows = await conn.fetch(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        """,
        schema,
        name,
    )
    return {row["column_name"]: row["data_type"] for row in rows}


def convert_value(value: Any, data_type: str) -> Any:
    if pd.isna(value):
        return None

    if data_type in {"smallint", "integer", "bigint"}:
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise ValueError(f"Cannot convert non-integer float {value} to int")
        return int(value)

    if data_type in {"numeric", "real", "double precision"}:
        return float(value)

    if data_type == "boolean":
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "t", "yes", "y"}:
                return True
            if lowered in {"0", "false", "f", "no", "n"}:
                return False
            raise ValueError(f"Cannot convert string {value} to boolean")
        return bool(value)

    if data_type.startswith("timestamp"):
        return pd.to_datetime(value).to_pydatetime()

    if data_type == "date":
        return pd.to_datetime(value).date()

    return str(value)


def normalize_row(row: Tuple[Any, ...], types: List[str]) -> Tuple[Any, ...]:
    normalized: List[Any] = []
    for value, data_type in zip(row, types):
        normalized.append(convert_value(value, data_type))
    return tuple(normalized)


async def insert_rows(dsn: str, table: str, df: pd.DataFrame) -> int:
    columns = list(df.columns)
    col_sql = ", ".join(f'"{col}"' for col in columns)
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
    sql = f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders})'

    if df.empty:
        return 0

    async with asyncpg.create_pool(dsn, statement_cache_size=0) as pool:
        async with pool.acquire() as conn:
            column_types_map = await fetch_column_types(conn, table)
            column_types = [column_types_map[col] for col in columns]
            rows = [normalize_row(tuple(row), column_types) for row in df.itertuples(index=False, name=None)]
            await conn.executemany(sql, rows)
            return len(rows)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Insert the first N rows from an Excel sheet into PostgreSQL."
    )
    parser.add_argument(
        "--dsn",
        required=True,
        help="PostgreSQL DSN, e.g. postgresql://user:pass@host:port/db",
    )
    parser.add_argument(
        "-p",
        "--path",
        default="server/Обработка БД.xlsx",
        help="Path to the Excel file (default: server/Обработка БД.xlsx)",
    )
    parser.add_argument(
        "-s",
        "--sheet",
        default=None,
        help="Sheet name or index (0-based). Defaults to the first sheet.",
    )
    parser.add_argument(
        "-r",
        "--rows",
        type=int,
        default=5,
        help="How many top rows to insert (default: 5).",
    )
    parser.add_argument(
        "-t",
        "--table",
        default="realty_data_raw",
        help='Target table name (default: "realty_data_raw").',
    )
    args = parser.parse_args()

    file_path = Path(args.path).expanduser()
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = load_rows(file_path, args.sheet, args.rows)
    inserted = await insert_rows(args.dsn, args.table, df)
    print(f"Inserted {inserted} rows into {args.table}")


if __name__ == "__main__":
    asyncio.run(main())
