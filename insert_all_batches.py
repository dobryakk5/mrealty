import argparse
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Tuple

import asyncpg
import pandas as pd


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
    if not rows:
        raise ValueError(f"Table {table} not found")
    return {row["column_name"]: row["data_type"] for row in rows}


def convert_value(value: Any, data_type: str) -> Any:
    if pd.isna(value):
        return None

    if data_type in {"smallint", "integer", "bigint"}:
        try:
            if isinstance(value, float):
                if value.is_integer():
                    return int(value)
                # Non-integer float in integer column — drop to NULL.
                return None
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    if data_type in {"numeric", "real", "double precision"}:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

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


def read_chunk(path: Path, sheet: str | int | None, skip: int, batch_size: int) -> pd.DataFrame:
    # skiprows skips data rows (header is row 0).
    return pd.read_excel(
        path,
        sheet_name=sheet if sheet is not None else 0,
        skiprows=range(1, skip + 1),
        nrows=batch_size,
    )


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Insert all rows from an Excel sheet into PostgreSQL in batches."
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
        "-b",
        "--batch-size",
        type=int,
        default=19000,
        help="Number of rows per batch (default: 19000).",
    )
    parser.add_argument(
        "--start-offset",
        type=int,
        default=0,
        help="Number of data rows to skip before inserting (default: 0). Use batch_size*(N-1) to start at batch N.",
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

    async with asyncpg.create_pool(args.dsn, statement_cache_size=0) as pool:
        async with pool.acquire() as conn:
            column_types_map = await fetch_column_types(conn, args.table)
            columns = list(column_types_map.keys())
            column_types = [column_types_map[col] for col in columns]

            col_sql = ", ".join(f'"{col}"' for col in columns)
            placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
            insert_sql = f'INSERT INTO "{args.table}" ({col_sql}) VALUES ({placeholders})'

            total_inserted = 0
            offset = max(args.start_offset, 0)
            batch_idx = offset // args.batch_size + 1

            while True:
                df = read_chunk(file_path, args.sheet, offset, args.batch_size)
                if df.empty:
                    break
                # Ensure column order matches the table.
                df = df[columns]
                rows = [normalize_row(tuple(row), column_types) for row in df.itertuples(index=False, name=None)]
                print(f"Batch {batch_idx}: inserting {len(rows)} rows (offset {offset})...")
                await conn.executemany(insert_sql, rows)
                inserted_now = len(rows)
                total_inserted += inserted_now
                offset += inserted_now
                print(f"Batch {batch_idx} done. Total inserted: {total_inserted}")
                batch_idx += 1

            print(f"Done. Inserted total {total_inserted} rows into {args.table}.")


if __name__ == "__main__":
    asyncio.run(main())
