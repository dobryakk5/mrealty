import argparse
from pathlib import Path

import pandas as pd


def infer_sql_type(series: pd.Series) -> str:
    """
    Infer a reasonable PostgreSQL column type from a pandas Series.
    Falls back to TEXT when unsure.
    """
    s = series.dropna()
    if s.empty:
        return "TEXT"

    if pd.api.types.is_bool_dtype(s):
        return "BOOLEAN"

    if pd.api.types.is_integer_dtype(s):
        return "BIGINT"

    if pd.api.types.is_float_dtype(s):
        # Treat float columns that only contain whole numbers as integers.
        if (s % 1 == 0).all():
            return "BIGINT"
        return "DOUBLE PRECISION"

    if pd.api.types.is_datetime64_any_dtype(s):
        return "TIMESTAMP"

    return "TEXT"


def build_create_table_sql(df: pd.DataFrame, table_name: str) -> str:
    column_defs = []
    for col in df.columns:
        col_type = infer_sql_type(df[col])
        column_defs.append(f'"{col}" {col_type}')
    columns_sql = ",\n  ".join(column_defs)
    return f'DROP TABLE IF EXISTS "{table_name}";\nCREATE TABLE "{table_name}" (\n  {columns_sql}\n);'


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a PostgreSQL CREATE TABLE statement from an Excel sheet."
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
        "-n",
        "--sample-rows",
        type=int,
        default=500,
        help="Number of rows to read for type inference (default: 500).",
    )
    parser.add_argument(
        "-t",
        "--table",
        default="realty_data_raw",
        help='Target table name for the CREATE TABLE statement (default: "realty_data_raw").',
    )
    args = parser.parse_args()

    file_path = Path(args.path).expanduser()
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_excel(file_path, sheet_name=args.sheet if args.sheet else 0, nrows=args.sample_rows)

    sql = build_create_table_sql(df, args.table)
    print(sql)


if __name__ == "__main__":
    main()
