import argparse
from pathlib import Path

import pandas as pd


def preview_excel(path: str, sheet: str | int | None, rows: int) -> None:
    file_path = Path(path).expanduser()
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    sheet_name = sheet if sheet else 0
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=rows)
    except ValueError as exc:
        # Surface available sheet names if a wrong sheet was provided.
        if sheet:
            workbook = pd.ExcelFile(file_path)
            available = ", ".join(workbook.sheet_names)
            raise ValueError(f"{exc}. Available sheets: {available}") from exc
        raise

    print(f"Preview from {file_path} (sheet={sheet_name!r}) — showing up to {rows} rows")
    print(df.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print the first N rows from an Excel sheet without loading the entire file."
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
        default=10,
        help="Number of rows to show from the top of the sheet (default: 10).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    preview_excel(args.path, args.sheet, args.rows)
