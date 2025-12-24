from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

from psycopg2 import sql
from psycopg2.extras import RealDictCursor


def _fetch_columns(cursor: RealDictCursor, table: str, schema: str = "public") -> set[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {row["column_name"] for row in cursor.fetchall()}


class PublicHistoryMeta:
    """Caches available columns for public.flats_history and public.flats_changes."""

    def __init__(self, cursor: RealDictCursor) -> None:
        self.history_columns = _fetch_columns(cursor, "flats_history", "public")
        self.change_columns = _fetch_columns(cursor, "flats_changes", "public")

    def has_history(self, column: str) -> bool:
        return column in self.history_columns

    def has_change(self, column: str) -> bool:
        return column in self.change_columns


def _filter_payload(payload: Dict[str, Any], allowed: Iterable[str]) -> Tuple[list[str], list[Any]]:
    columns: list[str] = []
    values: list[Any] = []
    allowed_set = set(allowed)
    for key, value in payload.items():
        if value is None or key not in allowed_set:
            continue
        columns.append(key)
        values.append(value)
    return columns, values


def _normalize_is_actual(value: Any) -> Any:
    """Переводит булевые статусы в 1/0 для smallint-колонок."""
    if isinstance(value, bool):
        return 1 if value else 0
    return value


def sync_ad_to_public_history(
    cursor: RealDictCursor,
    ad_row: Mapping[str, Any],
    *,
    meta: Optional[PublicHistoryMeta] = None,
    timestamp: Optional[datetime] = None,
) -> Optional[int]:
    """
    Ensures flats_history reflects the current ad state and logs previous values into flats_changes.
    Returns flats_history.id if updated/inserted, otherwise None.
    """
    if meta is None:
        meta = PublicHistoryMeta(cursor)
    if not meta.history_columns:
        return None

    ts = timestamp or datetime.utcnow()
    url = ad_row.get("url")
    house_id = ad_row.get("house_id")
    floor = ad_row.get("floor")
    rooms = ad_row.get("rooms")
    price = ad_row.get("price")
    status = _normalize_is_actual(ad_row.get("status"))
    description = ad_row.get("description")
    address = ad_row.get("address")

    existing: Optional[Dict[str, Any]] = None
    if url:
        cursor.execute(
            """
            SELECT id, price, is_actual
            FROM public.flats_history
            WHERE url = %s
            ORDER BY time_source_updated DESC NULLS LAST
            LIMIT 1
            """,
            (url,),
        )
        existing = cursor.fetchone()
    if existing is None and house_id is not None and floor is not None and rooms is not None:
        cursor.execute(
            """
            SELECT id, price, is_actual
            FROM public.flats_history
            WHERE house_id = %s AND floor = %s AND rooms = %s
            ORDER BY time_source_updated DESC NULLS LAST
            LIMIT 1
            """,
            (house_id, floor, rooms),
        )
        existing = cursor.fetchone()

    def _insert_change_snapshot(history_id: int, old_price: Any, old_status: Any) -> None:
        if not meta.change_columns:
            return
        if old_price is None and old_status is None:
            return
        payload = {
            "flats_history_id": history_id,
            "updated": ts,
            "price": old_price,
            "is_actual": old_status,
            "description": description,
        }
        cols, vals = _filter_payload(payload, meta.change_columns)
        if not cols:
            return
        cursor.execute(
            sql.SQL("INSERT INTO public.flats_changes ({cols}) VALUES ({vals})").format(
                cols=sql.SQL(", ").join(sql.Identifier(col) for col in cols),
                vals=sql.SQL(", ").join(sql.Placeholder() for _ in cols),
            ),
            vals,
        )

    if existing:
        history_id = existing["id"]
        old_price = existing.get("price")
        old_status = existing.get("is_actual")
        if ((price is not None and old_price != price) or (status is not None and old_status != status)):
            _insert_change_snapshot(history_id, old_price, old_status)

        updates: Dict[str, Any] = {}
        if price is not None and meta.has_history("price"):
            updates["price"] = price
        if status is not None and meta.has_history("is_actual"):
            updates["is_actual"] = status
        if meta.has_history("time_source_updated"):
            updates["time_source_updated"] = ts
        if description and meta.has_history("description"):
            updates["description"] = description
        if updates:
            cols = list(updates.keys())
            cursor.execute(
                sql.SQL("UPDATE public.flats_history SET {set_clause} WHERE id = %s").format(
                    set_clause=sql.SQL(", ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(col)) for col in cols
                    )
                ),
                (*updates.values(), history_id),
            )
        return history_id

    # Insert minimal history if not found
    base_payload = {
        "house_id": house_id,
        "floor": floor,
        "rooms": rooms,
        "url": url,
        "price": price,
        "is_actual": status,
        "time_source_created": ts,
        "time_source_updated": ts,
        "description": description,
        "address": address,
    }
    cols, vals = _filter_payload(base_payload, meta.history_columns)
    if not cols:
        return None
    cursor.execute(
        sql.SQL("INSERT INTO public.flats_history ({cols}) VALUES ({vals}) RETURNING id").format(
            cols=sql.SQL(", ").join(sql.Identifier(col) for col in cols),
            vals=sql.SQL(", ").join(sql.Placeholder() for _ in cols),
        ),
        vals,
    )
    row = cursor.fetchone()
    return int(row["id"]) if row else None
