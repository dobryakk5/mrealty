"""Pipeline helper that seeds users.ads from house and nearby data."""

from __future__ import annotations

import asyncio
import logging
import os
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aiohttp
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from models import PropertyData

logger = logging.getLogger(__name__)

REPORT_PARSER_BASE_URL = os.getenv("REPORT_PARSER_BASE_URL", "http://localhost:8008")


class AdsTableMeta:
    """Metadata about the users.ads table to map friendly keys to real columns."""

    FIELD_ALIASES: Dict[str, Sequence[str]] = {
        "id": ["id"],
        "flat_id": ["flat_id", "flatid"],
        "house_id": ["house_id", "houseid"],
        "url": ["url", "link", "source_url"],
        "price": ["price", "price_raw"],
        "rooms": ["rooms", "room_count"],
        "floor": ["floor", "current_floor"],
        "total_area": ["total_area", "area"],
        "living_area": ["living_area", "life_area"],
        "kitchen_area": ["kitchen_area", "kitchen"],
        "total_floors": ["total_floors", "floors"],
        "bathroom": ["bathroom"],
        "balcony": ["balcony"],
        "renovation": ["renovation"],
        "construction_year": ["construction_year", "built_year"],
        "house_type": ["house_type"],
        "ceiling_height": ["ceiling_height"],
        "furniture": ["furniture"],
        "metro_station": ["metro_station"],
        "metro_time": ["metro_time"],
        "metro_way": ["metro_way"],
        "tags": ["tags"],
        "description": ["description"],
        "status": ["status", "is_active"],
        "is_actual": ["is_actual"],
        "source": ["source", "source_name", "source_id"],
        "ads_from": ["from", "from_source", "ads_from", "source_flag"],
        "distance_m": ["distance_m", "dist_m"],
        "created_at": ["created_at", "created"],
        "updated_at": ["updated_at", "updated"],
        "views_today": ["views_today", "today_views"],
        "person_type": ["person_type"],
        "address": ["address"],
    }

    def __init__(self, cursor: RealDictCursor) -> None:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'users' AND table_name = 'ads'
            """
        )
        rows = cursor.fetchall()
        if not rows:
            raise RuntimeError("users.ads table not found in the configured database")
        self._columns: Dict[str, str] = {
            row["column_name"].lower(): row["column_name"] for row in rows
        }

    def find_column(self, canonical: str) -> Optional[str]:
        candidates = self.FIELD_ALIASES.get(canonical, [canonical])
        for candidate in candidates:
            normalized = candidate.lower()
            if normalized in self._columns:
                return self._columns[normalized]
        return None

    def build_columns(self, payload: Dict[str, Any]) -> Tuple[List[str], List[Any]]:
        columns: List[str] = []
        values: List[Any] = []
        for canonical, value in payload.items():
            target = self.find_column(canonical)
            if not target or value is None:
                continue
            columns.append(target)
            values.append(value)
        return columns, values

    @property
    def flat_column(self) -> Optional[str]:
        return self.find_column("flat_id")

    @property
    def url_column(self) -> Optional[str]:
        return self.find_column("url")


class ReportPipeline:
    """Central class that prepares ads for PDF reports."""

    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("FLAT_REPORTS_DSN or DATABASE_URL is required")
        self._dsn = dsn

    def prepare(
        self,
        flat_id: int,
        run_parser: bool = True,
        max_history: int = 3,
        max_nearby: int = 20,
        radius_m: Optional[int] = None,
    ) -> Dict[str, Any]:
        if flat_id <= 0:
            raise ValueError("flat_id must be positive")

        with psycopg2.connect(self._dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            meta = AdsTableMeta(cur)
            user_flat = self._fetch_user_flat(cur, flat_id)
            address = user_flat.get("address")
            if not address:
                raise ValueError("user_flat must include address")
            rooms = user_flat.get("rooms")
            floor = user_flat.get("floor")
            stored_radius = user_flat.get("radius_m")
            if stored_radius is None:
                stored_radius = user_flat.get("meters")
            radius_value: Optional[int]
            if stored_radius is not None:
                try:
                    radius_value = int(stored_radius)
                except (ValueError, TypeError):
                    radius_value = None
            else:
                radius_value = None
            target_radius = radius_m or radius_value or 1000

            house_id = self._resolve_house_id(cur, address)
            user_flat["house_id"] = house_id
            self._ensure_user_flat_house_id(cur, flat_id, house_id)
            self._ensure_user_flat_radius(cur, flat_id, target_radius)
            history_rows = self._fetch_history_ads(cur, house_id, rooms, floor, max_history)
            if not history_rows:
                raise ValueError("No entries found in public.flats_history for the resolved house_id")

            price_candidate = self._extract_price(history_rows)
            nearby_rows = self._fetch_nearby_ads(
                cur, address, rooms, price_candidate, self._extract_area(history_rows), self._extract_kitchen(history_rows), target_radius
            )

            payloads = self._build_payloads(history_rows, nearby_rows, user_flat, house_id, max_nearby)
            persisted: List[Dict[str, Any]] = []
            for payload in payloads:
                ad_id = self._upsert_ad(cur, meta, payload)
                if ad_id:
                    persisted.append({"id": ad_id, "url": payload.get("url")})

            parser_result: Dict[str, List[str]] = {"parsed": [], "errors": []}
            if run_parser and persisted:
                parsed_map = asyncio.run(self._parse_property_urls([item["url"] for item in persisted if item.get("url")] ))
                for item in persisted:
                    url = item.get("url")
                    if not url or url not in parsed_map:
                        continue
                    property_data = parsed_map[url]
                    if not property_data:
                        parser_result["errors"].append(url)
                        continue
                    updated = self._apply_property_data(cur, meta, item["id"], property_data)
                    if updated:
                        parser_result["parsed"].append(url)

            conn.commit()

        return {
            "flat_id": flat_id,
            "tg_user_id": user_flat.get("tg_user_id"),
            "house_id": house_id,
            "radius_m": target_radius,
            "history_ads": len(history_rows),
            "nearby_ads": len(nearby_rows),
            "prepared_ads": len(payloads),
            "persisted_ads": len(persisted),
            "parsed_ads": parser_result["parsed"],
            "parser_errors": parser_result["errors"],
        }

    def _fetch_user_flat(self, cursor: RealDictCursor, flat_id: int) -> Dict[str, Any]:
        cursor.execute("SELECT * FROM users.user_flats WHERE id = %s", (flat_id,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"user_flat id={flat_id} not found")
        return row

    def _resolve_house_id(self, cursor: RealDictCursor, address: str) -> int:
        cursor.execute(
            "SELECT result_id FROM public.get_house_id_by_address(%s) WHERE result_id IS NOT NULL LIMIT 1",
            (address,),
        )
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"house_id could not be resolved for address {address}")
        return int(row["result_id"])

    def _fetch_history_ads(
        self,
        cursor: RealDictCursor,
        house_id: int,
        rooms: Optional[int],
        floor: Optional[int],
        limit: int,
    ) -> List[Dict[str, Any]]:
        filters: List[str] = ["house_id = %s"]
        params: List[Any] = [house_id]
        if rooms is not None:
            filters.append("rooms = %s")
            params.append(rooms)
        if floor is not None:
            filters.append("floor = %s")
            params.append(floor)
        filters_clause = " AND ".join(filters)
        cursor.execute(
            sql.SQL(
                "SELECT * FROM public.flats_history WHERE "
                + filters_clause
                + " ORDER BY time_source_updated DESC NULLS LAST LIMIT %s"
            ),
            (*params, limit),
        )
        rows = cursor.fetchall()
        if rows:
            return rows
        cursor.execute(
            "SELECT * FROM public.flats_history WHERE house_id = %s ORDER BY time_source_updated DESC NULLS LAST LIMIT %s",
            (house_id, limit),
        )
        return cursor.fetchall()

    def _ensure_user_flat_house_id(self, cursor: RealDictCursor, flat_id: int, house_id: int) -> None:
        if not cursor or not house_id:
            return
        cursor.execute(
            "UPDATE users.user_flats SET house_id = %s WHERE id = %s AND (house_id IS NULL OR house_id = 0)",
            (house_id, flat_id),
        )

    def _ensure_user_flat_radius(self, cursor: RealDictCursor, flat_id: int, radius_m: int) -> None:
        if not cursor or radius_m is None:
            return
        cursor.execute(
            "UPDATE users.user_flats SET radius_m = %s WHERE id = %s",
            (radius_m, flat_id),
        )

    def _fetch_nearby_ads(
        self,
        cursor: RealDictCursor,
        address: str,
        rooms: Optional[int],
        price: Optional[int],
        area: Optional[Decimal],
        kitchen_area: Optional[Decimal],
        radius: int,
    ) -> List[Dict[str, Any]]:
        if price is None:
            return []
        cursor.execute(
            "SELECT * FROM public.find_nearby_apartments(%s, %s, %s, %s, %s, %s)",
            (address, rooms or 0, price, area, kitchen_area, radius),
        )
        return cursor.fetchall()

    def _build_payloads(
        self,
        history_rows: List[Dict[str, Any]],
        nearby_rows: List[Dict[str, Any]],
        user_flat: Dict[str, Any],
        house_id: int,
        max_nearby: int,
    ) -> List[Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for raw in history_rows:
            payload = self._translate_history(raw, user_flat, house_id)
            norm = self._normalize_url(payload.get("url"))
            if not norm or norm in seen:
                continue
            seen.add(norm)
            payloads.append(payload)

        for raw in nearby_rows[:max_nearby]:
            payload = self._translate_nearby(raw, user_flat)
            norm = self._normalize_url(payload.get("url"))
            if not norm or norm in seen:
                continue
            seen.add(norm)
            payloads.append(payload)

        return payloads

    def _translate_history(
        self, raw: Dict[str, Any], user_flat: Dict[str, Any], house_id: int
    ) -> Dict[str, Any]:
        return {
            "flat_id": user_flat["id"],
            "house_id": raw.get("house_id") or house_id,
            "url": raw.get("url"),
            "price": raw.get("price"),
            "rooms": raw.get("rooms"),
            "floor": raw.get("floor"),
            "total_area": raw.get("area"),
            "kitchen_area": raw.get("kitchen_area"),
            "status": bool(raw.get("is_actual")),
            "is_actual": bool(raw.get("is_actual")),
            "ads_from": 0,
            "distance_m": 0,
            "address": raw.get("address") or user_flat.get("address"),
            "created_at": raw.get("time_source_created"),
            "updated_at": raw.get("time_source_updated"),
        }

    def _translate_nearby(self, raw: Dict[str, Any], user_flat: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "flat_id": user_flat["id"],
            "house_id": raw.get("house_id") or user_flat.get("house_id"),
            "url": raw.get("url"),
            "price": raw.get("price"),
            "rooms": raw.get("rooms"),
            "floor": raw.get("floor"),
            "total_area": raw.get("area"),
            "kitchen_area": raw.get("kitchen_area"),
            "status": bool(raw.get("is_active")),
            "is_actual": bool(raw.get("is_active")),
            "ads_from": 2,
            "distance_m": raw.get("distance_m"),
            "address": raw.get("address") or user_flat.get("address"),
            "created_at": raw.get("created"),
            "updated_at": raw.get("updated"),
        }

    def _extract_price(self, rows: List[Dict[str, Any]]) -> Optional[int]:
        for row in rows:
            price = row.get("price")
            if price:
                return int(price)
        return None

    def _extract_area(self, rows: List[Dict[str, Any]]) -> Optional[Decimal]:
        for row in rows:
            area = row.get("area")
            if area is not None:
                return Decimal(area)
        return None

    def _extract_kitchen(self, rows: List[Dict[str, Any]]) -> Optional[Decimal]:
        for row in rows:
            kitchen = row.get("kitchen_area")
            if kitchen is not None:
                return Decimal(kitchen)
        return None

    @staticmethod
    def _normalize_url(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        normalized = url.strip().lower()
        normalized = normalized.split("?")[0].rstrip("/")
        return normalized

    def _upsert_ad(self, cursor: RealDictCursor, meta: AdsTableMeta, payload: Dict[str, Any]) -> Optional[int]:
        url = payload.get("url")
        if not url:
            return None
        existing_id: Optional[int] = None
        if meta.flat_column and meta.url_column:
            cursor.execute(
                sql.SQL(
                    "SELECT id FROM users.ads WHERE {flat} = %s AND {url} = %s LIMIT 1"
                ).format(
                    flat=sql.Identifier(meta.flat_column),
                    url=sql.Identifier(meta.url_column),
                ),
                (payload.get("flat_id"), url),
            )
            row = cursor.fetchone()
            if row:
                existing_id = int(row["id"])
        columns, values = meta.build_columns(payload)
        if existing_id:
            if not columns:
                return existing_id
            set_clause = ", ".join(f'"{col}" = %s' for col in columns)
            cursor.execute(f"UPDATE users.ads SET {set_clause} WHERE id = %s", (*values, existing_id))
            return existing_id
        if not columns:
            return None
        cols = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join(["%s"] * len(values))
        cursor.execute(
            f"INSERT INTO users.ads ({cols}) VALUES ({placeholders}) RETURNING id",
            values,
        )
        row = cursor.fetchone()
        return int(row["id"]) if row else None

    async def _parse_property_urls(self, urls: Iterable[str]) -> Dict[str, Optional[PropertyData]]:
        parsed: Dict[str, Optional[PropertyData]] = {}
        if not REPORT_PARSER_BASE_URL:
            return parsed

        allowed_keys = set(PropertyData.__annotations__.keys())
        async with aiohttp.ClientSession() as session:
            for url in urls:
                if not url or "cian.ru" not in url:
                    continue
                try:
                    async with session.get(
                        f"{REPORT_PARSER_BASE_URL}/api/parse/ext",
                        params={"url": url},
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as response:
                        if response.status != 200:
                            raise RuntimeError(f"status={response.status}")
                        payload = await response.json()
                        data = payload.get("data")
                        if not isinstance(data, dict):
                            logger.warning("Unexpected payload while parsing %s: %s", url, payload)
                            parsed[url] = None
                            continue
                        filtered = {k: v for k, v in data.items() if k in allowed_keys}
                        parsed[url] = PropertyData(**filtered)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Failed to parse %s via API: %s", url, exc)
                    parsed[url] = None
        return parsed

    def _apply_property_data(
        self, cursor: RealDictCursor, meta: AdsTableMeta, ad_id: int, property_data: PropertyData
    ) -> bool:
        updates: Dict[str, Any] = {}
        if property_data.price is not None:
            updates["price"] = Decimal(str(property_data.price))
        if property_data.rooms is not None:
            updates["rooms"] = int(property_data.rooms)
        floor_value = property_data.floor
        if floor_value is not None and str(floor_value).isdigit():
            updates["floor"] = int(floor_value)
        total_area = self._to_decimal(property_data.total_area)
        if total_area is not None:
            updates["total_area"] = total_area
        living_area = self._to_decimal(property_data.living_area)
        if living_area is not None:
            updates["living_area"] = living_area
        kitchen_area = self._to_decimal(property_data.kitchen_area)
        if kitchen_area is not None:
            updates["kitchen_area"] = kitchen_area
        total_floors = self._to_int(property_data.total_floors)
        if total_floors is not None:
            updates["total_floors"] = total_floors
        if property_data.bathroom:
            updates["bathroom"] = property_data.bathroom
        if property_data.balcony:
            updates["balcony"] = property_data.balcony
        if property_data.renovation:
            updates["renovation"] = property_data.renovation
        construction_year = self._to_int(property_data.construction_year)
        if construction_year is not None:
            updates["construction_year"] = construction_year
        if property_data.house_type:
            updates["house_type"] = property_data.house_type
        ceiling_height = self._to_decimal(property_data.ceiling_height)
        if ceiling_height is not None:
            updates["ceiling_height"] = ceiling_height
        if property_data.furniture:
            updates["furniture"] = property_data.furniture
        if property_data.metro_station:
            updates["metro_station"] = property_data.metro_station
        if property_data.metro_time is not None:
            metro_time = self._to_int(property_data.metro_time)
            if metro_time is not None:
                updates["metro_time"] = metro_time
        if property_data.metro_way:
            updates["metro_way"] = property_data.metro_way
        if property_data.tags:
            formatted = self._format_tags(property_data.tags)
            if formatted:
                updates["tags"] = formatted
        if property_data.description:
            updates["description"] = property_data.description
        if property_data.views_today is not None:
            views_today = self._to_int(property_data.views_today)
            if views_today is not None:
                updates["views_today"] = views_today
        if property_data.status is not None:
            updates["status"] = bool(property_data.status)
        if not updates:
            return False
        columns, values = meta.build_columns(updates)
        if not columns:
            return False
        set_clause = ", ".join(f'"{col}" = %s' for col in columns)
        cursor.execute(f"UPDATE users.ads SET {set_clause} WHERE id = %s", (*values, ad_id))
        return cursor.rowcount > 0

    @staticmethod
    def _to_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _format_tags(tags: Any) -> Optional[str]:
        if not tags:
            return None
        if isinstance(tags, str):
            return tags.strip()
        if isinstance(tags, (list, tuple)):
            cleaned = [str(item).strip() for item in tags if item]
            return "; ".join(cleaned) if cleaned else None
        return str(tags).strip()
