import os
import json
import asyncio
import pytz
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

_db_pool: asyncpg.Pool | None = None

async def _get_pool() -> asyncpg.Pool:
    global _db_pool
    if _db_pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set")
        _db_pool = await asyncpg.create_pool(DATABASE_URL)
    return _db_pool

async def init_schema(conn: asyncpg.Connection) -> None:
    await conn.execute("""
    CREATE SCHEMA IF NOT EXISTS users;

    CREATE TABLE IF NOT EXISTS users.requests (
        id SERIAL PRIMARY KEY,
        userid BIGINT NOT NULL,
        ts TIMESTAMP NOT NULL
    );

    DO $$
    BEGIN
       IF EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'users' AND table_name = 'requests'
           AND column_name = 'userid'
           AND data_type != 'bigint'
       ) THEN
         ALTER TABLE users.requests
         ALTER COLUMN userid TYPE BIGINT
           USING userid::bigint;
       END IF;
    END
    $$;

    CREATE TABLE IF NOT EXISTS users.listings (
        id SERIAL PRIMARY KEY,
        request_id INTEGER REFERENCES users.requests(id) ON DELETE CASCADE,
        url TEXT,
        status TEXT,
        labels TEXT,
        rooms INTEGER,
        price NUMERIC,
        total_views INTEGER,
        views_today INTEGER,
        unique_views INTEGER,
        floor INTEGER,
        floors INTEGER,
        total_area NUMERIC,
        living_area NUMERIC,
        kitchen_area NUMERIC,
        bathroom_num INTEGER,
        bathroom_type TEXT,
        balcony_num INTEGER,
        balcony_type TEXT,
        view_from_windows TEXT,
        renovation TEXT,
        furnished_binary BOOLEAN DEFAULT FALSE,
        year_built INTEGER,
        series TEXT,
        house_type TEXT,
        overlap_type TEXT,
        entrances INTEGER,
        heating TEXT,
        emergency TEXT,
        gas TEXT,
        ceiling_height NUMERIC,
        garbage_chute TEXT,
        parking TEXT,
        lifts_num INTEGER,
        lifts_type TEXT,
        min_metro INTEGER,
        metro TEXT,
        housing_type TEXT,
        address TEXT,
        ts TIMESTAMP NOT NULL,
        other JSONB
    );
    """)

def clean_numeric(value: any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(value)
        except InvalidOperation:
            return None
    text = str(value)
    cleaned = re.sub(r"[^0-9,\.\-]", "", text)
    cleaned = cleaned.replace(',', '.')
    parts = cleaned.split('.')
    if len(parts) > 2:
        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None

def to_str(value: any) -> str | None:
    return str(value) if value is not None else None

def parse_floor(text: any) -> tuple[int | None, int | None]:
    if not text:
        return None, None
    s = str(text).replace('\u00A0', ' ').strip().lower()
    m = re.search(r"(\d+)\s*(?:из|/)\s*(\d+)", s)
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.search(r"(\d+)\b", s)
    if m2:
        return int(m2.group(1)), None
    return None, None

def parse_count_type(text: any) -> tuple[int | None, str | None]:
    if not text:
        return None, None
    s = str(text).strip()
    m = re.match(r"(\d+)\s*(.*)", s)
    if m:
        num = int(m.group(1))
        dtype = m.group(2).strip() or None
        return num, dtype
    return None, s or None

async def save_listing(conn: asyncpg.Connection, listing: dict, request_id: int) -> None:
    listing = {k.replace('\u00A0', ' '): v for k, v in listing.items()}

    price = clean_numeric(listing.get('Цена_raw'))
    total_views = clean_numeric(listing.get('Всего просмотров'))
    views_today = clean_numeric(listing.get('Просмотров сегодня'))
    unique_views = clean_numeric(listing.get('Уникальных просмотров'))
    total_area = clean_numeric(listing.get('Общая площадь'))
    living_area = clean_numeric(listing.get('Жилая площадь'))
    kitchen_area = clean_numeric(listing.get('Площадь кухни'))
    ceiling_height = clean_numeric(listing.get('Высота потолков'))

    floor_num, floors = parse_floor(listing.get('Этаж'))
    bathroom_num, bathroom_type = parse_count_type(listing.get('Санузел'))
    balcony_num, balcony_type = parse_count_type(listing.get('Балкон/лоджия'))
    lifts_num, lifts_type = parse_count_type(listing.get('Количество лифтов'))
    min_metro, metro = parse_count_type(listing.get('Минут метро'))

    url = to_str(listing.get('URL'))
    status = to_str(listing.get('Статус'))
    labels = to_str(listing.get('Метки'))
    view_from_windows = to_str(listing.get('Вид из окон'))
    renovation = to_str(listing.get('Ремонт'))
    series = to_str(listing.get('Строительная серия'))
    house_type = to_str(listing.get('Тип дома'))
    overlap_type = to_str(listing.get('Тип перекрытий'))
    heating = to_str(listing.get('Отопление'))
    emergency = to_str(listing.get('Аварийность'))
    gas = to_str(listing.get('Газоснабжение'))
    garbage_chute = to_str(listing.get('Мусоропровод'))
    parking = to_str(listing.get('Парковка'))
    housing_type = to_str(listing.get('Тип жилья'))
    address = to_str(listing.get('Адрес'))

    furnished_binary = listing.get('Продаётся с мебелью') == 'Да'
    year_built_val = clean_numeric(listing.get('Год постройки'))
    year_built = int(year_built_val) if year_built_val is not None else None

    rooms = listing.get('Комнат') if isinstance(listing.get('Комнат'), int) else None
    entrances = listing.get('Подъезды') if isinstance(listing.get('Подъезды'), int) else None

    known_keys = {
        'URL','Статус','Метки','Комнат','Цена_raw','Всего просмотров','Просмотров сегодня','Уникальных просмотров',
        'Этаж','Общая площадь','Жилая площадь','Площадь кухни','Санузел','Балкон/лоджия','Вид из окон','Ремонт',
        'Продаётся с мебелью','Год постройки','Строительная серия','Тип дома','Тип перекрытий','Подъезды',
        'Отопление','Аварийность','Газоснабжение','Высота потолков','Мусоропровод','Парковка','Количество лифтов',
        'Тип жилья','Адрес','Минут метро'
    }
    other = {k: v for k, v in listing.items() if k not in known_keys}
    ts = datetime.now(pytz.timezone("Europe/Moscow")).replace(tzinfo=None, microsecond=0)

    # Insert listing with correct number of columns/placeholders
    await conn.execute("""
    INSERT INTO users.listings(
        request_id, url, status, labels, rooms, price,
        total_views, views_today, unique_views, floor, floors,
        total_area, living_area, kitchen_area, bathroom_num, bathroom_type,
        balcony_num, balcony_type, view_from_windows, renovation,
        furnished_binary, year_built, series, house_type, overlap_type,
        entrances, heating, emergency, gas, ceiling_height,
        garbage_chute, parking, lifts_num, lifts_type, min_metro, metro,
        housing_type, address, ts, other
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
        $31, $32, $33, $34, $35, $36, $37, $38, $39, $40
    )
    """,
        request_id, url, status, labels, rooms, price,
        total_views, views_today, unique_views, floor_num, floors,
        total_area, living_area, kitchen_area, bathroom_num, bathroom_type,
        balcony_num, balcony_type, view_from_windows, renovation,
        furnished_binary, year_built, series, house_type, overlap_type,
        entrances, heating, emergency, gas, ceiling_height,
        garbage_chute, parking, lifts_num, lifts_type, min_metro, metro,
        housing_type, address, ts, json.dumps(other, ensure_ascii=False)
    )

async def save_listings(listings: list[dict], user_id: int) -> None:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await init_schema(conn)
        if not isinstance(user_id, int):
            raise ValueError(f"user_id must be int, got {type(user_id).__name__}")
        ts = datetime.now(pytz.timezone("Europe/Moscow")).replace(tzinfo=None, microsecond=0)
        row = await conn.fetchrow(
            "INSERT INTO users.requests(userid, ts) VALUES($1::bigint, $2) RETURNING id",
            user_id, ts
        )
        request_id = row['id']
        for lst in listings:
            await save_listing(conn, lst, request_id)

# Example usage:
# asyncio.run(save_listings(list_of_dicts, 1234567890))
