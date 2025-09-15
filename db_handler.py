import os
import json
import asyncio
import re
import pytz
from datetime import datetime
from decimal import Decimal, InvalidOperation
import asyncpg
from typing import Any, Dict, List
from dotenv import load_dotenv

# Load environment
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

# Database pool
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
        lift_p INTEGER,
        lift_g INTEGER,
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
    cleaned = re.sub(r"[^0-9,\.\-]", "", text).replace(',', '.')
    parts = cleaned.split('.')
    if len(parts) > 2:
        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None

# Parse floor info
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

# Parse counts with type (e.g. bathrooms)
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

# Parse lifts into passenger and freight counts
def parse_lifts(text: any) -> tuple[int | None, int | None]:
    """
    Parses raw lift string and returns counts.
    Handles cases like:
      "2 пассажирских, 1 грузовой", "2 пассажирских", "1 грузовой".
    Strips quotes and spaces before parsing.
    """
    if not text:
        return None, None
    # Convert to string, lower case, strip surrounding quotes/spaces
    s = str(text).strip().lower().strip("'\"")
    # Find passenger (пассажирских) and freight (грузовых)
    p_nums = re.findall(r"(\d+)\s*пасс\w*", s)
    g_nums = re.findall(r"(\d+)\s*груз\w*", s)
    lift_p = int(p_nums[0]) if p_nums else None
    lift_g = int(g_nums[0]) if g_nums else None
    return lift_p, lift_g

# Main save_listing function
async def save_listing(conn: asyncpg.Connection, listing: dict, request_id: int) -> None:
    listing = {k.replace('\u00A0', ' '): v for k, v in listing.items()}

    # Убрали отладочный вывод лифтов

    # Numeric fields
    price = clean_numeric(listing.get('Цена_raw'))
    total_views = clean_numeric(listing.get('Всего просмотров'))
    views_today = clean_numeric(listing.get('Просмотров сегодня'))
    unique_views = clean_numeric(listing.get('Уникальных просмотров'))
    total_area = clean_numeric(listing.get('Общая площадь'))
    living_area = clean_numeric(listing.get('Жилая площадь'))
    kitchen_area = clean_numeric(listing.get('Площадь кухни'))
    ceiling_height = clean_numeric(listing.get('Высота потолков'))

    # Parse lifts correctly
    raw_lifts = listing.get('Количество лифтов')
    lift_p, lift_g = parse_lifts(raw_lifts)

    # Other parsed fields
    floor_num, floors = parse_floor(listing.get('Этаж'))
    bathroom_num, bathroom_type = parse_count_type(listing.get('Санузел'))
    balcony_num, balcony_type = parse_count_type(listing.get('Балкон/лоджия'))
    min_metro, metro = parse_count_type(listing.get('Минут метро'))

    # Strings and boolean
    url = listing.get('URL')
    status = listing.get('Статус')
    labels = listing.get('Метки')
    view_from_windows = listing.get('Вид из окон')
    renovation = listing.get('Ремонт')
    series = listing.get('Строительная серия')
    house_type = listing.get('Тип дома')
    overlap_type = listing.get('Тип перекрытий')
    heating = listing.get('Отопление')
    emergency = listing.get('Аварийность')
    gas = listing.get('Газоснабжение')
    garbage_chute = listing.get('Мусоропровод')
    parking = listing.get('Парковка')
    housing_type = listing.get('Тип жилья')
    address = listing.get('Адрес')
    furnished_binary = listing.get('Продаётся с мебелью') == 'Да'

    # Simple integer fields
    rooms = listing.get('Комнат') if isinstance(listing.get('Комнат'), int) else None
    entrances = listing.get('Подъезды') if isinstance(listing.get('Подъезды'), int) else None
    year_val = clean_numeric(listing.get('Год постройки'))
    year_built = int(year_val) if year_val is not None else None

    # Timestamp
    ts = datetime.now(pytz.timezone("Europe/Moscow")).replace(tzinfo=None, microsecond=0)

    # Other JSONB
    known = {
        'URL','Статус','Метки','Комнат','Цена_raw','Всего просмотров','Просмотров сегодня',
        'Уникальных просмотров','Этаж','Общая площадь','Жилая площадь','Площадь кухни',
        'Санузел','Балкон/лоджия','Вид из окон','Ремонт','Продаётся с мебелью','Год постройки',
        'Строительная серия','Тип дома','Тип перекрытий','Подъезды','Отопление','Аварийность',
        'Газоснабжение','Высота потолков','Мусоропровод','Парковка','Количество лифтов',
        'Минут метро','Метро','Тип жилья','Адрес'
    }
    other = {k: v for k, v in listing.items() if k not in known}

    # Execute insert
    await conn.execute(
        """
INSERT INTO users.listings(
    request_id, url, status, labels, rooms, price,
    total_views, views_today, unique_views, floor, floors,
    total_area, living_area, kitchen_area, bathroom_num, bathroom_type,
    balcony_num, balcony_type, view_from_windows, renovation,
    furnished_binary, year_built, series, house_type, overlap_type,
    entrances, heating, emergency, gas, ceiling_height,
    garbage_chute, parking, lift_p, lift_g,
    min_metro, metro, housing_type, address, ts, other
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16,
    $17, $18, $19, $20,
    $21, $22, $23, $24, $25,
    $26, $27, $28, $29, $30,
    $31, $32, $33, $34, $35,
    $36, $37, $38, $39, $40
)
        """,
        request_id, url, status, labels, rooms, price,
        total_views, views_today, unique_views, floor_num, floors,
        total_area, living_area, kitchen_area, bathroom_num, bathroom_type,
        balcony_num, balcony_type, view_from_windows, renovation,
        furnished_binary, year_built, series, house_type, overlap_type,
        entrances, heating, emergency, gas, ceiling_height,
        garbage_chute, parking, lift_p, lift_g,
        min_metro, metro, housing_type, address, ts,
        json.dumps(other, ensure_ascii=False)
    )

async def save_listings(listings: List[Dict], user_id: int) -> int:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await init_schema(conn)
        if not isinstance(user_id, int):
            raise ValueError(f"user_id must be int, got {type(user_id).__name__}")

        # отметка времени без микросекунд, без tzinfo
        ts = datetime.now(pytz.timezone("Europe/Moscow")).replace(tzinfo=None, microsecond=0)

        # создаём запись в requests и получаем её id
        row = await conn.fetchrow(
            "INSERT INTO users.requests(userid, ts) VALUES($1::bigint, $2) RETURNING id",
            user_id, ts
        )
        request_id = row["id"]

        # сохраняем каждый listing
        for lst in listings:
            await save_listing(conn, lst, request_id)

    # возвращаем request_id вызывающему
    return request_id

async def find_similar_ads_grouped(request_id: int) -> List[Dict[str, Any]]:
    """
    Подключается к БД так же, как в save_listings,
    выполняет SELECT из users.find_similar_ads_grouped
    и возвращает результат в виде списка dict.
    """
    # Получаем пул соединений
    pool: asyncpg.pool.Pool = await _get_pool()
    
    async with pool.acquire() as conn:
        records = await conn.fetch(
            """
            SELECT address, ads
            FROM users.find_similar_ads_grouped($1::int)
            """,
            request_id
        )

    # Преобразуем Record в питоновские dict и возвращаем
    return [
        {"address": rec["address"], "ads": rec["ads"]}
        for rec in records
    ]


async def call_update_ad(price: int | None, is_actual: int | None, code: int, url_id: int) -> None:
    """
    Вызывает хранимую процедуру users.update_ad с сигнатурой:
      (p_price bigint, p_is_actual smallint, p_code smallint, p_url_id numeric)
    price: новая цена (или None, чтобы не изменять)
    is_actual: 1 или 0 (или None, чтобы не изменять)
    code: источник (smallint), например 4
    url_id: идентификатор объявления
    """
    pool = await _get_pool()
    query = "CALL users.update_ad($1::bigint, $2::smallint, $3::smallint, $4::numeric)"
    async with pool.acquire() as conn:
        await conn.execute(query, price, is_actual, code, url_id)


async def get_web_domain() -> str:
    """
    Получает домен из таблицы users.params для параметра 'web'
    Возвращает домен без завершающего слэша
    """
    pool = await _get_pool()
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            "SELECT data FROM users.params WHERE code = 'web'"
        )

        if record and record['data']:
            domain = record['data'].strip()
            # Убираем завершающий слэш если есть
            return domain.rstrip('/')
        else:
            # Возвращаем домен по умолчанию если не найден в БД
            return "https://mrealty.netlify.app"


