#!/usr/bin/env python3
"""
Модуль для работы с таблицей ads_avito - парсинг AVITO в БД
"""

import os
import re
import asyncpg
from typing import Dict
from dotenv import load_dotenv

# Load environment
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

# Database pool для ads_avito
_avito_db_pool: asyncpg.Pool | None = None

async def _get_avito_pool() -> asyncpg.Pool:
    """Получает пул подключений для работы с ads_avito"""
    global _avito_db_pool
    if _avito_db_pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set")
        _avito_db_pool = await asyncpg.create_pool(DATABASE_URL)
    return _avito_db_pool


async def create_ads_avito_table() -> None:
    """Создает таблицу ads_avito для хранения объявлений с AVITO"""
    pool = await _get_avito_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ads_avito (
                id SERIAL PRIMARY KEY,
                url TEXT,
                avitoid NUMERIC,
                price BIGINT,
                rooms SMALLINT,
                area NUMERIC,
                floor SMALLINT,
                total_floors SMALLINT,
                complex TEXT,
                metro TEXT,
                min_metro SMALLINT,
                address TEXT,
                tags TEXT,
                person_type TEXT,
                person TEXT,
                object_type_id SMALLINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(avitoid)
            );

            CREATE INDEX IF NOT EXISTS idx_ads_avito_avitoid ON ads_avito(avitoid);
            """
        )


async def save_avito_ad(ad_data: Dict) -> None:
    """Сохраняет объявление AVITO в БД. При конфликте по avitoid — пропускает."""
    pool = await _get_avito_pool()
    async with pool.acquire() as conn:
        # Парсинг данных
        avitoid = None
        if ad_data.get('offer_id'):
            try:
                avitoid = int(ad_data['offer_id'])
            except (ValueError, TypeError):
                pass

        price = ad_data.get('price')

        # Обработка комнат: 0, если пришла строка, не являющаяся числом
        rooms = None
        raw_rooms = ad_data.get('rooms')
        if raw_rooms is None:
            rooms = None
        elif isinstance(raw_rooms, (int, float)):
            try:
                rooms = int(raw_rooms)
            except Exception:
                rooms = None
        elif isinstance(raw_rooms, str):
            s = raw_rooms.strip()
            if re.fullmatch(r"\d+", s):
                rooms = int(s)
            else:
                rooms = 0

        metro = ad_data.get('metro')
        min_metro = None
        if ad_data.get('walk_minutes'):
            try:
                min_metro = int(ad_data['walk_minutes'])
            except (ValueError, TypeError):
                pass

        address = None
        if ad_data.get('address'):
            address = ad_data['address']
        elif ad_data.get('geo_labels'):
            address = ', '.join(ad_data['geo_labels'])

        tags = None
        if ad_data.get('labels'):
            tags = ', '.join(ad_data['labels'])

        seller = ad_data.get('seller', {})
        person_type = None
        person = None
        if seller.get('type'):
            type_mapping = {
                'owner': 'собственник',
                'agency': 'агентство',
                'user': 'пользователь',
                'private': 'частное лицо',
                'developer': 'застройщик',
            }
            person_type = type_mapping.get(seller['type'], seller['type'])
        if seller.get('name'):
            clean_name = seller['name']
            for stop in ['Документы проверены', 'Посмотреть все объекты', 'Суперагент', '+7', 'Написать']:
                if stop in clean_name:
                    clean_name = clean_name.split(stop)[0].strip()
            person = clean_name

        query = (
            """
            INSERT INTO ads_avito (
                url, avitoid, price, rooms, area, floor, total_floors,
                complex, metro, min_metro, address, tags, person_type, person, object_type_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (avitoid) DO NOTHING
            """
        )

        result = await conn.execute(
            query,
            ad_data.get('URL'),
            avitoid,
            price,
            rooms,
            ad_data.get('area_m2'),
            ad_data.get('floor'),
            ad_data.get('floor_total'),
            ad_data.get('complex'),
            metro,
            min_metro,
            address,
            tags,
            person_type,
            person,
            (2 if ad_data.get('object_type_id') == 2 else 1)
        )

        if "INSERT 0 1" in result:
            print(f"[DB] Добавлено объявление AVITO {avitoid}: {ad_data.get('URL')}")
        else:
            print(f"[DB] Пропущено (дубликат) AVITO {avitoid}: {ad_data.get('URL')}")


async def create_avito_api_table() -> None:
    """Создает таблицу avito_api для API парсера"""
    pool = await _get_avito_pool()
    async with pool.acquire() as conn:
        # Удаляем старую таблицу если она существует
        await conn.execute("DROP TABLE IF EXISTS avito_api CASCADE")
        
        # Создаем новую таблицу с полем url
        await conn.execute(
            """
            CREATE TABLE avito_api (
                id SERIAL PRIMARY KEY,
                avitoid NUMERIC UNIQUE NOT NULL,
                title TEXT,
                deal_type SMALLINT, -- 1: продажа, 2: аренда
                price BIGINT,
                metro TEXT,
                url TEXT, -- URL без контекста
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        
        # Создаем индекс
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_avito_api_avitoid ON avito_api(avitoid)"
        )
        
        print("[DB] Таблица avito_api пересоздана с полем url")

async def save_avito_api_item(data: dict) -> None:
    """Сохраняет объявление в таблицу avito_api"""
    pool = await _get_avito_pool()
    async with pool.acquire() as conn:
        try:
            avitoid = None
            if data.get('offer_id'):
                try:
                    avitoid = int(data['offer_id'])
                except (ValueError, TypeError):
                    pass

            price = data.get('price')
            
            await conn.execute(
                """
                INSERT INTO avito_api (
                    avitoid, title, deal_type, price, metro, url
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (avitoid) DO NOTHING
                """,
                avitoid,
                data.get('title'),
                2 if data.get('deal_type') == 'rental' else 1,  # 2: аренда, 1: продажа
                price,
                data.get('metro'),
                data.get('url_clean')  # URL без контекста
            )
            print(f"[DB] Добавлено объявление API AVITO {data.get('offer_id')}")
            return True
        except Exception as e:
            print(f"[DB] Ошибка сохранения в avito_api: {e}")
            return False

