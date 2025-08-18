#!/usr/bin/env python3
"""
Модуль для работы с таблицей ads_avito - парсинг AVITO в БД
"""

import os
import re
import asyncpg
from typing import Dict
from dotenv import load_dotenv
from datetime import datetime, date

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
        # Создаем таблицу если её нет
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
                metro_id INTEGER, -- ID метро из таблицы metro
                min_metro SMALLINT,
                address TEXT,
                tags TEXT,
                person_type TEXT,
                person TEXT,
                object_type_id SMALLINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(avitoid)
            );

            CREATE INDEX IF NOT EXISTS idx_ads_avito_avitoid ON ads_avito(avitoid);
            CREATE INDEX IF NOT EXISTS idx_ads_avito_metro_id ON ads_avito(metro_id);
            """
        )
        
        # Добавляем поле updated_at если его нет
        try:
            await conn.execute("ALTER TABLE ads_avito ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            print("[DB] Поле updated_at добавлено в таблицу ads_avito")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("[DB] Поле updated_at уже существует в таблице ads_avito")
            else:
                print(f"[DB] Ошибка добавления поля updated_at: {e}")
        
        # Добавляем поле metro_id если его нет
        try:
            await conn.execute("ALTER TABLE ads_avito ADD COLUMN metro_id INTEGER")
            print("[DB] Поле metro_id добавлено в таблицу ads_avito")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("[DB] Поле metro_id уже существует в таблице ads_avito")
            else:
                print(f"[DB] Ошибка добавления поля metro_id: {e}")


async def convert_seller_type_to_number(seller_type):
    """Конвертирует текстовый тип продавца в число для БД"""
    if not seller_type:
        return 1  # По умолчанию "частное лицо"
    
    seller_type_lower = str(seller_type).lower()
    
    if 'собственник' in seller_type_lower:
        return 3
    elif 'агентство' in seller_type_lower or 'агент' in seller_type_lower:
        return 2
    elif 'застройщик' in seller_type_lower:
        return 4
    else:
        return 1  # "частное лицо" по умолчанию

async def save_avito_ad(ad_data: dict) -> bool:
    """Сохраняет объявление AVITO в БД. При конфликте по avitoid — пропускает."""
    pool = await _get_avito_pool()
    async with pool.acquire() as conn:
        # Парсинг данных
        avitoid = None
        if ad_data.get('avitoid'):
            try:
                avitoid = int(ad_data['avitoid'])
                print(f"🔍 Обработка avitoid: {ad_data.get('avitoid')} -> {avitoid}")
            except (ValueError, TypeError):
                print(f"❌ Ошибка конвертации avitoid: {ad_data.get('avitoid')}")
                pass
        else:
            print(f"⚠️ Поле avitoid не найдено в ad_data. Доступные поля: {list(ad_data.keys())}")
        
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
        if ad_data.get('tags'):
            tags = ad_data['tags']
        elif ad_data.get('labels'):
            tags = ', '.join(ad_data['labels'])

        seller = ad_data.get('seller', {})
        person_type = None
        person = None
        
        # ПРИОРИТЕТ: используем поле person_type из ad_data, если оно есть
        if ad_data.get('person_type'):
            person_type = ad_data['person_type']
            # Конвертируем текстовый тип в число для БД
            person_type = await convert_seller_type_to_number(person_type)
            # print(f"[DB] Используется поле person_type из ad_data: {person_type}")
        
        # ПРИОРИТЕТ: используем поле person из ad_data, если оно есть
        if ad_data.get('person'):
            person = ad_data['person']
            # print(f"[DB] Используется поле person из ad_data: {person}")
        else:
            # Fallback на старую логику
            if seller.get('type'):
                type_mapping = {
                    'owner': 'собственник',
                    'agency': 'агентство',
                    'user': 'пользователь',
                    'private': 'частное лицо',
                    'developer': 'застройщик',
                }
                person_type = type_mapping.get(seller['type'], seller['type'])
                # Конвертируем текстовый тип в число для БД
                person_type = await convert_seller_type_to_number(person_type)
            
            # Приоритет названию агентства из тегов
            if seller.get('agency_name'):
                person = seller['agency_name']
            elif seller.get('name'):
                clean_name = seller['name']
                for stop in ['Документы проверены', 'Посмотреть все объекты', 'Суперагент', '+7', 'Написать']:
                    if stop in clean_name:
                        clean_name = clean_name.split(stop)[0].strip()
                person = clean_name
        
        # Если тип продавца не определен, устанавливаем "частное лицо" (1) по умолчанию
        if person_type is None:
            person_type = 1  # частное лицо

        # Проверяем и нормализуем source_created
        source_created = ad_data.get('source_created')
        if source_created is None:
            print(f"ℹ️ source_created не указан, устанавливаем текущую дату")
            source_created = datetime.now().date()
        elif isinstance(source_created, str):
            print(f"⚠️ source_created передается как строка: '{source_created}', устанавливаем текущую дату")
            source_created = datetime.now().date()
        elif not isinstance(source_created, (datetime, date)):
            print(f"⚠️ source_created имеет неожиданный тип: {type(source_created)}, устанавливаем текущую дату")
            source_created = datetime.now().date()
        
        # SQL запрос для вставки/обновления
        query = """
            INSERT INTO ads_avito (
                url, avitoid, price, rooms, area, floor, total_floors, 
                complex, metro, metro_id, min_metro, address, tags, 
                person_type, person, source_created, object_type_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (avitoid) DO UPDATE SET
                url = EXCLUDED.url,
                price = EXCLUDED.price,
                rooms = EXCLUDED.rooms,
                area = EXCLUDED.area,
                floor = EXCLUDED.floor,
                total_floors = EXCLUDED.total_floors,
                complex = EXCLUDED.complex,
                metro = EXCLUDED.metro,
                metro_id = EXCLUDED.metro_id,
                min_metro = EXCLUDED.min_metro,
                address = EXCLUDED.address,
                tags = EXCLUDED.tags,
                person_type = EXCLUDED.person_type,
                person = EXCLUDED.person,
                source_created = EXCLUDED.source_created,
                object_type_id = EXCLUDED.object_type_id,
                updated_at = CURRENT_TIMESTAMP
            """

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
            ad_data.get('metro_id'),  # Добавляем metro_id
            min_metro,
            address,
            tags,
            person_type,
            person,
            source_created,  # Добавляем время публикации
            (2 if ad_data.get('object_type_id') == 2 else 1)  # object_type_id
        )

        # Проверяем результат операции
        if "INSERT 0 1" in result:
            pass  # Убираем лог
        elif "UPDATE 1" in result:
            pass  # Убираем лог
        else:
            pass  # Убираем лог
        return True


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
