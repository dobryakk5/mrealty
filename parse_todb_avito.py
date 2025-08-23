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
                metro_id INTEGER, -- ID метро из таблицы metro
                min_metro SMALLINT,
                address TEXT,
                tags TEXT,
                person_type TEXT,
                source_created TIMESTAMP, -- Время публикации на источнике
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
        except Exception as e:
            pass
        
        # Добавляем поле metro_id если его нет
        try:
            await conn.execute("ALTER TABLE ads_avito ADD COLUMN metro_id INTEGER")
        except Exception as e:
            pass
        
        # Добавляем поле source_created если его нет
        try:
            await conn.execute("ALTER TABLE ads_avito ADD COLUMN source_created TIMESTAMP")
        except Exception as e:
            pass
        
        # Создаем схему system если её нет
        await conn.execute("CREATE SCHEMA IF NOT EXISTS system")
        
        # Создаем таблицу для отслеживания пагинации Авито
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS system.avito_pagination_tracking (
            id SERIAL PRIMARY KEY,
            metro_id INTEGER NOT NULL,
            last_processed_page INTEGER DEFAULT 0,
            total_pages_processed INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(metro_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_avito_pagination_metro ON system.avito_pagination_tracking(metro_id);
        CREATE INDEX IF NOT EXISTS idx_avito_pagination_updated ON system.avito_pagination_tracking(last_updated DESC);
        """)
        
        print("[DB] Таблица system.avito_pagination_tracking создана успешно")


async def convert_seller_type_to_number(seller_type):
    """Конвертирует текстовый тип продавца в число для БД"""
    if not seller_type:
        return 1  # По умолчанию "частное лицо"
    
    seller_type_lower = str(seller_type).lower()
    
    if 'собственник' in seller_type_lower:
        return 3
    elif 'агентство' in seller_type_lower or 'агент' in seller_type_lower or 'реквизиты проверены' in seller_type_lower:
        return 2
    elif 'застройщик' in seller_type_lower:
        return 4
    elif 'документы проверены' in seller_type_lower:
        return 1  # "частное лицо" (private)
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
            except (ValueError, TypeError):
                pass
        else:
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
        if ad_data.get('tags'):
            tags = ad_data['tags']
        elif ad_data.get('labels'):
            tags = ', '.join(ad_data['labels'])

        seller = ad_data.get('seller', {})
        person_type = None
        
        # ПРИОРИТЕТ: используем поле person_type из ad_data, если оно есть
        if ad_data.get('person_type'):
            person_type = ad_data['person_type']
            # Конвертируем текстовый тип в число для БД
            person_type = await convert_seller_type_to_number(person_type)
            # print(f"[DB] Используется поле person_type из ad_data: {person_type}")
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
        
        # Если тип продавца не определен, устанавливаем "частное лицо" (1) по умолчанию
        if person_type is None:
            person_type = 1  # частное лицо

        # Проверяем и нормализуем source_created
        source_created = ad_data.get('source_created')
        if source_created is None:
            source_created = datetime.now()
        elif isinstance(source_created, str):
            source_created = datetime.now()
        elif isinstance(source_created, datetime):
            # Если это datetime, оставляем как есть
            pass
        elif isinstance(source_created, date):
            # Если это date, оставляем как есть
            pass
        else:
            source_created = datetime.now()
        
        # SQL запрос для вставки/обновления
        query = """
            INSERT INTO ads_avito (
                url, avitoid, price, rooms, area, floor, total_floors, 
                metro_id, min_metro, address, tags, 
                person_type, source_created, object_type_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (avitoid) DO UPDATE SET
                url = EXCLUDED.url,
                price = EXCLUDED.price,
                rooms = EXCLUDED.rooms,
                area = EXCLUDED.area,
                floor = EXCLUDED.floor,
                total_floors = EXCLUDED.total_floors,
                metro_id = EXCLUDED.metro_id,
                min_metro = EXCLUDED.min_metro,
                address = EXCLUDED.address,
                tags = EXCLUDED.tags,
                person_type = EXCLUDED.person_type,
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
            ad_data.get('metro_id'),  # Добавляем metro_id
            min_metro,
            address,
            tags,
            person_type,
            source_created,  # Добавляем время публикации
            (2 if ad_data.get('object_type_id') == 2 else 1)  # object_type_id
        )

        # Проверяем результат операции
        pass
        
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

# =============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С ПАГИНАЦИЕЙ АВИТО
# =============================================================================

async def get_avito_pagination_status(metro_id: int) -> dict | None:
    """Получает статус пагинации для конкретного метро"""
    try:
        pool = await _get_avito_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT metro_id, last_processed_page, total_pages_processed, last_updated
                FROM system.avito_pagination_tracking
                WHERE metro_id = $1
            """, metro_id)
            
            if row:
                return {
                    'metro_id': row['metro_id'],
                    'last_processed_page': row['last_processed_page'],
                    'total_pages_processed': row['total_pages_processed'],
                    'last_updated': row['last_updated']
                }
            return None
            
    except Exception as e:
        print(f"❌ Ошибка получения статуса пагинации для метро {metro_id}: {e}")
        return None

async def update_avito_pagination(metro_id: int, page_number: int) -> bool:
    """Обновляет статус пагинации для метро после обработки страницы"""
    try:
        pool = await _get_avito_pool()
        async with pool.acquire() as conn:
            # Используем UPSERT для обновления или создания записи
            await conn.execute("""
                INSERT INTO system.avito_pagination_tracking (metro_id, last_processed_page, total_pages_processed, last_updated)
                VALUES ($1, $2, 1, CURRENT_TIMESTAMP)
                ON CONFLICT (metro_id) DO UPDATE SET
                    last_processed_page = EXCLUDED.last_processed_page,
                    total_pages_processed = system.avito_pagination_tracking.total_pages_processed + 1,
                    last_updated = CURRENT_TIMESTAMP
            """, metro_id, page_number)
            
            return True
            
    except Exception as e:
        print(f"❌ Ошибка обновления пагинации для метро {metro_id}: {e}")
        return False

async def get_all_avito_pagination_status() -> list:
    """Получает статус пагинации для всех метро"""
    try:
        pool = await _get_avito_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT metro_id, last_processed_page, total_pages_processed, last_updated
                FROM system.avito_pagination_tracking
                ORDER BY metro_id
            """)
            
            result = []
            for row in rows:
                result.append({
                    'metro_id': row['metro_id'],
                    'last_processed_page': row['last_processed_page'],
                    'total_pages_processed': row['total_pages_processed'],
                    'last_updated': row['last_updated']
                })
            
            return result
            
    except Exception as e:
        print(f"❌ Ошибка получения статуса пагинации для всех метро: {e}")
        return []

async def reset_avito_pagination(metro_id: int = None) -> bool:
    """Сбрасывает статус пагинации (для всех метро или конкретного)"""
    try:
        pool = await _get_avito_pool()
        async with pool.acquire() as conn:
            if metro_id:
                # Сброс для конкретного метро
                await conn.execute("""
                    UPDATE system.avito_pagination_tracking
                    SET last_processed_page = 0, total_pages_processed = 0, last_updated = CURRENT_TIMESTAMP
                    WHERE metro_id = $1
                """, metro_id)
                print(f"✅ Сброшен статус пагинации для метро {metro_id}")
            else:
                # Сброс для всех метро
                await conn.execute("""
                    UPDATE system.avito_pagination_tracking
                    SET last_processed_page = 0, total_pages_processed = 0, last_updated = CURRENT_TIMESTAMP
                """)
                print("✅ Сброшен статус пагинации для всех метро")
            
            return True
            
    except Exception as e:
        print(f"❌ Ошибка сброса статуса пагинации: {e}")
        return False
