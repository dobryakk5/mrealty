"""
Модуль для работы с таблицей ads_cian - парсинг CIAN в БД
Отдельно от основного db_handler.py для других задач
"""

import os
import asyncpg
from typing import Dict
from dotenv import load_dotenv

# Load environment
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

# Database pool для ads_cian
_cian_db_pool: asyncpg.Pool | None = None

async def _get_cian_pool() -> asyncpg.Pool:
    """Получает пул подключений для работы с ads_cian"""
    global _cian_db_pool
    if _cian_db_pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set")
        _cian_db_pool = await asyncpg.create_pool(DATABASE_URL)
    return _cian_db_pool


async def create_ads_cian_table() -> None:
    """Создает таблицу ads_cian для хранения объявлений с CIAN"""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS ads_cian (
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(avitoid)
            );
            
            CREATE INDEX IF NOT EXISTS idx_ads_cian_avitoid ON ads_cian(avitoid);
            CREATE INDEX IF NOT EXISTS idx_ads_cian_price ON ads_cian(price);
            CREATE INDEX IF NOT EXISTS idx_ads_cian_rooms ON ads_cian(rooms);
            CREATE INDEX IF NOT EXISTS idx_ads_cian_metro ON ads_cian(metro);
            CREATE INDEX IF NOT EXISTS idx_ads_cian_person_type ON ads_cian(person_type);
            """)
            print("[DB] Таблица ads_cian создана успешно")
        except Exception as e:
            print(f"[DB] Ошибка создания таблицы ads_cian: {e}")
            raise


async def save_cian_ad(ad_data: Dict) -> None:
    """Сохраняет объявление CIAN в БД"""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            # Парсим данные
            avitoid = None
            if ad_data.get('offer_id'):
                try:
                    avitoid = int(ad_data['offer_id'])
                except (ValueError, TypeError):
                    pass

            price = ad_data.get('price')
            
            # Обработка комнат
            rooms = None
            if ad_data.get('rooms') == 'студия':
                rooms = 0
            elif ad_data.get('rooms'):
                try:
                    rooms = int(ad_data['rooms'])
                except (ValueError, TypeError):
                    pass

            # Парсим метро и время
            metro = None
            min_metro = None
            if ad_data.get('metro'):
                metro_str = ad_data['metro']
                # Убираем время в скобках из названия метро
                if '(' in metro_str:
                    metro = metro_str.split('(')[0].strip()
                else:
                    metro = metro_str
            
            if ad_data.get('walk_minutes'):
                try:
                    min_metro = int(ad_data['walk_minutes'])
                except (ValueError, TypeError):
                    pass

            # Адрес из geo_labels
            address = None
            if ad_data.get('geo_labels'):
                address = ', '.join(ad_data['geo_labels'])

            # Метки
            tags = None
            if ad_data.get('labels'):
                tags = ', '.join(ad_data['labels'])

            # Продавец
            seller = ad_data.get('seller', {})
            person_type = None
            person = None
            
            if seller.get('type'):
                type_mapping = {
                    'owner': 'собственник',
                    'agency': 'агентство', 
                    'user': 'пользователь',
                    'private': 'частное лицо',
                    'developer': 'застройщик'
                }
                person_type = type_mapping.get(seller['type'], seller['type'])
            
            if seller.get('name'):
                # Очищаем имя от лишнего текста
                clean_name = seller['name']
                stop_words = ['Документы проверены', 'Посмотреть все объекты', 'Суперагент', '+7', 'Написать']
                for stop_word in stop_words:
                    if stop_word in clean_name:
                        clean_name = clean_name.split(stop_word)[0].strip()
                person = clean_name

            # Вставляем данные
            query = """
            INSERT INTO ads_cian (
                url, avitoid, price, rooms, area, floor, total_floors, 
                complex, metro, min_metro, address, tags, person_type, person
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (avitoid) DO NOTHING
            """
            
            result = await conn.execute(query, 
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
                person
            )
            
            # Проверяем, была ли запись добавлена
            if "INSERT 0 1" in result:
                print(f"[DB] Добавлено объявление {avitoid}: {ad_data.get('URL')}")
            else:
                print(f"[DB] Пропущено (дубликат) объявление {avitoid}: {ad_data.get('URL')}")
            
        except Exception as e:
            print(f"[DB] Ошибка сохранения объявления: {e}")
            print(f"[DB] Данные: {ad_data}")
            raise


async def get_cian_stats() -> Dict:
    """Получает статистику по таблице ads_cian"""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            # Общая статистика
            total_count = await conn.fetchval("SELECT COUNT(*) FROM ads_cian")
            
            # Статистика по ценам
            price_stats = await conn.fetchrow("""
                SELECT 
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(price) as avg_price,
                    COUNT(price) as price_count
                FROM ads_cian 
                WHERE price IS NOT NULL
            """)
            
            # Статистика по комнатам
            rooms_stats = await conn.fetch("""
                SELECT rooms, COUNT(*) as count 
                FROM ads_cian 
                WHERE rooms IS NOT NULL
                GROUP BY rooms 
                ORDER BY rooms
            """)
            
            # Статистика по продавцам
            seller_stats = await conn.fetch("""
                SELECT person_type, COUNT(*) as count 
                FROM ads_cian 
                WHERE person_type IS NOT NULL
                GROUP BY person_type 
                ORDER BY count DESC
            """)
            
            # Топ метро
            metro_stats = await conn.fetch("""
                SELECT metro, COUNT(*) as count 
                FROM ads_cian 
                WHERE metro IS NOT NULL
                GROUP BY metro 
                ORDER BY count DESC
                LIMIT 10
            """)
            
            return {
                'total_count': total_count,
                'price_stats': dict(price_stats) if price_stats else {},
                'rooms_stats': [dict(row) for row in rooms_stats],
                'seller_stats': [dict(row) for row in seller_stats],
                'metro_stats': [dict(row) for row in metro_stats]
            }
            
        except Exception as e:
            print(f"[DB] Ошибка получения статистики: {e}")
            raise


async def search_cian_ads(filters: Dict = None) -> list:
    """Поиск объявлений в ads_cian с фильтрами"""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            base_query = """
                SELECT avitoid, url, price, rooms, area, floor, total_floors,
                       complex, metro, min_metro, address, person_type, person
                FROM ads_cian 
                WHERE 1=1
            """
            params = []
            param_count = 0
            
            if filters:
                # Фильтр по цене
                if filters.get('min_price'):
                    param_count += 1
                    base_query += f" AND price >= ${param_count}"
                    params.append(filters['min_price'])
                
                if filters.get('max_price'):
                    param_count += 1
                    base_query += f" AND price <= ${param_count}"
                    params.append(filters['max_price'])
                
                # Фильтр по комнатам
                if filters.get('rooms'):
                    param_count += 1
                    base_query += f" AND rooms = ${param_count}"
                    params.append(filters['rooms'])
                
                # Фильтр по метро
                if filters.get('metro'):
                    param_count += 1
                    base_query += f" AND metro ILIKE ${param_count}"
                    params.append(f"%{filters['metro']}%")
                
                # Фильтр по типу продавца
                if filters.get('person_type'):
                    param_count += 1
                    base_query += f" AND person_type = ${param_count}"
                    params.append(filters['person_type'])
            
            base_query += " ORDER BY price DESC NULLS LAST"
            
            # Лимит результатов
            limit = filters.get('limit', 50) if filters else 50
            param_count += 1
            base_query += f" LIMIT ${param_count}"
            params.append(limit)
            
            results = await conn.fetch(base_query, *params)
            return [dict(row) for row in results]
            
        except Exception as e:
            print(f"[DB] Ошибка поиска объявлений: {e}")
            raise


async def delete_old_cian_ads(days: int = 30) -> int:
    """Удаляет старые записи из ads_cian"""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            result = await conn.execute("""
                DELETE FROM ads_cian 
                WHERE created_at < NOW() - INTERVAL '%s days'
            """, days)
            
            deleted_count = int(result.split()[-1])
            print(f"[DB] Удалено {deleted_count} старых записей (старше {days} дней)")
            return deleted_count
            
        except Exception as e:
            print(f"[DB] Ошибка удаления старых записей: {e}")
            raise


async def close_cian_pool():
    """Закрывает пул подключений"""
    global _cian_db_pool
    if _cian_db_pool:
        await _cian_db_pool.close()
        _cian_db_pool = None
