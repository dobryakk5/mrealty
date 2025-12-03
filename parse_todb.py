"""
Модуль для работы с таблицей ads_cian - парсинг CIAN в БД
Отдельно от основного db_handler.py для других задач
"""

import os
import re
from datetime import datetime
import asyncpg
from typing import Dict, Optional
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
    """Создает таблицу ads_cian для хранения объявлений с CIAN и хранимую процедуру для сохранения"""
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
                metro_id INTEGER,
                min_metro SMALLINT,
                address TEXT,
                district_id INTEGER,
                tags TEXT,
                person_type TEXT,
                person TEXT,
                object_type_id SMALLINT,
                source_created TIMESTAMP NULL,
                processed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(avitoid)
            );

            CREATE INDEX IF NOT EXISTS idx_ads_cian_avitoid ON ads_cian(avitoid);
            CREATE INDEX IF NOT EXISTS idx_ads_cian_processed ON ads_cian(processed);

            """)

            # Удаляем старую функцию если существует (для сброса кэша типов)
            await conn.execute("""
            DROP FUNCTION IF EXISTS upsert_cian_ad(TEXT, NUMERIC, BIGINT, SMALLINT, NUMERIC, SMALLINT, SMALLINT, TEXT, INTEGER, SMALLINT, TEXT, INTEGER, TEXT, TEXT, TEXT, SMALLINT, TIMESTAMP, BOOLEAN);
            DROP FUNCTION IF EXISTS upsert_cian_ad(TEXT, NUMERIC, BIGINT, SMALLINT, NUMERIC, SMALLINT, SMALLINT, TEXT, INTEGER, SMALLINT, TEXT, INTEGER, TEXT, SMALLINT, TEXT, SMALLINT, TIMESTAMP, BOOLEAN);
            """)

            # Создаем хранимую процедуру для сохранения объявлений
            await conn.execute("""
            CREATE OR REPLACE FUNCTION upsert_cian_ad(
                p_url TEXT,
                p_avitoid NUMERIC,
                p_price BIGINT,
                p_rooms SMALLINT,
                p_area NUMERIC,
                p_floor SMALLINT,
                p_total_floors SMALLINT,
                p_complex TEXT,
                p_metro_id INTEGER,
                p_min_metro SMALLINT,
                p_address TEXT,
                p_district_id INTEGER,
                p_tags TEXT,
                p_person_type SMALLINT,
                p_person TEXT,
                p_object_type_id SMALLINT,
                p_source_created TIMESTAMP,
                p_should_mark_processed BOOLEAN
            ) RETURNS TABLE(
                operation_type TEXT,
                old_price BIGINT,
                new_price BIGINT,
                is_new_record BOOLEAN
            ) AS $$
            DECLARE
                v_old_price BIGINT;
                v_exists BOOLEAN;
                v_price_changed BOOLEAN;
            BEGIN
                -- Проверяем существование записи и получаем старую цену
                SELECT price INTO v_old_price
                FROM ads_cian
                WHERE avitoid = p_avitoid;

                v_exists := FOUND;
                v_price_changed := (v_old_price IS DISTINCT FROM p_price);

                -- Выполняем INSERT ... ON CONFLICT
                INSERT INTO ads_cian (
                    url, avitoid, price, rooms, area, floor, total_floors,
                    complex, metro_id, min_metro, address, district_id, tags,
                    person_type, person, object_type_id, source_created, processed
                ) VALUES (
                    p_url, p_avitoid, p_price, p_rooms, p_area, p_floor, p_total_floors,
                    p_complex, p_metro_id, p_min_metro, p_address, p_district_id, p_tags,
                    p_person_type, p_person, p_object_type_id, p_source_created, p_should_mark_processed
                )
                ON CONFLICT (avitoid) DO UPDATE SET
                    url = EXCLUDED.url,
                    price = EXCLUDED.price,
                    rooms = EXCLUDED.rooms,
                    area = EXCLUDED.area,
                    floor = EXCLUDED.floor,
                    total_floors = EXCLUDED.total_floors,
                    complex = EXCLUDED.complex,
                    metro_id = EXCLUDED.metro_id,
                    min_metro = EXCLUDED.min_metro,
                    address = EXCLUDED.address,
                    district_id = EXCLUDED.district_id,
                    tags = EXCLUDED.tags,
                    person_type = EXCLUDED.person_type,
                    person = EXCLUDED.person,
                    object_type_id = EXCLUDED.object_type_id,
                    source_created = EXCLUDED.source_created,
                    processed = CASE
                        WHEN ads_cian.price IS DISTINCT FROM EXCLUDED.price THEN FALSE
                        ELSE EXCLUDED.processed
                    END;

                -- Возвращаем результат
                IF NOT v_exists THEN
                    -- Новая запись
                    RETURN QUERY SELECT 'inserted'::TEXT, NULL::BIGINT, p_price, TRUE;
                ELSIF v_price_changed THEN
                    -- Обновление с изменением цены
                    RETURN QUERY SELECT 'updated'::TEXT, v_old_price, p_price, TRUE;
                ELSE
                    -- Дубликат без изменения цены
                    RETURN QUERY SELECT 'duplicate'::TEXT, v_old_price, p_price, FALSE;
                END IF;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # print("[DB] Таблица ads_cian создана успешно")  # Убрано из лога

            # Создаем схему system если её нет
            await conn.execute("CREATE SCHEMA IF NOT EXISTS system")
            
            # Создаем таблицу для отслеживания прогресса парсинга
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS system.parsing_progress (
                id SERIAL PRIMARY KEY,
                property_type INTEGER NOT NULL,
                time_period INTEGER,  -- Может быть NULL для отключения фильтра по времени
                current_metro_id INTEGER NOT NULL,
                source SMALLINT NOT NULL DEFAULT 4,  -- Источник парсинга (4=CIAN, 1=AVITO, 2=DOMCLICK, 3=YANDEX)
                time_upd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'active',
                total_metros INTEGER,
                processed_metros INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_parsing_progress_latest ON system.parsing_progress(property_type, time_period, source, time_upd DESC);
            CREATE INDEX IF NOT EXISTS idx_parsing_progress_no_time ON system.parsing_progress(property_type, source, time_upd DESC) WHERE time_period IS NULL;
            """)
            # print("[DB] Таблица system.parsing_progress создана успешно")  # Убрано из лога
            
            # Мигрируем существующие записи, помечая как обработанные те, что содержат исключаемые слова
            try:
                await migrate_existing_processed_records()
            except Exception as e:
                print(f"[DB] Предупреждение: не удалось выполнить миграцию существующих записей: {e}")
                
        except Exception as e:
            print(f"[DB] Ошибка создания таблицы ads_cian: {e}")
            raise


def _should_mark_as_processed(address: str, geo_labels: list = None) -> bool:
    """
    ФУНКЦИЯ НЕ ИСПОЛЬЗУЕТСЯ: Логика упрощена
    
    Старая логика: проверяла слова "область", "НАО" и т.д. для московских карточек
    Новая логика: 
    - Карточки из области (district_id = -1) → processed = TRUE
    - Московские карточки → processed = FALSE (всегда обрабатываем)
    """
    # Слова, при наличии которых объявление помечается как обработанное
    exclude_words = ["Новомосковский", "НАО", "ТАО", "область"]
    
    # Проверяем адрес
    if address:
        address_lower = address.lower()
        for word in exclude_words:
            if word.lower() in address_lower:
                return True
    
    # Проверяем geo_labels
    if geo_labels:
        for geo_item in geo_labels:
            if geo_item:
                geo_str = str(geo_item).lower()
                for word in exclude_words:
                    if word.lower() in geo_str:
                        return True
    
    return False

async def save_cian_ad(ad_data: Dict) -> bool:
    """
    Сохраняет объявление CIAN в БД.

    Возвращает:
        True - если объявление добавлено как новое ИЛИ если цена изменилась
        False - если дубликат без изменения цены

    При дубликате:
        - Если цена изменилась: обновляет все поля и устанавливает processed=FALSE
        - Если цена не изменилась: обновляет все поля кроме processed
    """
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

            # Парсим метро и время
            min_metro = None
            metro_id = None
            
            # Получаем metro_id по cian_id из таблицы metro
            station_cian_id = ad_data.get('station_cian_id')
            if station_cian_id:
                metro_id = await conn.fetchval("""
                    SELECT id FROM metro 
                    WHERE cian_id = $1
                    LIMIT 1
                """, station_cian_id)
            
            if ad_data.get('walk_minutes'):
                try:
                    min_metro = int(ad_data['walk_minutes'])
                except (ValueError, TypeError):
                    pass

            # Адрес/район из geo_labels по правилам:
            # 1) Фильтруем элементы, содержащие метро
            # 2) Проверяем все элементы на наличие "р-н" - выносим в district_id
            # 3) Остальные элементы объединяем в address
            address = None
            district_id = None
            
            # Проверяем, не является ли карточка из области (district_id = -1)
            if ad_data.get('district_id') == -1:
                # Карточка из области - сохраняем адрес как есть
                if ad_data.get('geo_labels'):
                    address = ', '.join(ad_data['geo_labels'])
                district_id = -1
            else:
                # Обычная московская карточка - применяем стандартную логику
                if ad_data.get('geo_labels'):
                    # Фильтруем элементы, исключая метро
                    metro_stop_words = ['м.', 'м ', 'метро', 'станция', 'станции']
                    filtered_geo = []
                    
                    for geo_item in ad_data['geo_labels']:
                        geo_str = str(geo_item).strip()
                        if geo_str:
                            # Проверяем, не содержит ли элемент метро
                            geo_lower = geo_str.lower()
                            is_metro = any(stop_word in geo_lower for stop_word in metro_stop_words)
                            
                            if not is_metro:
                                filtered_geo.append(geo_str)
                    
                    if filtered_geo:
                        # Ищем элемент с районом
                        district_item = None
                        address_items = []
                        
                        # Обрабатываем элементы по порядку
                        district_found = False
                        for i, item in enumerate(filtered_geo):
                            # Если район уже найден, все последующие элементы идут в address
                            if district_found:
                                address_items.append(item)
                                continue
                            
                            # Проверяем, является ли элемент районом
                            district_patterns = [
                                r'\bр-?н\b',           # р-н, рн
                                r'\bрайон\b',          # район
                                r'\bр-он\b',           # р-он
                                r'\bр\.н\.\b',         # р.н.
                            ]
                            
                            is_district = False
                            for pattern in district_patterns:
                                if re.search(pattern, item, re.IGNORECASE):
                                    is_district = True
                                    # Убираем все варианты написания района
                                    cleaned_item = re.sub(r'\bр-?н\b|\bрайон\b|\bр-он\b|\bр\.н\.\b', '', item, flags=re.IGNORECASE).strip()
                                    district_item = cleaned_item
                                    district_found = True
                                    break
                            
                            if is_district:
                                # Найден район - не добавляем в адрес
                                pass
                            else:
                                # Не район - добавляем в адрес
                                address_items.append(item)
                        
                        # Формируем адрес и ищем district_id
                        if district_item:
                            # Ищем district_id по названию района
                            district_id = await conn.fetchval("""
                                SELECT id FROM districts 
                                WHERE LOWER(name) = LOWER($1)
                                LIMIT 1
                            """, district_item)
                        
                        if address_items:
                            address = ', '.join(address_items)

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
                    'owner': 3,
                    'agency': 2,
                    'user': 5,
                    'private': 1,
                    'developer': 4,
                    'unknown': None  # Неизвестный тип → NULL в БД
                }
                person_type = type_mapping.get(seller['type'], None)  # Для неизвестных типов → NULL
            
            if seller.get('name'):
                # Очищаем имя от лишнего текста
                clean_name = seller['name']
                stop_words = ['Документы проверены', 'Посмотреть все объекты', 'Суперагент', '+7', 'Написать']
                for stop_word in stop_words:
                    if stop_word in clean_name:
                        clean_name = clean_name.split(stop_word)[0].strip()
                person = clean_name

            # Определяем, нужно ли пометить объявление как обработанное
            if district_id == -1:
                # Карточка из области - сразу помечаем как обработанную
                should_mark_processed = True
            else:
                # Обычная московская карточка - всегда помечаем как необработанную
                should_mark_processed = False

            # Нормализуем source_created (ожидается datetime для TIMESTAMP)
            created_value = ad_data.get('created_dt')
            if isinstance(created_value, str):
                try:
                    created_value = datetime.strptime(created_value, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    created_value = None
            elif not (hasattr(created_value, 'year') and hasattr(created_value, 'hour')):
                created_value = None

            # Очищаем название жилищного комплекса
            complex_name = ad_data.get('complex')
            if complex_name:
                # Убираем "ЖК" и кавычки
                complex_name = re.sub(r'^ЖК\s*[«"]?', '', complex_name)  # Убираем "ЖК " в начале
                complex_name = re.sub(r'[»"]$', '', complex_name)  # Убираем кавычки в конце
                complex_name = complex_name.strip()  # Убираем лишние пробелы

            # Вызываем хранимую процедуру
            result = await conn.fetchrow("""
                SELECT * FROM upsert_cian_ad($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            """,
                ad_data.get('URL'),
                avitoid,
                price,
                rooms,
                ad_data.get('area_m2'),
                ad_data.get('floor'),
                ad_data.get('floor_total'),
                complex_name,
                metro_id,
                min_metro,
                address,
                district_id,
                tags,
                person_type,
                person,
                (1 if ad_data.get('property_type') == 2 else 2 if ad_data.get('property_type') == 1 else None),  # 2=новостройка→1, 1=вторичка→2, не указан→NULL
                created_value,
                should_mark_processed
            )

            # Обрабатываем результат от хранимой процедуры
            operation_type = result['operation_type']
            old_price_val = result['old_price']
            new_price_val = result['new_price']
            is_new = result['is_new_record']

            if operation_type == 'inserted':
                print(f"[DB] ➕ Добавлено объявление {avitoid}: {ad_data.get('URL')}")
                return True
            elif operation_type == 'updated':
                print(f"[DB] 💰 Обновлена цена объявления {avitoid}: {old_price_val} → {new_price_val} (processed=FALSE)")
                return True
            else:  # duplicate
                print(f"[DB] 🔄 Пропущено (дубликат без изменений) объявление {avitoid}")
                return False
            
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
    """Закрывает пул соединений с БД"""
    global _cian_db_pool
    if _cian_db_pool:
        await _cian_db_pool.close()
        _cian_db_pool = None
        print("[DB] Пул соединений с БД закрыт")

async def create_parsing_session(property_type: int, time_period: int = None, total_metros: int = None, source: int = 4) -> int:
    """Создает новую сессию парсинга для all запуска. Возвращает ID сессии. source = 4 для CIAN, 1 для AVITO."""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            # Получаем первую станцию метро для начала (metro.id для записи в parsing_progress)
            first_metro = await conn.fetchrow("""
                SELECT id, cian_id FROM metro 
                ORDER BY id 
                LIMIT 1
            """)
            
            if not first_metro:
                raise Exception("Не найдено ни одной станции метро")
            
            # Создаем запись о новой сессии (записываем metro.id)
            session_id = await conn.fetchval("""
                INSERT INTO system.parsing_progress 
                (property_type, time_period, current_metro_id, source, total_metros, status)
                VALUES ($1, $2, $3, $5, $4, 'active')
                RETURNING id
            """, property_type, time_period, first_metro['id'], total_metros, source)
            
            source_name = "CIAN" if source == 4 else "AVITO" if source == 1 else f"источник {source}"
            print(f"[PROGRESS] Создана сессия парсинга ID {session_id} для {source_name}, начинаем с метро ID {first_metro['id']} (CIAN ID: {first_metro['cian_id']})")
            return session_id
            
        except Exception as e:
            print(f"[PROGRESS] Ошибка создания сессии парсинга: {e}")
            raise

async def update_parsing_progress(session_id: int, current_metro_id: int, processed_count: int = None):
    """Обновляет прогресс парсинга - текущую станцию (metro.id) и время обновления."""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            update_fields = ["current_metro_id = $2", "time_upd = CURRENT_TIMESTAMP"]
            params = [session_id, current_metro_id]
            
            if processed_count is not None:
                update_fields.append("processed_metros = $3")
                params.append(processed_count)
            
            query = f"""
                UPDATE system.parsing_progress 
                SET {', '.join(update_fields)}
                WHERE id = $1
            """
            
            await conn.execute(query, *params)
            print(f"[PROGRESS] Обновлен прогресс сессии {session_id}: метро ID {current_metro_id}")
            
        except Exception as e:
            print(f"[PROGRESS] Ошибка обновления прогресса: {e}")

async def complete_parsing_session(session_id: int):
    """Завершает сессию парсинга - устанавливает status = 'completed'."""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                UPDATE system.parsing_progress 
                SET status = 'completed', time_upd = CURRENT_TIMESTAMP
                WHERE id = $1
            """, session_id)
            
            print(f"[PROGRESS] Завершена сессия парсинга ID {session_id}")
            
        except Exception as e:
            print(f"[PROGRESS] Ошибка завершения сессии: {e}")

async def check_session_time_limit(property_type: int, time_period: int = None, source: int = 4, min_hours_between_sessions: int = 6) -> tuple:
    """Проверяет, можно ли запустить новую сессию парсинга с учетом временных ограничений.
    
    Args:
        property_type: Тип недвижимости
        time_period: Период времени (может быть None)
        source: Источник данных (1=AVITO, 4=CIAN)
        min_hours_between_sessions: Минимальное количество часов между сессиями
        
    Returns:
        tuple: (can_start: bool, last_session: dict or None, hours_since_completion: float or None)
    """
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            # Ищем последнюю завершенную сессию
            if time_period is None:
                last_session = await conn.fetchrow("""
                    SELECT id, current_metro_id, total_metros, processed_metros, status, time_upd, created_at
                    FROM system.parsing_progress 
                    WHERE property_type = $1 AND time_period IS NULL AND source = $2 AND status = 'completed'
                    ORDER BY time_upd DESC 
                    LIMIT 1
                """, property_type, source)
            else:
                last_session = await conn.fetchrow("""
                    SELECT id, current_metro_id, total_metros, processed_metros, status, time_upd, created_at
                    FROM system.parsing_progress 
                    WHERE property_type = $1 AND time_period = $2 AND source = $3 AND status = 'completed'
                    ORDER BY time_upd DESC 
                    LIMIT 1
                """, property_type, time_period, source)
            
            source_name = "CIAN" if source == 4 else "AVITO" if source == 1 else f"источник {source}"
            
            if not last_session:
                print(f"[SESSION_CHECK] Завершенных сессий для {source_name} не найдено - можно начинать")
                return True, None, None
            
            # Вычисляем время, прошедшее с окончания последней сессии
            from datetime import datetime
            now = datetime.now()
            last_completion = last_session['time_upd']
            
            # Если time_upd is timezone-aware, делаем now тоже timezone-aware
            if last_completion.tzinfo is not None:
                import pytz
                if now.tzinfo is None:
                    now = pytz.timezone('Europe/Moscow').localize(now)
            else:
                # Если time_upd is timezone-naive, убираем timezone из now
                if now.tzinfo is not None:
                    now = now.replace(tzinfo=None)
            
            time_diff = now - last_completion
            hours_since_completion = time_diff.total_seconds() / 3600
            
            can_start = hours_since_completion >= min_hours_between_sessions
            
            print(f"[SESSION_CHECK] Последняя сессия {source_name} завершена {hours_since_completion:.1f} часов назад")
            print(f"[SESSION_CHECK] Минимальный интервал: {min_hours_between_sessions} часов")
            print(f"[SESSION_CHECK] Можно запускать новую сессию: {'Да' if can_start else 'Нет'}")
            
            if not can_start:
                remaining_hours = min_hours_between_sessions - hours_since_completion
                print(f"[SESSION_CHECK] До следующего запуска осталось: {remaining_hours:.1f} часов")
            
            return can_start, dict(last_session), hours_since_completion
            
        except Exception as e:
            print(f"[SESSION_CHECK] Ошибка проверки временных ограничений: {e}")
            return True, None, None  # В случае ошибки разрешаем запуск

async def get_last_parsing_progress(property_type: int, time_period: int = None, source: int = 4) -> dict:
    """Получает последний прогресс парсинга для указанных параметров. Ищет только по указанному source."""
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            # Для NULL time_period используем специальный запрос
            if time_period is None:
                progress = await conn.fetchrow("""
                    SELECT id, current_metro_id, total_metros, processed_metros, status
                    FROM system.parsing_progress 
                    WHERE property_type = $1 AND time_period IS NULL AND source = $2
                    ORDER BY time_upd DESC 
                    LIMIT 1
                """, property_type, source)
            else:
                progress = await conn.fetchrow("""
                    SELECT id, current_metro_id, total_metros, processed_metros, status
                    WHERE property_type = $1 AND time_period = $2 AND source = $3
                    ORDER BY time_upd DESC 
                    LIMIT 1
                """, property_type, time_period, source)
            
            if progress:
                source_name = "CIAN" if source == 4 else "AVITO" if source == 1 else f"источник {source}"
                print(f"[PROGRESS] Найден прогресс для {source_name}: сессия {progress['id']}, метро ID {progress['current_metro_id']}, статус: {progress['status']}")
                return dict(progress)
            else:
                source_name = "CIAN" if source == 4 else "AVITO" if source == 1 else f"источник {source}"
                print(f"[PROGRESS] Прогресс не найден для {source_name}, property_type={property_type}, time_period={time_period}")
                return None
                
        except Exception as e:
            print(f"[PROGRESS] Ошибка получения прогресса: {e}")
            return None

async def get_all_metro_stations() -> list:
    """Получает все московские станции метро из БД для обработки (is_msk IS NOT FALSE)"""
    try:
        pool = await _get_cian_pool()
        async with pool.acquire() as conn:
            stations = await conn.fetch("""
                SELECT id, name, cian_id
                FROM metro
                WHERE is_msk IS NOT FALSE
                ORDER BY id
            """)
            return [dict(station) for station in stations]
    except Exception as e:
        print(f"❌ Ошибка получения станций метро: {e}")
        return []

async def migrate_existing_processed_records():
    """
    ФУНКЦИЯ ОТКЛЮЧЕНА: Миграция существующих записей не выполняется
    Логика обработки адресов с "область" и "НАО" теперь работает на этапе парсинга:
    карточки из области получают district_id = -1 и автоматически processed = TRUE
    """
    print("[MIGRATION] Функция миграции отключена - обработка только для новых записей")
    return 0

async def get_unprocessed_cian_ads(limit: int = None) -> list:
    """
    Получает необработанные объявления CIAN (processed = FALSE)
    
    Args:
        limit: Максимальное количество записей для получения
        
    Returns:
        Список необработанных объявлений
    """
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            query = """
                SELECT * FROM ads_cian 
                WHERE processed = FALSE 
                ORDER BY created_at DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            records = await conn.fetch(query)
            return [dict(record) for record in records]
            
        except Exception as e:
            print(f"[DB] Ошибка получения необработанных объявлений: {e}")
            return []

async def get_cian_ads_processing_stats() -> dict:
    """
    Получает статистику по обработке объявлений CIAN
    
    Returns:
        Словарь со статистикой
    """
    pool = await _get_cian_pool()
    async with pool.acquire() as conn:
        try:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE processed = TRUE) as processed,
                    COUNT(*) FILTER (WHERE processed = FALSE) as unprocessed
                FROM ads_cian
            """)
            
            if stats:
                return {
                    'total': stats['total'],
                    'processed': stats['processed'],
                    'unprocessed': stats['unprocessed'],
                    'processed_percentage': round((stats['processed'] / stats['total'] * 100), 2) if stats['total'] > 0 else 0
                }
            else:
                return {'total': 0, 'processed': 0, 'unprocessed': 0, 'processed_percentage': 0}
                
        except Exception as e:
            print(f"[DB] Ошибка получения статистики: {e}")
            return {'total': 0, 'processed': 0, 'unprocessed': 0, 'processed_percentage': 0}
