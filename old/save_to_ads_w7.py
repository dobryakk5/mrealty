#!/usr/bin/env python3
"""
Сохраняет данные из нового MLS API в таблицу ads_w7 с обновленными правилами маппинга
"""

import asyncio
import asyncpg
import json
import re
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

class W7DataSaver:
    def __init__(self):
        # Используем DATABASE_URL из .env файла
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise RuntimeError("DATABASE_URL не найден в .env файле")
        
    def extract_person_name(self, external_seller_2: str) -> Optional[str]:
        """Извлекает имя персоны из external_seller_2 для поля person"""
        
        if not external_seller_2:
            return None
        
        # НОВОЕ ПРАВИЛО: Формат "Собственник [Имя], ИСТОЧНИК ID номер"
        # Извлекаем имя после "Собственник "
        if external_seller_2.startswith('Собственник '):
            # Ищем имя после "Собственник " до запятой или до конца строки
            name_part = external_seller_2[12:]  # Убираем "Собственник "
            
            # Если есть запятая, берем все до запятой
            if ',' in name_part:
                name = name_part.split(',')[0].strip()
            else:
                name = name_part.strip()
            
            # Если имя пустое (случай "Собственник, CIAN ID"), возвращаем None
            return name if name else None
        
        # ДОПОЛНИТЕЛЬНОЕ ПРАВИЛО: Случай "Собственник, ИСТОЧНИК ID номер"
        if external_seller_2.startswith('Собственник,'):
            return None  # Нет имени
        
        # АГЕНТСКИЕ ПРАВИЛА:
        # "Агентство Владис, Агент Екатерина Трубихова, CIAN ID 27425722" -> "Владис"
        if external_seller_2.startswith('Агентство '):
            # Ищем название агентства после "Агентство " до запятой
            agency_part = external_seller_2[10:]  # Убираем "Агентство "
            if ',' in agency_part:
                agency_name = agency_part.split(',')[0].strip()
                return agency_name if agency_name else None
        
        # "Агент Александр Беседин, CIAN ID 74358" -> "Александр Беседин"
        if external_seller_2.startswith('Агент '):
            # Ищем имя агента после "Агент " до запятой
            agent_part = external_seller_2[6:]  # Убираем "Агент "
            if ',' in agent_part:
                agent_name = agent_part.split(',')[0].strip()
                return agent_name if agent_name else None
        
        # СТАРОЕ ПРАВИЛО: Формат [Имя](ссылка) - извлекаем текст между [ и ]
        match = re.search(r'\[([^\]]+)\]', external_seller_2)
        if match:
            return match.group(1)
        
        # Если нет скобок, возвращаем как есть
        return external_seller_2.strip()

    def map_walls_material(self, walls_material_id: Optional[int]) -> Optional[int]:
        """
        Маппинг материала стен:
        1,2 панельный → 1
        3,11 монолитный → 3
        4,5 кирпичный → 2
        """
        if walls_material_id is None:
            return None

        # Панельный: 1,2 → 1
        if walls_material_id in [1, 2]:
            return 1

        # Кирпичный: 4,5 → 2
        elif walls_material_id in [4, 5]:
            return 2

        # Монолитный: 3,11 → 3
        elif walls_material_id in [3, 11]:
            return 3

        # Остальные значения сохраняем как null
        else:
            return None

    def get_person_type_id(self, external_seller_2: str, is_owner: bool = False) -> Optional[int]:
        """
        Определяет тип персоны по external_seller_2 и флагу is_published_by_probable_owner:
        1 – Частное лицо,
        2 – Агентство,
        3 – Собственник
        """
        
        if not external_seller_2:
            return None
        
        # НОВОЕ ПРАВИЛО: Если is_published_by_probable_owner = True, то всегда 3 (Собственник)
        if is_owner:
            return 3  # Собственник
        
        # СТАРЫЕ ПРАВИЛА для агентств и частных лиц
        seller_lower = external_seller_2.lower()
        
        # ОБНОВЛЕННЫЕ ПРАВИЛА: Добавляем известные агентства
        known_agencies = [
            'миэль', 'ндв-недвижимость', 'хоумап', 'горобмен', 'мгсн', 
            'простор-недвижимость', 'твоя квартира', 'московский квартал',
            'остоженка', 'тандем', 'бест фактори недвижимость', 'волен выбор',
            'комфорт-5', 'династия-недвижимость', 'дом мажор', 'всё просто',
            'pr', 'привлекательные'
        ]
        
        # Проверяем по названию агентства
        if any(agency in seller_lower for agency in known_agencies):
            return 2  # Агентство
        elif any(word in seller_lower for word in ['агентство', 'агент']):
            return 2  # Агентство
        elif any(word in seller_lower for word in ['частный', 'собственник']):
            return 3  # Собственник
        else:
            return 1  # Частное лицо (по умолчанию)

    def parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Парсит datetime строку из API"""
        
        if not datetime_str:
            return None
        
        try:
            # Формат: "2025-09-12T03:41:52.908Z"
            if datetime_str.endswith('Z'):
                # Убираем Z и парсим как UTC
                dt_str = datetime_str[:-1]
                return datetime.fromisoformat(dt_str)
            else:
                return datetime.fromisoformat(datetime_str)
        except Exception:
            return None

    def get_source_id(self, media_name: str) -> Optional[int]:
        """
        Определяет source_id по media_name:
        1 - AVITO
        3 - Яндекс
        4 - ЦИАН
        6 - Youla
        11 - Domclick
        """
        
        if not media_name:
            return None
        
        media_lower = media_name.lower()
        
        if 'avito' in media_lower:
            return 1
        elif 'яндекс' in media_lower:
            return 3
        elif 'cian' in media_lower:
            return 4
        elif 'youla' in media_lower:
            return 6
        elif 'domclick' in media_lower:
            return 11
        else:
            return None

    async def get_metro_id(self, conn, metro_name: str) -> Optional[int]:
        """Получает metro_id по названию станции"""
        
        if not metro_name:
            return None
        
        # Убираем всё начиная с 'м.' (включая сам 'м.')
        if 'м.' in metro_name:
            clean_name = metro_name.split('м.')[0].strip()
        else:
            clean_name = metro_name.strip()
        
        try:
            result = await conn.fetchrow('SELECT id FROM metro WHERE name = $1', clean_name)
            return result['id'] if result else None
        except Exception:
            return None

    async def get_district_id(self, conn, district_name: str) -> Optional[int]:
        """Получает district_id по названию района, убирая ' р-н'"""
        
        if not district_name:
            return None
        
        # Убираем ' р-н' из названия
        clean_name = district_name.replace(' р-н', '').strip()
        
        try:
            result = await conn.fetchrow('SELECT id FROM districts WHERE name = $1', clean_name)
            return result['id'] if result else None
        except Exception:
            return None

    def process_record(self, ad: Dict[str, Any]) -> Dict[str, Any]:
        """Обрабатывает одну запись для сохранения в ads_w7 (структура как ads_cian)"""
        
        # Извлекаем телефоны
        phone_list = ad.get('phone_list', [])
        phone = phone_list[0].get('number') if phone_list and phone_list[0].get('number') else None
        
        # Формируем адрес (только улица + дом, без района)
        address_parts = []
        for field in ['geo_cache_street_name', 'geo_cache_building_name']:
            value = ad.get(field)
            if value:
                address_parts.append(str(value))
        
        address = ', '.join(address_parts) if address_parts else None
        
        # Извлекаем person только из external_seller_2
        external_seller_2 = ad.get('external_seller_2', '')
        is_owner = ad.get('is_published_by_probable_owner', False)  # Проверяем флаг собственника
        
        person = self.extract_person_name(external_seller_2)

        # Удаляем "Агентство " из начала person, если есть
        if person and person.startswith('Агентство '):
            person = person[10:]  # Убираем "Агентство " (10 символов)

        # Убираем всё после первой запятой
        if person and ',' in person:
            person = person.split(',')[0].strip()

        person_type_id = self.get_person_type_id(external_seller_2, is_owner)

        # Маппинг материала стен
        walls_material_type_id = self.map_walls_material(ad.get('walls_material_type_id'))
        
        # Определяем source_id
        source_id = self.get_source_id(ad.get('media_name'))
        
        # Вычисляем km_do_metro
        walking_minutes = ad.get('walking_access_1', 0)
        km_do_metro = round(walking_minutes * 0.08, 2) if walking_minutes else None
        
        # ОБНОВЛЕННОЕ ПРАВИЛО: если rooms строка или None, то ставим 0
        rooms_raw = ad.get('total_room_count')
        if isinstance(rooms_raw, str) or rooms_raw is None:
            rooms = 0
        else:
            rooms = rooms_raw
        
        # Изображения - только количество в API, ссылок нет
        # images = None
        
        # Извлекаем дополнительные поля
        ceiling_height = ad.get('ceiling_height')
        # Конвертируем ceiling_height из cm в mm если нужно (проверяем разумность значения)
        if ceiling_height and ceiling_height < 50:  # Если меньше 50, то скорее всего в метрах
            ceiling_height = int(ceiling_height * 100)  # Конвертируем в см
        elif ceiling_height and ceiling_height > 1000:  # Если больше 1000, то в мм
            ceiling_height = int(ceiling_height / 10)  # Конвертируем в см
        
        # Обработанная запись для БД ads_w7 (структура как ads_cian)
        record = {
            'id': ad.get('w6_offer_id'),
            'url': ad.get('external_url'),
            'avitoid': ad.get('external_id'),  # external_id из API
            'price': ad.get('price_rub'),
            'rooms': rooms,  # ОБНОВЛЕНО: проверка на строку
            'area': ad.get('total_square'),
            'floor': ad.get('storey'),
            'total_floors': ad.get('storeys_count'),
            'complex': ad.get('geo_cache_housing_complex_name'),  # ЖК из API
            'min_metro': walking_minutes,  # В минутах
            'address': address,
            'tags': None,
            'person_type': person_type_id,
            'person': person,
            'created_at': self.parse_datetime(ad.get('creation_datetime')),  # Время создания с сервера
            'object_type_id': ad.get('realty_type_id'),  # 1 = квартира
            'source_updated': self.parse_datetime(ad.get('pub_datetime')),  # Время публикации с сервера
            'metro_name': ad.get('geo_cache_subway_station_name_1'),  # Для поиска metro_id
            'district_name': ad.get('geo_cache_district_name'),  # Для поиска district_id
            'metro_id': None,  # Будет заполнено в save_records_to_db
            'district_id': None,  # Будет заполнено в save_records_to_db
            'processed': False,
            'debug': None,  # Можно добавить отладочную информацию
            'proc_at': datetime.now(),  # SYSDATE - когда обрабатываем
            'ceiling_height': ceiling_height,  # НОВОЕ: высота потолков в см
            'balcony_type_id': ad.get('balcony_type_id'),  # НОВОЕ: тип балкона
            'kitchen_square': ad.get('kitchen_square'),  # НОВОЕ: площадь кухни
            'life_square': ad.get('life_square'),  # НОВОЕ: жилая площадь
            'built_year': ad.get('built_year'),  # НОВОЕ: год постройки
            'building_batch_name': ad.get('building_batch_name'),  # НОВОЕ: серия дома
            'walls_material_type_id': walls_material_type_id,  # НОВОЕ: тип материала стен (с маппингом)
            'is_actual': ad.get('deal_status_id') == 1,  # НОВОЕ: признак активности объявления (1=активное, 3=неактивное)
            'guid': ad.get('guid')  # НОВОЕ: GUID объявления
        }
        
        return record

    def extract_images(self, ad: Dict[str, Any]) -> Optional[List[str]]:
        """Извлекает список ссылок на изображения из объявления"""
        
        image_urls = []
        
        # Проверяем поле photo_list (если есть)
        photo_list = ad.get('photo_list', [])
        if isinstance(photo_list, list) and photo_list:
            for photo in photo_list:
                if isinstance(photo, dict):
                    # Ищем URL в разных возможных полях
                    url = photo.get('url') or photo.get('large_url') or photo.get('medium_url') or photo.get('small_url')
                    if url:
                        image_urls.append(url)
                elif isinstance(photo, str):
                    # Если photo - просто строка URL
                    image_urls.append(photo)
        
        # Проверяем поле video_list для видео (если нужно)
        video_list = ad.get('video_list', [])
        if isinstance(video_list, list) and video_list:
            for video in video_list:
                if isinstance(video, dict):
                    url = video.get('url') or video.get('preview_url')
                    if url:
                        image_urls.append(f"VIDEO: {url}")
                elif isinstance(video, str):
                    image_urls.append(f"VIDEO: {video}")
        
        return image_urls if image_urls else None

    async def create_ads_w7_table(self, conn):
        """Создает таблицу ads_w7 если она не существует (структура как ads_cian)"""
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.ads_w7 (
            id integer PRIMARY KEY,
            url text,
            avitoid numeric,
            price bigint,
            rooms smallint,
            area numeric,
            floor smallint,
            total_floors smallint,
            complex text,
            min_metro smallint,
            address text,
            tags text,
            person_type smallint,
            person text,
            created_at timestamp without time zone DEFAULT NOW(),
            object_type_id smallint,
            source_updated timestamp without time zone DEFAULT NOW(),
            metro_id smallint,
            district_id smallint,
            processed boolean DEFAULT false,
            debug jsonb,
            proc_at timestamp without time zone,
            ceiling_height smallint,
            balcony_type_id smallint,
            kitchen_square numeric(5,2),
            life_square numeric(5,2),
            built_year smallint,
            building_batch_name text,
            walls_material_type_id smallint,
            is_actual boolean DEFAULT true,
            guid text
        )
        """
        
        await conn.execute(create_table_sql)
        print("✅ Таблица ads_w7 проверена/создана")

    async def save_records_to_db(self, records: List[Dict[str, Any]]) -> bool:
        """Сохраняет записи в БД ads_w7"""
        
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Создаем таблицу если нужно
            await self.create_ads_w7_table(conn)
            
            # Подготавливаем SQL для вставки/обновления
            insert_sql = """
            INSERT INTO public.ads_w7 (
                id, url, avitoid, price, rooms, area, floor, total_floors, complex,
                min_metro, address, tags, person_type, person, created_at,
                object_type_id, source_updated, metro_id, district_id, processed, debug, proc_at,
                ceiling_height, balcony_type_id, kitchen_square, life_square,
                built_year, building_batch_name, walls_material_type_id, is_actual, guid
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31
            )
            ON CONFLICT (id) DO UPDATE SET
                url = EXCLUDED.url,
                avitoid = EXCLUDED.avitoid,
                price = EXCLUDED.price,
                rooms = EXCLUDED.rooms,
                area = EXCLUDED.area,
                floor = EXCLUDED.floor,
                total_floors = EXCLUDED.total_floors,
                complex = EXCLUDED.complex,
                min_metro = EXCLUDED.min_metro,
                address = EXCLUDED.address,
                tags = EXCLUDED.tags,
                person_type = EXCLUDED.person_type,
                person = EXCLUDED.person,
                object_type_id = EXCLUDED.object_type_id,
                source_updated = EXCLUDED.source_updated,
                processed = EXCLUDED.processed,
                ceiling_height = EXCLUDED.ceiling_height,
                balcony_type_id = EXCLUDED.balcony_type_id,
                kitchen_square = EXCLUDED.kitchen_square,
                life_square = EXCLUDED.life_square,
                built_year = EXCLUDED.built_year,
                building_batch_name = EXCLUDED.building_batch_name,
                walls_material_type_id = EXCLUDED.walls_material_type_id,
                is_actual = EXCLUDED.is_actual,
                guid = EXCLUDED.guid
            """
            
            saved_count = 0
            
            for record in records:
                try:
                    # Получаем metro_id и district_id
                    metro_id = await self.get_metro_id(conn, record.get('metro_name'))
                    district_id = await self.get_district_id(conn, record.get('district_name'))
                    
                    
                    await conn.execute(
                        insert_sql,
                        record['id'], record['url'], record['avitoid'], record['price'],
                        record['rooms'], record['area'], record['floor'], record['total_floors'],
                        record['complex'], record['min_metro'], record['address'], record['tags'],
                        record['person_type'], record['person'], record['created_at'],
                        record['object_type_id'], record['source_updated'], metro_id,
                        district_id, record['processed'], record['debug'], record['proc_at'],
                        record['ceiling_height'], record['balcony_type_id'], record['kitchen_square'], record['life_square'],
                        record['built_year'], record['building_batch_name'], record['walls_material_type_id'],
                        record['is_actual'], record['guid']
                    )
                    saved_count += 1
                    # Подробный лог убран - только счетчик
                    
                except Exception as e:
                    print(f"❌ Ошибка сохранения записи ID {record.get('id', 'unknown')}: {e}")
            
            await conn.close()
            
            print(f"\n🎉 Успешно сохранено {saved_count} из {len(records)} записей в ads_w7")
            return saved_count > 0
            
        except Exception as e:
            print(f"💥 Ошибка подключения к БД: {e}")
            return False

    async def load_and_save_moscow_records(self):
        """Загружает и сохраняет 800 записей из Moscow JSON файла в ads_w7"""
        
        print("🚀 СОХРАНЕНИЕ 800 МОСКОВСКИХ ЗАПИСЕЙ В ads_w7")
        print("=" * 50)
        
        try:
            # Загружаем данные из файла
            with open('moscow_combined_2pages_20250913_012122.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            ads = data.get('all_ads', [])
            print(f"📋 Загружено {len(ads)} объявлений из файла")
            
            if not ads:
                print("❌ Нет данных для обработки")
                return
            
            # Обрабатываем каждую запись
            processed_records = []
            
            print("\n📊 ОБРАБОТКА ЗАПИСЕЙ:")
            print("-" * 30)
            
            for i, ad in enumerate(ads, 1):
                record = self.process_record(ad)
                processed_records.append(record)
                
                print(f"🏠 {i}. ID: {record['id']}")
                print(f"   Person: \"{record['person']}\" (тип: {record['person_type']})")
                print(f"   Source: {ad.get('media_name')} → source_id будет определен отдельно")
                print(f"   Price: {record['price']:,} руб" if record['price'] else "   Price: None")
                print(f"   Address: {record['address']}")
                print(f"   Min metro: {record['min_metro']} мин")
                print()
            
            # Сохраняем в БД
            print("💾 СОХРАНЕНИЕ В БАЗУ ДАННЫХ ads_w7:")
            print("-" * 40)
            
            success = await self.save_records_to_db(processed_records)
            
            if success:
                print(f"\n🎉 ВСЕ ЗАПИСИ УСПЕШНО СОХРАНЕНЫ В ads_w7!")
                
                # Показываем статистику
                person_types = {}
                sources = {}
                for record in processed_records:
                    ptype = record['person_type']
                    person_types[ptype] = person_types.get(ptype, 0) + 1
                
                print(f"📊 Статистика person_type:")
                type_names = {1: "Частное лицо", 2: "Агентство", 3: "Собственник"}
                for ptype, count in person_types.items():
                    type_name = type_names.get(ptype, "Неизвестно")
                    print(f"   {ptype} ({type_name}): {count}")
                    
            else:
                print("❌ Ошибка при сохранении в БД")
                
        except FileNotFoundError:
            print("❌ Файл moscow_combined_2pages_20250913_012122.json не найден")
        except Exception as e:
            print(f"💥 Ошибка: {e}")

async def main():
    saver = W7DataSaver()
    await saver.load_and_save_moscow_records()

if __name__ == "__main__":
    asyncio.run(main())