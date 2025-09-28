#!/usr/bin/env python3
"""
Модуль для получения расширенных данных по GUID из Baza Winner API
Вынесен из flexible_collector1.py для использования в realty_parser_server
"""

import asyncio
import aiohttp
import asyncpg
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация токенов (будут загружены из БД)
ACCESS_TOKEN = None
USER_ID = "594465"
ORDER_ID = "813ea25b-faae-4de4-9597-840f80f42495"
WSCG = None

async def load_config_from_db():
    """Загружает конфигурацию из таблицы users.params"""
    global ACCESS_TOKEN, WSCG

    try:
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            print("❌ DATABASE_URL не найден в .env файле")
            return False

        conn = await asyncpg.connect(database_url)

        # Загружаем токены
        records = await conn.fetch('SELECT code, data FROM users.params WHERE code IN ($1, $2)', 'w7_token', 'w7_WSCG')

        for record in records:
            if record['code'] == 'w7_token':
                ACCESS_TOKEN = record['data']
            elif record['code'] == 'w7_WSCG':
                WSCG = record['data']

        await conn.close()

        if ACCESS_TOKEN and WSCG:
            print("✅ Конфигурация загружена из БД")
            return True
        else:
            print("❌ Не все токены найдены в БД")
            return False

    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации из БД: {e}")
        return False

def get_current_utc_timestamp():
    """Генерирует текущее UTC время в формате wsct"""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

class ExtendedDataCollector:
    """Класс для получения расширенных данных по GUID из Baza Winner API"""

    def __init__(self):
        self.user_id = USER_ID
        self.access_token = ACCESS_TOKEN
        self.order_id = ORDER_ID
        self.wscg = WSCG
        self.wsct = get_current_utc_timestamp()
        self._config_loaded = False

        self.base_url = "https://mls.baza-winner.ru"
        self.endpoint = f"/v2/users/{self.user_id}/orders/{self.order_id}/items/_search.json"

        # Основные параметры запроса
        self.query_params = {
            "project_code": "w7",
            "pack_history": "1",
            "except_null": "1",
            "return_restricted": "1",
            "wscg": self.wscg,
            "wsct": self.wsct
        }

        # Заголовки (будут обновлены после загрузки конфигурации)
        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Content-Type': 'application/json',
            'Origin': 'https://w7.baza-winner.ru',
            'Referer': 'https://w7.baza-winner.ru/',
            'Access-Token': self.access_token,
            'access_token': self.access_token,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

    async def ensure_config_loaded(self):
        """Убеждается, что конфигурация загружена из БД"""
        if not self._config_loaded:
            success = await load_config_from_db()
            if success:
                # Обновляем токены в экземпляре
                self.access_token = ACCESS_TOKEN
                self.wscg = WSCG

                # Обновляем заголовки
                self.headers['Access-Token'] = self.access_token
                self.headers['access_token'] = self.access_token

                # Обновляем параметры запроса
                self.query_params['wscg'] = self.wscg

                self._config_loaded = True
            else:
                raise Exception("Не удалось загрузить конфигурацию из БД")

        return True

    def create_extended_payload_by_guid(self, guid: str) -> Dict[str, Any]:
        """Создает payload для получения расширенных данных по GUID"""

        # Полный список полей из расширенного API (DSL v3)
        fields = [
            "guid", "object_guid", "realty_type_name", "total_room_count", "offer_room_count",
            "is_studio", "is_free_planning", "offer_part_count", "total_part_count",
            "is_construction_address", "geo_cache_housing_complex_name", "address",
            "geo_cache_subway_station_name_1", "walking_access_1", "transport_access_1",
            "geo_cache_subway_station_name_2", "walking_access_2", "transport_access_2",
            "geo_cache_subway_station_name_3", "walking_access_3", "transport_access_3",
            "geo_cache_subway_station_name_4", "walking_access_4", "transport_access_4",
            "price", "base_currency_id", "pay_period_type_name", "price_change_date",
            "price_change_type_id", "pub_datetime", "creation_datetime", "realty_type_id",
            "phone_list", "phone_list_xz", "geo_cache_highway_name_1", "city_remoteness_1",
            "geo_cache_highway_name_2", "city_remoteness_2", "house_square", "electricity_type_name",
            "land_square", "heating_type_name", "security_type_name", "sewerage_type_name",
            "plumbing_type_name", "gas_type_name", "has_pool", "is_registration_available",
            "realty", "build_length", "build_width", "build_height", "roof_material_type_id",
            "roof_material_type_name", "ceiling_height", "ceiling_material_type_id",
            "ceiling_material_type_name", "walls_material_type_id", "walls_material_type_name",
            "floor_type_id", "floor_type_name", "building_type_id", "building_type_name",
            "location_type_id", "location_type_name", "location", "ownership_type_id",
            "ownership_type_name", "total_square", "has_carservice", "electricity_type_id",
            "has_carwash", "heating_type_id", "has_basement", "plumbing_type_id",
            "has_repair_block", "has_tire", "has_video_observation", "has_pit",
            "has_pass_entry", "elevator_type_id", "elevator_type_name", "has_hour_security",
            "gate_type_id", "gate_type_name", "deal_type_id", "has_furniture",
            "has_kitchen_furniture", "has_refrigerator", "has_conditioner", "has_tv",
            "has_dishwasher", "has_washing_machine", "is_pet_allowed", "has_internet",
            "is_children_allowed", "has_glass_packet", "life_square", "kitchen_square",
            "balcony_type_name", "storey", "storeys_count", "water_closet_type_name",
            "window_overlook_type_name", "sale_type_name", "rent_type_id", "rent_type_name",
            "parking_type_name", "apartment_condition_type_name", "territory_type_name",
            "min_square", "max_square", "entrance_type_name", "office_class_name",
            "is_accessible_by_auto", "is_accessible_by_rail", "note", "video_list",
            "w6_offer_id", "photo_list", "phone_list.is_black", "phone_list.black_note",
            "has_online_presentation", "land_category_id", "land_category_name",
            "cadastral_number", "water_closet_location_id", "water_closet_location_name",
            "bedroom_count", "has_sauna", "has_shower", "has_stall", "building_batch_name",
            "media_name", "external_url", "external_id", "area", "is_new_building",
            "apartment_condition_type_id", "agency_commission", "prepayment",
            "are_communal_payments_included", "has_deposit", "deposit", "is_auction",
            "square_explication", "external_seller_2", "winner_only", "is_plinth_basement",
            "vat_type_id", "vat_type_name", "vat_value"
        ]

        payload = {
            "filters": {
                "guid": guid
            },
            "conditions": {
                "realty_section": {"code": ["flat"]},
                "area": {"code": ["msk"]},
                "deal_type": {"code": ["sale"]}
            },
            "from": 0,
            "size": 1,
            "dsl_version": 3,
            "fields": fields
        }

        return payload

    async def get_photo_list_by_guid(self, guid: str) -> Optional[Dict[str, Any]]:
        """Получает детализированный список фото по GUID объявления"""

        # Убеждаемся, что конфигурация загружена
        await self.ensure_config_loaded()

        payload = {
            "filters": {
                "guid": guid
            },
            "conditions": {
                "realty_section": {"code": ["flat"]},
                "area": {"code": ["msk"]},
                "deal_type": {"code": ["sale"]}
            },
            "from": 0,
            "size": 1,
            "dsl_version": 3,
            "fields": ["photo_list"]
        }

        url = self.base_url + self.endpoint

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    params=self.query_params,
                    json=payload,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:

                    if response.status == 200:
                        data = await response.json()

                        if 'advs' in data and len(data['advs']) > 0:
                            return data['advs'][0]
                        else:
                            print(f"❌ Не найдено объявление с GUID: {guid}")
                            return None

                    else:
                        text = await response.text()
                        print(f"❌ HTTP {response.status}: {text[:200]}")
                        return None

        except Exception as e:
            print(f"💥 Ошибка запроса photo_list: {e}")
            return None

    async def get_extended_data_by_guid(self, guid: str) -> Optional[Dict[str, Any]]:
        """Получает расширенные данные по GUID объявления"""

        # Убеждаемся, что конфигурация загружена
        await self.ensure_config_loaded()

        payload = self.create_extended_payload_by_guid(guid)
        url = self.base_url + self.endpoint

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    params=self.query_params,
                    json=payload,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:

                    if response.status == 200:
                        data = await response.json()

                        if 'advs' in data and len(data['advs']) > 0:
                            return data['advs'][0]
                        else:
                            print(f"❌ Объявление с GUID {guid} не найдено")
                            return None

                    else:
                        text = await response.text()
                        print(f"❌ HTTP {response.status}: {text[:200]}")
                        return None

        except Exception as e:
            print(f"💥 Ошибка запроса для GUID {guid}: {e}")
            return None

    def convert_to_cian_format(self, extended_data: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертирует расширенные данные в формат, похожий на ЦИАН"""

        if not extended_data:
            return {}

        # Маппинг полей из расширенного формата в формат ЦИАН
        cian_format = {}

        # Основные поля
        cian_format['URL'] = extended_data.get('external_url')
        cian_format['Комнат'] = extended_data.get('total_room_count')
        cian_format['Цена_raw'] = extended_data.get('price')

        # Площади
        cian_format['Общая площадь'] = extended_data.get('total_square')
        cian_format['Жилая площадь'] = extended_data.get('life_square')
        cian_format['Площадь кухни'] = extended_data.get('kitchen_square')

        # Этаж
        if extended_data.get('storey') and extended_data.get('storeys_count'):
            cian_format['Этаж'] = extended_data.get('storey')
            cian_format['Всего этажей'] = extended_data.get('storeys_count')

        # Характеристики
        cian_format['Санузел'] = extended_data.get('water_closet_type_name')
        cian_format['Балкон/лоджия'] = extended_data.get('balcony_type_name')
        cian_format['Ремонт'] = extended_data.get('apartment_condition_type_name')
        cian_format['Тип дома'] = extended_data.get('building_type_name')
        cian_format['Высота потолков'] = extended_data.get('ceiling_height')

        # Материал стен
        if extended_data.get('walls_material_type_name'):
            cian_format['Материал стен'] = extended_data.get('walls_material_type_name')

        # Год постройки - пытаемся извлечь из разных полей
        year = None
        if extended_data.get('built_year'):
            year = extended_data.get('built_year')

        if year:
            cian_format['Год постройки'] = year

        # Серия дома
        if extended_data.get('building_batch_name'):
            cian_format['Строительная серия'] = extended_data.get('building_batch_name')

        # Адрес (убираем "Москва г., " из начала)
        address = extended_data.get('address', '')
        if address and address.startswith('Москва г., '):
            address = address[11:]  # Убираем "Москва г., "
        cian_format['Адрес'] = address

        # Метро
        metro_station = extended_data.get('geo_cache_subway_station_name_1')
        metro_time = extended_data.get('walking_access_1')

        if metro_station and metro_time:
            cian_format['Минут метро'] = f"{metro_time} {metro_station}"

        # Описание из заметок
        if extended_data.get('note'):
            cian_format['Описание'] = extended_data.get('note')

        # Дополнительные характеристики (булевы значения)
        if extended_data.get('has_furniture'):
            cian_format['Мебель'] = 'есть' if extended_data['has_furniture'] else 'нет'

        if extended_data.get('has_refrigerator'):
            cian_format['Холодильник'] = 'есть' if extended_data['has_refrigerator'] else 'нет'

        if extended_data.get('has_tv'):
            cian_format['Телевизор'] = 'есть' if extended_data['has_tv'] else 'нет'

        if extended_data.get('has_washing_machine'):
            cian_format['Стиральная машина'] = 'есть' if extended_data['has_washing_machine'] else 'нет'

        if extended_data.get('has_dishwasher'):
            cian_format['Посудомоечная машина'] = 'есть' if extended_data['has_dishwasher'] else 'нет'

        if extended_data.get('has_conditioner'):
            cian_format['Кондиционер'] = 'есть' if extended_data['has_conditioner'] else 'нет'

        # Разрешения
        if extended_data.get('is_pet_allowed') is not None:
            cian_format['Можно с животными'] = 'да' if extended_data['is_pet_allowed'] else 'нет'

        if extended_data.get('is_children_allowed') is not None:
            cian_format['Можно с детьми'] = 'да' if extended_data['is_children_allowed'] else 'нет'

        # Количество спален
        if extended_data.get('bedroom_count'):
            cian_format['Количество спален'] = extended_data.get('bedroom_count')

        # Кадастровый номер
        if extended_data.get('cadastral_number'):
            cian_format['Кадастровый номер'] = extended_data.get('cadastral_number')

        # Статус объявления
        cian_format['Статус'] = 'Активно'  # Данные получены из активного источника

        # Источник
        cian_format['Источник'] = extended_data.get('media_name', 'Baza Winner')

        # Фотографии (конвертируем ID в рабочие URL, только 1024x768)
        photo_list = extended_data.get('photo_list', '')
        if photo_list and isinstance(photo_list, str):
            photo_ids = photo_list.split(',')
            photo_urls = []

            for photo_id in photo_ids:
                photo_id = photo_id.strip()
                if photo_id:
                    # URL для фотографий Baza Winner в размере 1024x768
                    photo_url = f"https://images.baza-winner.ru/{photo_id}_1024x768"
                    photo_urls.append(photo_url)

            if photo_urls:
                cian_format['Фотографии'] = photo_urls
                cian_format['Количество фото'] = len(photo_urls)

        # Системные поля
        cian_format['ID объявления'] = extended_data.get('w6_offer_id')
        cian_format['External ID'] = extended_data.get('external_id')

        # Временные метки
        if extended_data.get('creation_datetime'):
            cian_format['Дата создания'] = extended_data.get('creation_datetime')

        if extended_data.get('pub_datetime'):
            cian_format['Дата публикации'] = extended_data.get('pub_datetime')

        # Телефоны
        if extended_data.get('phone_list'):
            phones = []
            for phone in extended_data['phone_list']:
                if isinstance(phone, dict) and phone.get('number'):
                    phones.append(phone['number'])
            if phones:
                cian_format['Телефоны'] = '; '.join(phones)

        # Продавец
        if extended_data.get('external_seller_2'):
            cian_format['Продавец'] = extended_data.get('external_seller_2')

        # Фотографии (если есть)
        if extended_data.get('photo_list'):
            cian_format['photo_urls'] = extended_data.get('photo_list')

        # Убираем пустые значения
        cleaned_format = {k: v for k, v in cian_format.items() if v is not None and v != ''}

        return cleaned_format

# Создаем глобальный экземпляр для использования в других модулях
extended_collector = ExtendedDataCollector()

async def get_property_by_guid(guid: str) -> Optional[Dict[str, Any]]:
    """Получает данные объявления по GUID в формате ЦИАН"""
    try:
        # Получаем расширенные данные
        extended_data = await extended_collector.get_extended_data_by_guid(guid)

        if not extended_data:
            return None

        # Конвертируем в формат ЦИАН
        cian_format = extended_collector.convert_to_cian_format(extended_data)

        return cian_format

    except Exception as e:
        print(f"❌ Ошибка получения данных по GUID {guid}: {e}")
        return None

# Для обратной совместимости с flexible_collector1.py
async def get_extended_data_by_guid(guid: str) -> Optional[Dict[str, Any]]:
    """Получает сырые расширенные данные по GUID (без конвертации)"""
    return await extended_collector.get_extended_data_by_guid(guid)