#!/usr/bin/env python3
"""
Гибкий сборщик с настраиваемыми фильтрами для Baza-Winner MLS API
"""

import asyncio
import aiohttp
import json
import argparse
import asyncpg
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union

from save_to_ads_w7 import W7DataSaver

# Конфигурация токенов (загружается из БД)
ACCESS_TOKEN = None
USER_ID = "594465"
ORDER_ID = "813ea25b-faae-4de4-9597-840f80f42495"
WSCG = None

# ========== НАСТРОЙКИ ФИЛЬТРОВ ==========
# Здесь задаются все параметры поиска

# Комнатность [0,1] (0 = студии, 1 = 1к, 2 = 2к, и т.д.)
ROOMS = "all"  # Студии и 1-комнатные

# Локация
LOCATION = "moscow_old_only"  # moscow_all, moscow_no_zelenograd, moscow_old_only

# Источники
MEDIA = "all"  # all, avito_only, cian_only, yandex_only

# Тип здания
BUILDING_TYPE = "old_only"  # old_only, new_only, all

# Тип продавца
SELLER_TYPE = "all"  # all, owner_only

# Статус сделки
STATUS = "all"  # active, inactive, all

# Цены (в рублях, None = без ограничения)
MIN_PRICE = None
MAX_PRICE = None

# Период публикации (дни)
PUBLISHED_DAYS_AGO = 7
IS_FIRST_PUBLISHED = True

# Пагинация
PAGE_SIZE = 400
MAX_PAGES = 1000

def get_current_utc_timestamp():
    """Генерирует текущее UTC время в формате wsct"""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

async def load_config_from_db():
    """Загружает конфигурацию из таблицы users.params"""
    try:
        conn = await asyncpg.connect(
            host='localhost',
            port=5432,
            user='postgres',
            password='123',
            database='realty'
        )

        records = await conn.fetch('SELECT code, data FROM users.params WHERE code IN ($1, $2)', 'w7_token', 'w7_WSCG')

        config = {}
        for record in records:
            config[record['code']] = record['data']

        await conn.close()

        print(f"📋 Найденные w7 параметры:")
        for code, data in config.items():
            print(f"   {code}: {data[:50]}{'...' if len(str(data)) > 50 else ''}")

        return config

    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации из БД: {e}")
        print("⚠️ Используйте резервные значения или проверьте подключение к БД")
        return {}

class FlexibleCollector:
    def __init__(self, access_token=None, wscg=None):
        self.user_id = USER_ID
        self.access_token = access_token or ACCESS_TOKEN
        self.order_id = ORDER_ID
        self.wscg = wscg or WSCG
        self.wsct = get_current_utc_timestamp()
        
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
        
        # Заголовки
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

    def create_base_payload(self, from_index: int = 0, size: int = 400, published_days_ago: int = 50, is_first_published: bool = False) -> Dict[str, Any]:
        """Создает базовый payload с обязательными полями"""

        # Агрегации - всегда одинаковые
        aggregations = {
            "avg_price_rub": True,
            "avg_meter_price_rub": True,
            "avg_total_price_rub": True,
            "avg_sotka_price_rub": True
        }

        # Поля для выборки - все нужные поля
        fields = [
            "guid", "deal_status_id", "user_deal_status_id", "winner_relevance", "w6_offer_id",
            "external_id", "area", "is_new_building", "object_guid", "free_mode_relevance",
            "is_selected", "is_favorite", "is_hidden", "is_exclusive_mark1", "is_sended_to_viewboard",
            "is_liked_on_viewboard", "is_disliked_on_viewboard", "is_monitored", "has_online_presentation",
            "photo_count", "video_count", "total_room_count", "offer_room_count", "is_studio",
            "is_free_planning", "realty_type_id", "geo_cache_subway_station_name_1", "geo_subway_station_guid_1",
            "transport_access_1", "walking_access_1", "geo_cache_subway_station_name_2", "geo_subway_station_guid_2",
            "transport_access_2", "walking_access_2", "geo_cache_subway_station_name_3", "geo_subway_station_guid_3",
            "transport_access_3", "walking_access_3", "geo_cache_subway_station_name_4", "geo_subway_station_guid_4",
            "transport_access_4", "walking_access_4", "geo_cache_state_name", "geo_cache_region_name",
            "geo_cache_town_name", "geo_cache_street_name", "geo_cache_building_name", "geo_cache_town_name_2",
            "geo_cache_settlement_name", "geo_cache_estate_object_name", "geo_cache_district_name",
            "geo_cache_micro_district_name", "geo_cache_street_name_2", "is_construction_address",
            "geo_cache_housing_complex_name", "storey", "storeys_count", "walls_material_type_id",
            "building_batch_name", "balcony_type_id", "water_closet_type_id", "total_square", "life_square",
            "kitchen_square", "ceiling_height", "price_rub", "agency_bonus", "agency_bonus_type_id",
            "agency_bonus_currency_type_id", "pub_datetime", "offer_pub_duration", "media_id", "media_name",
            "broker.short_name", "broker.url", "external_url", "external_seller_2", "phone_list.is_black",
            "phone_list.black_note", "is_published_by_probable_owner", "phone_list_xz", "creation_datetime",
            "deal_type_id", "security_type_id", "user_note", "price_change_date", "price_change_type_id",
            "video_list", "built_year", "client_association_list", "ownership_type_id", "sale_type_name",
            "rooms_adjacency_type_id"
        ]

        # Сортировка - всегда одинаковая
        sort = [
            {"winner_relevance": {"order": "desc"}},
            {"w6_offer_id": {"order": "desc"}}
        ]

        # Базовые условия - общие для всех запросов
        published_filter = {"days": published_days_ago}
        if is_first_published:
            published_filter["is_first_published"] = True

        base_conditions = {
            "published_days_ago": published_filter,
            "realty_section": {"code": ["flat"]},
            "deal_type": {"code": ["sale"]},
            "is_deal_actual": True
        }
        
        # Базовые фильтры - общие для всех запросов
        base_filters = {
            "is_hidden": False,
            "use_or_offer_mark": True
        }
        
        # Базовые миксины - общие для всех запросов
        base_mixins = {
            "is_selected": True,
            "is_hidden": False
        }
        
        return {
            "aggregations": aggregations,
            "fields": fields,
            "sort": sort,
            "from": from_index,
            "size": size,
            "conditions": base_conditions,
            "filters": base_filters,
            "mixins": base_mixins,
            "dsl_version": 2
        }

    # ========== ФИЛЬТРЫ ЛОКАЦИИ ==========
    
    def set_location_moscow_all(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Москва со всеми районами (включая Зеленоград и Новую Москву)"""
        payload["conditions"]["area"] = {"code": ["msk"]}
        return payload
    
    def set_location_moscow_no_zelenograd(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Москва без Зеленограда, но с Новой Москвой"""
        payload["conditions"]["area"] = {"code": ["msk"]}
        payload["conditions"]["geo_without_zelenograd"] = True
        return payload
    
    def set_location_moscow_old_only(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только старая Москва (без Зеленограда и Новой Москвы)"""
        payload["conditions"]["area"] = {"code": ["msk"]}
        payload["conditions"]["geo_without_zelenograd"] = True
        payload["conditions"]["geo_is_new_msk"] = False
        return payload

    # ========== ФИЛЬТРЫ ИСТОЧНИКОВ ==========
    
    def set_media_all(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Все источники"""
        payload["conditions"]["media"] = {"id": [21, 17, 23]}  # Яндекс, ЦИАН, АВИТО
        return payload
    
    def set_media_avito_only(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только Авито"""
        payload["conditions"]["media"] = {"id": [23]}  # ID Авито
        return payload
    
    def set_media_cian_only(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только ЦИАН"""
        payload["conditions"]["media"] = {"id": [17]}  # ID ЦИАН
        return payload
    
    def set_media_yandex_only(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только Яндекс"""
        payload["conditions"]["media"] = {"id": [21]}  # ID Яндекс
        return payload

    # ========== ФИЛЬТРЫ КОМНАТ ==========
    
    def set_rooms_all(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Все типы квартир"""
        # Для "все комнаты" НЕ указываем total_room_count вообще
        if "total_room_count" in payload["conditions"]:
            del payload["conditions"]["total_room_count"]
        # Также не ограничиваем студии
        if "is_studio" in payload["conditions"]:
            del payload["conditions"]["is_studio"]
        return payload
    
    def set_rooms_1k(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только 1-комнатные"""
        payload["conditions"]["total_room_count"] = [1]
        return payload
    
    def set_rooms_2k(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только 2-комнатные"""
        payload["conditions"]["total_room_count"] = [2]
        return payload
    
    def set_rooms_3k(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только 3-комнатные"""
        payload["conditions"]["total_room_count"] = [3]
        return payload
    
    def set_rooms_custom(self, payload: Dict[str, Any], rooms: List[int]) -> Dict[str, Any]:
        """Кастомный список комнат (0 = студии, 1 = 1к, 2 = 2к, и т.д.)"""

        # Разделяем студии (0) и обычные комнаты (1,2,3...)
        studios_needed = 0 in rooms
        regular_rooms = [r for r in rooms if r > 0]

        # Если нужны только студии
        if studios_needed and not regular_rooms:
            payload["conditions"]["is_studio"] = True
            # Убираем total_room_count если был задан
            if "total_room_count" in payload["conditions"]:
                del payload["conditions"]["total_room_count"]
        # Если нужны только обычные комнаты
        elif regular_rooms and not studios_needed:
            # Обрабатываем случай с 6+ комнатами
            if 6 in regular_rooms:
                # Если только 6, то делаем "6+"
                if regular_rooms == [6]:
                    payload["conditions"]["total_room_count"] = ["6+"]
                else:
                    # Если 6 есть в списке с другими комнатами, заменяем на "6+"
                    other_rooms = [r for r in regular_rooms if r < 6]
                    payload["conditions"]["total_room_count"] = other_rooms + ["6+"]
            else:
                payload["conditions"]["total_room_count"] = regular_rooms
            payload["conditions"]["is_studio"] = False
        # Если нужны и студии, и обычные комнаты
        elif studios_needed and regular_rooms:
            # Обрабатываем случай с 6+ комнатами
            if 6 in regular_rooms:
                # Если только 6, то делаем "6+"
                if regular_rooms == [6]:
                    payload["conditions"]["total_room_count"] = ["6+"]
                else:
                    # Если 6 есть в списке с другими комнатами, заменяем на "6+"
                    other_rooms = [r for r in regular_rooms if r < 6]
                    payload["conditions"]["total_room_count"] = other_rooms + ["6+"]
            else:
                payload["conditions"]["total_room_count"] = regular_rooms
            # Удаляем фильтр is_studio, чтобы включить студии
            if "is_studio" in payload["conditions"]:
                del payload["conditions"]["is_studio"]

        return payload

    # ========== ФИЛЬТРЫ ТИПА ЖИЛЬЯ ==========
    
    def set_old_building_only(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только вторичка"""
        payload["conditions"]["is_new_building"] = False
        return payload
    
    def set_new_building_only(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только новостройки"""
        payload["conditions"]["is_new_building"] = True
        return payload
    
    def set_building_all(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Все типы зданий"""
        # Убираем фильтр is_new_building вообще
        if "is_new_building" in payload["conditions"]:
            del payload["conditions"]["is_new_building"]
        return payload

    # ========== ФИЛЬТРЫ ЦЕН ==========
    
    def set_price_range(self, payload: Dict[str, Any], min_price: Optional[int] = None, max_price: Optional[int] = None) -> Dict[str, Any]:
        """Диапазон цен в рублях"""
        price_filter = {}
        if min_price is not None:
            price_filter["gte"] = min_price
        if max_price is not None:
            price_filter["lte"] = max_price
        
        if price_filter:
            payload["conditions"]["price_rub"] = price_filter
        return payload

    # ========== ФИЛЬТРЫ ТИПА ПРОДАВЦА ==========
    
    def set_owner_only(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только объявления от собственников"""
        payload["conditions"]["is_published_by_probable_owner"] = True
        return payload
    
    def set_all_sellers(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Все продавцы (собственники + агенты)"""
        # Убираем фильтр is_published_by_probable_owner вообще
        if "is_published_by_probable_owner" in payload["conditions"]:
            del payload["conditions"]["is_published_by_probable_owner"]
        return payload

    # ========== ФИЛЬТРЫ СТУДИЙ ==========

    def set_studio_only(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только студии"""
        payload["conditions"]["is_studio"] = True
        return payload

    def set_no_studio(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Исключить студии"""
        payload["conditions"]["is_studio"] = False
        return payload

    def set_studio_all(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Все квартиры (включая студии)"""
        # Убираем фильтр is_studio вообще
        if "is_studio" in payload["conditions"]:
            del payload["conditions"]["is_studio"]
        return payload

    # ========== ФИЛЬТРЫ СТАТУСА СДЕЛКИ ==========

    def set_deal_active(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только активные объявления"""
        payload["conditions"]["is_deal_actual"] = True
        return payload

    def set_deal_inactive(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Только неактивные/снятые объявления"""
        payload["conditions"]["is_deal_actual"] = False
        return payload

    def set_deal_all(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Все объявления (активные и неактивные)"""
        # Убираем фильтр is_deal_actual вообще
        if "is_deal_actual" in payload["conditions"]:
            del payload["conditions"]["is_deal_actual"]
        return payload


    # ========== ОСНОВНОЙ МЕТОД СОЗДАНИЯ ЗАПРОСА ==========
    
    def create_search_payload(self,
                            from_index: int = 0,
                            size: int = 400,
                            # Локация
                            location: str = "moscow_old_only",  # moscow_all, moscow_no_zelenograd, moscow_old_only
                            # Источники
                            media: str = "all",  # all, avito_only, cian_only, yandex_only
                            # Комнаты
                            rooms: Union[str, List[int]] = "all",  # all, 1k, 2k, 3k, или [1,2,3]
                            # Тип здания
                            building_type: str = "old_only",  # old_only, new_only, all
                            # Студии
                            studio: str = "all",  # all, studio_only, no_studio
                            # Тип продавца
                            seller_type: str = "all",  # all, owner_only
                            # Статус сделки
                            status: str = "active",  # active, inactive, all
                            # Цены
                            min_price: Optional[int] = None,
                            max_price: Optional[int] = None,
                            # Период публикации
                            published_days_ago: int = 50,
                            # Только новые объявления (первая публикация)
                            is_first_published: bool = False
                            ) -> Dict[str, Any]:
        """
        Создает payload с гибкими фильтрами

        Параметры:
        - location: "moscow_all", "moscow_no_zelenograd", "moscow_old_only"
        - media: "all", "avito_only", "cian_only"
        - rooms: "all", "1k", "2k", "3k" или список [1,2,3]
        - building_type: "old_only", "new_only", "all"
        - studio: "all", "studio_only", "no_studio"
        - seller_type: "all", "owner_only"
        - status: "active", "inactive", "all"
        - min_price, max_price: диапазон цен в рублях
        """
        
        # Создаем базовый payload
        payload = self.create_base_payload(from_index, size, published_days_ago, is_first_published)
        
        # Применяем фильтры локации
        if location == "moscow_all":
            payload = self.set_location_moscow_all(payload)
        elif location == "moscow_no_zelenograd":
            payload = self.set_location_moscow_no_zelenograd(payload)
        elif location == "moscow_old_only":
            payload = self.set_location_moscow_old_only(payload)
        
        # Применяем фильтры источников
        if media == "all":
            payload = self.set_media_all(payload)
        elif media == "avito_only":
            payload = self.set_media_avito_only(payload)
        elif media == "cian_only":
            payload = self.set_media_cian_only(payload)
        elif media == "yandex_only":
            payload = self.set_media_yandex_only(payload)
        
        # Применяем фильтры комнат
        if isinstance(rooms, list):
            payload = self.set_rooms_custom(payload, rooms)
            # Для списка комнат НЕ применяем дополнительные фильтры студий
            # так как set_rooms_custom уже всё настроил правильно
            studio_filters_applied = True
        elif rooms == "all":
            payload = self.set_rooms_all(payload)
            studio_filters_applied = False
        elif rooms == "1k":
            payload = self.set_rooms_1k(payload)
            studio_filters_applied = False
        elif rooms == "2k":
            payload = self.set_rooms_2k(payload)
            studio_filters_applied = False
        elif rooms == "3k":
            payload = self.set_rooms_3k(payload)
            studio_filters_applied = False
        else:
            studio_filters_applied = False

        # Применяем фильтры типа здания
        if building_type == "old_only":
            payload = self.set_old_building_only(payload)
        elif building_type == "new_only":
            payload = self.set_new_building_only(payload)
        elif building_type == "all":
            payload = self.set_building_all(payload)

        # Применяем фильтры студий ТОЛЬКО если они не были применены в set_rooms_custom
        if not studio_filters_applied:
            if studio == "studio_only":
                payload = self.set_studio_only(payload)
            elif studio == "no_studio":
                payload = self.set_no_studio(payload)
            elif studio == "all":
                payload = self.set_studio_all(payload)

        # Применяем фильтры типа продавца
        if seller_type == "owner_only":
            payload = self.set_owner_only(payload)
        elif seller_type == "all":
            payload = self.set_all_sellers(payload)

        # Применяем фильтры статуса сделки
        if status == "active":
            payload = self.set_deal_active(payload)
        elif status == "inactive":
            payload = self.set_deal_inactive(payload)
        elif status == "all":
            payload = self.set_deal_all(payload)

        # Применяем фильтры цен
        if min_price is not None or max_price is not None:
            payload = self.set_price_range(payload, min_price, max_price)

        return payload

    async def search(self, **search_params) -> Optional[Dict[str, Any]]:
        """Выполняет поиск с заданными параметрами"""

        payload = self.create_search_payload(**search_params)
        url = self.base_url + self.endpoint

        print(f"🔍 Поиск с параметрами: {search_params}")

        # Отладочный вывод для проверки is_first_published
        if "published_days_ago" in payload["conditions"]:
            print(f"📅 Параметр published_days_ago: {payload['conditions']['published_days_ago']}")

        # Отладочный вывод для проверки комнат
        if "total_room_count" in payload["conditions"]:
            print(f"🏠 Параметр total_room_count: {payload['conditions']['total_room_count']}")
        else:
            print(f"🏠 Параметр total_room_count: НЕ УКАЗАН (все комнаты)")

        # Отладочный вывод для проверки студий
        if "is_studio" in payload["conditions"]:
            print(f"🏢 Параметр is_studio: {payload['conditions']['is_studio']}")
        else:
            print(f"🏢 Параметр is_studio: НЕ УКАЗАН (включая студии)")
        
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
                        
                        if 'advs' in data:
                            hits = data['advs']
                            total = data.get('meta', {}).get('total', 0)
                            
                            print(f"✅ Найдено {len(hits)} объявлений из {total} общих")
                            return data
                        else:
                            print(f"❌ Нет поля 'advs' в ответе")
                            return None
                            
                    else:
                        text = await response.text()
                        print(f"❌ HTTP {response.status}: {text[:200]}")
                        return None
                        
        except Exception as e:
            print(f"💥 Ошибка запроса: {e}")
            return None

# ========== ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ ==========

async def example_1k_moscow_old():
    """Пример: 1-комнатные в старой Москве, только вторичка, все источники"""
    
    collector = FlexibleCollector()
    
    result = await collector.search(
        location="moscow_old_only",    # Старая Москва
        media="all",                   # Все источники
        rooms="1k",                    # 1-комнатные
        building_type="old_only",      # Только вторичка
        size=400                       # Размер выборки
    )
    
    if result:
        ads = result.get('advs', [])
        print(f"📊 Получено {len(ads)} однушек в старой Москве")
        
        # Сохраняем результат
        filename = f"1k_moscow_old_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"💾 Сохранено в: {filename}")

async def example_owners_only():
    """Пример: только от собственников"""
    
    collector = FlexibleCollector()
    
    result = await collector.search(
        location="moscow_old_only",       # Старая Москва
        media="all",                      # Все источники
        rooms="1k",                       # 1-комнатные
        building_type="old_only",         # Только вторичка
        seller_type="owner_only",         # ТОЛЬКО СОБСТВЕННИКИ
        size=400                          # Размер выборки
    )
    
    if result:
        ads = result.get('advs', [])
        print(f"📊 Получено {len(ads)} объявлений от собственников")
        
        # Сохраняем результат
        filename = f"owners_only_1k_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"💾 Сохранено в: {filename}")

async def example_custom_filters():
    """Пример: кастомные фильтры"""

    collector = FlexibleCollector()

    result = await collector.search(
        location="moscow_no_zelenograd",  # Москва без Зеленограда
        media="cian_only",                # Только ЦИАН
        rooms=[2, 3],                     # 2 и 3-комнатные
        building_type="all",              # Любые здания
        seller_type="all",                # Все продавцы
        min_price=10_000_000,            # От 10 млн
        max_price=25_000_000,            # До 25 млн
        size=500                         # Размер выборки
    )

    if result:
        ads = result.get('advs', [])
        print(f"📊 Получено {len(ads)} квартир по кастомным фильтрам")

async def example_studios_only():
    """Пример: только студии в старой Москве"""

    collector = FlexibleCollector()

    result = await collector.search(
        location="moscow_old_only",       # Старая Москва
        media="all",                      # Все источники
        rooms="all",                      # Все комнатности (для студий не важно)
        building_type="old_only",         # Только вторичка
        studio="studio_only",             # ТОЛЬКО СТУДИИ
        seller_type="all",                # Все продавцы
        size=400                          # Размер выборки
    )

    if result:
        ads = result.get('advs', [])
        print(f"📊 Получено {len(ads)} студий в старой Москве")

        # Сохраняем результат
        filename = f"studios_only_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"💾 Сохранено в: {filename}")

async def example_no_studios():
    """Пример: исключить студии из поиска"""

    collector = FlexibleCollector()

    result = await collector.search(
        location="moscow_old_only",       # Старая Москва
        media="all",                      # Все источники
        rooms="1k",                       # 1-комнатные
        building_type="old_only",         # Только вторичка
        studio="no_studio",               # ИСКЛЮЧИТЬ СТУДИИ
        seller_type="all",                # Все продавцы
        size=400                          # Размер выборки
    )

    if result:
        ads = result.get('advs', [])
        print(f"📊 Получено {len(ads)} 1к квартир без студий")

async def example_inactive_deals():
    """Пример: поиск снятых объявлений"""

    collector = FlexibleCollector()

    result = await collector.search(
        location="moscow_old_only",       # Старая Москва
        media="all",                      # Все источники
        rooms="1k",                       # 1-комнатные
        building_type="old_only",         # Только вторичка
        deal_status="inactive",           # ТОЛЬКО СНЯТЫЕ ОБЪЯВЛЕНИЯ
        seller_type="all",                # Все продавцы
        published_days_ago=30,            # За последние 30 дней
        size=400                          # Размер выборки
    )

    if result:
        ads = result.get('advs', [])
        print(f"📊 Получено {len(ads)} снятых объявлений за 30 дней")

        # Сохраняем результат
        filename = f"inactive_deals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"💾 Сохранено в: {filename}")

async def example_new_ads_all_rooms():
    """Пример: все комнаты, только новые объявления за неделю"""

    collector = FlexibleCollector()

    result = await collector.search(
        location="moscow_old_only",       # Старая Москва
        media="all",                      # Все источники (Яндекс, ЦИАН, АВИТО)
        rooms="all",                      # ВСЕ КОМНАТЫ
        building_type="old_only",         # Только вторичка
        deal_status="active",             # Только активные
        seller_type="all",                # Все продавцы
        published_days_ago=7,             # За последнюю неделю
        is_first_published=True,          # ТОЛЬКО НОВЫЕ ОБЪЯВЛЕНИЯ
        size=400                          # Размер выборки
    )

    if result:
        ads = result.get('advs', [])
        print(f"📊 Получено {len(ads)} новых объявлений за неделю")

        # Сохраняем результат
        filename = f"new_ads_all_rooms_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"💾 Сохранено в: {filename}")

def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Гибкий сборщик недвижимости')

    parser.add_argument('--days', type=int, default=PUBLISHED_DAYS_AGO,
                       help=f'Количество дней для поиска (по умолчанию: {PUBLISHED_DAYS_AGO})')

    parser.add_argument('--rooms', type=str, default=ROOMS,
                       help=f'Комнаты: "all" или через запятую, 0=студии (по умолчанию: {ROOMS})')

    parser.add_argument('--status', type=str, default=STATUS,
                       choices=['active', 'inactive', 'all'],
                       help=f'Статус сделок: active, inactive, all (по умолчанию: {STATUS})')

    return parser.parse_args()

async def main():
    """Основной сбор с настройками из аргументов и переменных"""

    # Загружаем конфигурацию из БД
    config = await load_config_from_db()

    access_token = config.get('w7_token')
    wscg = config.get('w7_WSCG')

    if not access_token or not wscg:
        print("❌ Не удалось загрузить токены из БД. Проверьте конфигурацию.")
        return

    print("✅ Токены загружены из БД успешно!\n")

    # Парсим аргументы командной строки
    args = parse_args()

    # Переопределяем настройки из аргументов
    days = args.days

    # Правильно обрабатываем rooms - АРГУМЕНТЫ В ПРИОРИТЕТЕ
    if args.rooms == "all":
        rooms = "all"
    elif args.rooms != ROOMS:  # Если аргумент отличается от дефолта, используем аргумент
        rooms = [int(r.strip()) for r in args.rooms.split(',') if r.strip().isdigit()]
    elif ROOMS == "all":  # Иначе используем конфиг
        rooms = "all"
    else:
        rooms = [int(r.strip()) for r in ROOMS.split(',') if r.strip().isdigit()]

    status = args.status

    if isinstance(rooms, str) and rooms == "all":
        rooms_desc = "ВСЕ КОМНАТЫ"
    else:
        rooms_desc = ', '.join([f"{'студии' if r == 0 else f'{r}к'}" for r in rooms])

    print(f"🏠 СБОР КВАРТИР: {rooms_desc.upper()}")
    print(f"📍 Локация: {LOCATION}")
    print(f"📺 Источники: {MEDIA}")
    print(f"🏢 Тип зданий: {BUILDING_TYPE}")
    print(f"👤 Продавцы: {SELLER_TYPE}")
    print(f"📊 Статус сделок: {status}")
    print(f"📅 Период: {days} дней")
    print(f"🆕 Только новые объявления (first_published): {'ДА' if IS_FIRST_PUBLISHED else 'НЕТ'}")
    print("=" * 60)

    collector = FlexibleCollector(access_token=access_token, wscg=wscg)

    all_ads = []
    total_found = 0

    for page in range(MAX_PAGES):
        print(f"\n📄 Обработка страницы {page + 1}/{MAX_PAGES}...")

        result = await collector.search(
            from_index=page * PAGE_SIZE,      # Смещение для пагинации
            size=PAGE_SIZE,                   # Размер страницы
            location=LOCATION,                # Используем переменную
            media=MEDIA,                      # Используем переменную
            rooms=rooms,                      # Используем аргумент
            building_type=BUILDING_TYPE,      # Используем переменную
            seller_type=SELLER_TYPE,          # Используем переменную
            status=status,                    # Используем аргумент
            min_price=MIN_PRICE,              # Используем переменную
            max_price=MAX_PRICE,              # Используем переменную
            published_days_ago=days,          # Используем аргумент
            is_first_published=IS_FIRST_PUBLISHED  # Используем переменную
        )
        
        if result and 'advs' in result:
            ads = result['advs']
            total_found = result.get('meta', {}).get('total', 0)
            
            all_ads.extend(ads)
            
            # Сохраняем страницу в БД сразу
            saver = W7DataSaver()
            processed_records = []
            
            for ad in ads:
                record = saver.process_record(ad)
                processed_records.append(record)
            
            success = await saver.save_records_to_db(processed_records)
            
            if success:
                print(f"✅ Страница {page + 1} сохранена: {len(processed_records)} объявлений")
            else:
                print(f"❌ Ошибка сохранения страницы {page + 1}")
            
            # Если объявлений меньше размера страницы, значит это последняя страница
            if len(ads) < PAGE_SIZE:
                print(f"📝 Последняя страница достигнута")
                break
        else:
            print(f"❌ Ошибка получения страницы {page + 1}")
            break
    
    if all_ads:
        print(f"\n✅ ИТОГО ОБРАБОТАНО: {len(all_ads)} объявлений из {total_found} общих за {MAX_PAGES} страниц")
        print(f"🎉 ВСЕ СТРАНИЦЫ СОХРАНЕНЫ В БД!")
    else:
        print("❌ Не найдено объявлений")

if __name__ == "__main__":
    asyncio.run(main())