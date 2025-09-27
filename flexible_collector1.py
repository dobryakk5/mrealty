#!/usr/bin/env python3
"""
Расширенный сборщик для получения детальных данных по GUID объявления
Использует DSL версия 3 с полным набором полей
"""

import asyncio
import aiohttp
import json
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union

# Конфигурация токенов (те же что и в flexible_collector.py)
ACCESS_TOKEN = "Qpnp3I7VkJVTSVVFBLyRQ0x9UXiqiqYpXMiHNovwgZ66ccbYOh44LwtW7Lijv4AF"
USER_ID = "594465"
ORDER_ID = "813ea25b-faae-4de4-9597-840f80f42495"
WSCG = "baa71fea-53b9-4039-9c74-38784bd7315e"

def get_current_utc_timestamp():
    """Генерирует текущее UTC время в формате wsct"""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

class ExtendedCollector:
    def __init__(self):
        self.user_id = USER_ID
        self.access_token = ACCESS_TOKEN
        self.order_id = ORDER_ID
        self.wscg = WSCG
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

    async def get_extended_data_by_guid(self, guid: str) -> Optional[Dict[str, Any]]:
        """Получает расширенные данные по GUID объявления"""

        payload = self.create_extended_payload_by_guid(guid)
        url = self.base_url + self.endpoint

        print(f"🔍 Получение расширенных данных для GUID: {guid}")

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
                            ad = data['advs'][0]

                            print(f"✅ Получены расширенные данные для GUID {guid}")
                            print(f"📊 Количество полей: {len(ad.keys())}")

                            return ad
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

    async def get_multiple_extended_data(self, guids: List[str]) -> Dict[str, Any]:
        """Получает расширенные данные для множественных GUID"""

        results = {}
        total = len(guids)

        print(f"🚀 Получение расширенных данных для {total} объявлений")
        print("=" * 60)

        for i, guid in enumerate(guids, 1):
            print(f"\n📄 Обработка {i}/{total}: {guid}")

            extended_data = await self.get_extended_data_by_guid(guid)

            if extended_data:
                results[guid] = extended_data
                print(f"✅ Успешно получены данные")
            else:
                results[guid] = None
                print(f"❌ Ошибка получения данных")

        print(f"\n🎉 ИТОГО: Получено {len([r for r in results.values() if r is not None])} из {total} объявлений")

        return results

async def example_single_guid():
    """Пример получения расширенных данных для одного GUID"""

    collector = ExtendedCollector()

    # Тестовый GUID из предыдущего примера
    test_guid = "0D5F135E-7791-0000-0FA2-005B7F230000"

    extended_data = await collector.get_extended_data_by_guid(test_guid)

    if extended_data:
        print("\n📋 РАСШИРЕННЫЕ ДАННЫЕ:")
        print("=" * 50)

        # Показываем ключевые поля
        key_fields = [
            'guid', 'object_guid', 'price', 'total_room_count',
            'total_square', 'life_square', 'kitchen_square',
            'storey', 'storeys_count', 'address', 'media_name',
            'external_url', 'realty_type_name', 'walls_material_type_name',
            'balcony_type_name', 'water_closet_type_name', 'apartment_condition_type_name',
            'has_furniture', 'has_refrigerator', 'is_pet_allowed',
            'bedroom_count', 'cadastral_number', 'note'
        ]

        for field in key_fields:
            value = extended_data.get(field, 'НЕТ ДАННЫХ')
            print(f"{field}: {value}")

        # Сохраняем полные данные в файл
        filename = f"extended_data_{test_guid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(extended_data, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Полные данные сохранены в: {filename}")

async def example_multiple_guids():
    """Пример получения расширенных данных для нескольких GUID"""

    collector = ExtendedCollector()

    # Список тестовых GUID (можно взять из базы данных)
    test_guids = [
        "0D5F135E-7791-0000-0FA2-005B7F230000",
        "09261C9A-8991-0000-0000-0063E9CF0000",
        "09CD7DFC-5991-0000-0000-006332050000"
    ]

    results = await collector.get_multiple_extended_data(test_guids)

    # Сохраняем результаты
    filename = f"multiple_extended_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n💾 Все расширенные данные сохранены в: {filename}")

def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Расширенный сборщик данных по GUID')

    parser.add_argument('--guid', type=str,
                       help='GUID объявления для получения расширенных данных')

    parser.add_argument('--guids', type=str,
                       help='Список GUID через запятую')

    parser.add_argument('--file', type=str,
                       help='Файл с GUID (по одному на строку)')

    return parser.parse_args()

async def main():
    """Основная функция"""

    args = parse_args()

    collector = ExtendedCollector()

    if args.guid:
        # Одиночный GUID
        print(f"🎯 Получение расширенных данных для GUID: {args.guid}")
        extended_data = await collector.get_extended_data_by_guid(args.guid)

        if extended_data:
            filename = f"extended_{args.guid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(extended_data, f, ensure_ascii=False, indent=2)
            print(f"💾 Данные сохранены в: {filename}")

    elif args.guids:
        # Множественные GUID из аргумента
        guids = [guid.strip() for guid in args.guids.split(',') if guid.strip()]
        print(f"🎯 Получение расширенных данных для {len(guids)} GUID")

        results = await collector.get_multiple_extended_data(guids)

        filename = f"extended_multiple_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"💾 Данные сохранены в: {filename}")

    elif args.file:
        # GUID из файла
        try:
            with open(args.file, 'r', encoding='utf-8') as f:
                guids = [line.strip() for line in f if line.strip()]

            print(f"🎯 Получение расширенных данных для {len(guids)} GUID из файла {args.file}")

            results = await collector.get_multiple_extended_data(guids)

            filename = f"extended_from_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"💾 Данные сохранены в: {filename}")

        except FileNotFoundError:
            print(f"❌ Файл {args.file} не найден")

    else:
        # Пример по умолчанию
        print("🧪 Запуск примера с тестовым GUID")
        await example_single_guid()

if __name__ == "__main__":
    asyncio.run(main())