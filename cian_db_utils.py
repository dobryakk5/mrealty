#!/usr/bin/env python3
"""
Утилиты для работы с таблицей ads_cian
Демонстрация дополнительных функций
"""

import asyncio
from parse_todb import get_cian_stats, search_cian_ads, delete_old_cian_ads


async def show_stats():
    """Показывает статистику по объявлениям"""
    print("СТАТИСТИКА ПО ОБЪЯВЛЕНИЯМ CIAN")
    print("=" * 50)
    
    stats = await get_cian_stats()
    
    print(f"Всего объявлений: {stats['total_count']}")
    print()
    
    # Статистика по ценам
    if stats['price_stats']:
        ps = stats['price_stats']
        print("ЦЕНЫ:")
        print(f"  Минимальная: {ps.get('min_price', 0):,} ₽")
        print(f"  Максимальная: {ps.get('max_price', 0):,} ₽")
        print(f"  Средняя: {ps.get('avg_price', 0):,.0f} ₽")
        print(f"  С ценой: {ps.get('price_count', 0)}")
        print()
    
    # Статистика по комнатам
    if stats['rooms_stats']:
        print("КОМНАТЫ:")
        for room_stat in stats['rooms_stats']:
            rooms = room_stat['rooms']
            count = room_stat['count']
            room_name = 'студия' if rooms == 0 else f"{rooms}-комн"
            print(f"  {room_name}: {count}")
        print()
    
    # Статистика по продавцам
    if stats['seller_stats']:
        print("ПРОДАВЦЫ:")
        for seller_stat in stats['seller_stats']:
            print(f"  {seller_stat['person_type']}: {seller_stat['count']}")
        print()
    
    # Топ метро
    if stats['metro_stats']:
        print("ТОП-10 СТАНЦИЙ МЕТРО:")
        for metro_stat in stats['metro_stats']:
            print(f"  {metro_stat['metro']}: {metro_stat['count']}")
        print()


async def search_examples():
    """Примеры поиска объявлений"""
    print("ПРИМЕРЫ ПОИСКА")
    print("=" * 50)
    
    # Поиск дешевых квартир
    print("1. Квартиры до 10 млн ₽:")
    cheap_ads = await search_cian_ads({
        'max_price': 10000000,
        'limit': 5
    })
    for ad in cheap_ads:
        print(f"   {ad['rooms']}-комн, {ad['price']:,} ₽, {ad['metro']}, {ad['person_type']}")
    print()
    
    # Поиск студий
    print("2. Студии:")
    studios = await search_cian_ads({
        'rooms': 0,
        'limit': 3
    })
    for ad in studios:
        print(f"   Студия, {ad['price']:,} ₽, {ad['metro']}, {ad['area']} м²")
    print()
    
    # Поиск по метро
    print("3. Квартиры у метро 'Китай-город':")
    metro_ads = await search_cian_ads({
        'metro': 'Китай-город',
        'limit': 3
    })
    for ad in metro_ads:
        print(f"   {ad['rooms']}-комн, {ad['price']:,} ₽, {ad['min_metro']} мин пешком")
    print()
    
    # Поиск от собственников
    print("4. Объявления от собственников:")
    owner_ads = await search_cian_ads({
        'person_type': 'собственник',
        'limit': 3
    })
    for ad in owner_ads:
        print(f"   {ad['rooms']}-комн, {ad['price']:,} ₽, {ad['person']}")
    print()


async def main():
    """Главная функция демонстрации"""
    await show_stats()
    await search_examples()
    
    # Можно также очистить старые записи (раскомментируйте при необходимости)
    # deleted = await delete_old_cian_ads(days=30)
    # print(f"Удалено старых записей: {deleted}")


if __name__ == '__main__':
    asyncio.run(main())
