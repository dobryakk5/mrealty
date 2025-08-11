#!/usr/bin/env python3
"""
Проверка записи в БД для CIAN объявления 317810086
"""

import asyncio
import os
from parse_todb import get_cian_stats, search_cian_ads

async def check_record():
    print("Проверяем запись в БД для CIAN объявления 317810086...")
    
    try:
        # Ищем конкретную запись по avitoid
        search_results = await search_cian_ads(filters={'limit': 1000})
        
        # Фильтруем по URL, содержащему нужный ID
        target_record = None
        for record in search_results:
            if '317810086' in record.get('url', ''):
                target_record = record
                break
        
        if target_record:
            print(f"Найдена целевая запись:")
            print(f"  avitoid: {target_record['avitoid']}")
            print(f"  URL: {target_record['url']}")
            print(f"  rooms: {target_record['rooms']}")
            print(f"  area: {target_record['area']}")
            print(f"  floor: {target_record['floor']}")
            print(f"  total_floors: {target_record['total_floors']}")
            print(f"  price: {target_record['price']}")
        else:
            print("Целевая запись 317810086 не найдена в БД")
            
        print(f"\nВсего записей в таблице: {len(search_results)}")
            
        # Общая статистика
        print("\n" + "="*50)
        stats = await get_cian_stats()
        print("Общая статистика по таблице ads_cian:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
            
    except Exception as e:
        print(f"Ошибка при проверке БД: {e}")

if __name__ == "__main__":
    asyncio.run(check_record())
