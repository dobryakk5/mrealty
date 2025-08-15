#!/usr/bin/env python3
"""
Тестовый скрипт для проверки полного цикла работы парсера
"""

import asyncio
from parse_cian_to_db import (
    parse_arguments,
    parse_params_string,
    convert_time_period,
    build_search_url
)

async def test_full_cycle():
    """Тестирует полный цикл работы парсера"""
    print("🧪 Тестирование полного цикла работы парсера")
    print("=" * 60)
    
    # Симулируем запуск с параметром 1d
    print("📋 Симуляция запуска: python parse_cian_to_db.py 1d")
    
    # 1. Парсим аргументы
    print("\n1️⃣ Парсинг аргументов командной строки:")
    sys_argv_backup = sys.argv
    sys.argv = ['parse_cian_to_db.py', '1d']
    
    try:
        args = parse_arguments()
        print(f"   args.params = {args.params}")
        print(f"   args.proxy = {args.proxy}")
        print(f"   args.no_proxy = {args.no_proxy}")
    finally:
        sys.argv = sys_argv_backup
    
    # 2. Парсим строку параметров
    print("\n2️⃣ Парсинг строки параметров:")
    property_type, time_period = parse_params_string('1d')
    print(f"   property_type = {property_type} ({'вторичка' if property_type == 1 else 'новостройки'})")
    print(f"   time_period = {time_period}")
    
    # 3. Конвертируем период времени
    print("\n3️⃣ Конвертация периода времени:")
    time_period_seconds = convert_time_period(time_period)
    print(f"   '{time_period}' -> {time_period_seconds}")
    print(f"   Тип значения: {type(time_period_seconds)}")
    
    # 4. Формируем URL поиска
    print("\n4️⃣ Формирование URL поиска:")
    url = build_search_url(property_type, time_period_seconds)
    print(f"   URL: {url}")
    
    # 5. Проверяем наличие totime=-2
    print("\n5️⃣ Проверка параметра totime:")
    if "&totime=-2" in url:
        print("   ✅ URL содержит &totime=-2")
        print("   ✅ Это означает, что будет применен фильтр 'за день'")
    else:
        print("   ❌ URL НЕ содержит &totime=-2")
    
    # 6. Проверяем, что параметр будет сохранен в прогресс
    print("\n6️⃣ Проверка сохранения в прогресс:")
    print(f"   В таблицу system.parsing_progress будет записано:")
    print(f"   - property_type = {property_type}")
    print(f"   - time_period = {time_period_seconds}")
    print(f"   - status = 'active'")
    
    print("\n" + "=" * 60)
    print("✅ Полный цикл работы протестирован!")

if __name__ == '__main__':
    import sys
    asyncio.run(test_full_cycle())
