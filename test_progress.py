#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы с прогрессом парсинга
"""

import asyncio
from parse_todb import (
    create_ads_cian_table,
    create_parsing_session,
    update_parsing_progress,
    complete_parsing_session,
    get_last_parsing_progress
)

async def test_progress():
    """Тестирует функции работы с прогрессом"""
    print("🧪 Тестирование системы прогресса парсинга")
    print("=" * 50)
    
    # Создаем таблицы
    await create_ads_cian_table()
    
    # Тест 1: Создание сессии для вторички за день
    print("\n1️⃣ Создание сессии для вторички за день (property_type=1, time_period=-2)")
    session_id_1 = await create_parsing_session(property_type=1, time_period=-2, total_metros=5)
    print(f"   Создана сессия ID: {session_id_1}")
    
    # Тест 2: Создание сессии для новостроек за неделю
    print("\n2️⃣ Создание сессии для новостроек за неделю (property_type=2, time_period=604800)")
    session_id_2 = await create_parsing_session(property_type=2, time_period=604800, total_metros=3)
    print(f"   Создана сессия ID: {session_id_2}")
    
    # Тест 3: Создание сессии без ограничений по времени
    print("\n3️⃣ Создание сессии без ограничений по времени (property_type=1, time_period=None)")
    session_id_3 = await create_parsing_session(property_type=1, time_period=None, total_metros=4)
    print(f"   Создана сессия ID: {session_id_3}")
    
    # Тест 4: Обновление прогресса
    print("\n4️⃣ Обновление прогресса сессии 1")
    await update_parsing_progress(session_id_1, current_metro_id=10, processed_count=2)
    
    # Тест 5: Получение последнего прогресса
    print("\n5️⃣ Получение последнего прогресса для вторички за день")
    progress_1 = await get_last_parsing_progress(property_type=1, time_period=-2)
    if progress_1:
        print(f"   Найден прогресс: сессия {progress_1['id']}, метро ID {progress_1['current_metro_id']}")
        print(f"   Статус: {progress_1['status']}, обработано: {progress_1['processed_metros']}")
    
    print("\n6️⃣ Получение последнего прогресса для новостроек за неделю")
    progress_2 = await get_last_parsing_progress(property_type=2, time_period=604800)
    if progress_2:
        print(f"   Найден прогресс: сессия {progress_2['id']}, метро ID {progress_2['current_metro_id']}")
        print(f"   Статус: {progress_2['status']}, обработано: {progress_2['processed_metros']}")
    
    print("\n7️⃣ Получение последнего прогресса без ограничений по времени")
    progress_3 = await get_last_parsing_progress(property_type=1, time_period=None)
    if progress_3:
        print(f"   Найден прогресс: сессия {progress_3['id']}, метро ID {progress_3['current_metro_id']}")
        print(f"   Статус: {progress_3['status']}, обработано: {progress_3['processed_metros']}")
    
    # Тест 8: Завершение сессий
    print("\n8️⃣ Завершение сессий")
    await complete_parsing_session(session_id_1)
    await complete_parsing_session(session_id_2)
    await complete_parsing_session(session_id_3)
    
    print("\n✅ Тестирование завершено!")

if __name__ == '__main__':
    asyncio.run(test_progress())
