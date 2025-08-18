#!/usr/bin/env python3
"""
Пример использования метода parse_single_metro для парсинга одного метро
"""

import asyncio
import os
from dotenv import load_dotenv
from parse_avito_1metro import EnhancedMetroParser

async def parse_metro_example():
    """Пример парсинга одного метро"""
    
    # Загружаем переменные окружения
    load_dotenv()
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        return
    
    # Создаем парсер
    parser = EnhancedMetroParser()
    parser.database_url = database_url
    
    # Параметры для парсинга
    metro_id = 1        # ID метро из таблицы metro
    max_pages = 3       # Количество страниц для парсинга
    max_cards = 15      # Количество карточек на странице (0 = все карточки)
    
    print(f"🎯 Парсим метро ID={metro_id}, страниц={max_pages}, карточек={max_cards}")
    print("=" * 60)
    
    # Запускаем парсинг
    success, saved_count, total_cards = await parser.parse_single_metro(
        metro_id=metro_id,
        max_pages=max_pages,
        max_cards=max_cards
    )
    
    # Выводим результат
    if success:
        print(f"\n🎉 Парсинг завершен успешно!")
        print(f"📊 Результат:")
        print(f"   • Успешно: {success}")
        print(f"   • Сохранено в БД: {saved_count}")
        print(f"   • Всего карточек: {total_cards}")
    else:
        print(f"\n❌ Парсинг завершен с ошибками")

async def parse_multiple_metros():
    """Пример парсинга нескольких метро"""
    
    # Загружаем переменные окружения
    load_dotenv()
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        return
    
    # Список метро для парсинга
    metros_to_parse = [
        {"id": 1, "pages": 2, "cards": 10},
        {"id": 2, "pages": 1, "cards": 15},
        {"id": 3, "pages": 3, "cards": 20},
    ]
    
    print(f"🚀 Парсим {len(metros_to_parse)} метро")
    print("=" * 60)
    
    total_saved = 0
    total_cards = 0
    
    for metro_config in metros_to_parse:
        # Создаем новый парсер для каждого метро
        parser = EnhancedMetroParser()
        parser.database_url = database_url
        
        print(f"\n🎯 Парсим метро ID={metro_config['id']}")
        
        # Запускаем парсинг
        success, saved_count, cards_count = await parser.parse_single_metro(
            metro_id=metro_config['id'],
            max_pages=metro_config['pages'],
            max_cards=metro_config['cards']
        )
        
        if success:
            total_saved += saved_count
            total_cards += cards_count
            print(f"✅ Метро {metro_config['id']}: {saved_count}/{cards_count} сохранено")
        else:
            print(f"❌ Метро {metro_config['id']}: ошибка парсинга")
    
    # Итоговая статистика
    print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Метро обработано: {len(metros_to_parse)}")
    print(f"   Всего карточек: {total_cards}")
    print(f"   Всего сохранено: {total_saved}")

if __name__ == "__main__":
    # Пример 1: Парсинг одного метро
    print("=== ПРИМЕР 1: Парсинг одного метро ===")
    asyncio.run(parse_metro_example())
    
    print("\n" + "="*80 + "\n")
    
    # Пример 2: Парсинг нескольких метро
    print("=== ПРИМЕР 2: Парсинг нескольких метро ===")
    asyncio.run(parse_multiple_metros())
