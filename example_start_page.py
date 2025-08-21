#!/usr/bin/env python3
"""
Пример использования парсера с начальной страницы
"""

import asyncio
from parse_avito_1metro import EnhancedMetroParser
from dotenv import load_dotenv
import os

async def example_start_from_page():
    """Пример запуска парсера с определенной страницы"""
    
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
    
    # Настройки парсера
    metro_id = 1           # ID метро из таблицы metro
    max_pages = 5          # Парсить 5 страниц
    max_cards = 20         # По 20 карточек на странице
    start_page = 3         # Начать с 3-й страницы
    
    print(f"🚀 Запуск парсера с настройками:")
    print(f"   • Метро ID: {metro_id}")
    print(f"   • Страниц: {max_pages}")
    print(f"   • Карточек на странице: {max_cards}")
    print(f"   • Начальная страница: {start_page}")
    print("=" * 50)
    
    # Запускаем парсинг
    success, saved_count, total_cards = await parser.parse_single_metro(
        metro_id=metro_id,
        max_pages=max_pages,
        max_cards=max_cards,
        start_page=start_page
    )
    
    if success:
        print(f"\n🎉 Парсинг завершен успешно!")
        print(f"   • Сохранено карточек: {saved_count}")
        print(f"   • Всего обработано: {total_cards}")
    else:
        print(f"\n❌ Парсинг завершен с ошибками")

async def example_resume_parsing():
    """Пример возобновления парсинга с определенной страницы"""
    
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
    
    # Настройки для возобновления парсинга
    metro_id = 1           # ID метро из таблицы metro
    max_pages = 10         # Парсить до 10 страниц
    max_cards = 25         # По 25 карточек на странице
    start_page = 7         # Возобновить с 7-й страницы
    
    print(f"🔄 Возобновление парсинга с настройками:")
    print(f"   • Метро ID: {metro_id}")
    print(f"   • Максимум страниц: {max_pages}")
    print(f"   • Карточек на странице: {max_cards}")
    print(f"   • Возобновляем с страницы: {start_page}")
    print("=" * 50)
    
    # Запускаем парсинг
    success, saved_count, total_cards = await parser.parse_single_metro(
        metro_id=metro_id,
        max_pages=max_pages,
        max_cards=max_cards,
        start_page=start_page
    )
    
    if success:
        print(f"\n🎉 Парсинг возобновлен и завершен успешно!")
        print(f"   • Сохранено карточек: {saved_count}")
        print(f"   • Всего обработано: {total_cards}")
    else:
        print(f"\n❌ Парсинг завершен с ошибками")

if __name__ == "__main__":
    print("📚 Примеры использования парсера с начальной страницы")
    print("=" * 60)
    
    # Выберите пример для запуска:
    print("1. Запуск с определенной страницы")
    print("2. Возобновление парсинга")
    print("=" * 60)
    
    # Раскомментируйте нужный пример:
    
    # Пример 1: Запуск с 3-й страницы
    asyncio.run(example_start_from_page())
    
    # Пример 2: Возобновление с 7-й страницы
    # asyncio.run(example_resume_parsing())
