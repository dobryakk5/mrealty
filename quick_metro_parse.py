#!/usr/bin/env python3
"""
Быстрый парсинг одного метро с параметрами командной строки
Использование: python quick_metro_parse.py <metro_id> <max_pages> [max_cards]
"""

import asyncio
import sys
import os
from dotenv import load_dotenv
from parse_avito_1metro import EnhancedMetroParser

# =============================================================================
# НАСТРОЙКИ ПО УМОЛЧАНИЮ - ИЗМЕНИТЕ ИХ ПОД ВАШИ НУЖДЫ
# =============================================================================

# Метро по умолчанию (ID из таблицы metro)
DEFAULT_METRO_ID = 11

# Количество страниц по умолчанию (0 = все страницы)
DEFAULT_MAX_PAGES = 0

# Количество карточек на странице по умолчанию (0 = все карточки)
DEFAULT_MAX_CARDS = 0

# Начальная страница по умолчанию (1 = первая страница)
DEFAULT_START_PAGE = 1

# =============================================================================
# КОНЕЦ НАСТРОЕК
# =============================================================================

async def quick_parse_metro(metro_id, max_pages, max_cards=None, start_page=1):
    """
    Быстрый парсинг одного метро
    
    Args:
        metro_id (int): ID метро из таблицы metro
        max_pages (int): Количество страниц для парсинга
        max_cards (int, optional): Количество карточек на странице
        start_page (int, optional): Номер страницы, с которой начать парсинг
    """
    
    # Загружаем переменные окружения
    load_dotenv()
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        print("💡 Создайте файл .env с содержимым:")
        print("   DATABASE_URL=postgresql://username:password@host:port/database")
        return False
    
    # Создаем парсер
    parser = EnhancedMetroParser()
    parser.database_url = database_url
    
    print(f"🚀 Быстрый парсинг метро ID={metro_id}")
    print(f"📄 Страниц: {max_pages if max_pages > 0 else 'все'}")
    print(f"📊 Карточек на странице: {max_cards if max_cards and max_cards > 0 else 'все'}")
    if start_page > 1:
        print(f"🚀 Начинаем с страницы: {start_page}")
    print("=" * 60)
    
    # Запускаем парсинг
    success, saved_count, total_cards = await parser.parse_single_metro(
        metro_id=metro_id,
        max_pages=max_pages,
        max_cards=max_cards,
        start_page=start_page
    )
    
    # Выводим результат
    if success:
        print(f"\n🎉 Парсинг завершен успешно!")
        print(f"📊 Результат:")
        print(f"   • Сохранено в БД: {saved_count}")
        print(f"   • Всего карточек: {total_cards}")
        return True
    else:
        print(f"\n❌ Парсинг завершен с ошибками")
        return False

def main():
    """Основная функция с обработкой аргументов командной строки"""
    
    # Проверяем количество аргументов
    if len(sys.argv) < 2:
        print("🚀 Запуск с настройками по умолчанию")
        print(f"⚙️ Текущие настройки:")
        print(f"   • Метро ID: {DEFAULT_METRO_ID}")
        print(f"   • Страниц: {DEFAULT_MAX_PAGES if DEFAULT_MAX_PAGES > 0 else 'все'}")
        print(f"   • Карточек на странице: {DEFAULT_MAX_CARDS if DEFAULT_MAX_CARDS > 0 else 'все'}")
        print(f"   • Начальная страница: {DEFAULT_START_PAGE}")
        print("=" * 60)
        
        # Запускаем парсинг с настройками по умолчанию
        success = asyncio.run(quick_parse_metro(DEFAULT_METRO_ID, DEFAULT_MAX_PAGES, DEFAULT_MAX_CARDS, DEFAULT_START_PAGE))
        
        if success:
            sys.exit(0)  # Успешное завершение
        else:
            sys.exit(1)  # Ошибка
    
    # Если указан ID метро, но нужно показать справку
    if len(sys.argv) == 2 and sys.argv[1] in ['-h', '--help', 'help', '?']:
        print("💡 Использование:")
        print("   python quick_metro_parse.py                                    # Запуск с настройками по умолчанию")
        print("   python quick_metro_parse.py <metro_id>                         # Метро ID, остальное по умолчанию")
        print("   python quick_metro_parse.py <metro_id> <pages>                 # Метро ID + страницы, карточки по умолчанию")
        print("   python quick_metro_parse.py <metro_id> <pages> <cards>         # Метро ID + страницы + карточки")
        print("   python quick_metro_parse.py <metro_id> <pages> <cards> <start> # Все параметры + начальная страница")
        print("\n📝 Примеры:")
        print("   python quick_metro_parse.py                    # Метро ID=95, все страницы, все карточки")
        print("   python quick_metro_parse.py 1                 # Метро ID=1, все страницы, все карточки")
        print("   python quick_metro_parse.py 1 3               # Метро ID=1, 3 страницы, все карточки")
        print("   python quick_metro_parse.py 2 1 15            # Метро ID=2, 1 страница, 15 карточек")
        print("   python quick_metro_parse.py 5 0 0             # Метро ID=5, все страницы, все карточки")
        print("   python quick_metro_parse.py 3 2 0             # Метро ID=3, 2 страницы, все карточки")
        print("   python quick_metro_parse.py 1 5 20 3          # Метро ID=1, 5 страниц, 20 карточек, начать с 3-й страницы")
        print(f"\n⚙️ Текущие настройки по умолчанию:")
        print(f"   • Метро ID: {DEFAULT_METRO_ID}")
        print(f"   • Страниц: {DEFAULT_MAX_PAGES if DEFAULT_MAX_PAGES > 0 else 'все'}")
        print(f"   • Карточек на странице: {DEFAULT_MAX_CARDS if DEFAULT_MAX_CARDS > 0 else 'все'}")
        print(f"   • Начальная страница: {DEFAULT_START_PAGE}")
        print("\n💡 Специальные значения:")
        print(f"   • Страниц = 0: парсить все доступные страницы")
        print(f"   • Карточек = 0: парсить все карточки на странице")
        print(f"   • Начальная страница = 1: начать с первой страницы")
        return
    
    try:
        # Парсим аргументы
        metro_id = int(sys.argv[1])
        
        # Если указано количество страниц, используем его, иначе берем по умолчанию
        if len(sys.argv) > 2:
            max_pages = int(sys.argv[2])
        else:
            max_pages = DEFAULT_MAX_PAGES
            print(f"📄 Используем количество страниц по умолчанию: {max_pages}")
        
        # Если указано количество карточек, используем его, иначе берем по умолчанию
        if len(sys.argv) > 3:
            max_cards = int(sys.argv[3])
        else:
            max_cards = DEFAULT_MAX_CARDS
            print(f"📊 Используем количество карточек по умолчанию: {max_cards if max_cards > 0 else 'все'}")
        
        # Если указана начальная страница, используем её, иначе берем по умолчанию
        if len(sys.argv) > 4:
            start_page = int(sys.argv[4])
        else:
            start_page = DEFAULT_START_PAGE
            if start_page > 1:
                print(f"🚀 Используем начальную страницу по умолчанию: {start_page}")
        
        # Проверяем корректность параметров
        if metro_id <= 0:
            print("❌ ID метро должен быть положительным числом")
            return
        
        if max_pages < 0:
            print("❌ Количество страниц должно быть неотрицательным числом")
            return
        
        if max_cards is not None and max_cards < 0:
            print("❌ Количество карточек должно быть неотрицательным числом")
            return
        
        if start_page < 1:
            print("❌ Начальная страница должна быть положительным числом")
            return
        
        # Запускаем парсинг
        success = asyncio.run(quick_parse_metro(metro_id, max_pages, max_cards, start_page))
        
        if success:
            sys.exit(0)  # Успешное завершение
        else:
            sys.exit(1)  # Ошибка
            
    except ValueError as e:
        print(f"❌ Ошибка: неверный формат числа - {e}")
        print("💡 Убедитесь, что metro_id и max_pages - это целые числа")
    except KeyboardInterrupt:
        print("\n⚠️ Парсинг прерван пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
