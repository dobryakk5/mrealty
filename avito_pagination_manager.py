#!/usr/bin/env python3
"""
Менеджер для работы с таблицей отслеживания пагинации Авито

Использование:
  python avito_pagination_manager.py --status                    # Показать статус всех метро
  python avito_pagination_manager.py --metro-id 1 --status      # Статус конкретного метро
  python avito_pagination_manager.py --metro-id 1 --reset       # Сбросить статус метро
  python avito_pagination_manager.py --reset-all                # Сбросить статус всех метро
  python avito_pagination_manager.py --create-table             # Создать таблицу пагинации
"""

import asyncio
import argparse
import sys
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Импортируем функции для работы с пагинацией
try:
    from parse_todb_avito import (
        get_avito_pagination_status,
        get_all_avito_pagination_status,
        reset_avito_pagination,
        create_ads_avito_table
    )
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("💡 Убедитесь, что файл parse_todb_avito.py доступен")
    sys.exit(1)

async def show_metro_status(metro_id: int = None):
    """Показывает статус пагинации для метро"""
    if metro_id:
        # Статус конкретного метро
        status = await get_avito_pagination_status(metro_id)
        if status:
            print(f"\n📊 Статус пагинации для метро ID {metro_id}:")
            print(f"   • Последняя обработанная страница: {status['last_processed_page']}")
            print(f"   • Всего страниц обработано: {status['total_pages_processed']}")
            print(f"   • Последнее обновление: {status['last_updated']}")
        else:
            print(f"ℹ️ Для метро ID {metro_id} нет записей о пагинации")
    else:
        # Статус всех метро
        all_status = await get_all_avito_pagination_status()
        if all_status:
            print(f"\n📊 Статус пагинации для всех метро ({len(all_status)}):")
            print("-" * 60)
            for status in all_status:
                print(f"Метро ID {status['metro_id']:>3}: "
                      f"страница {status['last_processed_page']:>3}, "
                      f"всего {status['total_pages_processed']:>3}, "
                      f"обновлено {status['last_updated'].strftime('%Y-%m-%d %H:%M')}")
        else:
            print("ℹ️ Нет записей о пагинации для любого метро")

async def reset_metro_pagination(metro_id: int = None):
    """Сбрасывает статус пагинации"""
    if metro_id:
        success = await reset_avito_pagination(metro_id)
        if success:
            print(f"✅ Статус пагинации для метро ID {metro_id} сброшен")
        else:
            print(f"❌ Не удалось сбросить статус пагинации для метро ID {metro_id}")
    else:
        success = await reset_avito_pagination()
        if success:
            print("✅ Статус пагинации для всех метро сброшен")
        else:
            print("❌ Не удалось сбросить статус пагинации для всех метро")

async def create_pagination_table():
    """Создает таблицу пагинации"""
    try:
        await create_ads_avito_table()
        print("✅ Таблица пагинации создана/проверена")
    except Exception as e:
        print(f"❌ Ошибка создания таблицы пагинации: {e}")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description='Менеджер для работы с таблицей отслеживания пагинации Авито',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Группа для выбора действия
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--status', action='store_true',
                            help='Показать статус пагинации')
    action_group.add_argument('--reset', action='store_true',
                            help='Сбросить статус пагинации')
    action_group.add_argument('--reset-all', action='store_true',
                            help='Сбросить статус пагинации для всех метро')
    action_group.add_argument('--create-table', action='store_true',
                            help='Создать таблицу пагинации')
    
    # Параметры
    parser.add_argument('--metro-id', type=int,
                       help='ID метро для операций')
    
    args = parser.parse_args()
    
    # Проверяем DATABASE_URL
    if not os.getenv('DATABASE_URL'):
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        print("💡 Создайте файл .env с содержимым:")
        print("   DATABASE_URL=postgresql://username:password@host:port/database")
        return False
    
    try:
        if args.create_table:
            await create_pagination_table()
        elif args.status:
            await show_metro_status(args.metro_id)
        elif args.reset:
            if not args.metro_id:
                print("❌ Ошибка: для --reset необходимо указать --metro-id")
                return False
            await reset_metro_pagination(args.metro_id)
        elif args.reset_all:
            await reset_metro_pagination()
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка выполнения: {e}")
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️ Программа прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        sys.exit(1)
