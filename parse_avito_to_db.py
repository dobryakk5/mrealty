#!/usr/bin/env python3
"""
Парсер всех метро Москвы с гибкими параметрами
Использование: python parse_avito_to_db.py [опции]

Опции:
  --metro-ids 1,2,3     Список конкретных metro.id через запятую
  --exclude 4,5,6       Исключить определенные metro.id
  --max-pages N          Максимальное количество страниц (0 = все)
  --max-cards N          Максимальное количество карточек на странице (0 = все)
  --all                  Парсить все метро Москвы
  --help                 Показать справку

Примеры:
  python parse_avito_to_db.py --all                    # Все метро, все страницы, все карточки
  python parse_avito_to_db.py --metro-ids 1,2,3       # Только метро 1,2,3
  python parse_avito_to_db.py --exclude 4,5 --max-pages 2  # Все метро кроме 4,5, максимум 2 страницы
  python parse_avito_to_db.py --metro-ids 1,2 --max-cards 10  # Метро 1,2, максимум 10 карточек на странице
"""

import asyncio
import sys
import os
import argparse
from dotenv import load_dotenv
from parse_avito_1metro import EnhancedMetroParser

# =============================================================================
# НАСТРОЙКИ ПО УМОЛЧАНИЮ
# =============================================================================

# Количество страниц по умолчанию (0 = все страницы)
DEFAULT_MAX_PAGES = 0

# Количество карточек на странице по умолчанию (0 = все карточки)
DEFAULT_MAX_CARDS = 0

# Задержка между метро (в секундах)
METRO_DELAY = 10

# =============================================================================
# КОНЕЦ НАСТРОЕК
# =============================================================================

class MetroBatchParser:
    """Парсер для пакетной обработки метро"""
    
    def __init__(self, database_url):
        self.database_url = database_url
        self.parser = None
        self.stats = {
            'total_metro': 0,
            'successful_metro': 0,
            'failed_metro': 0,
            'total_cards': 0,
            'total_saved': 0
        }
    
    async def get_moscow_metro_list(self, exclude_ids=None):
        """Получает список всех метро Москвы из БД"""
        try:
            import asyncpg
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем все метро Москвы (is_msk is not false)
            if exclude_ids and len(exclude_ids) > 0:
                # Проверяем, что список не пустой
                if len(exclude_ids) == 0:
                    query = """
                        SELECT id, name, avito_id 
                        FROM metro 
                        WHERE is_msk IS NOT FALSE 
                        ORDER BY id
                    """
                    print(f"🔍 SQL запрос без исключений (пустой список): {query}")
                    result = await conn.fetch(query)
                else:
                    exclude_placeholders = ','.join([f'${i+1}' for i in range(len(exclude_ids))])
                    query = f"""
                        SELECT id, name, avito_id 
                        FROM metro 
                        WHERE is_msk IS NOT FALSE 
                        AND id NOT IN ({exclude_placeholders})
                        ORDER BY id
                    """
                    print(f"🔍 SQL запрос с исключением: {query}")
                    print(f"🔍 Параметры исключения: {exclude_ids}")
                    result = await conn.fetch(query, *exclude_ids)
            else:
                query = """
                    SELECT id, name, avito_id 
                    FROM metro 
                    WHERE is_msk IS NOT FALSE 
                    ORDER BY id
                """
                print(f"🔍 SQL запрос без исключений: {query}")
                result = await conn.fetch(query)
            
            await conn.close()
            
            metro_list = []
            for row in result:
                metro_list.append({
                    'id': row['id'],
                    'name': row['name'],
                    'avito_id': row['avito_id']
                })
            
            print(f"✅ Найдено {len(metro_list)} метро в БД")
            return metro_list
            
        except Exception as e:
            print(f"❌ Ошибка получения списка метро: {e}")
            print(f"🔍 Тип ошибки: {type(e).__name__}")
            if hasattr(e, '__cause__') and e.__cause__:
                print(f"🔍 Причина: {e.__cause__}")
            return []
    
    async def get_specific_metro_list(self, metro_ids):
        """Получает список конкретных метро по ID"""
        try:
            import asyncpg
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем конкретные метро
            placeholders = ','.join([f'${i+1}' for i in range(len(metro_ids))])
            query = f"""
                SELECT id, name, avito_id 
                FROM metro 
                WHERE id IN ({placeholders})
                ORDER BY id
            """
            result = await conn.fetch(query, *metro_ids)
            
            await conn.close()
            
            metro_list = []
            for row in result:
                metro_list.append({
                    'id': row['id'],
                    'name': row['name'],
                    'avito_id': row['avito_id']
                })
            
            return metro_list
            
        except Exception as e:
            print(f"❌ Ошибка получения списка метро: {e}")
            return []
    
    async def parse_single_metro(self, metro_info, max_pages, max_cards):
        """Парсит одно метро"""
        try:
            metro_id = metro_info['id']
            metro_name = metro_info['name']
            metro_avito_id = metro_info['avito_id']
            
            print(f"\n🚀 Парсинг метро: {metro_name} (ID: {metro_id}, avito_id: {metro_avito_id})")
            print(f"📄 Страниц: {max_pages if max_pages > 0 else 'все'}")
            print(f"📊 Карточек на странице: {max_cards if max_cards and max_cards > 0 else 'все'}")
            print("=" * 60)
            
            # Создаем новый парсер для каждого метро
            self.parser = EnhancedMetroParser()
            self.parser.database_url = self.database_url
            self.parser.max_pages = max_pages
            self.parser.max_cards = max_cards
            
            # Запускаем парсинг
            success, saved_count, total_cards = await self.parser.parse_single_metro(
                metro_id=metro_id,
                max_pages=max_pages,
                max_cards=max_cards
            )
            
            # Обновляем статистику
            if success:
                self.stats['successful_metro'] += 1
                self.stats['total_cards'] += total_cards
                self.stats['total_saved'] += saved_count
                print(f"✅ Метро {metro_name} обработано успешно")
                print(f"   • Карточек: {total_cards}")
                print(f"   • Сохранено: {saved_count}")
            else:
                self.stats['failed_metro'] += 1
                print(f"❌ Метро {metro_name} обработано с ошибками")
            
            return success
            
        except Exception as e:
            print(f"❌ Критическая ошибка парсинга метро {metro_info.get('name', 'Unknown')}: {e}")
            self.stats['failed_metro'] += 1
            return False
    
    async def parse_metro_batch(self, metro_list, max_pages, max_cards):
        """Парсит пакет метро"""
        if not metro_list:
            print("❌ Список метро пуст")
            return False
        
        self.stats['total_metro'] = len(metro_list)
        
        print(f"\n🎯 Начинаем парсинг {len(metro_list)} метро")
        print(f"📄 Максимум страниц: {max_pages if max_pages > 0 else 'все'}")
        print(f"📊 Максимум карточек: {max_cards if max_cards and max_cards > 0 else 'все'}")
        print("=" * 60)
        
        for i, metro_info in enumerate(metro_list, 1):
            print(f"\n📍 Метро {i}/{len(metro_list)}")
            
            # Парсим метро
            success = await self.parse_single_metro(metro_info, max_pages, max_cards)
            
            # Задержка между метро (кроме последнего)
            if i < len(metro_list):
                print(f"⏳ Ждем {METRO_DELAY} секунд перед следующим метро...")
                await asyncio.sleep(METRO_DELAY)
        
        # Выводим итоговую статистику
        self.print_final_stats()
        
        return True
    
    def print_final_stats(self):
        """Выводит итоговую статистику"""
        print("\n" + "=" * 60)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 60)
        print(f"🎯 Всего метро: {self.stats['total_metro']}")
        print(f"✅ Успешно: {self.stats['successful_metro']}")
        print(f"❌ С ошибками: {self.stats['failed_metro']}")
        print(f"📊 Всего карточек: {self.stats['total_cards']}")
        print(f"💾 Сохранено в БД: {self.stats['total_saved']}")
        
        if self.stats['total_metro'] > 0:
            success_rate = (self.stats['successful_metro'] / self.stats['total_metro']) * 100
            print(f"📈 Процент успеха: {success_rate:.1f}%")

async def main():
    """Основная функция"""
    
    # Загружаем переменные окружения
    load_dotenv()
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        print("💡 Создайте файл .env с содержимым:")
        print("   DATABASE_URL=postgresql://username:password@host:port/database")
        return False
    
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(
        description='Парсер всех метро Москвы с гибкими параметрами',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Группа для выбора метро
    metro_group = parser.add_mutually_exclusive_group(required=True)
    metro_group.add_argument('--metro-ids', type=str, 
                           help='Список metro.id через запятую (например: 1,2,3)')
    metro_group.add_argument('--exclude', type=str,
                           help='Исключить metro.id через запятую (например: 4,5,6)')
    metro_group.add_argument('--all', action='store_true',
                           help='Парсить все метро Москвы')
    
    # Параметры парсинга
    parser.add_argument('--max-pages', type=int, default=DEFAULT_MAX_PAGES,
                       help=f'Максимальное количество страниц (0 = все, по умолчанию: {DEFAULT_MAX_PAGES})')
    parser.add_argument('--max-cards', type=int, default=DEFAULT_MAX_CARDS,
                       help=f'Максимальное количество карточек на странице (0 = все, по умолчанию: {DEFAULT_MAX_CARDS})')
    
    args = parser.parse_args()
    
    # Создаем парсер
    batch_parser = MetroBatchParser(database_url)
    
    try:
        # Определяем список метро для парсинга
        if args.metro_ids:
            # Конкретные метро
            metro_ids = [int(x.strip()) for x in args.metro_ids.split(',')]
            print(f"🎯 Парсинг конкретных метро: {metro_ids}")
            metro_list = await batch_parser.get_specific_metro_list(metro_ids)
            
        elif args.exclude:
            # Все метро с исключением
            exclude_ids = [int(x.strip()) for x in args.exclude.split(',')]
            print(f"🎯 Парсинг всех метро Москвы, исключая: {exclude_ids}")
            print(f"🔍 Тип exclude_ids: {type(exclude_ids)}, длина: {len(exclude_ids)}")
            print(f"🔍 Значения: {exclude_ids}")
            metro_list = await batch_parser.get_moscow_metro_list(exclude_ids)
            
        else:
            # Все метро
            print("🎯 Парсинг всех метро Москвы")
            metro_list = await batch_parser.get_moscow_metro_list()
        
        if not metro_list:
            print("❌ Не найдено метро для парсинга")
            return False
        
        print(f"✅ Найдено {len(metro_list)} метро для парсинга:")
        for metro in metro_list:
            print(f"   • {metro['name']} (ID: {metro['id']}, avito_id: {metro['avito_id']})")
        
        # Запускаем парсинг
        success = await batch_parser.parse_metro_batch(
            metro_list, 
            args.max_pages, 
            args.max_cards
        )
        
        return success
        
    except KeyboardInterrupt:
        print("\n⚠️ Парсинг прерван пользователем")
        return False
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
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
