#!/usr/bin/env python3
"""
Парсер всех метро Москвы с гибкими параметрами
Использование: python parse_avito_to_db.py [опции]

СИСТЕМА ОТСЛЕЖИВАНИЯ ПРОГРЕССА:
- Прогресс парсинга сохраняется в таблице system.parsing_progress
- Поле source = 1 для AVITO (4=CIAN, 2=DOMCLICK, 3=YANDEX)
- Система автоматически восстанавливается после прерывания
- При парсинге всех метро (--all) прогресс сохраняется по каждой станции

ЛОГИКА НАЧАЛЬНОЙ СТРАНИЦЫ (--start-page):
- Параметр применяется только к первому метро в списке
- Это позволяет возобновить парсинг конкретного метро с определенной страницы
- Все последующие метро всегда начинают с 1-й страницы

Опции:
  --metro-ids 1,2,3     Список конкретных metro.id через запятую
  --exclude 4,5,6       Исключить определенные metro.id
  --max-pages N          Максимальное количество страниц (0 = все)
  --max-cards N          Максимальное количество карточек на странице (0 = все)
  --start-page N         Начать с определенной страницы (только для первого метро, по умолчанию: 1)
  --all                  Парсить все метро Москвы
  --help                 Показать справку

Примеры:
  python parse_avito_to_db.py --all                    # Все метро, все страницы, все карточки
  python parse_avito_to_db.py --metro-ids 1,2,3       # Только метро 1,2,3
  python parse_avito_to_db.py --exclude 4,5 --max-pages 2  # Все метро кроме 4,5, максимум 2 страницы
  python parse_avito_to_db.py --metro-ids 1,2 --max-cards 10  # Метро 1,2, максимум 10 карточек на странице
  python parse_avito_to_db.py --metro-ids 1 --start-page 5  # Метро 1, начать с 5-й страницы
  python parse_avito_to_db.py --metro-ids 1,2,3 --start-page 3  # Метро 1,2,3: 1-е с 3-й страницы, остальные с 1-й
  python parse_avito_to_db.py --all --start-page 3    # Все метро: первое с 3-й страницы, остальные с 1-й
"""

import asyncio
import sys
import os
import argparse
import time
from dotenv import load_dotenv
from parse_avito_1metro import EnhancedMetroParser

# Импортируем функции работы с БД для отслеживания прогресса
from parse_todb import (
    create_parsing_session,
    update_parsing_progress,
    complete_parsing_session,
    get_last_parsing_progress
)

# =============================================================================
# НАСТРОЙКИ ПО УМОЛЧАНИЮ
# =============================================================================

# Количество страниц по умолчанию (0 = все страницы)
DEFAULT_MAX_PAGES = 0

# Количество карточек на странице по умолчанию (0 = все карточки)
DEFAULT_MAX_CARDS = 0

# Начальная страница по умолчанию (1 = первая страница)
DEFAULT_START_PAGE = 1

# Задержка между метро (в секундах)
METRO_DELAY = 10

# Тип источника для AVITO в системе parsing_progress
AVITO_SOURCE = 1

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
    
    async def parse_single_metro(self, metro_info, max_pages, max_cards, start_page=1):
        """Парсит одно метро
        
        Args:
            metro_info: Информация о метро
            max_pages: Максимальное количество страниц
            max_cards: Максимальное количество карточек на странице
            start_page: Номер страницы, с которой начать парсинг
        """
        try:
            metro_id = metro_info['id']
            metro_name = metro_info['name']
            metro_avito_id = metro_info['avito_id']
            
            print(f"\n🚀 Парсинг метро: {metro_name} (ID: {metro_id}, avito_id: {metro_avito_id})")
            print(f"📄 Страниц: {max_pages if max_pages > 0 else 'все'}")
            print(f"📊 Карточек на странице: {max_cards if max_cards and max_cards > 0 else 'все'}")
            if start_page > 1:
                print(f"🚀 Начинаем с страницы: {start_page}")
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
                max_cards=max_cards,
                start_page=start_page
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
    
    async def parse_metro_batch(self, metro_list, max_pages, max_cards, start_page=1, use_progress_tracking=False):
        """
        Парсит пакет метро
        
        Args:
            metro_list: Список метро для парсинга
            max_pages: Максимальное количество страниц
            max_cards: Максимальное количество карточек на странице
            start_page: Номер страницы, с которой начать парсинг
            use_progress_tracking: Включить отслеживание прогресса (только для --all)
        """
        if not metro_list:
            print("❌ Список метро пуст")
            return False
        
        self.stats['total_metro'] = len(metro_list)
        
        # Инициализация системы отслеживания прогресса
        session_id = None
        current_index = 0
        
        if use_progress_tracking:
            print("🔄 Проверяем наличие незавершенной сессии парсинга...")
            
            # Проверяем, есть ли незавершенная сессия для AVITO
            progress = await get_last_parsing_progress(1, None, AVITO_SOURCE)  # property_type=1, source=1 для AVITO
            
            if progress:
                print(f"🔍 Найдена сессия: ID={progress['id']}, статус={progress['status']}")
                print(f"   • Обработано метро: {progress['processed_metros']}/{progress['total_metros']}")
                print(f"   • Текущее метро ID: {progress['current_metro_id']}")
            else:
                print("🔍 Активная сессия не найдена")
            
            if progress and progress['status'] == 'active':
                # Продолжаем с места остановки
                print(f"🔄 Продолжаем незавершенную сессию {progress['id']}")
                print(f"   • Обработано метро: {progress['processed_metros']}")
                print(f"   • Последнее метро ID: {progress['current_metro_id']}")
                session_id = progress['id']
                
                # Используем processed_metros для определения следующего индекса
                # processed_metros = количество уже обработанных метро
                # current_index = индекс следующего метро для обработки
                current_index = progress['processed_metros']
                
                if current_index < len(metro_list):
                    next_metro = metro_list[current_index]
                    print(f"🔄 Продолжаем с метро {current_index + 1}/{len(metro_list)}: {next_metro['name']} (ID: {next_metro['id']})")
                else:
                    print(f"⚠️ Все метро уже обработаны (processed_metros={progress['processed_metros']}, total={len(metro_list)})")
                    # Завершаем сессию, так как все метро уже обработаны
                    await complete_parsing_session(session_id)
                    print(f"✅ Сессия {session_id} завершена - все метро уже обработаны")
                    self.print_final_stats()
                    return True
            else:
                # Создаем новую сессию
                print("🆕 Создаем новую сессию парсинга AVITO")
                session_id = await create_parsing_session(1, None, len(metro_list), AVITO_SOURCE)
                current_index = 0
        
        print(f"\n🎯 Начинаем парсинг {len(metro_list)} метро")
        print(f"📄 Максимум страниц: {max_pages if max_pages > 0 else 'все'}")
        print(f"📊 Максимум карточек: {max_cards if max_cards and max_cards > 0 else 'все'}")
        if use_progress_tracking:
            print(f"🔄 Отслеживание прогресса: {'включено' if session_id else 'выключено'}")
        print("=" * 60)
        
        # Обрабатываем метро начиная с текущего индекса
        print(f"🔄 Начинаем обработку с индекса {current_index} (метро {current_index + 1}/{len(metro_list)})")
        
        for i in range(current_index, len(metro_list)):
            metro_info = metro_list[i]
            print(f"\n📍 Метро {i+1}/{len(metro_list)}: {metro_info['name']} (ID: {metro_info['id']})")
            
            # ЛОГИКА НАЧАЛЬНОЙ СТРАНИЦЫ:
            # start_page применяется только к первому метро в списке (current_index)
            # Это позволяет возобновить парсинг конкретного метро с определенной страницы
            # Все последующие метро всегда начинают с 1-й страницы
            current_start_page = start_page if i == current_index else 1
            
            # Парсим метро
            success = await self.parse_single_metro(metro_info, max_pages, max_cards, current_start_page)
            
            # Обновляем прогресс ПОСЛЕ успешной обработки метро
            if use_progress_tracking and session_id:
                await update_parsing_progress(session_id, metro_info['id'], i + 1)
                print(f"📊 Прогресс обновлен: {i+1}/{len(metro_list)} метро обработано")
            
            # Задержка между метро (кроме последнего)
            if i < len(metro_list) - 1:
                print(f"⏳ Ждем {METRO_DELAY} секунд перед следующим метро...")
                await asyncio.sleep(METRO_DELAY)
        
        # Завершаем сессию если использовали отслеживание прогресса
        if use_progress_tracking and session_id:
            await complete_parsing_session(session_id)
            print(f"✅ Сессия парсинга {session_id} завершена")
        
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
    parser.add_argument('--start-page', type=int, default=DEFAULT_START_PAGE,
                       help=f'Начать с определенной страницы (только для первого метро, по умолчанию: {DEFAULT_START_PAGE})')
    
    args = parser.parse_args()
    
    # Валидация параметров
    if args.start_page < 1:
        print("❌ Ошибка: --start-page должен быть положительным числом")
        return False
    
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
        
        # Выводим настройки парсинга
        print(f"\n⚙️ Настройки парсинга:")
        print(f"   • Страниц: {args.max_pages if args.max_pages > 0 else 'все'}")
        print(f"   • Карточек на странице: {args.max_cards if args.max_cards > 0 else 'все'}")
        if args.start_page > 1:
            print(f"   • Начальная страница: {args.start_page} (только для первого метро)")
            print(f"   • Остальные метро: начнут с 1-й страницы")
        print("=" * 60)
        
        # Запускаем парсинг
        # Используем отслеживание прогресса только для парсинга всех метро (--all)
        use_progress_tracking = args.all
        
        success = await batch_parser.parse_metro_batch(
            metro_list, 
            args.max_pages, 
            args.max_cards,
            args.start_page,
            use_progress_tracking
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
