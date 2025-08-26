#!/usr/bin/env python3
"""
Парсер всех метро Москвы с гибкими параметрами
Использование: python parse_avito_to_db.py [опции]

СИСТЕМА ОТСЛЕЖИВАНИЯ ПРОГРЕССА:
- Прогресс парсинга сохраняется в таблице system.parsing_progress
- Поле source = 1 для AVITO (4=CIAN, 2=DOMCLICK, 3=YANDEX)
- Система автоматически восстанавливается после прерывания
- При парсинге всех метро (--all) прогресс сохраняется по каждой станции

СИСТЕМА ОТСЛЕЖИВАНИЯ ПАГИНАЦИИ АВИТО:
- Каждая страница сохраняется в таблице system.avito_pagination_tracking
- Автоматическое возобновление с последней обработанной страницы
- Отслеживание количества обработанных страниц по каждому метро
- Возможность сброса прогресса для конкретного метро или всех

НОВАЯ ЛОГИКА ОБРАБОТКИ ПУСТЫХ МЕТРО:
- Если первая страница метро пустая (нет объявлений), парсинг завершается успешно
- Автоматический переход к следующему метро без ошибок
- Поддержка сообщения "Ничего не найдено на выбранных станциях метро"
- ДОПОЛНИТЕЛЬНО: Если обнаружен раздел "Вас может заинтересовать" с объявлениями
  других метро, парсинг также завершается, так как эти объявления не нужны

ЛОГИКА НАЧАЛЬНОЙ СТРАНИЦЫ (--start-page):
- Параметр применяется только к первому метро в списке
- Это позволяет возобновить парсинг конкретного метро с определенной страницы
- Все последующие метро всегда начинают с 1-й страницы
- Если start_page не указан, автоматически продолжается с последней обработанной страницы

ЛОГИКА ОГРАНИЧЕНИЯ ПО ДАТЕ (--max-days):
- Если указан параметр max_days > 0, парсинг ограничивается свежими объявлениями
- При парсинге каждой страницы проверяется дата последнего объявления
- Если дата объявления раньше чем max_days дней назад, парсинг метро завершается
- Это позволяет парсить только актуальные объявления и экономить время
- При max_days = 0 (по умолчанию) ограничения по дате нет

ЛОГИКА МНОЖЕСТВЕННЫХ МЕТРО (--multiple-metro):
- Позволяет парсить несколько метро одной ссылкой Avito
- Формат: --multiple-metro 1-56-62-96-98 (ID метро через дефис)
- Avito автоматически объединяет результаты по всем метро
- Значительно ускоряет парсинг при необходимости обработать несколько соседних метро
- Поддерживает до 10 метро в одной ссылке (рекомендуемый лимит)
- Совместим со всеми другими параметрами (--max-days, --max-pages, --max-cards)

ЛОГИКА АВТОМАТИЧЕСКОЙ ГРУППИРОВКИ МЕТРО (--batch-size):
- Автоматически группирует все метро в пачки заданного размера
- Каждая пачка парсится одной ссылкой (как --multiple-metro)
- Идеально для парсинга всех 400 метро Москвы
- Рекомендуемый размер пачки: 5-10 метро
- Значительно ускоряет массовый парсинг (экономия 60-80% времени)
- Совместим со всеми параметрами парсинга

Опции:
  --metro-ids 1,2,3     Список конкретных metro.id через запятую
  --exclude 4,5,6       Исключить определенные metro.id
  --max-pages N          Максимальное количество страниц (0 = все)
  --max-cards N          Максимальное количество карточек на странице (0 = все)
  --start-page N         Начать с определенной страницы (только для первого метро, по умолчанию: 1)
  --max-days N           Максимальный возраст объявлений в днях (0 = все объявления, по умолчанию: 0)
  --multiple-metro 1-56-62-96-98  Парсить несколько метро одной ссылкой (через дефис)
  --batch-size N         Автоматически группировать метро в пачки по N штук (5-10 = оптимально)
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
  python parse_avito_to_db.py --all --max-days 14     # Все метро, только объявления за последние 2 недели
  python parse_avito_to_db.py --metro-ids 1,2 --max-days 7   # Метро 1,2, только объявления за последнюю неделю
  python parse_avito_to_db.py --multiple-metro 1-56-62-96-98  # 5 метро одной ссылкой
  python parse_avito_to_db.py --multiple-metro 1-56-62 --max-days 7  # 3 метро одной ссылкой, только свежие
  python parse_avito_to_db.py --all --batch-size 5    # Все метро группами по 5 (80 пачек)
  python parse_avito_to_db.py --all --batch-size 10   # Все метро группами по 10 (40 пачек)
  python parse_avito_to_db.py --all --batch-size 5 --max-days 7  # Все метро по 5, только свежие
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

# Импортируем функции для работы с пагинацией Авито
from parse_todb_avito import (
    get_avito_pagination_status,
    update_avito_pagination,
    get_all_avito_pagination_status,
    reset_avito_pagination
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

# Максимальный возраст объявлений по умолчанию (0 = все объявления)
DEFAULT_MAX_DAYS = 0

# Множественные метро по умолчанию (пустая строка = парсинг по одному метро)
DEFAULT_MULTIPLE_METRO = ''

# Размер пачки метро по умолчанию (0 = отключено)
DEFAULT_BATCH_SIZE = 0

# Задержка между метро убрана для ускорения процесса
# METRO_DELAY = 10

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
                        AND avito_id IS NOT NULL
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
                        AND avito_id IS NOT NULL
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
                    AND avito_id IS NOT NULL
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
                AND avito_id IS NOT NULL
                AND is_msk IS NOT FALSE
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
    
    async def parse_single_metro(self, metro_info, max_pages, max_cards, start_page=1, max_days=0):
        """Парсит одно метро
        
        Args:
            metro_info: Информация о метро
            max_pages: Максимальное количество страниц
            max_cards: Максимальное количество карточек на странице
            start_page: Номер страницы, с которой начать парсинг
            max_days: Максимальный возраст объявлений в днях (0 = все объявления)
        """
        try:
            metro_id = metro_info['id']
            metro_name = metro_info['name']
            metro_avito_id = metro_info['avito_id']
            
            # Проверяем статус пагинации в БД
            pagination_status = await get_avito_pagination_status(metro_id)
            if pagination_status:
                last_processed_page = pagination_status['last_processed_page']
                total_pages_processed = pagination_status['total_pages_processed']
                print(f"📊 Статус пагинации для метро ID {metro_id}:")
                print(f"   • Последняя обработанная страница: {last_processed_page}")
                print(f"   • Всего страниц обработано: {total_pages_processed}")
                
                # Если start_page не указан, начинаем со следующей страницы
                if start_page == 1 and last_processed_page > 0:
                    start_page = last_processed_page + 1
                    print(f"🔄 Автоматически продолжаем с страницы: {start_page}")
            else:
                print(f"🆕 Первый запуск для метро ID {metro_id}")
            
            print(f"\n🚀 Парсинг метро: {metro_name} (ID: {metro_id}, avito_id: {metro_avito_id})")
            print(f"📄 Страниц: {max_pages if max_pages > 0 else 'все'}")
            print(f"📊 Карточек на странице: {max_cards if max_cards and max_cards > 0 else 'все'}")
            if max_days > 0:
                print(f"⏰ Максимум дней: {max_days} (только свежие объявления)")
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
                start_page=start_page,
                max_days=max_days
            )
            
            # Обновляем статистику
            if success:
                self.stats['successful_metro'] += 1
                self.stats['total_cards'] += total_cards
                self.stats['total_saved'] += saved_count
                
                if total_cards == 0:
                    print(f"✅ Метро {metro_name} обработано успешно (нет объявлений)")
                    print(f"   • Карточек: {total_cards} (пустое метро)")
                    print(f"   • Сохранено: {saved_count}")
                else:
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
    
    async def parse_metro_batch(self, metro_list, max_pages, max_cards, start_page=1, use_progress_tracking=False, max_days=0):
        """
        Парсит пакет метро
        
        ЛОГИКА НАЧАЛЬНОЙ СТРАНИЦЫ:
        - При продолжении с тем же метро: начинаем с последней обработанной страницы
        - При переходе на следующее метро: ВСЕГДА начинаем с 1-й страницы
        
        Args:
            metro_list: Список метро для парсинга
            max_pages: Максимальное количество страниц
            max_cards: Максимальное количество карточек на странице
            start_page: Номер страницы, с которой начать парсинг
            use_progress_tracking: Включить отслеживание прогресса (только для --all)
            max_days: Максимальный возраст объявлений в днях (0 = все объявления)
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
                
                # ЛОГИКА ПРОДОЛЖЕНИЯ: проверяем, завершено ли текущее метро
                expected_metro_id = progress['current_metro_id']
                
                # Сначала проверяем, можно ли продолжить с текущего метро ID 36
                current_metro_can_continue = False
                current_metro_index = None
                
                # Ищем текущее метро в списке
                for idx, metro in enumerate(metro_list):
                    if metro['id'] == expected_metro_id:
                        current_metro_index = idx
                        # Проверяем, можно ли продолжить с этого метро
                        if metro['avito_id'] and metro.get('is_msk') is not False:
                            current_metro_can_continue = True
                        break
                
                if current_metro_can_continue:
                    # Продолжаем с текущего метро ID 36
                    current_index = current_metro_index
                    print(f"✅ Продолжаем с текущего метро: ID {expected_metro_id}")
                    print(f"   • Проверка avito_id: ✅ {expected_metro_id} имеет avito_id")
                    print(f"   • Проверка is_msk: ✅ {expected_metro_id} московское метро")
                    
                    # Проверяем прогресс по страницам для этого метро
                    # last_processed_page обновляется после каждой успешно обработанной страницы
                    try:
                        from parse_todb_avito import get_avito_pagination_status
                        pagination_status = await get_avito_pagination_status(expected_metro_id)
                        if pagination_status and pagination_status['last_processed_page'] > 0:
                            # Есть прогресс по страницам, продолжаем со следующей
                            start_page = pagination_status['last_processed_page'] + 1
                            print(f"   • Прогресс по страницам: найдена страница {pagination_status['last_processed_page']}")
                            print(f"   • Всего страниц обработано: {pagination_status['total_pages_processed']}")
                            print(f"   • Продолжаем с страницы: {start_page}")
                            print(f"   • Последнее обновление: {pagination_status['last_updated']}")
                        else:
                            # Нет прогресса по страницам, начинаем с 1-й
                            start_page = 1
                            print(f"   • Прогресс по страницам: не найден")
                            print(f"   • Начинаем с страницы: {start_page}")
                    except Exception as e:
                        print(f"   • Ошибка проверки прогресса по страницам: {e}")
                        start_page = 1
                        print(f"   • Начинаем с страницы: {start_page} (по умолчанию)")
                else:
                    # Текущее метро нельзя продолжить, ищем следующее
                    print(f"⚠️ Текущее метро ID {expected_metro_id} нельзя продолжить")
                    
                    # Ищем следующее метро после expected_metro_id с проверкой avito_id и is_msk
                    next_metro_id = None
                    skipped_metros = []
                    skipped_reasons = []
                    
                    for metro in metro_list:
                        if metro['id'] > expected_metro_id:
                            # Проверяем оба условия: avito_id IS NOT NULL и is_msk IS NOT FALSE
                            if metro['avito_id'] and metro.get('is_msk') is not False:
                                next_metro_id = metro['id']
                                break
                            else:
                                skipped_metros.append(metro['id'])
                                # Записываем причину пропуска
                                if not metro['avito_id']:
                                    skipped_reasons.append(f"ID {metro['id']}: нет avito_id")
                                if metro.get('is_msk') is False:
                                    skipped_reasons.append(f"ID {metro['id']}: не московское")
                    
                    if next_metro_id:
                        # Нашли следующее метро с avito_id и is_msk, продолжаем с него
                        for idx, metro in enumerate(metro_list):
                            if metro['id'] == next_metro_id:
                                current_index = idx
                                break
                        print(f"✅ Переходим к следующему метро: ID {next_metro_id} (после {expected_metro_id})")
                        print(f"   • Проверка avito_id: ✅ {next_metro_id} имеет avito_id")
                        print(f"   • Проверка is_msk: ✅ {next_metro_id} московское метро")
                        print(f"   • 🆕 НОВОЕ МЕТРО: начинаем с 1-й страницы")
                        
                        # СБРАСЫВАЕМ start_page для нового метро - всегда начинаем с 1-й страницы
                        start_page = 1
                        
                        # Показываем пропущенные метро и причины
                        if skipped_metros:
                            print(f"   • Пропущены метро: {skipped_metros}")
                            print(f"   • Причины пропуска:")
                            for reason in skipped_reasons:
                                print(f"     - {reason}")
                    else:
                        # Следующего метро с avito_id и is_msk нет, значит все обработано
                        print(f"✅ Все метро после ID {expected_metro_id} с avito_id и is_msk уже обработаны")
                        
                        # Показываем все пропущенные метро и причины
                        if skipped_metros:
                            print(f"   • Пропущены метро: {skipped_metros}")
                            print(f"   • Причины пропуска:")
                            for reason in skipped_reasons:
                                print(f"     - {reason}")
                        
                        await complete_parsing_session(session_id)
                        print(f"✅ Сессия {session_id} завершена - все метро уже обработаны")
                        self.print_final_stats()
                        return True
                
                if current_index < len(metro_list):
                    next_metro = metro_list[current_index]
                    print(f"🔄 Продолжаем с метро {current_index + 1}/{len(metro_list)}: {next_metro['name']} (ID: {next_metro['id']})")
                else:
                    print(f"⚠️ Все метро уже обработаны")
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
        if max_days > 0:
            print(f"⏰ Максимум дней: {max_days} (только свежие объявления)")
        if use_progress_tracking:
            print(f"🔄 Отслеживание прогресса: {'включено' if session_id else 'выключено'}")
            
            # Диагностика продолжения сессии
            if session_id and progress:
                print(f"🔍 ПРОДОЛЖЕНИЕ СЕССИИ:")
                print(f"   • Последнее обработанное метро ID: {progress['current_metro_id']}")
                print(f"   • Обработано метро: {progress['processed_metros']}")
                print(f"   • Всего метро в сессии: {progress['total_metros']}")
                print(f"   • Текущий список метро: {len(metro_list)}")
        
        print("=" * 60)
        
        # Обрабатываем метро начиная с текущего индекса
        print(f"🔄 Начинаем обработку с индекса {current_index} (метро {current_index + 1}/{len(metro_list)})")
        
        # Логика продолжения сессии теперь корректна
        # Сначала пытаемся продолжить с текущего метро, если не получается - переходим к следующему
        
        # Определяем начальную страницу для текущего метро
        current_start_page = start_page if 'start_page' in locals() else 1
        
        for i in range(current_index, len(metro_list)):
            metro_info = metro_list[i]
            print(f"\n📍 Метро {i+1}/{len(metro_list)}: {metro_info['name']} (ID: {metro_info['id']})")
            
            # ЛОГИКА НАЧАЛЬНОЙ СТРАНИЦЫ:
            # start_page применяется только к первому метро в списке (current_index)
            # Это позволяет возобновить парсинг конкретного метро с определенной страницы
            # Все последующие метро всегда начинают с 1-й страницы
            # ПРИ ПЕРЕХОДЕ НА НОВОЕ МЕТРО start_page сбрасывается в 1
            if i == current_index and 'start_page' in locals():
                current_start_page = start_page
                print(f"   • 🔄 ПРОДОЛЖЕНИЕ: начинаем с страницы {current_start_page}")
            else:
                current_start_page = 1
                print(f"   • 🆕 НОВОЕ МЕТРО: начинаем с страницы {current_start_page}")
            
            # Парсим метро
            success = await self.parse_single_metro(metro_info, max_pages, max_cards, current_start_page, max_days)
            
            # Обновляем прогресс ПОСЛЕ успешной обработки метро
            if use_progress_tracking and session_id:
                await update_parsing_progress(session_id, metro_info['id'], i + 1)
                print(f"📊 Прогресс обновлен: {i+1}/{len(metro_list)} метро обработано")
            
            # Пауза между метро убрана для ускорения процесса
            # if i < len(metro_list) - 1:
            #     print(f"⏳ Ждем {METRO_DELAY} секунд перед следующим метро...")
            #     await asyncio.sleep(METRO_DELAY)
        
        # Завершаем сессию если использовали отслеживание прогресса
        if use_progress_tracking and session_id:
            await complete_parsing_session(session_id)
            print(f"✅ Сессия парсинга {session_id} завершена")
        
        # Выводим итоговую статистику
        self.print_final_stats()
        
        return True
    
    async def parse_multiple_metro_single_link(self, metro_ids, max_pages, max_cards, start_page=1, max_days=0):
        """
        Парсит множественные метро одной ссылкой
        
        Args:
            metro_ids (list): Список ID метро для парсинга
            max_pages (int): Максимальное количество страниц
            max_cards (int): Максимальное количество карточек на странице
            start_page (int): Номер страницы, с которой начать парсинг
            max_days (int): Максимальный возраст объявлений в днях
        
        Returns:
            bool: True если парсинг успешен
        """
        try:
            if not metro_ids or len(metro_ids) < 2:
                print("❌ Для множественного парсинга нужно минимум 2 метро")
                return False
            
            print(f"\n🚀 Парсинг множественных метро одной ссылкой")
            print(f"📍 Метро: {metro_ids}")
            print(f"📄 Максимум страниц: {max_pages if max_pages > 0 else 'все'}")
            print(f"📊 Максимум карточек: {max_cards if max_cards and max_cards > 0 else 'все'}")
            if max_days > 0:
                print(f"⏰ Максимум дней: {max_days} (только свежие объявления)")
            if start_page > 1:
                print(f"🚀 Начинаем с страницы: {start_page}")
            print("=" * 60)
            
            # Создаем парсер для множественных метро
            self.parser = EnhancedMetroParser()
            self.parser.database_url = self.database_url
            self.parser.max_pages = max_pages
            self.parser.max_cards = max_cards
            self.parser.max_days = max_days
            
            # Запускаем парсинг множественных метро
            success, saved_count, total_cards = await self.parser.parse_single_metro(
                metro_id=metro_ids[0],  # Первое метро как основное
                max_pages=max_pages,
                max_cards=max_cards,
                start_page=start_page,
                max_days=max_days,
                multiple_metro_ids=metro_ids  # Передаем список всех метро
            )
            
            # Обновляем статистику
            if success:
                self.stats['successful_metro'] = len(metro_ids)
                self.stats['total_metro'] = len(metro_ids)
                self.stats['total_cards'] = total_cards
                self.stats['total_saved'] = saved_count
                
                print(f"✅ Множественные метро обработаны успешно")
                print(f"   • Метро: {len(metro_ids)}")
                print(f"   • Карточек: {total_cards}")
                print(f"   • Сохранено: {saved_count}")
            else:
                self.stats['failed_metro'] = len(metro_ids)
                self.stats['total_metro'] = len(metro_ids)
                print(f"❌ Множественные метро обработаны с ошибками")
            
            # Выводим итоговую статистику
            self.print_final_stats()
            
            return success
            
        except Exception as e:
            print(f"❌ Критическая ошибка парсинга множественных метро: {e}")
            return False
    
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

    def create_metro_batches(self, metro_list, batch_size):
        """
        Создает пачки метро для парсинга
        
        Args:
            metro_list (list): Список всех метро
            batch_size (int): Размер пачки
            
        Returns:
            list: Список пачек метро
        """
        if batch_size <= 0 or not metro_list:
            return [metro_list] if metro_list else []
        
        batches = []
        for i in range(0, len(metro_list), batch_size):
            batch = metro_list[i:i + batch_size]
            batches.append(batch)
        
        print(f"📦 Создано {len(batches)} пачек метро по {batch_size} метро в каждой")
        for i, batch in enumerate(batches):
            metro_names = [metro['name'] for metro in batch]
            metro_ids = [metro['id'] for metro in batch]
            print(f"   Пачка {i+1}: {metro_names} (ID: {metro_ids})")
        
        return batches
    
    async def parse_metro_batches(self, metro_batches, max_pages, max_cards, start_page, max_days):
        """
        Парсит пачки метро
        
        Args:
            metro_batches (list): Список пачек метро
            max_pages (int): Максимальное количество страниц
            max_cards (int): Максимальное количество карточек на странице
            start_page (int): Номер страницы, с которой начать парсинг
            max_days (int): Максимальный возраст объявлений в днях
        
        Returns:
            bool: True если все пачки обработаны успешно
        """
        if not metro_batches:
            print("❌ Список пачек метро пуст")
            return False
        
        total_batches = len(metro_batches)
        successful_batches = 0
        failed_batches = 0
        
        print(f"\n🚀 Начинаем парсинг {total_batches} пачек метро")
        print(f"📄 Максимум страниц: {max_pages if max_pages > 0 else 'все'}")
        print(f"📊 Максимум карточек: {max_cards if max_cards and max_cards > 0 else 'все'}")
        if max_days > 0:
            print(f"⏰ Максимум дней: {max_days} (только свежие объявления)")
        if start_page > 1:
            print(f"🚀 Начинаем с страницы: {start_page} (только для первой пачки)")
        print("=" * 60)
        
        for i, metro_batch in enumerate(metro_batches):
            print(f"\n📍 Пачка {i+1}/{total_batches}: {len(metro_batch)} метро")
            
            # Определяем начальную страницу для пачки
            current_start_page = start_page if i == 0 else 1
            
            if current_start_page > 1:
                print(f"   • 🔄 ПРОДОЛЖЕНИЕ: начинаем с страницы {current_start_page}")
            else:
                print(f"   • 🆕 НОВАЯ ПАЧКА: начинаем с страницы {current_start_page}")
            
            # Извлекаем ID метро из пачки
            metro_ids = [metro['id'] for metro in metro_batch]
            
            # Парсим пачку метро одной ссылкой
            success = await self.parse_multiple_metro_single_link(
                metro_ids, 
                max_pages, 
                max_cards,
                current_start_page,
                max_days
            )
            
            if success:
                successful_batches += 1
                print(f"✅ Пачка {i+1} обработана успешно")
            else:
                failed_batches += 1
                print(f"❌ Пачка {i+1} обработана с ошибками")
            
            # Пауза между пачками для стабильности
            if i < total_batches - 1:
                print(f"⏳ Пауза 3 секунды перед следующей пачкой...")
                await asyncio.sleep(3)
        
        # Итоговая статистика по пачкам
        print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА ПО ПАЧКАМ:")
        print(f"   • Всего пачек: {total_batches}")
        print(f"   • Успешно: {successful_batches}")
        print(f"   • С ошибками: {failed_batches}")
        
        if total_batches > 0:
            success_rate = (successful_batches / total_batches) * 100
            print(f"   • Процент успеха: {success_rate:.1f}%")
        
        return failed_batches == 0

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
    metro_group.add_argument('--multiple-metro', type=str,
                           help='Список metro.id через дефис для парсинга одной ссылкой (например: 1-56-62-96-98)')
    
    # Параметры парсинга
    parser.add_argument('--max-pages', type=int, default=DEFAULT_MAX_PAGES,
                       help=f'Максимальное количество страниц (0 = все, по умолчанию: {DEFAULT_MAX_PAGES})')
    parser.add_argument('--max-cards', type=int, default=DEFAULT_MAX_CARDS,
                       help=f'Максимальное количество карточек на странице (0 = все, по умолчанию: {DEFAULT_MAX_CARDS})')
    parser.add_argument('--start-page', type=int, default=DEFAULT_START_PAGE,
                       help=f'Начать с определенной страницы (только для первого метро, по умолчанию: {DEFAULT_START_PAGE})')
    parser.add_argument('--max-days', type=int, default=DEFAULT_MAX_DAYS,
                       help='Максимальный возраст объявлений в днях (0 = все объявления, по умолчанию: 0)')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                       help='Размер пачки метро для автоматической группировки (0 = отключено, по умолчанию: 0)')
    
    args = parser.parse_args()
    
    # Валидация параметров
    if args.start_page < 1:
        print("❌ Ошибка: --start-page должен быть положительным числом")
        return False
    
    # НОВАЯ ЛОГИКА: Валидация batch-size
    if args.batch_size < 0:
        print("❌ Ошибка: --batch-size должен быть неотрицательным числом")
        return False
    
    if args.batch_size > 0 and args.batch_size < 2:
        print("❌ Ошибка: --batch-size должен быть минимум 2 для группировки метро")
        return False
    
    if args.batch_size > 15:
        print("⚠️ Предупреждение: --batch-size больше 15 может вызвать проблемы с Avito")
        print("   Рекомендуемый размер пачки: 5-10 метро")
    
    # НОВАЯ ЛОГИКА: Обработка множественных метро
    multiple_metro_ids = []
    if args.multiple_metro:
        try:
            # Парсим строку вида "1-56-62-96-98" в список [1, 56, 62, 96, 98]
            multiple_metro_ids = [int(x.strip()) for x in args.multiple_metro.split('-')]
            if len(multiple_metro_ids) < 2:
                print("❌ Ошибка: --multiple-metro должен содержать минимум 2 ID метро")
                return False
            if len(multiple_metro_ids) > 10:
                print("⚠️ Предупреждение: количество метро превышает рекомендуемый лимит (10)")
            print(f"🚀 Множественные метро: {multiple_metro_ids}")
        except ValueError:
            print("❌ Ошибка: --multiple-metro должен содержать числа, разделенные дефисами")
            return False
    
    # Создаем парсер
    batch_parser = MetroBatchParser(database_url)
    
    try:
        # Определяем список метро для парсинга
        if args.multiple_metro and multiple_metro_ids:
            # Парсинг множественных метро одной ссылкой
            print(f"🎯 Парсинг множественных метро одной ссылкой: {multiple_metro_ids}")
            metro_list = await batch_parser.get_specific_metro_list(multiple_metro_ids)
            
        elif args.metro_ids:
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
        if args.max_days > 0:
            print(f"   • Максимальный возраст объявлений: {args.max_days} дней")
            print(f"   • Парсинг только свежих объявлений")
        if args.batch_size > 0:
            print(f"   • Автоматическая группировка: по {args.batch_size} метро в пачке")
            estimated_batches = (len(metro_list) + args.batch_size - 1) // args.batch_size
            print(f"   • Ожидаемое количество пачек: ~{estimated_batches}")
            print(f"   • Экономия времени: ~60-80% по сравнению с парсингом по одному метро")
        print("=" * 60)
        
        # Запускаем парсинг
        # Используем отслеживание прогресса только для парсинга всех метро (--all)
        use_progress_tracking = args.all
        
        # НОВАЯ ЛОГИКА: Определяем режим парсинга
        if args.batch_size > 0:
            # Автоматическая группировка метро в пачки
            print(f"📦 Автоматическая группировка метро в пачки по {args.batch_size}")
            metro_batches = batch_parser.create_metro_batches(metro_list, args.batch_size)
            
            success = await batch_parser.parse_metro_batches(
                metro_batches,
                args.max_pages, 
                args.max_cards,
                args.start_page,
                args.max_days
            )
            
        elif args.multiple_metro and multiple_metro_ids:
            # Парсинг множественных метро одной ссылкой
            success = await batch_parser.parse_multiple_metro_single_link(
                multiple_metro_ids, 
                args.max_pages, 
                args.max_cards,
                args.start_page,
                args.max_days
            )
        else:
            # Обычный парсинг по одному метро
            success = await batch_parser.parse_metro_batch(
                metro_list, 
                args.max_pages, 
                args.max_cards,
                args.start_page,
                use_progress_tracking,
                args.max_days
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
