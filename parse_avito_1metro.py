#!/usr/bin/env python3
"""
Улучшенный парсер карточек с правильным определением metro.avito_id

ОСОБЕННОСТИ:
- Постраничное сохранение данных в БД для надежности
- Возможность возобновления парсинга с любой страницы
- Гибридный подход к парсингу карточек
- Автоматическое определение metro.avito_id
- Ограничение по дате объявлений (только свежие объявления)
"""

import json  # Нужен для загрузки cookies
import os
import time
import asyncio
import signal
import asyncpg
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import re
from datetime import datetime, timedelta
import base64
import gzip
import random

# Импорт функций для работы с БД
try:
    from parse_todb_avito import create_ads_avito_table, save_avito_ad
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    print("⚠️ Модуль parse_todb_avito не найден, сохранение в БД недоступно")

# Попытка импорта конфигурации
try:
    from config_parser import *
    CONFIG_LOADED = True
except ImportError:
    CONFIG_LOADED = False
    print("⚠️ Файл config_parser.py не найден, используются значения по умолчанию")

# Глобальная переменная для парсера (для обработчика сигналов)
global_parser = None

def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения"""
    print(f"\n⚠️ Получен сигнал {signum}, завершаем работу...")
    if global_parser:
        global_parser.cleanup()
    print("✅ Парсер корректно завершен")
    exit(0)

class EnhancedMetroParser:
    def __init__(self):
        # Настройки из конфигурации или значения по умолчанию
        if CONFIG_LOADED:
            self.max_cards = MAX_CARDS_PER_PAGE
            self.max_pages = MAX_PAGES
            self.metro_id = METRO_ID
            self.page_delay = PAGE_DELAY
            self.page_load_delay = PAGE_LOAD_DELAY
            self.cards_load_timeout = CARDS_LOAD_TIMEOUT
            self.headless_mode = HEADLESS_MODE
            self.cookies_file = COOKIES_FILE
            
            # Настройки плавной прокрутки
            self.enable_smooth_scroll = ENABLE_SMOOTH_SCROLL
            self.scroll_pause = SCROLL_PAUSE
            self.max_scroll_attempts = MAX_SCROLL_ATTEMPTS
            
            # Настройки базы данных
            self.enable_db_save = ENABLE_DB_SAVE
            
            # НОВЫЕ ПАРАМЕТРЫ для гибридного парсинга
            self.stream_cards_count = getattr(globals(), 'STREAM_CARDS_COUNT', 5)  # Первые N карточек потоково
            self.batch_cards_count = getattr(globals(), 'BATCH_CARDS_COUNT', 45)  # Остальные M карточек пачками
            self.batch_size = getattr(globals(), 'BATCH_SIZE', 10)  # Размер пакета для второй части
            
            # НОВЫЙ ПАРАМЕТР для ограничения по дате
            self.max_days = getattr(globals(), 'MAX_DAYS', 0)  # Максимальный возраст объявлений в днях
            
            # НОВЫЕ ПАРАМЕТРЫ для множественных метро
            self.multiple_metro_ids = getattr(globals(), 'MULTIPLE_METRO_IDS', [])  # Список ID метро для парсинга одной ссылкой
            self.multiple_metro_avito_ids = []  # Список avito_id для множественных метро
            self.max_metro_per_link = getattr(globals(), 'MAX_METRO_PER_LINK', 50)  # Максимум метро в одной ссылке (увеличено для больших батчей)
            
            print("✅ Конфигурация загружена из config_parser.py")
        else:
            self.max_cards = 15  # Количество карточек для парсинга (по умолчанию)
            self.max_pages = 1  # Количество страниц для парсинга
            self.metro_id = 1   # ID метро из таблицы metro
            self.page_delay = 5
            self.page_load_delay = 5
            self.cards_load_timeout = 30
            self.headless_mode = False
            self.cookies_file = "avito_cookies.json"  # Используем существующий файл
            
            # Настройки плавной прокрутки по умолчанию
            self.enable_smooth_scroll = True
            self.scroll_pause = 1.5
            self.max_scroll_attempts = 10
            
            # Настройки базы данных по умолчанию
            self.enable_db_save = True
            
            # НОВЫЕ ПАРАМЕТРЫ для гибридного парсинга (по умолчанию)
            self.stream_cards_count = 5   # Первые 5 карточек потоково
            self.batch_cards_count = 45   # Остальные 45 карточек пачками
            self.batch_size = 10          # Размер пакета для второй части (по умолчанию: 10)
            
            # НОВЫЙ ПАРАМЕТР для ограничения по дате
            self.max_days = 0  # Максимальный возраст объявлений в днях
            
            # НОВЫЕ ПАРАМЕТРЫ для множественных метро
            self.multiple_metro_ids = []  # Список ID метро для парсинга одной ссылкой
            self.multiple_metro_avito_ids = []  # Список avito_id для множественных метро
            self.max_metro_per_link = 50  # Максимум метро в одной ссылке (увеличено для больших батчей)
        
        self.driver = None
        self.database_url = None
        self.metro_avito_id = None  # avito_id для этого метро
        self.tags_dictionary = None  # Кэш для словаря тегов
        self.last_connection_error = False  # Флаг для отслеживания ошибок соединения
        
        # Устанавливаем DATABASE_URL из переменной окружения
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            self.database_url = database_url
            print(f"✅ DATABASE_URL установлен: {database_url[:20]}...")
        else:
            print("⚠️ DATABASE_URL не установлен в .env, сохранение в БД недоступно")
            self.enable_db_save = False
        
        # Проверяем доступность функций БД
        if not DB_AVAILABLE:
            print("⚠️ Модуль parse_todb_avito недоступен, сохранение в БД отключено")
            self.enable_db_save = False
    
    def convert_relative_time_to_date(self, relative_time):
        """Преобразует относительное время в datetime.datetime
        
        Всегда возвращает datetime.datetime для единообразия.
        Если время не указано, устанавливается полдень (12:00).
        """
        try:
            if not relative_time:
                return None
                
            now = datetime.now()
            relative_time_lower = relative_time.lower().strip()
            
            # Паттерны для относительного времени
            if 'сегодня' in relative_time_lower:
                return now  # Возвращаем полный datetime для сегодня
            elif 'вчера' in relative_time_lower:
                yesterday = now - timedelta(days=1)
                return yesterday  # Возвращаем полный datetime для вчера
            elif 'позавчера' in relative_time_lower:
                day_before_yesterday = now - timedelta(days=2)
                return day_before_yesterday  # Возвращаем полный datetime для позавчера
            
            # Паттерны с количеством времени
            time_patterns = [
                (r'(\d+)\s*(час|часа|часов)\s*назад', 'hours'),
                (r'(\d+)\s*(день|дня|дней)\s*назад', 'days'),
                (r'(\d+)\s*(недел|неделя|недели|недель|неделю)\s*назад', 'weeks'),
                (r'(\d+)\s*(месяц|месяца|месяцев|месяц)\s*назад', 'months')
            ]
            
            for pattern, unit in time_patterns:
                match = re.search(pattern, relative_time_lower)
                if match:
                    count = int(match.group(1))
                    
                    if unit == 'hours':
                        target_time = now - timedelta(hours=count)
                        return target_time  # Возвращаем полный datetime для часов
                    elif unit == 'days':
                        target_time = now - timedelta(days=count)
                    elif unit == 'weeks':
                        target_time = now - timedelta(weeks=count)
                    elif unit == 'months':
                        # Приблизительно, месяц = 30 дней
                        target_time = now - timedelta(days=count * 30)
                    
                    # Для дней/недель/месяцев устанавливаем полдень (12:00)
                    target_date = target_time.date()
                    return datetime.combine(target_date, datetime.min.time().replace(hour=12))
            
            # Если это конкретная дата (например, "12 июля 13:35")
            month_names = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
            }
            
            # Ищем формат "12 июля" или "12 июля 13:35"
            for month_name, month_num in month_names.items():
                if month_name in relative_time_lower:
                    # Ищем день перед названием месяца
                    day_match = re.search(r'(\d{1,2})\s+' + month_name, relative_time_lower)
                    if day_match:
                        day = int(day_match.group(1))
                        current_year = now.year
                        
                        # Проверяем, есть ли время в строке
                        time_match = re.search(r'(\d{1,2}):(\d{1,2})', relative_time_lower)
                        if time_match:
                            # Есть время - используем его
                            hour = int(time_match.group(1))
                            minute = int(time_match.group(2))
                        else:
                            # Время не указано - устанавливаем полдень (12:00)
                            hour = 12
                            minute = 0
                        
                        # Создаем datetime
                        try:
                            card_datetime = datetime(current_year, month_num, day, hour, minute)
                            
                            # Если дата в будущем, значит это прошлый год
                            if card_datetime > now:
                                card_datetime = datetime(current_year - 1, month_num, day, hour, minute)
                            
                            return card_datetime
                        except ValueError:
                            continue
            
            # Ищем формат "12.07" или "12.07.2024"
            date_dot_patterns = [
                r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # 12.07.2024
                r'(\d{1,2})\.(\d{1,2})'            # 12.07
            ]
            
            for pattern in date_dot_patterns:
                match = re.search(pattern, relative_time_lower)
                if match:
                    if len(match.groups()) == 3:
                        # Формат "12.07.2024"
                        day = int(match.group(1))
                        month = int(match.group(2))
                        year = int(match.group(3))
                    else:
                        # Формат "12.07"
                        day = int(match.group(1))
                        month = int(match.group(2))
                        year = now.year
                        
                        # Если месяц уже прошел в этом году, значит это текущий год
                        if month < now.month:
                            year = now.year
                        else:
                            year = now.year - 1
                    
                    # Устанавливаем полдень (12:00) для дат без времени
                    try:
                        result = datetime(year, month, day, 12, 0)
                        return result
                    except ValueError:
                        continue
            
            # Если ничего не распарсили, возвращаем None
            return None
            
        except Exception as e:
            print(f"⚠️ Ошибка парсинга относительного времени '{relative_time}': {e}")
            return None
    
    async def get_metro_avito_id(self):
        """Получает avito_id для метро из БД"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем avito_id для метро
            result = await conn.fetchrow("""
                SELECT id, name, avito_id 
                FROM metro 
                WHERE id = $1
            """, self.metro_id)
            
            await conn.close()
            
            if result:
                self.metro_avito_id = result['avito_id']
                self.metro_name = result['name']
                print(f"📍 Метро: {self.metro_name} (ID: {self.metro_id}, avito_id: {self.metro_avito_id})")
                return True
            else:
                print(f"❌ Метро с ID {self.metro_id} не найдено в БД")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка получения avito_id для метро: {e}")
            return False
    
    async def get_multiple_metro_avito_ids(self, metro_ids):
        """Получает avito_id для множественных метро из БД
        
        Args:
            metro_ids (list): Список ID метро для парсинга
            
        Returns:
            bool: True если все метро найдены, False в противном случае
        """
        try:
            if not metro_ids or len(metro_ids) == 0:
                print("❌ Список ID метро пуст")
                return False
            
            if len(metro_ids) > self.max_metro_per_link:
                print(f"⚠️ Количество метро ({len(metro_ids)}) превышает лимит ({self.max_metro_per_link})")
                print(f"   Ограничиваем до {self.max_metro_per_link} метро")
                metro_ids = metro_ids[:self.max_metro_per_link]
            
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем avito_id для всех метро
            placeholders = ','.join([f'${i+1}' for i in range(len(metro_ids))])
            query = f"""
                SELECT id, name, avito_id 
                FROM metro 
                WHERE id IN ({placeholders})
                AND avito_id IS NOT NULL
                ORDER BY array_position(ARRAY[{placeholders}], id)
            """
            
            result = await conn.fetch(query, *metro_ids)
            await conn.close()
            
            if result:
                self.multiple_metro_ids = [row['id'] for row in result]
                self.multiple_metro_avito_ids = [row['avito_id'] for row in result]
                metro_names = [row['name'] for row in result]
                
                print(f"📍 Множественные метро ({len(result)}):")
                for i, (metro_id, metro_name, avito_id) in enumerate(zip(self.multiple_metro_ids, metro_names, self.multiple_metro_avito_ids)):
                    print(f"   {i+1}. {metro_name} (ID: {metro_id}, avito_id: {avito_id})")
                
                # Устанавливаем первое метро как основное для совместимости
                if self.multiple_metro_ids:
                    self.metro_id = self.multiple_metro_ids[0]
                    self.metro_avito_id = self.multiple_metro_avito_ids[0]
                    self.metro_name = metro_names[0]
                
                return True
            else:
                print(f"❌ Не найдено метро с ID {metro_ids} в БД")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка получения avito_id для множественных метро: {e}")
            return False
    
    async def find_metro_id_by_name(self, metro_name):
        """Находит ID метро по названию станции"""
        try:
            if not metro_name or metro_name == 'не указано':
                return None

            conn = await asyncpg.connect(self.database_url)

            # Очищаем название от лишних символов
            clean_name = metro_name.strip()

            # Сначала ищем точное совпадение
            result = await conn.fetchrow("""
                SELECT id FROM metro
                WHERE LOWER(name) = LOWER($1)
                AND is_msk IS NOT FALSE
            """, clean_name)

            if result:
                await conn.close()
                return result['id']

            # Если точное совпадение не найдено, ищем частичное
            result = await conn.fetchrow("""
                SELECT id FROM metro
                WHERE LOWER(name) LIKE LOWER($1)
                AND is_msk IS NOT FALSE
                ORDER BY LENGTH(name) ASC
                LIMIT 1
            """, f'%{clean_name}%')

            await conn.close()

            if result:
                return result['id']
            else:
                print(f"⚠️ Станция метро '{metro_name}' не найдена в БД")
                return None

        except Exception as e:
            print(f"❌ Ошибка поиска metro_id для станции '{metro_name}': {e}")
            return None

    def get_total_pages_count(self, page_content=None):
        """Определяет общее количество страниц для текущего метро из пагинации"""
        try:
            if page_content is None:
                # Если контент страницы не передан, используем текущую страницу
                page_content = self.driver
            
            total_pages = None
            
            # Метод 1: Поиск по пагинации (самый надежный)
            try:
                # Ищем элементы пагинации
                pagination_elements = page_content.find_elements(By.CSS_SELECTOR, 
                    '[data-marker="pagination-button"], .pagination-item, .pagination__item, .pagination-item')
                
                if pagination_elements:
                    # Ищем последний элемент пагинации (обычно это последняя страница)
                    page_numbers = []
                    for elem in pagination_elements:
                        try:
                            # Пытаемся извлечь номер страницы из текста
                            text = elem.text.strip()
                            if text.isdigit():
                                page_numbers.append(int(text))
                            # Также проверяем атрибуты
                            href = elem.get_attribute('href')
                            if href and 'p=' in href:
                                match = re.search(r'p=(\d+)', href)
                                if match:
                                    page_numbers.append(int(match.group(1)))
                        except:
                            continue
                    
                    if page_numbers:
                        total_pages = max(page_numbers)
                        print(f"📊 Найдено {total_pages} страниц по пагинации")
            except Exception as e:
                print(f"⚠️ Ошибка поиска по пагинации: {e}")
            
            # Метод 2: Поиск по счетчику объявлений и расчет
            if total_pages is None:
                try:
                    # Ищем счетчик общего количества объявлений
                    count_elements = page_content.find_elements(By.CSS_SELECTOR, 
                        '[data-marker="page-title-count"], .page-title-count, .results-count, .search-results-count')
                    
                    for elem in count_elements:
                        try:
                            text = elem.text.strip()
                            # Ищем число в тексте (например: "1 677 объявлений")
                            match = re.search(r'(\d+(?:\s*\d+)*)', text)
                            if match:
                                total_ads = int(match.group(1).replace(' ', ''))
                                # Обычно на странице 50 объявлений, рассчитываем количество страниц
                                calculated_pages = (total_ads + 49) // 50  # Округляем вверх
                                total_pages = calculated_pages
                                print(f"📊 Рассчитано {total_pages} страниц по {total_ads} объявлениям")
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"⚠️ Ошибка расчета по счетчику объявлений: {e}")
            
            # Метод 3: Поиск по URL последней страницы
            if total_pages is None:
                try:
                    # Ищем ссылку на последнюю страницу
                    last_page_links = page_content.find_elements(By.CSS_SELECTOR, 
                        'a[href*="p="], [data-marker="pagination-button"][href*="p="]')
                    
                    max_page = 1
                    for link in last_page_links:
                        try:
                            href = link.get_attribute('href')
                            if href and 'p=' in href:
                                match = re.search(r'p=(\d+)', href)
                                if match:
                                    page_num = int(match.group(1))
                                    max_page = max(max_page, page_num)
                        except:
                            continue
                    
                    if max_page > 1:
                        total_pages = max_page
                        print(f"📊 Найдено {total_pages} страниц по ссылкам")
                except Exception as e:
                    print(f"⚠️ Ошибка поиска по ссылкам: {e}")
            
            if total_pages:
                print(f"🎯 Общее количество страниц для метро {self.metro_name}: {total_pages}")
                return total_pages
            else:
                print(f"⚠️ Не удалось определить общее количество страниц для метро {self.metro_name}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка определения количества страниц: {e}")
            return None
    
    def load_cookies(self):
        """Загружает зафиксированные cookies"""
        try:
            if not os.path.exists(self.cookies_file):
                print(f"❌ Файл cookies не найден: {self.cookies_file}")
                return False
            
            with open(self.cookies_file, 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            
            # Проверяем структуру данных
            if 'cookies' not in cookies_data or 'timestamp' not in cookies_data:
                print("❌ Неверная структура файла cookies")
                return False
            
            # Проверяем количество cookies
            if len(cookies_data['cookies']) < 10:
                print(f"⚠️ Мало cookies: {len(cookies_data['cookies'])} (ожидается минимум 10)")
            
            # Проверяем timestamp
            from datetime import datetime
            try:
                timestamp_str = cookies_data['timestamp']
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                now = datetime.now()
                age_hours = (now - timestamp).total_seconds() / 3600
                
                print(f"✅ Загружены cookies от {timestamp_str}")
                # print(f"📊 Количество cookies: {len(cookies_data['cookies'])}")  # Убрано из лога
                # print(f"⏰ Возраст cookies: {age_hours:.1f} часов")  # Убрано из лога
                
                # Предупреждение если cookies старые
                if age_hours > 24:
                    print("⚠️ Cookies старше 24 часов, могут быть неактуальны")
                if age_hours > 72:
                    print("🚨 Cookies старше 72 часов, рекомендуется обновить")
                
            except Exception as e:
                print(f"⚠️ Не удалось проверить возраст cookies: {e}")
                print(f"✅ Загружены cookies от {cookies_data['timestamp']}")
                # print(f"📊 Количество cookies: {len(cookies_data['cookies'])}")  # Убрано из лога
            
            return cookies_data
            
        except Exception as e:
            print(f"❌ Ошибка загрузки cookies: {e}")
            return False
    
    def setup_selenium(self):
        """Настраивает Selenium WebDriver"""
        try:
            options = Options()
            
            # Базовые настройки
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            
            # Headless режим из конфигурации
            if hasattr(self, 'headless_mode') and self.headless_mode:
                options.add_argument("--headless")
                # print("🔒 Браузер запущен в headless режиме")  # Убрано из лога
            
            # User-Agent
            options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Размер окна
            options.add_argument("--window-size=1920,1080")
            
            # Дополнительные настройки для обхода блокировок
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            options.add_argument("--disable-features=VizDisplayCompositor")
            
            # Дополнительные настройки для обхода блокировок
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-extensions-except")
            options.add_argument("--disable-plugins-discovery")
            options.add_argument("--no-first-run")
            options.add_argument("--no-default-browser-check")
            options.add_argument("--disable-default-apps")
            options.add_argument("--disable-sync")
            options.add_argument("--disable-translate")
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-backgrounding-occluded-windows")
            options.add_argument("--disable-renderer-backgrounding")
            options.add_argument("--disable-features=TranslateUI")
            options.add_argument("--disable-ipc-flooding-protection")
            
            # Убираем признаки автоматизации
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_experimental_option("prefs", {
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
                "profile.managed_default_content_settings.images": 2
            })
            
            # print("🔧 Создаем браузер...")  # Убрано из лога
            self.driver = webdriver.Chrome(options=options)
            
            # Убираем webdriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Дополнительные скрипты для маскировки автоматизации
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
            """)
            
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ru-RU', 'ru', 'en-US', 'en']
                });
            """)
            
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'permissions', {
                    get: () => ({
                        query: () => Promise.resolve({ state: 'granted' })
                    })
                });
            """)
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания браузера: {e}")
            return False
    
    def reload_browser(self):
        """Перезагружает браузер для решения проблем с stale elements"""
        try:
            print("🔄 Перезагружаем браузер для стабильности...")
            
            # Закрываем текущий драйвер
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            
            # Небольшая пауза
            time.sleep(2)
            
            # Создаем новый драйвер
            if not self.setup_selenium():
                return False
            
            # Применяем cookies заново
            cookies_data = self.load_cookies()
            if cookies_data:
                if not self.apply_cookies(cookies_data):
                    print("⚠️ Не удалось применить cookies после перезагрузки")
            
            print("✅ Браузер успешно перезагружен")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка перезагрузки браузера: {e}")
            return False
    
    def restore_driver_connection(self):
        """Восстанавливает соединение с WebDriver при потере соединения"""
        try:
            print("🔄 Восстанавливаем соединение с WebDriver...")
            
            # Устанавливаем флаг ошибки соединения
            self.last_connection_error = True
            
            # Закрываем текущий драйвер
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            
            # Небольшая пауза
            time.sleep(3)
            
            # Создаем новый драйвер
            if not self.setup_selenium():
                print("❌ Не удалось создать новый WebDriver")
                return False
            
            # Применяем cookies заново
            cookies_data = self.load_cookies()
            if cookies_data:
                if not self.apply_cookies(cookies_data):
                    print("⚠️ Не удалось применить cookies после восстановления")
            
            # Возвращаемся на текущую страницу
            if hasattr(self, 'current_page_url') and self.current_page_url:
                try:
                    self.driver.get(self.current_page_url)
                    time.sleep(2)
                    print("✅ Соединение восстановлено, вернулись на текущую страницу")
                    return True
                except Exception as e:
                    print(f"⚠️ Не удалось вернуться на страницу: {e}")
                    return False
            
            print("✅ Соединение восстановлено")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка восстановления соединения: {e}")
            return False
    
    def restore_driver_and_continue(self, current_page, metro_url):
        """Восстанавливает WebDriver и возвращается на текущую страницу для продолжения парсинга"""
        try:
            print(f"🔄 Восстанавливаем WebDriver для продолжения парсинга со страницы {current_page}...")
            
            # Устанавливаем флаг ошибки соединения
            self.last_connection_error = True
            
            # Закрываем текущий драйвер
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            
            # Небольшая пауза
            time.sleep(3)
            
            # Создаем новый драйвер
            if not self.setup_selenium():
                print("❌ Не удалось создать новый WebDriver")
                return False
            
            # Применяем cookies заново
            cookies_data = self.load_cookies()
            if cookies_data:
                if not self.apply_cookies(cookies_data):
                    print("⚠️ Не удалось применить cookies после восстановления")
            
            # Возвращаемся на текущую страницу
            if metro_url:
                try:
                    self.driver.get(metro_url)
                    time.sleep(2)
                    print(f"✅ WebDriver восстановлен, вернулись на страницу {current_page}")
                    return True
                except Exception as e:
                    print(f"⚠️ Не удалось вернуться на страницу {current_page}: {e}")
                    return False
            
            print("✅ WebDriver восстановлен")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка восстановления WebDriver: {e}")
            return False
    
    def safe_parse_card_with_restore(self, card_element, card_index, max_retries=3):
        """Парсит карточку с автоматическим восстановлением WebDriver при потере соединения"""
        for attempt in range(max_retries):
            try:
                # Парсим карточку
                card_data = self.parse_card(card_element)
                if card_data:
                    return card_data
                else:
                    print(f"⚠️ Карточка {card_index + 1} не дала данных")
                    return None
                    
            except Exception as e:
                error_msg = str(e).lower()
                
                # Проверяем на ошибку соединения
                if 'connection refused' in error_msg or 'errno 111' in error_msg or 'max retries exceeded' in error_msg:
                    print(f"🔄 Попытка {attempt + 1}/{max_retries}: WebDriver потерял соединение, восстанавливаем...")
                    
                    if not self.restore_driver_connection():
                        print("❌ Не удалось восстановить WebDriver")
                        return None
                    
                    # Получаем свежие элементы после восстановления
                    try:
                        fresh_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                        if card_index < len(fresh_cards):
                            card_element = fresh_cards[card_index]
                            print(f"✅ Получены свежие элементы после восстановления")
                        else:
                            print(f"⚠️ Карточка {card_index + 1} недоступна после восстановления")
                            return None
                    except Exception as restore_error:
                        print(f"❌ Ошибка получения элементов после восстановления: {restore_error}")
                        return None
                    
                    time.sleep(2)  # Пауза после восстановления
                    continue
                    
                # Для других ошибок (stale element, timeout) используем обычную retry логику
                elif ('stale element' in error_msg or 'element not found' in error_msg or 'timeout' in error_msg) and attempt < max_retries - 1:
                    print(f"🔄 Попытка {attempt + 1}/{max_retries}: {error_msg}, пробуем еще раз...")
                    time.sleep(0.5)
                    continue
                else:
                    print(f"❌ Ошибка карточки {card_index + 1}: {e}")
                    return None
        
        return None
    
    def safe_parse_seller_info(self, card_element, card_data=None, max_retries=3):
        """Безопасно парсит информацию о продавце с автоматическим восстановлением WebDriver при потере соединения"""
        for attempt in range(max_retries):
            try:
                # Парсим информацию о продавце
                seller_data = self.parse_seller_info(card_element, card_data)
                return seller_data
                    
            except Exception as e:
                error_msg = str(e).lower()
                
                # Проверяем на ошибку соединения
                if 'connection refused' in error_msg or 'errno 111' in error_msg or 'max retries exceeded' in error_msg:
                    print(f"🔄 Попытка {attempt + 1}/{max_retries}: WebDriver потерял соединение при парсинге продавца, восстанавливаем...")
                    
                    if not self.restore_driver_connection():
                        print("❌ Не удалось восстановить WebDriver")
                        return {
                            'type': 'ошибка восстановления',
                            'full_text': 'ошибка восстановления'
                        }
                    
                    time.sleep(2)  # Пауза после восстановления
                    continue
                    
                # Для других ошибок возвращаем пустой результат
                else:
                    print(f"⚠️ Ошибка парсинга продавца: {e}")
                    return {
                        'type': 'ошибка парсинга',
                        'full_text': 'ошибка парсинга'
                    }
        
        return {
            'type': 'ошибка после всех попыток',
            'full_text': 'ошибка после всех попыток'
        }
    
    def apply_cookies(self, cookies_data):
        """Применяет cookies к драйверу"""
        try:
            if not cookies_data or 'cookies' not in cookies_data:
                print("❌ Данные cookies отсутствуют или некорректны")
                return False
            
            # print(f"📊 Найдено cookies для применения: {len(cookies_data['cookies'])}")  # Убрано из лога
            
            # Сначала переходим на домен
            # print("🌐 Переходим на AVITO...")  # Убрано из лога
            self.driver.get("https://avito.ru")
            time.sleep(5)  # Увеличиваем время ожидания
            
            # Проверяем, что страница загрузилась
            try:
                # Ждем появления элемента, подтверждающего загрузку
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # print("✅ Страница AVITO загружена")  # Убрано из лога
            except:
                print("⚠️ Страница AVITO загрузилась частично, продолжаем...")
            
            # Применяем cookies
            applied_count = 0
            failed_count = 0
            
            for i, cookie in enumerate(cookies_data['cookies']):
                try:
                    # Проверяем обязательные поля
                    if 'name' not in cookie or 'value' not in cookie:
                        print(f"⚠️ Cookie {i+1} пропущен - отсутствуют обязательные поля")
                        failed_count += 1
                        continue
                    
                    cookie_dict = {
                        'name': cookie['name'],
                        'value': cookie['value'],
                        'domain': cookie.get('domain', ''),
                        'path': cookie.get('path', '/')
                    }
                    
                    # Добавляем дополнительные поля только если они есть
                    if cookie.get('expiry'):
                        cookie_dict['expiry'] = cookie['expiry']
                    if cookie.get('secure'):
                        cookie_dict['secure'] = cookie['secure']
                    if cookie.get('httpOnly'):
                        cookie_dict['httpOnly'] = cookie['httpOnly']
                    if cookie.get('sameSite'):
                        cookie_dict['sameSite'] = cookie['sameSite']
                    
                    # Применяем cookie
                    self.driver.add_cookie(cookie_dict)
                    applied_count += 1
                    
                    # Выводим информацию о важных cookies
                    if cookie['name'] in ['_avisc', 'srv_id', 'buyer_location_id']:
                        # print(f"🔐 Применен важный cookie: {cookie['name']}")  # Убрано из лога
                        pass
                    
                except Exception as e:
                    print(f"⚠️ Ошибка применения cookie {i+1} ({cookie.get('name', 'unknown')}): {e}")
                    failed_count += 1
                    continue
            
            print(f"✅ Применено cookies: {applied_count}")
            if failed_count > 0:
                print(f"⚠️ Не удалось применить: {failed_count}")
            
            # Проверяем, что cookies действительно применились
            current_cookies = self.driver.get_cookies()
            print(f"📊 Текущих cookies в браузере: {len(current_cookies)}")
            
            # Обновляем страницу с примененными cookies
            print("🔄 Обновляем страницу с cookies...")
            self.driver.refresh()
            time.sleep(5)
            
            # Проверяем, что мы все еще на AVITO
            current_url = self.driver.current_url
            if 'avito.ru' in current_url:
                print(f"✅ Страница обновлена, текущий URL: {current_url}")
            else:
                print(f"⚠️ Страница перенаправлена на: {current_url}")
            
            return applied_count > 0
            
        except Exception as e:
            print(f"❌ Ошибка применения cookies: {e}")
            return False
    
    def get_metro_url(self):
        """Получает URL для метро с правильным avito_id
        
        Поддерживает как одиночные, так и множественные метро
        """
        if self.multiple_metro_avito_ids and len(self.multiple_metro_avito_ids) > 1:
            # Генерируем URL для множественных метро
            metro_param = '-'.join(map(str, self.multiple_metro_avito_ids))
            base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
            metro_url = f"{base_url}?metro={metro_param}&footWalkingMetro=20"
            print(f"🔗 Генерируем URL для {len(self.multiple_metro_avito_ids)} метро: {metro_param}")
            return metro_url
        elif self.metro_avito_id:
            # Генерируем URL для одиночного метро (совместимость)
            base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
            metro_url = f"{base_url}?metro={self.metro_avito_id}&footWalkingMetro=20"
            return metro_url
        else:
            print("❌ avito_id для метро не определен")
            return None
    
    def generate_search_context(self) -> str:
        """Генерирует простой context для каждого запроса
        
        ВАЖНО: Данные сжимаются только один раз для избежания дублирования gzip header
        """
        import base64
        import gzip
        import json
        import random
        
        context_data = {
            "fromPage": "catalog",
            "timestamp": random.randint(1000000000, 9999999999),
            "sessionId": random.randint(100000, 999999)
        }
        try:
            # Создаем JSON строку
            json_str = json.dumps(context_data, separators=(',', ':'))
            
            # Сжимаем ОДИН раз
            compressed = gzip.compress(json_str.encode('utf-8'))
            
            # Кодируем в base64
            encoded = base64.b64encode(compressed).decode('utf-8')
            
            # Проверяем, что нет дублирования
            if encoded.startswith('H4sI'):
                print(f"[CONTEXT] ⚠️ Обнаружен дублированный gzip header в encoded данных!")
                print(f"[CONTEXT] Это означает, что данные уже были сжаты ранее")
                # Возвращаем простой контекст без дополнительного сжатия
                return "H4sIAAAAAAAA_wEjANz_YToxOntzOjg6ImZyb21QYWdlIjtzOjc6ImNhdGFsb2ciO312FITcIwAAAA"
            
            result = f"H4sIAAAAAAAA_{encoded}"
            
            # Финальная проверка на дублирование
            if result.count('H4sIAAAAAAAA_') > 1:
                print(f"[CONTEXT] ❌ КРИТИЧЕСКАЯ ОШИБКА: Дублированный gzip header!")
                print(f"[CONTEXT] Возвращаем fallback контекст")
                return "H4sIAAAAAAAA_wEjANz_YToxOntzOjg6ImZyb21QYWdlIjtzOjc6ImNhdGFsb2ciO312FITcIwAAAA"
            
            print(f"[CONTEXT] ✅ Контекст сгенерирован корректно (длина: {len(result)})")
            return result
            
        except Exception as e:
            print(f"[CONTEXT] Ошибка генерации: {e}")
            return "H4sIAAAAAAAA_wEjANz_YToxOntzOjg6ImZyb21QYWdlIjtzOjc6ImNhdGFsb2ciO312FITcIwAAAA"
    
    def clean_url_path(self, url_path: str) -> str:
        """Очищает URL от всех параметров, оставляя только путь"""
        if not url_path:
            return ""
        if "?" in url_path:
            url_path = url_path.split("?")[0]
        return url_path
    
    def get_metro_url_with_page(self, page=1):
        """Получает URL для метро с пагинацией и context
        
        Поддерживает как одиночные, так и множественные метро
        """
        if self.multiple_metro_avito_ids and len(self.multiple_metro_avito_ids) > 1:
            # Генерируем URL для множественных метро
            metro_param = '-'.join(map(str, self.multiple_metro_avito_ids))
            base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
            metro_url = f"{base_url}?metro={metro_param}&s=104&footWalkingMetro=20"
            
            # Добавляем пагинацию (Avito использует параметр p)
            if page > 1:
                metro_url += f"&p={page}"
            
            # Генерируем новый context для каждой страницы
            context = self.generate_search_context()
            
            # Проверяем контекст на дублирование перед добавлением в URL
            if context and context.count('H4sIAAAAAAAA_') > 1:
                print(f"[CONTEXT] ❌ ОШИБКА: Обнаружен дублированный gzip header в контексте!")
                print(f"[CONTEXT] Используем fallback контекст для страницы {page}")
                context = "H4sIAAAAAAAA_wEjANz_YToxOntzOjg6ImZyb21QYWdlIjtzOjc6ImNhdGFsb2ciO312FITcIwAAAA"
            
            metro_url += f"&context={context}"
            
            print(f"[CONTEXT] Страница {page}: context добавлен в URL для {len(self.multiple_metro_avito_ids)} метро")
            return metro_url
            
        elif self.metro_avito_id:
            # Генерируем URL для одиночного метро (совместимость)
            base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
            metro_url = f"{base_url}?metro={self.metro_avito_id}&s=104&footWalkingMetro=20"
            
            # Добавляем пагинацию (Avito использует параметр p)
            if page > 1:
                metro_url += f"&p={page}"
            
            # Генерируем новый context для каждой страницы
            context = self.generate_search_context()
            
            # Проверяем контекст на дублирование перед добавлением в URL
            if context and context.count('H4sIAAAAAAAA_') > 1:
                print(f"[CONTEXT] ❌ ОШИБКА: Обнаружен дублированный gzip header в контексте!")
                print(f"[CONTEXT] Используем fallback контекст для страницы {page}")
                context = "H4sIAAAAAAAA_wEjANz_YToxOntzOjg6ImZyb21QYWdlIjtzOjc6ImNhdGFsb2ciO312FITcIwAAAA"
            
            metro_url += f"&context={context}"
            
            print(f"[CONTEXT] Страница {page}: context добавлен в URL")
            return metro_url
        else:
            print("❌ avito_id для метро не определен")
            return None
    
    def wait_for_dom_stability(self, timeout=15):
        """Упрощенная версия: простая пауза + проверка на пустую страницу
        
        Заменяет сложную логику ожидания DOM на простую паузу в 1 секунду
        и быструю проверку на пустую страницу.
        """
        try:
            print("⏳ Простая пауза для стабилизации DOM (1 сек)...")
            
            # Простая пауза вместо сложной логики
            time.sleep(1)
            
            # БЫСТРАЯ ПРОВЕРКА на пустую страницу
            page_text = self.driver.page_source.lower()
            empty_indicators = [
                'объявлений не найдено',
                'ничего не найдено',
                'по вашему запросу ничего не найдено',
                'нет объявлений',
                'объявления не найдены',
                'не найдено',
                'пустой результат',
                'поиск не дал результатов',
                'ничего не найдено на выбранных станциях метро',  # Ключевой маркер для завершения
                'вас может заинтересовать'  # Раздел с объявлениями других метро - НЕ ОБРАБАТЫВАТЬ
            ]
            
            # Проверяем на пустую страницу
            if any(indicator in page_text for indicator in empty_indicators):
                # Определяем конкретный маркер для лучшего логирования
                found_indicators = [indicator for indicator in empty_indicators if indicator in page_text]
                
                if 'вас может заинтересовать' in page_text:
                    print("ℹ️ Обнаружен раздел 'Вас может заинтересовать' - объявления других метро")
                    print("ℹ️ Нужно обработать карточки ДО этого раздела")
                    # НЕ завершаем парсинг - продолжаем обработку найденных карточек
                    return False
                elif 'ничего не найдено на выбранных станциях метро' in page_text:
                    print("ℹ️ Обнаружен маркер 'Ничего не найдено на выбранных станциях метро'")
                    print("🔄 Завершаем парсинг метро - переходим к следующему")
                    return True
                else:
                    print(f"ℹ️ Страница пустая - найдены индикаторы: {found_indicators}")
                    print("🔄 Завершаем парсинг метро - переходим к следующему")
                    return True
            
            print("✅ DOM стабилизирован (простая пауза)")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка ожидания стабилизации DOM: {e}")
            return False

    def wait_for_cards_load(self, timeout=30):
        """Ждет загрузки карточек или определяет, что страница пустая
        
        КЛЮЧЕВАЯ ЛОГИКА: Если найден маркер "Ничего не найдено на выбранных станциях метро",
        парсинг метро завершается НЕЗАВИСИМО от наличия карточек, так как Avito может
        показывать объявления с других метро, которые не нужны.
        
        ДОПОЛНИТЕЛЬНО: Если найден раздел "Вас может заинтересовать", парсинг также
        завершается, так как это означает переход к объявлениям других метро.
        """
        try:
            print("⏳ Проверяем загрузку карточек...")
            
            # Сначала проверяем, есть ли уже карточки на странице
            initial_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
            if initial_cards:
                print(f"✅ Карточки уже загружены: {len(initial_cards)}")
                return True
            
            # Если карточек нет, проверяем на пустую страницу
            page_text = self.driver.page_source.lower()
            empty_indicators = [
                'объявлений не найдено',
                'ничего не найдено',
                'по вашему запросу ничего не найдено',
                'нет объявлений',
                'объявления не найдены',
                'не найдено',
                'пустой результат',
                'поиск не дал результатов',
                'результатов поиска не найдено',
                'объявления отсутствуют',
                'ничего не найдено на выбранных станциях метро',  # Ключевой маркер для завершения
                'вас может заинтересовать'  # Раздел с объявлениями других метро - НЕ ОБРАБАТЫВАТЬ
            ]
            
            # Проверяем на пустую страницу
            if any(indicator in page_text for indicator in empty_indicators):
                # Определяем конкретный маркер для лучшего логирования
                found_indicators = [indicator for indicator in empty_indicators if indicator in page_text]
                
                if 'вас может заинтересовать' in page_text:
                    print("ℹ️ Обнаружен раздел 'Вас может заинтересовать' - объявления других метро")
                    print("🔄 Завершаем парсинг метро - переходим к следующему")
                elif 'ничего не найдено на выбранных станциях метро' in page_text:
                    print("ℹ️ Обнаружен маркер 'Ничего не найдено на выбранных станциях метро'")
                    print("🔄 Завершаем парсинг метро - переходим к следующему")
                else:
                    print(f"ℹ️ Страница пустая - найдены индикаторы: {found_indicators}")
                    print("🔄 Завершаем парсинг метро - переходим к следующему")
                
                return True  # Возвращаем True, чтобы корректно обработать пустую страницу
            
            # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: ищем карточки на странице
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                if not cards:
                    print("ℹ️ Страница пустая - карточки не найдены")
                    return True
                else:
                    print(f"ℹ️ Найдено карточек на странице: {len(cards)}")
            except Exception as e:
                print(f"⚠️ Ошибка поиска карточек: {e}")
                # Продолжаем с текстовой проверкой
            
            # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: ждем появления карточек с таймаутом
            start_time = time.time()
            while time.time() - start_time < timeout:
                cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                if cards:
                    print(f"✅ Карточки загружены: {len(cards)}")
                    return True
                time.sleep(0.5)  # Проверяем каждые 0.5 секунды
            
            # Если таймаут истек, проверяем еще раз на пустую страницу
            print(f"⚠️ Таймаут ожидания карточек ({timeout}с), финальная проверка...")
            page_text = self.driver.page_source.lower()
            if any(indicator in page_text for indicator in empty_indicators):
                print("ℹ️ Страница пустая - объявлений не найдено")
                return True
            
            # Если ничего не нашли, считаем страницу загруженной
            print("ℹ️ Карточки не найдены, но страница загружена")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка проверки загрузки карточек: {e}")
            return False
    
    def smooth_scroll_and_load_cards(self, target_cards=20, scroll_pause=1.5):
        """Плавно прокручивает страницу и загружает карточки"""
        try:
            # Проверяем, включена ли плавная прокрутка
            if not self.enable_smooth_scroll:
                print("⏭️ Плавная прокрутка отключена, пропускаем...")
                return target_cards
            

            
            # Используем настройки из конфигурации
            actual_scroll_pause = self.scroll_pause if hasattr(self, 'scroll_pause') else scroll_pause
            max_attempts = self.max_scroll_attempts if hasattr(self, 'max_scroll_attempts') else 10
            
            initial_cards = 0
            current_cards = 0
            scroll_attempts = 0
            
            while current_cards < target_cards and scroll_attempts < max_attempts:
                try:
                    # Получаем текущее количество карточек
                    cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                    current_cards = len(cards)
                    
                    if current_cards > initial_cards:
                        initial_cards = current_cards
                    
                    # Если достигли цели, прекращаем
                    if current_cards >= target_cards:
                        break
                    
                    # Плавно прокручиваем вниз
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    
                    # Ждем загрузки новых карточек
                    time.sleep(actual_scroll_pause)
                    
                    # Проверяем, появились ли новые карточки
                    new_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                    if len(new_cards) <= current_cards:
                        time.sleep(actual_scroll_pause * 2)  # Увеличиваем паузу
                    
                    scroll_attempts += 1
                    
                except Exception as e:
                    print(f"⚠️ Ошибка при прокрутке: {e}")
                    time.sleep(actual_scroll_pause * 2)
                    scroll_attempts += 1
                    continue
            
            # Финальная проверка
            final_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
            
            if scroll_attempts >= max_attempts:
                pass
            
            return len(final_cards)
            
        except Exception as e:
            print(f"❌ Ошибка плавной прокрутки: {e}")
            # Возвращаем количество карточек, которые успели загрузиться
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                return len(cards)
            except:
                return 0
    
    def parse_full_page(self, target_cards=50):
        """Парсинг всей страницы сразу (без прокрутки)"""
        try:
            print(f"🔄 Парсим все карточки сразу (цель: {target_cards})...")
            
            # Получаем все доступные карточки
            all_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
            total_cards = len(all_cards)
            
            print(f"📊 Найдено карточек на странице: {total_cards}")
            
            if total_cards == 0:
                print("⚠️ Карточки не найдены на странице")
                return []
            
            # Ограничиваем количество карточек для парсинга
            cards_to_parse = min(target_cards, total_cards)
            print(f"🎯 Будем парсить карточек: {cards_to_parse}")
            
            parsed_cards = []
            
            # Парсим карточки по 5 за раз с retry логикой
            for i in range(0, cards_to_parse, 5):  # Шаг 5: 0, 5, 10, 15...
                # Retry логика для группы
                max_group_retries = 3
                group_retry_count = 0
                group_success = False
                
                while group_retry_count < max_group_retries and not group_success:
                    try:
                        # КРИТИЧНО: Получаем СВЕЖИЕ элементы для каждой группы
                        fresh_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                        
                        # Определяем диапазон для текущей группы (5 карточек)
                        start_idx = i
                        end_idx = min(i + 5, cards_to_parse)
                        group_size = end_idx - start_idx
                        
                        if group_retry_count == 0:
                            print(f"🔄 Парсим группу карточек {start_idx+1}-{end_idx} ({group_size} карточек)...")
                        else:
                            print(f"🔄 Повтор группы {start_idx+1}-{end_idx} (попытка {group_retry_count + 1}/{max_group_retries})...")
                        
                        # БЫСТРЫЙ пакетный парсинг группы из 5 карточек
                        group_parsed_count = 0
                        
                        # Собираем все карточки группы для быстрого парсинга
                        group_cards = []
                        for j in range(start_idx, end_idx):
                            if j >= len(fresh_cards):
                                print(f"⚠️ Карточка {j+1} недоступна, пропускаем")
                                continue
                            group_cards.append((j, fresh_cards[j]))
                        
                        # Пакетно парсим все карточки группы
                        for j, card in group_cards:
                            try:
                                # Обычная обработка для всех карточек
                                card_data = self.parse_card(card)
                                if card_data:
                                    card_data['card_number'] = j + 1  # Правильная нумерация по позиции
                                    card_data['raw_text'] = card.text.strip()
                                    parsed_cards.append(card_data)
                                    group_parsed_count += 1
                                    print(f"   ✅ Спарсена карточка {j+1} (всего: {len(parsed_cards)})")
                                else:
                                    print(f"   ⚠️ Карточка {j+1} не дала данных")
                                        
                            except Exception as e:
                                print(f"   ❌ Ошибка карточки {j+1}: {e}")
                                continue
                        
                        print(f"✅ Группа {start_idx+1}-{end_idx} завершена: +{group_parsed_count} карточек ({len(parsed_cards)} всего)")
                        group_success = True  # Группа успешно обработана
                            
                    except Exception as e:
                        error_msg = str(e).lower()
                        if 'stale element' in error_msg and group_retry_count < max_group_retries - 1:
                            print(f"🔄 Stale element в группе {start_idx+1}-{end_idx}, повторяем... (попытка {group_retry_count + 1}/{max_group_retries})")
                            group_retry_count += 1
                            time.sleep(0.5)  # Небольшая пауза перед повтором
                            continue
                        else:
                            print(f"❌ Ошибка парсинга группы {start_idx+1}-{end_idx}: {e}")
                            if group_retry_count >= max_group_retries - 1:
                                print(f"⏹️ Группа {start_idx+1}-{end_idx} пропущена после {max_group_retries} попыток")
                            break
            
            print(f"✅ Парсинг завершен: {len(parsed_cards)} карточек из {cards_to_parse}")
            return parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга всех карточек: {e}")
            return parsed_cards if 'parsed_cards' in locals() else []
    
    def parse_full_page_with_elements(self, cards_elements, target_cards=50):
        """Парсит все карточки на странице используя уже полученные элементы (без повторного поиска)"""
        try:
            print(f"🎯 Парсим страницу с уже полученными элементами")
            
            total_cards = len(cards_elements)
            print(f"📊 Всего карточек на странице: {total_cards}")
            
            # Ограничиваем количество карточек для парсинга
            cards_to_parse = min(target_cards, total_cards)
            print(f"🎯 Будем парсить карточек: {cards_to_parse}")
            
            parsed_cards = []
            
            # Парсим карточки по 5 за раз (без retry логики, так как элементы уже получены)
            for i in range(0, cards_to_parse, 5):  # Шаг 5: 0, 5, 10, 15...
                # Определяем диапазон для текущей группы (5 карточек)
                start_idx = i
                end_idx = min(i + 5, cards_to_parse)
                group_size = end_idx - start_idx
                
                print(f"🔄 Парсим группу карточек {start_idx+1}-{end_idx} ({group_size} карточек)...")
                
                # Пакетно парсим все карточки группы
                group_parsed_count = 0
                for j in range(start_idx, end_idx):
                    try:
                        if j >= len(cards_elements):
                            print(f"⚠️ Карточка {j+1} недоступна, пропускаем")
                            continue
                            
                        card = cards_elements[j]
                        card_data = self.parse_card(card)
                        if card_data:
                            card_data['card_number'] = j + 1  # Правильная нумерация по позиции
                            card_data['raw_text'] = card.text.strip()
                            parsed_cards.append(card_data)
                            group_parsed_count += 1
                            print(f"   ✅ Спарсена карточка {j+1} (всего: {len(parsed_cards)})")
                        else:
                            print(f"   ⚠️ Карточка {j+1} не дала данных")
                                    
                    except Exception as e:
                        print(f"   ❌ Ошибка карточки {j+1}: {e}")
                        continue
                
                print(f"✅ Группа {start_idx+1}-{end_idx} завершена: +{group_parsed_count} карточек ({len(parsed_cards)} всего)")
            
            print(f"✅ Парсинг завершен: {len(parsed_cards)} карточек из {cards_to_parse}")
            return parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга с готовыми элементами: {e}")
            return []
    
    def parse_hybrid_approach(self, cards_elements, target_cards=50):
        """
        Парсинг как в старом скрипте: первые N карточек потоково с двойным парсингом первой,
        остальные M - пачками как в старом скрипте
        """
        try:
            print(f"🔄 Парсинг как в старом скрипте: первые {self.stream_cards_count} потоково + остальные {self.batch_cards_count} пачками")
            
            total_cards = len(cards_elements)
            print(f"📊 Всего карточек на странице: {total_cards}")
            
            # Ограничиваем количество карточек для парсинга
            cards_to_parse = min(target_cards, total_cards)
            print(f"🎯 Будем парсить карточек: {cards_to_parse}")
            
            parsed_cards = []
            
            # ЭТАП 1: Первые N карточек - потоково (как в старом скрипте)
            stream_count = min(self.stream_cards_count, cards_to_parse)
            if stream_count > 0:
                print(f"🔄 ЭТАП 1: Парсим первые {stream_count} карточек потоково...")
                
                for i in range(stream_count):
                    max_retries = 3  # Максимальное количество попыток для каждой карточки
                    retry_count = 0
                    card_parsed = False
                    
                    print(f"   🎯 Начинаем парсинг карточки {i+1} (максимум {max_retries} попыток)")
                    
                    while retry_count < max_retries and not card_parsed:
                        try:
                            print(f"      🔄 Попытка {retry_count + 1}/{max_retries} для карточки {i+1}")
                            
                            # Получаем свежие элементы перед каждой попыткой
                            fresh_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                            
                            if i >= len(fresh_cards):
                                print(f"⚠️ Карточка {i+1} недоступна в свежих элементах, пропускаем")
                                break
                            
                            card = fresh_cards[i]
                            
                            # Парсим карточку
                            card_data = self.safe_parse_card_with_restore(card, i)
                            if card_data:
                                card_data['card_number'] = i + 1
                                card_data['raw_text'] = card.text.strip()
                                parsed_cards.append(card_data)
                                print(f"   ✅ Спарсена карточка {i+1} (потоково)")
                                card_parsed = True
                                
                                # ОСОБЕННОСТЬ: После 2-й карточки парсим первую заново для стабильности
                                if i == 1:  # Только после 2-й карточки (индекс 1)
                                    print("      🔄 После успешной карточки 2 парсим первую заново для стабильности...")
                                    try:
                                        # Получаем свежие элементы
                                        fresh_cards_refresh = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                                        if len(fresh_cards_refresh) > 0:
                                            first_card_refresh = fresh_cards_refresh[0]
                                            # Парсим первую карточку безопасным способом
                                            first_card_data = self.safe_parse_card_with_restore(first_card_refresh, 0)
                                            if first_card_data:
                                                print("      ✅ Первая карточка успешно перепарсена для стабильности")
                                                # ВАЖНО: Если первая карточка не была в результатах, добавляем её
                                                if not any(card.get('card_number') == 1 for card in parsed_cards):
                                                    first_card_data['card_number'] = 1
                                                    first_card_data['raw_text'] = first_card_refresh.text.strip()
                                                    parsed_cards.insert(0, first_card_data)  # Вставляем в начало
                                                    print("      ✅ Первая карточка добавлена в результаты после перепарсинга")
                                            else:
                                                print("      ⚠️ Первая карточка не дала данных при перепарсинге")
                                        else:
                                            print("      ⚠️ Не удалось получить элементы для перепарсинга первой карточки")
                                    except Exception as refresh_error:
                                        print(f"      ⚠️ Ошибка перепарсинга первой карточки: {refresh_error}")
                                elif i == 0:
                                    print("      ✅ Обычный парсинг первой карточки успешен")
                                else:
                                    print(f"      ✅ Карточка {i+1} успешно спарсена")
                            else:
                                print(f"   ⚠️ Карточка {i+1} не дала данных")
                                # НЕ делаем break - позволяем retry логике работать
                                raise Exception("Карточка не дала данных")
                                        
                        except Exception as e:
                            error_msg = str(e).lower()
                            retry_count += 1
                            
                            print(f"   🔄 Попытка {retry_count}/{max_retries} для карточки {i+1} завершилась с ошибкой")
                            print(f"      Тип ошибки: {type(e).__name__}")
                            print(f"      Сообщение: {str(e)[:100]}...")
                            
                            # Проверяем на stale element и другие ошибки, которые можно повторить
                            if ('stale element' in error_msg or 'element not found' in error_msg or 'timeout' in error_msg) and retry_count < max_retries:
                                print(f"   🔄 Ошибка для карточки {i+1}, пробуем еще раз... (попытка {retry_count}/{max_retries})")
                                time.sleep(0.5)  # Пауза как в старом скрипте
                                continue
                            else:
                                print(f"   ❌ Ошибка карточки {i+1} (попытка {retry_count}/{max_retries}): {e}")
                                break
                    
                    # Если карточка не была спарсена после всех попыток
                    if not card_parsed:
                        print(f"   ❌ Карточка {i+1} не удалась после {max_retries} попыток")
                
                print(f"✅ ЭТАП 1 завершен: {len(parsed_cards)} карточек")
            
            # ЭТАП 2: Остальные карточки - пачками по 5 в группе
            remaining_cards = cards_to_parse - stream_count
            if remaining_cards > 0:
                print(f"🔄 ЭТАП 2: Парсим остальные {remaining_cards} карточек пачками по 5...")
                
                # Парсим карточки группами по 5
                batch_size = 5
                for start_idx in range(stream_count, cards_to_parse, batch_size):
                    end_idx = min(start_idx + batch_size, cards_to_parse)
                    group_size = end_idx - start_idx
                    
                    print(f"🔄 Парсим группу карточек {start_idx+1}-{end_idx} ({group_size} карточек)...")
                    
                    # Парсим все карточки в текущей группе
                    group_parsed_count = 0
                    for j in range(start_idx, end_idx):
                        try:
                            # Получаем свежие элементы перед каждой попыткой (как в старом скрипте)
                            fresh_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                            if j >= len(fresh_cards):
                                print(f"⚠️ Карточка {j+1} недоступна в свежих элементах, пропускаем")
                                continue
                                
                            card = fresh_cards[j]
                            
                            # Парсим карточку с автоматическим восстановлением при потере соединения
                            card_data = self.safe_parse_card_with_restore(card, j)
                            if card_data:
                                card_data['card_number'] = j + 1
                                card_data['raw_text'] = card.text.strip()
                                parsed_cards.append(card_data)
                                group_parsed_count += 1
                                # print(f"   ✅ Спарсена карточка {j+1} (пачкой)")  # Убрано из лога
                            else:
                                print(f"   ⚠️ Карточка {j+1} не дала данных")
                                    
                        except Exception as e:
                            error_msg = str(e).lower()
                            
                            # Для пакетного парсинга тоже добавляем retry логику как в старом скрипте
                            if 'stale element' in error_msg or 'element not found' in error_msg:
                                print(f"   🔄 Stale element для карточки {j+1}, получаем свежие элементы...")
                                try:
                                    # Получаем свежие элементы и повторяем попытку
                                    fresh_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                                    if j < len(fresh_cards):
                                        card = fresh_cards[j]
                                        card_data = self.safe_parse_card_with_restore(card, j)
                                        if card_data:
                                            card_data['card_number'] = j + 1
                                            card_data['raw_text'] = card.text.strip()
                                            parsed_cards.append(card_data)
                                            group_parsed_count += 1
                                            # print(f"   ✅ Спарсена карточка {j+1} (пачкой, после retry)")  # Убрано из лога
                                        else:
                                            print(f"   ⚠️ Карточка {j+1} не дала данных (после retry)")
                                    else:
                                        print(f"   ⚠️ Карточка {j+1} недоступна после retry")
                                except Exception as retry_error:
                                    print(f"   ❌ Ошибка retry для карточки {j+1}: {retry_error}")
                            else:
                                print(f"   ❌ Ошибка карточки {j+1}: {e}")
                    
                    print(f"✅ Группа {start_idx+1}-{end_idx} завершена: {group_parsed_count} карточек")
                
                print(f"✅ ЭТАП 2 завершен: {len(parsed_cards)} карточек")
            
            return parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга карточки: {e}")
            return parsed_cards if 'parsed_cards' in locals() else []

    def parse_card_with_schema(self, card_element):
        """Парсит карточку используя Schema.org разметку"""
        try:
            # 1. Проверяем наличие Schema.org разметки
            try:
                itemtype = card_element.get_attribute('itemtype')
                if itemtype and 'schema.org/Product' in itemtype:
                    pass  # Schema.org разметка найдена
            except:
                pass  # Не удалось проверить Schema.org разметку
            
            # 2. Ищем заголовок по Schema.org
            try:
                title_elem = card_element.find_element(By.CSS_SELECTOR, 'a.snippet-link')
                title = title_elem.get_attribute('title')
            except:
                title = None
            
            # 3. Ищем цену по Schema.org
            try:
                price_elem = card_element.find_element(By.CSS_SELECTOR, 'span[data-marker="item-price"]')
                price = price_elem.text.strip()
            except:
                price = None
            
            # 4. Ищем ссылку по Schema.org
            try:
                link_elem = card_element.find_element(By.CSS_SELECTOR, 'a.snippet-link')
                link = link_elem.get_attribute('href')
            except:
                link = None
            
            # 5. Ищем все элементы с Schema.org атрибутами
            try:
                schema_elements = card_element.find_elements(By.CSS_SELECTOR, '[itemprop]')
                if schema_elements:
                    pass  # Элементы с itemprop найдены
            except:
                pass  # Ошибка поиска Schema.org элементов
            
            # 6. Ищем структурированные данные (JSON-LD)
            try:
                json_ld_scripts = card_element.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')
                if json_ld_scripts:
                    pass  # JSON-LD скрипты найдены
                else:
                    # Ищем в родительских элементах
                    try:
                        parent = card_element.find_element(By.XPATH, './..')
                        json_ld_scripts = parent.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')
                        if json_ld_scripts:
                            pass  # JSON-LD скрипты найдены в родительском элементе
                    except:
                        pass
            except:
                pass  # Ошибка поиска JSON-LD
            
            # 7. Анализируем структуру карточки
            try:
                # Ищем все div элементы с классами
                divs = card_element.find_elements(By.CSS_SELECTOR, 'div[class*="item"]')
                item_divs = []
                for div in divs:
                    try:
                        class_name = div.get_attribute('class')
                        if 'item' in class_name:
                            item_divs.append((class_name, div.text.strip()[:100]))
                    except:
                        continue
                
                if item_divs:
                    print(f"✅ Найдено {len(item_divs)} div элементов с 'item' в классе:")
                    for class_name, text in item_divs[:5]:  # Показываем первые 5
                        print(f"   • {class_name}: {text}...")
                else:
                    print(f"❌ Div элементы с 'item' в классе не найдены")
            except Exception as e:
                print(f"⚠️ Ошибка анализа структуры: {e}")
            
            # 8. Ищем специфичные элементы Avito
            try:
                # Ищем элементы с data-marker
                data_markers = card_element.find_elements(By.CSS_SELECTOR, '[data-marker]')
                if data_markers:
                    pass  # Элементы с data-marker найдены
                else:
                    pass  # Элементы с data-marker не найдены
            except:
                pass  # Ошибка поиска data-marker
            
            print("=" * 60)
            
        except Exception as e:
            print(f"❌ Ошибка парсинга с Schema.org: {e}")

    def parse_card_with_javascript(self, card_element):
        """Парсит карточку используя JavaScript для обхода stale element проблем"""
        try:
            print("      🔍 JavaScript парсинг: начинаем...")
            
            # Используем JavaScript для получения данных карточки
            js_script = """
            function parseCard(card) {
                try {
                    const data = {};
                    
                    // Получаем заголовок
                    const titleElem = card.querySelector('a.snippet-link');
                    if (titleElem) {
                        data.title = titleElem.getAttribute('title') || titleElem.textContent.trim();
                    }
                    
                    // Получаем цену
                    const priceElem = card.querySelector('span[data-marker="item-price"]');
                    if (priceElem) {
                        data.price = priceElem.textContent.trim();
                    }
                    
                    // Получаем ссылку
                    if (titleElem) {
                        data.link = titleElem.href;
                    }
                    
                    // Получаем адрес
                    const addressElem = card.querySelector('[data-marker="item-address"]');
                    if (addressElem) {
                        data.address = addressElem.textContent.trim();
                    }
                    
                    // Получаем описание
                    const descElem = card.querySelector('[data-marker="item-description"]');
                    if (descElem) {
                        data.description = descElem.textContent.trim();
                    }
                    
                    // Получаем ID объявления
                    const idElem = card.querySelector('[data-marker="item"]');
                    if (idElem) {
                        data.item_id = idElem.getAttribute('data-item-id');
                    }
                    
                    return data;
                } catch (e) {
                    return null;
                }
            }
            return parseCard(arguments[0]);
            """
            
            result = self.driver.execute_script(js_script, card_element)
            
            if result and isinstance(result, dict):
                print("      ✅ JavaScript парсинг успешен")
                return result
            else:
                print("      ⚠️ JavaScript парсинг не дал данных")
                return None
                
        except Exception as e:
            print(f"      ❌ Ошибка JavaScript парсинга: {e}")
            return None

    async def prepare_data_for_db(self, card_data):
        """Подготавливает данные карточки для сохранения в БД ads_avito"""
        try:
            from datetime import datetime
            
            # Создаем структуру данных для БД
            db_data = {}
            
            # URL
            db_data['URL'] = card_data.get('url', '')
            
            # ID объявления
            db_data['avitoid'] = card_data.get('item_id', '')
            
            # Отладочная информация для avitoid
            if db_data['avitoid']:
                pass
            else:
                print(f"⚠️ avitoid не найден в card_data: {card_data.get('item_id', 'НЕТ')}")
            
            # Заголовок
            db_data['title'] = card_data.get('title', '')
            
            # Цена
            price = card_data.get('price', '')
            if price and price != 'Не найдено':
                # Извлекаем число из цены "19 500 000 ₽"
                price_match = re.search(r'([\d\s]+)', price)
                if price_match:
                    price_str = price_match.group(1).replace(' ', '')
                    try:
                        db_data['price'] = int(price_str)
                    except:
                        db_data['price'] = None
                else:
                    db_data['price'] = None
            else:
                db_data['price'] = None
            
            # Комнаты
            rooms = card_data.get('rooms', '')
            if rooms == 'студия':
                db_data['rooms'] = 0
            elif isinstance(rooms, str) and rooms.isdigit():
                db_data['rooms'] = int(rooms)
            else:
                db_data['rooms'] = None
            
            # Площадь
            area = card_data.get('area', '')
            if area and area != 'не определено':
                try:
                    db_data['area_m2'] = float(area)
                except:
                    db_data['area_m2'] = None
            else:
                db_data['area_m2'] = None
            
            # Этаж
            floor = card_data.get('floor', '')
            if floor and floor != 'не определено':
                try:
                    db_data['floor'] = int(floor)
                except:
                    db_data['floor'] = None
            else:
                db_data['floor'] = None
            
            # Всего этажей
            total_floors = card_data.get('total_floors', '')
            if total_floors and total_floors != 'не определено':
                try:
                    db_data['floor_total'] = int(total_floors)
                except:
                    db_data['floor_total'] = None
            else:
                db_data['floor_total'] = None
            
            # Комплекс - отключено
            db_data['complex'] = ""
            
            # Метро - НЕ сохраняем название в БД, только metro_id
            # Название метро остается в card_data для внутренней логики
            # db_data['metro'] = None  # Убираем название метро из БД
            
            # ID метро из таблицы metro - ищем по названию станции из объявления
            metro_name_from_ad = card_data.get('metro_name', '')
            if metro_name_from_ad and metro_name_from_ad != 'не указано':
                # Пытаемся найти metro_id по названию станции из объявления
                found_metro_id = await self.find_metro_id_by_name(metro_name_from_ad)
                if found_metro_id:
                    db_data['metro_id'] = found_metro_id
                    print(f"   ✅ Найдено metro_id={found_metro_id} для станции '{metro_name_from_ad}'")
                else:
                    # Если не нашли, используем metro_id парсера по умолчанию
                    db_data['metro_id'] = self.metro_id
                    print(f"   ⚠️ Станция '{metro_name_from_ad}' не найдена, используем metro_id={self.metro_id}")
            else:
                # Если название метро не указано, используем metro_id парсера
                db_data['metro_id'] = self.metro_id
                print(f"   📍 Название метро не указано, используем metro_id={self.metro_id}")
            
            # Время до метро
            time_to_metro = card_data.get('time_to_metro', '')
            if time_to_metro and time_to_metro != 'не указано':
                try:
                    db_data['walk_minutes'] = int(time_to_metro)
                except:
                    db_data['walk_minutes'] = None
            else:
                db_data['walk_minutes'] = None
            
            # Адрес - берем только street_house (без метро)
            address = card_data.get('street_house', '')
            if not address or address == "Не найдено":
                # Fallback: если street_house нет, берем полный адрес и отделяем первую строку
                full_address = card_data.get('address', '')
                if full_address and full_address != "Не найдено":
                    # Берем только первую строку (до переноса)
                    address = full_address.split('\n')[0].strip()
                else:
                    address = ''
            db_data['address'] = address
            
            # Теги - парсим из характеристик карточки
            tags = card_data.get('tags', [])
            if tags:
                # Объединяем теги в строку через запятую
                db_data['tags'] = ', '.join(tags)
            else:
                db_data['tags'] = ''
            
            # Информация о продавце - извлекаем из характеристик
            seller_info = self.extract_seller_info_from_params(card_data.get('params', ''))
            if seller_info:
                db_data['seller'] = seller_info
            else:
                # Fallback на старый метод
                seller_type = card_data.get('type', '')
                if seller_type == 'собственник':
                    db_data['seller'] = {'type': 'owner'}
                elif seller_type == 'агентство':
                    db_data['seller'] = {'type': 'agency'}
                else:
                    db_data['seller'] = {'type': 'unknown'}
            
            # Информация о продавце из тегов (если есть)
            if 'seller_info' in card_data and card_data['seller_info']:
                seller_info_from_tags = card_data['seller_info']
                
                # Если в тегах есть информация о продавце, используем её
                if seller_info_from_tags.get('name'):
                    # Определяем тип продавца по тегам
                    tags = card_data.get('tags', [])
                    tags_text = ' '.join(tags).lower()
                    
                    if 'собственник' in tags_text:
                        seller_info_from_tags['type'] = 'собственник'
                    elif 'реквизиты проверены' in tags_text:
                        seller_info_from_tags['type'] = 'агентство'
                    elif 'документы проверены' in tags_text or 'частное лицо' in tags_text:
                        seller_info_from_tags['type'] = 'частное лицо'
                    elif 'застройщик' in tags_text:
                        seller_info_from_tags['type'] = 'застройщик'
                    else:
                        seller_info_from_tags['type'] = 'не определено'
                    
                    # Если тип определен по тегам, переопределяем
                    if seller_info_from_tags['type'] != 'не определено':
                        if seller_info_from_tags['type'] == 'агентство' and 'реквизиты проверены' in tags_text:
                            seller_info_from_tags['type'] = 'агентство'
                        elif seller_info_from_tags['type'] == 'собственник':
                            seller_info_from_tags['type'] = 'собственник'
                        elif seller_info_from_tags['type'] == 'застройщик':
                            seller_info_from_tags['type'] = 'застройщик'
                        elif seller_info_from_tags['type'] == 'частное лицо':
                            seller_info_from_tags['type'] = 'частное лицо'
                    
                    # Обновляем основную информацию о продавце
                    db_data['seller'] = seller_info_from_tags
            
            # Информация о продавце - определяем только person_type
            seller_info = card_data.get('seller_info', {})
            
            # Определяем тип продавца
            if seller_info and seller_info.get('type'):
                db_data['person_type'] = seller_info['type']
            elif 'seller_info' in card_data and card_data['seller_info'].get('type'):
                db_data['person_type'] = card_data['seller_info']['type']
            else:
                db_data['person_type'] = 'не определено'
            
            # Если тип продавца не определен, пробуем определить по тегам
            if db_data['person_type'] == 'не определено':
                tags = card_data.get('tags', [])
                tags_text = ' '.join(tags).lower()
                
                if 'собственник' in tags_text:
                    db_data['person_type'] = 'собственник'
                elif 'реквизиты проверены' in tags_text:
                    db_data['person_type'] = 'агентство'
                elif 'документы проверены' in tags_text or 'частное лицо' in tags_text:
                    db_data['person_type'] = 'частное лицо'
                elif 'застройщик' in tags_text:
                    db_data['person_type'] = 'застройщик'
                else:
                    # Если тип не определен, устанавливаем "частное лицо" по умолчанию
                    db_data['person_type'] = 'частное лицо'
            
            # Время публикации
            published_time = card_data.get('published_time', '')
            if published_time and published_time != 'Не найдено':
                # Конвертируем относительное время в дату
                source_created = self.convert_relative_time_to_date(published_time)
                db_data['source_created'] = source_created
                pass
            else:
                print(f"ℹ️ Время публикации не найдено, устанавливаем текущий datetime")
                db_data['source_created'] = datetime.now()
            
            # Текущее время
            db_data['updated_at'] = datetime.now()
            
            # Тип объекта (1 - квартира, 2 - комната)
            db_data['object_type_id'] = 1  # По умолчанию квартира
            
            return db_data
            
        except Exception as e:
            return None
    
    def parse_tags_from_params(self, params_text):
        """Парсит теги из характеристик карточки"""
        try:
            if not params_text or params_text == "Не найдено":
                return [], {}
            
            # Разбиваем текст на строки
            lines = params_text.strip().split('\n')
            tags = []
            seller_info = {}
            
            # Получаем список тегов из загруженного словаря (из БД)
            known_tags = list(self.get_tags_dictionary())
            
            # Ищем теги - они идут отдельными строками после цены "за м²"
            found_price_line = False
            found_metro_line = False
            tag_lines = []
            
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                
                # Находим строку с ценой "за м²"
                if 'за м²' in line:
                    found_price_line = True
                    continue
                
                # После строки с ценой ищем теги в следующих строках
                if found_price_line and not found_metro_line:
                    # Проверяем, является ли текущая строка адресом (содержит метро)
                    if self.is_metro_line(line):
                        found_metro_line = True
                        break
                    
                    # Если это не адрес, проверяем, является ли строкой тегом
                    is_known_tag = False
                    for known in known_tags:
                        if known.lower() == line.lower():
                            tags.append(known)
                            is_known_tag = True
                            break
                    
                    # Если строка не является известным тегом, но может быть тегом
                    if not is_known_tag:
                        # Проверяем, что это не пустая строка и не слишком длинная
                        if line and len(line) < 50:
                            # Добавляем как потенциальный тег
                            tag_lines.append(line)
                            tags.append(line)
            
            # Извлекаем информацию о продавце из последних строк
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i].strip()
                if not line:
                    continue
                
                # Ищем строку с названием агентства и статусом проверки
                if 'реквизиты проверены' in line.lower():
                    parts = [p.strip() for p in line.split(',')]
                    
                    # Ищем название агентства перед "Реквизиты проверены"
                    for j, part in enumerate(parts):
                        if 'реквизиты проверены' in part.lower():
                            # Берем предыдущую часть как название агентства
                            if j > 0:
                                agency_name = parts[j-1].strip()
                                if agency_name and len(agency_name) > 2:
                                    seller_info['agency_name'] = agency_name
                                    seller_info['type'] = 'agency'
                                    print(f"🏢 Найдено агентство: {agency_name}")
                                    break
                    
                    if seller_info:
                        break
                
                # Ищем строку с именем продавца и количеством объявлений
                # Пример: "Владис, 1930 завершённых объявлений, Реквизиты проверены"
                elif ',' in line and any(word in line.lower() for word in ['завершённых', 'завершенных', 'объявлений']):
                    parts = [p.strip() for p in line.split(',')]
                    
                    if len(parts) >= 3:
                        # Первая часть - имя продавца
                        seller_name = parts[0].strip()
                        if seller_name and len(seller_name) > 2:
                            seller_info['name'] = seller_name
                            print(f"👤 Найдено имя продавца: {seller_name}")
                        
                        # Последняя часть - статус проверки
                        last_part = parts[-1].strip()
                        if 'реквизиты проверены' in last_part.lower():
                            seller_info['type'] = 'agency'  # Агентство
                        elif 'документы проверены' in last_part.lower():
                            seller_info['type'] = 'private'  # Частное лицо с проверенными документами
                        else:
                            seller_info['type'] = 'unknown'
                        
                        break
            
            return tags, seller_info
            
        except Exception as e:
            print(f"❌ Ошибка парсинга тегов: {e}")
            return [], {}
    
    def is_metro_line(self, line):
        """Проверяет, является ли строка строкой с метро (адресом)
        
        УЛУЧШЕННАЯ ЛОГИКА: Не используем жестко заданный список станций,
        а опираемся на структуру данных - метро идет рядом с адресом.
        """
        try:
            line_lower = line.lower()
            
            # Признаки строки с метро/адресом:
            
            # 1. Содержит время до метро
            time_pattern = r'\d+\s*мин'
            has_time = bool(re.search(time_pattern, line_lower))
            
            # 2. Содержит названия улиц/районов
            street_indicators = ['ул.', 'улица', 'проспект', 'пр.', 'переулок', 'пер.', 
                               'площадь', 'пл.', 'бульвар', 'б-р', 'шоссе', 'ш.',
                               'набережная', 'наб.', 'тупик', 'проезд', 'линия']
            has_street = any(indicator in line_lower for indicator in street_indicators)
            
            # 3. Содержит запятую и цифры (улица, дом)
            has_address_format = ',' in line and re.search(r'\d+', line)
            
            # 4. Содержит типичные слова для метро
            metro_indicators = ['станция', 'метро', 'мин.', 'пешком', 'до']
            has_metro_words = any(word in line_lower for word in metro_indicators)
            
            # 5. Короткая строка без служебных слов (потенциальное название станции)
            service_words = ['квартира', 'комната', 'студия', 'апартаменты', 'этаж', 'м²', 
                           'рублей', 'руб', 'собственник', 'агентство', 'проверено',
                           'новостройка', 'вторичка', 'ремонт', 'состояние', 'планировка']
            is_clean_name = (len(line) < 50 and 
                           not any(word in line_lower for word in service_words) and
                           not re.search(r'\d+\s*м²', line_lower) and  # Не площадь
                           not re.search(r'\d+\s*руб', line_lower) and   # Не цена
                           not re.search(r'\d+/\d+\s*эт', line_lower))   # Не этаж
            
            # ЛОГИКА ОПРЕДЕЛЕНИЯ:
            
            # Если есть время + что-то еще - точно метро/адрес
            if has_time and (has_street or has_address_format or has_metro_words):
                return True
            
            # Если есть улица + адресный формат - точно адрес
            if has_street and has_address_format:
                return True
                
            # Если есть явные слова метро + чистое название - вероятно метро
            if has_metro_words and is_clean_name:
                return True
            
            # Если короткая чистая строка после строки с улицей - вероятно станция метро
            if is_clean_name and len(line.strip()) > 3:
                # Дополнительная проверка: не должно быть технических терминов
                tech_terms = ['лифт', 'парковка', 'балкон', 'ремонт', 'окна', 'двор']
                if not any(term in line_lower for term in tech_terms):
                    return True
            
            return False
            
        except Exception as e:
            print(f"⚠️ Ошибка проверки строки с метро: {e}")
            return False
    
    def extract_seller_info_from_params(self, params_text):
        """Извлекает информацию о продавце из характеристик карточки"""
        try:
            if not params_text or params_text == "Не найдено":
                return None
            
            seller_info = {}
            
            # Ищем информацию о продавце в конце текста
            # Обычно это последние строки с именем, количеством объявлений и статусом
            lines = params_text.strip().split('\n')
            
            # Ищем последние строки с информацией о продавце
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i].strip()
                if not line:
                    continue
                
                # Ищем строку с именем продавца и количеством объявлений
                # Пример: "Владис, 1930 завершённых объявлений, Реквизиты проверены"
                if ',' in line and any(word in line.lower() for word in ['завершённых', 'завершенных', 'объявлений']):
                    parts = [p.strip() for p in line.split(',')]
                    
                    if len(parts) >= 3:
                        # Первая часть - имя продавца
                        seller_name = parts[0].strip()
                        if seller_name and len(seller_name) > 2:
                            seller_info['name'] = seller_name
                        
                        # Последняя часть - статус проверки
                        last_part = parts[-1].strip()
                        if 'реквизиты проверены' in last_part.lower():
                            seller_info['type'] = 'agency'  # Агентство
                        elif 'документы проверены' in last_part.lower():
                            seller_info['type'] = 'private'  # Частное лицо с проверенными документами
                        else:
                            seller_info['type'] = 'unknown'
                        
                        break
            
            # Если не нашли в последних строках, ищем по ключевым словам
            if not seller_info:
                text_lower = params_text.lower()
                
                # СОБСТВЕННИК ТОЛЬКО ЕСЛИ В ТЕГАХ "Собственник"
                if 'агентство' in text_lower or 'агент' in text_lower:
                    seller_info['type'] = 'agency'
                elif 'застройщик' in text_lower:
                    seller_info['type'] = 'developer'
                else:
                    seller_info['type'] = 'unknown'
            
            return seller_info if seller_info else None
            
        except Exception as e:
            print(f"❌ Ошибка извлечения информации о продавце: {e}")
            return None
    
    def parse_title(self, title_text):
        """Парсит заголовок на компоненты"""
        try:
            title_data = {}
            
            # Паттерн для количества комнат
            rooms_pattern = r'(\d+)-к\.|(\d+)\s*комнат|студия|Студия'
            rooms_match = re.search(rooms_pattern, title_text)
            if rooms_match:
                if 'студия' in rooms_match.group(0).lower():
                    title_data['rooms'] = 'студия'
                else:
                    title_data['rooms'] = rooms_match.group(1) if rooms_match.group(1) else rooms_match.group(2)
            else:
                title_data['rooms'] = 'не определено'
            
            # Паттерн для площади
            area_pattern = r'(\d+(?:[,\d]+)?)\s*м²'
            area_match = re.search(area_pattern, title_text)
            if area_match:
                title_data['area'] = area_match.group(1).replace(',', '.')
            else:
                title_data['area'] = 'не определено'
            
            # Паттерн для этажа и этажности
            floor_pattern = r'(\d+)/(\d+)\s*эт'
            floor_match = re.search(floor_pattern, title_text)
            if floor_match:
                title_data['floor'] = floor_match.group(1)
                title_data['total_floors'] = floor_match.group(2)
            else:
                title_data['floor'] = 'не определено'
                title_data['total_floors'] = 'не определено'
            
            return title_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга заголовка: {e}")
            return {
                'rooms': 'ошибка парсинга',
                'area': 'ошибка парсинга',
                'floor': 'ошибка парсинга',
                'total_floors': 'ошибка парсинга'
            }
    
    def parse_address(self, address_text):
        """Парсит адрес на компоненты: улица, дом, метро, время до метро"""
        try:
            address_data = {}
            
            # Разделяем адрес по строкам
            lines = address_text.strip().split('\n')
            
            if len(lines) >= 2:
                # Первая строка - улица и дом
                street_line = lines[0].strip()
                
                # Парсим улицу и дом из первой строки
                # Пример: "Поварская ул., 8/1к1" -> улица: "Поварская ул.", дом: "8/1к1"
                street_parts = street_line.split(',')
                if len(street_parts) >= 2:
                    street = street_parts[0].strip()
                    house = street_parts[1].strip()
                    address_data['street_house'] = f"{street}, {house}"
                else:
                    address_data['street_house'] = street_line
                
                # Вторая строка - метро и время
                metro_line = lines[1].strip()
                
                # Парсим метро и время до метро
                # Пример: "Арбатская, до 5 мин." -> метро: "Арбатская", время: 5
                metro_parts = metro_line.split(',')
                metro_name = None
                time_to_metro = None
                
                for part in metro_parts:
                    part = part.strip()
                    if part:
                        # Ищем время до метро (расширенные паттерны)
                        time_patterns = [
                            r'(\d+)\s*мин',  # "5 мин"
                            r'до\s*(\d+)\s*мин',  # "до 5 мин"
                            r'(\d+)\s*минут',  # "5 минут"
                            r'(\d+)\s*мин\.',  # "5 мин."
                            r'(\d+)(?=\s|$)'  # просто цифра в конце строки или перед пробелом
                        ]
                        
                        time_found = False
                        for pattern in time_patterns:
                            time_match = re.search(pattern, part)
                            if time_match:
                                time_to_metro = int(time_match.group(1))
                                time_found = True
                                break
                        
                        if not time_found:
                            # Если это не время, то это потенциальное название метро
                            # Проверяем, что в строке нет только цифр
                            if not metro_name and not part.isdigit():
                                # Дополнительная очистка названия станции
                                clean_name = re.sub(r'\b(до|пешком|мин\.?|минут)\b', '', part).strip()
                                if clean_name and len(clean_name) > 1:
                                    metro_name = clean_name
                
                # Сохраняем метро и время
                address_data['metro_name'] = metro_name if metro_name else 'не указано'
                address_data['time_to_metro'] = str(time_to_metro) if time_to_metro else 'не указано'
                
            else:
                # Если только одна строка, считаем её адресом
                address_data['street_house'] = address_text.strip()
                address_data['metro_name'] = 'не указано'
                address_data['time_to_metro'] = 'не указано'
            
            return address_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга адреса: {e}")
            return {
                'street_house': 'ошибка парсинга',
                'metro_name': 'ошибка парсинга',
                'time_to_metro': 'ошибка парсинга'
            }
    
    def parse_seller_info(self, card_element, card_data=None):
        """Парсит информацию о продавце и время создания объявления"""
        try:
            seller_data = {}
            
            # ПРИОРИТЕТ: если в тегах уже есть "Собственник", сохраняем его
            owner_from_tags = False
            if card_data and 'tags' in card_data and card_data['tags']:
                tags_text = ' '.join(card_data['tags']).lower()
                if 'собственник' in tags_text:
                    owner_from_tags = True
                    seller_data['type'] = 'собственник'
            
            # Ищем информацию о продавце в характеристиках карточки
            try:
                # Пробуем найти по data-marker для характеристик
                params_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-specific-params"]')
                if params_elem:
                    params_text = params_elem.text.strip()
                    
                    # Ищем тип продавца в тексте характеристик (НЕ перезаписываем "собственник" из тегов)
                    if not owner_from_tags:
                        seller_data['type'] = self.determine_seller_type(params_text)
                    seller_data['full_text'] = params_text
                    
                    # Ищем время создания объявления в характеристиках
                    creation_time = self.extract_creation_time(params_text)
                    if creation_time:
                        seller_data['creation_time'] = creation_time
                    
                else:
                    if not owner_from_tags:
                        seller_data['type'] = 'не найдено'
                    seller_data['full_text'] = 'не найдено'
                    
            except:
                # Если не нашли по data-marker, пробуем найти по тексту
                try:
                    # Ищем любой текст, содержащий информацию о продавце (НЕ перезаписываем "собственник" из тегов)
                    if not owner_from_tags:
                        all_text = card_element.text.lower()
                        seller_data['type'] = self.determine_seller_type_from_text(all_text)
                    seller_data['full_text'] = 'найдено в общем тексте'
                except:
                    if not owner_from_tags:
                        seller_data['type'] = 'не найдено'
                    seller_data['full_text'] = 'не найдено'
            
            # Дополнительно ищем время создания объявления в других местах
            if 'creation_time' not in seller_data:
                creation_time = self.find_creation_time_in_card(card_element)
                if creation_time:
                    seller_data['creation_time'] = creation_time
            
            # Ищем информацию о продавце после времени публикации
            person_info = self.find_person_info_after_time(card_element, card_data)
            if person_info:
                # ЗАЩИЩАЕМ тип "собственник" от перезаписи
                if owner_from_tags and person_info.get('type') != 'собственник':
                    person_info['type'] = 'собственник'
                
                seller_data.update(person_info)
            
            return seller_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга продавца: {e}")
            return {
                'type': 'ошибка парсинга',
                'full_text': 'ошибка парсинга'
            }
    
    def find_person_info_after_time(self, card_element, card_data=None):
        """Ищет информацию о продавце после времени публикации и в конце карточки"""
        try:
            person_info = {}
            
            # Получаем весь текст карточки
            all_text = card_element.text
            lines = all_text.split('\n')
            
            # Ищем строку с временем публикации
            time_line_index = -1
            for i, line in enumerate(lines):
                line_lower = line.lower().strip()
                # Ищем время публикации
                if any(word in line_lower for word in ['назад', 'сегодня', 'вчера', 'позавчера', 'января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']):
                    time_line_index = i
                    break
            
            # Ищем информацию о продавце в конце карточки (последние 10 строк)
            end_lines = lines[-10:] if len(lines) > 10 else lines
            
            # Собираем информацию о продавце из конца карточки
            seller_lines = []
            seller_name = None
            
            for i, line in enumerate(end_lines):
                line = line.strip()
                if not line:
                    continue
                
                line_lower = line.lower()
                
                # Пропускаем технические строки
                if any(tech_word in line_lower for tech_word in ['написать', 'показать телефон', 'избранное', 'поделиться']):
                    continue
                
                # Ищем количество объявлений (это маркер конца информации о продавце)
                if any(word in line_lower for word in ['завершённых', 'завершенных', 'объявлений']):
                    # Если нашли количество объявлений, то имя продавца должно быть в предыдущей строке
                    if i > 0:
                        prev_line = end_lines[i-1].strip()
                        if prev_line and len(prev_line) > 2:
                            # Проверяем, что это не служебная информация
                            prev_line_lower = prev_line.lower()
                            if not any(word in prev_line_lower for word in ['проверено', 'росреестр', 'собственник', 'агентство', 'документы', 'лифт', 'парковка', 'балкон', 'двор', 'окна', 'написать', 'показать', 'телефон', 'завершённых', 'завершенных', 'объявлений']):
                                # Берем ПОЛНУЮ строку как имя продавца (может быть несколько слов)
                                seller_lines.append(line)
                                if not seller_name:  # Если еще не нашли имя продавца
                                    seller_name = line
                                    # print(f"👤 Найдено ПОЛНОЕ имя продавца в строке с запятой: {seller_name}")
                    
                    seller_lines.append(line)
                    continue
                
                # Ищем информацию о продавце
                if any(word in line_lower for word in ['реквизиты проверены', 'документы проверены']):
                    seller_lines.append(line)
                
                # Ищем имя/название (обычно это первая строка с информацией о продавце)
                if (len(line) > 2 and 
                    not any(word in line_lower for word in ['проверено', 'росреестр', 'собственник', 'агентство', 'документы', 'лифт', 'парковка', 'балкон', 'двор', 'окна', 'написать', 'показать', 'телефон']) and
                    not re.search(r'\d+', line) and
                    ',' in line):
                    # Берем ПОЛНУЮ строку как имя продавца (может быть несколько слов)
                    seller_lines.append(line)
                    if not seller_name:  # Если еще не нашли имя продавца
                        seller_name = line
                        # print(f"👤 Найдено ПОЛНОЕ имя продавца в строке с запятой: {seller_name}")
            
            # Сохраняем найденную информацию (только для определения типа продавца)
            if seller_name:
                person_info['name'] = seller_name
            
            # Сохраняем информацию для определения типа продавца
            if seller_lines:
                person_info['all_text_after_time'] = seller_lines
            
            # Если тип продавца не определен, устанавливаем "частное лицо" по умолчанию
            if not person_info.get('type') or person_info.get('type') == 'не определено':
                person_info['type'] = 'частное лицо'
            
            return person_info
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # Проверяем на ошибку соединения
            if 'connection refused' in error_msg or 'errno 111' in error_msg or 'max retries exceeded' in error_msg:
                print(f"⚠️ WebDriver потерял соединение при поиске информации о продавце: {e}")
                # Возвращаем пустой результат, чтобы вызывающий код мог обработать восстановление
                return {}
            else:
                print(f"⚠️ Ошибка поиска информации о продавце: {e}")
                return {}
    
    def determine_seller_type(self, params_text):
        """Определяет тип продавца из текста характеристик"""
        try:
            if not params_text:
                return 'частное лицо'  # По умолчанию "частное лицо"
            
            text_lower = params_text.lower()
            
            # ТИП ПРОДАВЦА ОПРЕДЕЛЯЕТСЯ ТОЛЬКО ПО ТЕГАМ, НЕ ПО ТЕКСТУ ХАРАКТЕРИСТИК
            # Убираем проверку слова "собственник" из текста
            if 'агентств' in text_lower or 'агент' in text_lower:
                return 'агентство'
            elif 'застройщик' in text_lower:
                return 'застройщик'
            elif 'документы проверены' in text_lower or 'частное лицо' in text_lower:
                return 'частное лицо'
            elif 'реквизиты проверены' in text_lower:
                return 'агентство'  # Обычно агентства
            else:
                return 'частное лицо'  # По умолчанию "частное лицо"
                
        except Exception as e:
            print(f"⚠️ Ошибка определения типа продавца: {e}")
            return 'частное лицо'  # По умолчанию "частное лицо"
    
    def determine_seller_type_from_text(self, all_text):
        """Определяет тип продавца из общего текста карточки"""
        try:
            text_lower = all_text.lower()
            
            # ТИП ПРОДАВЦА ОПРЕДЕЛЯЕТСЯ ТОЛЬКО ПО ТЕГАМ, НЕ ПО ТЕКСТУ КАРТОЧКИ
            # Убираем проверку слова "собственник" из текста
            if any(word in text_lower for word in ['агентство', 'агент', 'риэлтор', 'агентств']):
                return 'агентство'
            elif any(word in text_lower for word in ['застройщик', 'строительная компания', 'застройщик']):
                return 'застройщик'
            elif any(word in text_lower for word in ['реквизиты проверены']):
                return 'агентство'
            elif any(word in text_lower for word in ['документы проверены', 'частное лицо']):
                return 'частное лицо'
            elif any(word in text_lower for word in ['надёжный партнёр', 'суперагент']):
                return 'агентство'
            else:
                return 'частное лицо'  # По умолчанию "частное лицо"
        except Exception as e:
            print(f"⚠️ Ошибка определения типа продавца из текста: {e}")
            return 'частное лицо'  # По умолчанию "частное лицо"
    
    def extract_creation_time(self, params_text):
        """Извлекает время создания объявления из характеристик"""
        try:
            if not params_text:
                return None
            
            # Ищем время в конце текста (обычно перед названием продавца)
            lines = params_text.strip().split('\n')
            
            # Идем с конца, ищем строку с временем
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i].strip()
                if not line:
                    continue
                
                # Ищем время создания объявления
                # Паттерны: "2 часа назад", "вчера", "сегодня", "3 дня назад" и т.д.
                time_patterns = [
                    r'(\d+)\s*(час|часа|часов)\s*назад',
                    r'(\d+)\s*(день|дня|дней)\s*назад',
                    r'(\d+)\s*(недел|неделя|недели|недель|неделю)\s*назад',
                    r'(\d+)\s*(месяц|месяца|месяцев|месяц)\s*назад',
                    r'вчера',
                    r'сегодня',
                    r'позавчера'
                ]
                
                for pattern in time_patterns:
                    match = re.search(pattern, line.lower())
                    if match:
                        if 'вчера' in line.lower():
                            return 'вчера'
                        elif 'сегодня' in line.lower():
                            return 'сегодня'
                        elif 'позавчера' in line.lower():
                            return 'позавчера'
                        else:
                            # Извлекаем количество и единицу времени
                            count = match.group(1)
                            unit = match.group(2)
                            return f"{count} {unit} назад"
            
            return None
            
        except Exception as e:
            print(f"⚠️ Ошибка извлечения времени создания: {e}")
            return None
    
    def find_creation_time_in_card(self, card_element):
        """Ищет время создания объявления в других местах карточки"""
        try:
            # Ищем в элементе с датой публикации
            try:
                time_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-date"]')
                time_text = time_elem.text.strip()
                if time_text:
                    return time_text
            except:
                pass
            
            # Ищем в описании карточки
            try:
                desc_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-description"]')
                desc_text = desc_elem.text.strip()
                if desc_text:
                    # Ищем время в описании
                    time_patterns = [
                        r'(\d+)\s*(час|часа|часов)\s*назад',
                        r'(\d+)\s*(день|дня|дней)\s*назад',
                        r'(\d+)\s*(недел|неделя|недели|недель|неделю)\s*назад',
                        r'(\d+)\s*(месяц|месяца|месяцев|месяц)\s*назад',
                        r'вчера',
                        r'сегодня',
                        r'позавчера'
                    ]
                    
                    for pattern in time_patterns:
                        match = re.search(pattern, desc_text.lower())
                        if match:
                            if 'вчера' in desc_text.lower():
                                return 'вчера'
                            elif 'сегодня' in desc_text.lower():
                                return 'сегодня'
                            elif 'позавчера' in desc_text.lower():
                                return 'позавчера'
                            else:
                                count = match.group(1)
                                unit = match.group(2)
                                return f"{count} {unit} назад"
            except:
                pass
            
            return None
            
        except Exception as e:
            print(f"⚠️ Ошибка поиска времени создания в карточке: {e}")
            return None
    
    def parse_card(self, card_element):
        """Парсит одну карточку"""
        try:
            # Убираем проверку на stale element - пусть retry логика работает
            # try:
            #     # Проверяем, что элемент все еще привязан к DOM
            #     card_element.is_enabled()
            # except Exception as stale_error:
            #     print(f"❌ Элемент карточки стал недействительным (stale): {stale_error}")
            #     return None
            
            card_data = {}
            
            # ID объявления
            try:
                item_id = card_element.get_attribute('data-item-id')
                if item_id:
                    card_data['item_id'] = item_id
                else:
                    print(f"⚠️ item_id не найден в атрибуте data-item-id")
            except Exception as e:
                print(f"❌ Ошибка парсинга item_id: {e}")
                pass
            
            # Заголовок
            try:
                title_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-title"]')
                title_text = title_elem.text.strip()
                card_data['title'] = title_text
                
                # Парсим заголовок на компоненты
                title_components = self.parse_title(title_text)
                card_data.update(title_components)
                
            except:
                card_data['title'] = "Не найдено"
                card_data.update({
                    'rooms': 'не найдено',
                    'area': 'не найдено',
                    'floor': 'не найдено',
                    'total_floors': 'не найдено'
                })
            
            # Цена
            try:
                price_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-price"]')
                card_data['price'] = price_elem.text.strip()
            except:
                card_data['price'] = "Не найдено"
            
            # Адрес/метро
            try:
                address_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-address"]')
                address_text = address_elem.text.strip()
                card_data['address'] = address_text
                
                # Парсим адрес на компоненты
                address_components = self.parse_address(address_text)
                card_data.update(address_components)
                
            except:
                # Если не нашли по data-marker, ищем в характеристиках
                try:
                    # Ищем адрес в характеристиках (последние строки с метро)
                    params_text = card_data.get('params', '')
                    if params_text and params_text != "Не найдено":
                        lines = params_text.strip().split('\n')
                        
                        # Ищем строки с адресом (содержат метро)
                        address_lines = []
                        for line in lines:
                            line = line.strip()
                            if line and self.is_metro_line(line):
                                address_lines.append(line)
                        
                        if len(address_lines) >= 2:
                            # Первая строка - улица и дом, вторая - метро и время
                            address_text = '\n'.join(address_lines[:2])
                            address_components = self.parse_address(address_text)
                            card_data.update(address_components)
                            card_data['address'] = address_text
                        elif len(address_lines) == 1:
                            # Только одна строка с адресом
                            address_text = address_lines[0]
                            address_components = self.parse_address(address_text)
                            card_data.update(address_components)
                            card_data['address'] = address_text
                        else:
                            # Адрес не найден
                            card_data.update({
                                'street_house': 'не найдено',
                                'metro_name': 'не указано',
                                'time_to_metro': 'не указано'
                            })
                            card_data['address'] = "Не найдено"
                    else:
                        card_data.update({
                            'street_house': 'не найдено',
                            'metro_name': 'не указано',
                            'time_to_metro': 'не указано'
                        })
                        card_data['address'] = "Не найдено"
                        
                except Exception as e:
                    print(f"⚠️ Ошибка поиска адреса в характеристиках: {e}")
                    card_data.update({
                        'street_house': 'не найдено',
                        'metro_name': 'не указано',
                        'time_to_metro': 'не указано'
                    })
                    card_data['address'] = "Не найдено"
            
            # Характеристики
            try:
                # Пробуем разные селекторы для характеристик
                params_elem = None
                params_text = ""
                
                # Селектор 1: стандартный
                try:
                    params_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-specific-params"]')
                    params_text = params_elem.text.strip()
                except:
                    pass
                
                # Селектор 2: альтернативный
                if not params_elem:
                    try:
                        params_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-params"]')
                        params_text = params_elem.text.strip()
                    except:
                        pass
                
                # Селектор 3: по классу
                if not params_elem:
                    try:
                        params_elem = card_element.find_element(By.CSS_SELECTOR, '.item-specific-params')
                        params_text = params_elem.text.strip()
                    except:
                        pass
                
                # Селектор 4: по тексту, содержащему характеристики
                if not params_elem:
                    try:
                        # Ищем любой элемент с текстом, содержащим характерные слова
                        all_elements = card_element.find_elements(By.CSS_SELECTOR, '*')
                        found_elements = []
                        
                        for elem in all_elements:
                            try:
                                text = elem.text.strip()
                                if text and any(word in text.lower() for word in ['проверено', 'росреестр', 'собственник', 'агентство', 'документы']):
                                    found_elements.append((elem.tag_name, text[:100]))
                                    if not params_elem:  # Берем первый найденный
                                        params_elem = elem
                                        params_text = text
                            except:
                                continue
                    except:
                        pass
                
                # Селектор 5: ищем в описании карточки
                if not params_elem:
                    try:
                        desc_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-description"]')
                        if desc_elem:
                            desc_text = desc_elem.text.strip()
                            # Проверяем, содержит ли описание теги
                            if any(word in desc_text.lower() for word in ['проверено', 'росреестр', 'собственник', 'агентство', 'документы', 'лифт', 'парковка', 'балкон']):
                                params_elem = desc_elem
                                params_text = desc_text
                    except:
                        pass
                
                # Селектор 6: ищем теги между "за м²" и адресом
                if not params_elem:
                    try:
                        all_text = card_element.text
                        lines = all_text.split('\n')
                        tag_lines = []
                        
                        # Ищем позицию "за м²" и адреса
                        price_per_sqm_index = -1
                        address_index = -1
                        
                        for i, line in enumerate(lines):
                            line_lower = line.lower().strip()
                            
                            # Ищем строку с "за м²"
                            if 'за м²' in line_lower and price_per_sqm_index == -1:
                                price_per_sqm_index = i
                                break
                            
                            # Ищем адрес
                            if self.is_address_line(line) and address_index == -1:
                                address_index = i
                                break
                        
                        # Если нашли оба индекса, извлекаем теги между ними
                        if price_per_sqm_index != -1 and address_index != -1 and address_index > price_per_sqm_index:
                            for i in range(price_per_sqm_index + 1, address_index):
                                line = lines[i].strip()
                                if line and len(line) > 2:
                                    # Проверяем, что это не пустая строка и не слишком короткая
                                    tag_lines.append(line)
                        
                        if tag_lines:
                            params_text = '\n'.join(tag_lines)
                            # Устанавливаем params_elem как сам card_element, чтобы теги сохранились
                            params_elem = card_element
                    except:
                        pass
                
                # Селектор 7: ищем теги в описании по Schema.org
                if not params_elem:
                    try:
                        desc_elem = card_element.find_element(By.CSS_SELECTOR, '[itemprop="description"]')
                        if desc_elem:
                            desc_text = desc_elem.text.strip()
                            if desc_text:
                                # Проверяем, содержит ли описание теги
                                if any(word in desc_text.lower() for word in ['проверено', 'росреестр', 'собственник', 'агентство', 'документы', 'лифт', 'парковка', 'балкон']):
                                    params_elem = desc_elem
                                    params_text = desc_text
                    except:
                        pass
                
                # Селектор 8: выводим сырое содержимое всех элементов с data-marker
                try:
                    data_markers = card_element.find_elements(By.CSS_SELECTOR, '[data-marker]')
                    if data_markers:
                        pass  # Элементы с data-marker найдены
                except:
                    pass  # Ошибка поиска элементов с data-marker
                
                # Селектор 9: выводим сырое содержимое всех div элементов с классами
                try:
                    divs = card_element.find_elements(By.CSS_SELECTOR, 'div[class]')
                    if divs:
                        pass  # Div элементы с классами найдены
                except:
                    pass  # Ошибка поиска div элементов
                
                # Селектор 10: выводим сырое содержимое всех span элементов
                try:
                    spans = card_element.find_elements(By.CSS_SELECTOR, 'span')
                    if spans:
                        pass  # Span элементы найдены
                except:
                    pass  # Ошибка поиска span элементов
                
                if params_elem and params_text:
                    card_data['params'] = params_text
                    
                    # Парсим теги из характеристик
                    tags, seller_info = self.parse_tags_from_params(params_text)
                    card_data['tags'] = tags
                    
                    # Сохраняем информацию о продавце из тегов
                    if seller_info:
                        card_data['seller_info'] = seller_info
                        if seller_info.get('agency_name'):
                            pass  # Агентство найдено
                    
                    if tags:
                        pass  # Теги найдены
                else:
                    card_data['params'] = "Не найдено"
                    card_data['tags'] = []
                    card_data['seller_info'] = {}
                
                # Итоговая диагностика селекторов
                if params_elem:
                    pass  # Селектор работает
                else:
                    pass  # Селектор не работает
                
            except Exception as e:
                print(f"⚠️ Ошибка парсинга характеристик: {e}")
                card_data['params'] = "Не найдено"
                card_data['tags'] = []
            
            # Название комплекса - отключено
            card_data['complex_name'] = ""
            
            # Описание
            try:
                description_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-description"]')
                card_data['description'] = description_elem.text.strip()
            except:
                card_data['description'] = "Не найдено"
            
            # Ссылка на карточку
            try:
                link_elem = card_element.find_element(By.CSS_SELECTOR, 'a[data-marker="item-title"]')
                raw_url = link_elem.get_attribute('href')
                # Очищаем URL от context и других параметров для сохранения в БД
                card_data['url'] = self.clean_url_path(raw_url)
            except:
                card_data['url'] = "Не найдено"
            
            # Время публикации
            try:
                time_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-date"]')
                card_data['published_time'] = time_elem.text.strip()
            except:
                card_data['published_time'] = "Не найдено"
            
            # Информация о продавце
            seller_info = self.safe_parse_seller_info(card_element, card_data)
            
            # Объединяем информацию о продавце, избегая дублирования
            if seller_info:
                # Если у нас уже есть информация о продавце из тегов, объединяем её
                if 'seller_info' in card_data:
                    existing_seller = card_data['seller_info']
                    # Объединяем, приоритет у новой информации
                    merged_seller = {**existing_seller, **seller_info}
                    card_data['seller_info'] = merged_seller
                    
                    # Обновляем основные поля продавца
                    if 'type' in seller_info:
                        card_data['seller_type'] = seller_info['type']
                    if 'creation_time' in seller_info:
                        card_data['creation_time'] = seller_info['creation_time']
                else:
                    # Если информации о продавце не было, добавляем новую
                    card_data['seller_info'] = seller_info
                    if 'type' in seller_info:
                        card_data['seller_type'] = seller_info['type']
                    if 'creation_time' in seller_info:
                        card_data['creation_time'] = seller_info['creation_time']
            
            # Проверяем, есть ли время создания объявления
            if 'creation_time' not in card_data:
                # Если не нашли в seller_info, ищем в других местах
                creation_time = self.find_creation_time_in_card(card_element)
                if creation_time:
                    card_data['creation_time'] = creation_time
            
            # Убираем формирование поля person - оставляем только person_type
            
            return card_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга карточки: {e}")
            return None
    
    async def save_to_db(self, parsed_cards):
        """Сохраняет карточки в БД"""
        if not DB_AVAILABLE or not self.enable_db_save:
            print(f"❌ Сохранение в БД отключено")
            return False
            
        try:
            # Создаем таблицу если её нет
            await create_ads_avito_table()
            
            saved_count = 0
            for i, card in enumerate(parsed_cards):
                try:
                    # Подготавливаем данные для БД
                    db_data = await self.prepare_data_for_db(card)
                    if db_data:
                        # Сохраняем в БД
                        await save_avito_ad(db_data)
                        saved_count += 1
                    else:
                        print(f"⚠️ Карточка {i+1}: prepare_data_for_db вернул None")
                except Exception as e:
                    print(f"❌ Ошибка сохранения карточки {i+1}: {e}")
                    pass
            
            pass
            return saved_count > 0
            
        except Exception as e:
            print(f"❌ Общая ошибка в save_to_db: {e}")
            return False
    
    def parse_metro_page(self):
        """Парсит страницу с метро"""
        try:
            print(f"🎯 Парсим страницу с метро ID = {self.metro_id} (avito_id = {self.metro_avito_id})")
            
            # Получаем URL
            metro_url = self.get_metro_url()
            if not metro_url:
                return []
                
            print(f"🌐 URL: {metro_url}")
            
            # Переходим на страницу
            self.driver.get(metro_url)
            time.sleep(self.page_load_delay)
            
            # Проверяем текущий URL
            current_url = self.driver.current_url
            print(f"📍 Текущий URL: {current_url}")
            
            # Получаем все карточки (без ожидания загрузки)
            cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
            print(f"📊 Найдено карточек: {len(cards)}")
            
            # Парсим первые несколько карточек
            parsed_cards = []
            for i, card in enumerate(cards[:self.max_cards]):
                card_data = self.parse_card(card)
                if card_data:
                    card_data['card_number'] = i + 1
                    parsed_cards.append(card_data)
                else:
                    print(f"❌ Ошибка парсинга карточки {i+1}")
            
            return parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга страницы: {e}")
            return []
    
    def parse_metro_page_by_number(self, page=1):
        """Парсит конкретную страницу с метро по номеру
        
        ОПТИМИЗАЦИЯ: Ожидание DOM стабилизации убрано для ускорения.
        Пустые страницы определяются по отсутствию элементов [data-marker="item"].
        НОВАЯ ЛОГИКА: Добавлена поддержка сообщения "Ничего не найдено на выбранных станциях метро"
        для корректного определения пустых страниц.
        """
        try:
            # Получаем URL для конкретной страницы
            metro_url = self.get_metro_url_with_page(page)
            if not metro_url:
                return []
            
            # Сохраняем текущий URL для возможного восстановления
            self.current_page_url = metro_url
            
            # Профилактический рестарт браузера каждые 10 страниц
            if page % 10 == 0:
                print(f"🔄 Профилактический рестарт браузера на странице {page}...")
                if not self.restore_driver_connection():
                    print("❌ Не удалось выполнить профилактический рестарт браузера")
                    return []
                print("✅ Профилактический рестарт браузера выполнен успешно")
                # После профилактического рестарта браузер уже находится на нужной странице
                # Пропускаем повторный переход
                skip_page_navigation = True
            else:
                skip_page_navigation = False
            
            # Переходим на страницу (только если не был выполнен профилактический рестарт)
            if not skip_page_navigation:
                try:
                    self.driver.get(metro_url)
                except Exception as e:
                    error_msg = str(e).lower()
                    if ('connection refused' in error_msg or 'errno 111' in error_msg or 
                        'max retries exceeded' in error_msg or 'read timeout' in error_msg or
                        'timeout' in error_msg):
                        
                        print(f"🔄 Ошибка соединения при переходе на страницу {page}: {e}")
                        print("🔄 Выполняем рестарт браузера для продолжения парсинга...")
                        
                        if self.restore_driver_and_continue(page, metro_url):
                            print(f"✅ Браузер восстановлен, продолжаем парсинг страницы {page}")
                            # Продолжаем с той же страницы, но не вызываем рекурсивно
                            # Просто продолжаем выполнение текущего метода
                        else:
                            print(f"❌ Не удалось восстановить браузер для страницы {page}")
                            return []
                    else:
                        raise e  # Пробрасываем другие ошибки
            
            # Выводим сообщение о обработке страницы
            print(f"страница {page} ({metro_url}) обработана")
            
            # Проверяем, не изменился ли URL после загрузки
            try:
                current_url = self.driver.current_url
                if current_url != metro_url:
                    print(f"[URL_CHECK] ⚠️ URL изменился после загрузки!")
                    print(f"[URL_CHECK] Ожидаемый: {metro_url}")
                    print(f"[URL_CHECK] Текущий: {current_url}")
                    
                    # Проверяем на дублирование в текущем URL
                    if 'context=' in current_url:
                        context_start = current_url.find('context=') + 8
                        context_end = current_url.find('&', context_start)
                        if context_end == -1:
                            context_end = len(current_url)
                        
                        current_context = current_url[context_start:context_end]
                        if current_context.count('H4sIAAAAAAAA_') > 1:
                            print(f"[URL_CHECK] ❌ Обнаружен дублированный gzip header в текущем URL!")
                            print(f"[URL_CHECK] Это может быть причиной проблем с парсингом")
            except Exception as e:
                print(f"[URL_CHECK] Ошибка проверки URL: {e}")
            
            # Увеличиваем время ожидания для загрузки страницы и DOM
            print(f"⏳ Ожидаем загрузку страницы {page}...")
            time.sleep(5)  # Увеличиваем с 2 до 5 секунд
            
            # Дополнительная проверка готовности страницы
            # Простая и надежная логика загрузки карточек (как в старой версии)
            try:
                # Ждем стабилизации DOM после загрузки страницы
                if not self.wait_for_dom_stability():
                    print(f"❌ Не удалось дождаться стабилизации DOM на странице {page}")
                    return []
                
                # Ждем загрузки карточек
                if not self.wait_for_cards_load():
                    print(f"❌ Не удалось дождаться загрузки карточек на странице {page}")
                    return []
                
                # Получаем все загруженные карточки
                cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                
                # Если карточек нет, возвращаем пустой список
                if not cards:
                    print(f"ℹ️ Страница {page} не содержит карточек")
                    return []
                
                print(f"📊 Найдено карточек на странице {page}: {len(cards)}")
                    
            except Exception as e:
                error_msg = str(e).lower()
                if ('connection refused' in error_msg or 'errno 111' in error_msg or 
                    'max retries exceeded' in error_msg or 'read timeout' in error_msg or
                    'timeout' in error_msg):
                    
                    print(f"🔄 Ошибка соединения при поиске элементов на странице {page}: {e}")
                    print("🔄 Выполняем рестарт браузера для продолжения парсинга...")
                    
                    if self.restore_driver_and_continue(page, metro_url):
                        print(f"✅ Браузер восстановлен, продолжаем поиск элементов на странице {page}")
                        # Повторяем поиск элементов после восстановления
                        try:
                            cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                        except Exception as retry_error:
                            print(f"❌ Ошибка поиска элементов после восстановления: {retry_error}")
                            return []
                    else:
                        print(f"❌ Не удалось восстановить браузер для страницы {page}")
                        return []
                else:
                    raise e  # Пробрасываем другие ошибки
            
            # Логируем результат поиска
            if cards:
                print(f"📊 Найдено карточек на странице {page}: {len(cards)}")
            else:
                print(f"⚠️ Карточки не найдены на странице {page}, но продолжаем парсинг")
                        

            
            # ИСПОЛЬЗУЕМ ГИБРИДНЫЙ ПОДХОД с настраиваемыми параметрами
            if self.max_cards > 0:
                target_cards = min(self.max_cards, 50)  # Ограничиваем максимум 50
            else:
                target_cards = 50  # Если max_cards = 0, парсим все 50 карточек
            
            # Гибридный парсинг с настраиваемыми параметрами
            parsed_cards = self.parse_hybrid_approach(cards, target_cards)
            
            # Добавляем номер страницы к каждой карточке
            for card_data in parsed_cards:
                card_data['page_number'] = page
            
            return parsed_cards
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # Проверяем на ошибки соединения
            if ('connection refused' in error_msg or 'errno 111' in error_msg or 
                'max retries exceeded' in error_msg or 'read timeout' in error_msg or
                'timeout' in error_msg):
                
                print(f"🔄 Ошибка соединения на странице {page}: {e}")
                print("🔄 Выполняем рестарт браузера для продолжения парсинга...")
                
                if self.restore_driver_and_continue(page, metro_url):
                    print(f"✅ Браузер восстановлен, продолжаем парсинг страницы {page}")
                    # Возвращаем пустой результат, чтобы вызывающий код мог обработать восстановление
                    # и повторить попытку для той же страницы
                    return []
                else:
                    print(f"❌ Не удалось восстановить браузер для страницы {page}")
                    return []
            else:
                print(f"❌ Ошибка парсинга страницы {page}: {e}")
                return []
    
    async def parse_multiple_pages(self, start_page=1):
        """Парсит несколько страниц с метро
        
        Данные сохраняются в БД после каждой страницы для обеспечения
        надежности и возможности возобновления парсинга с любой страницы.
        
        ОПТИМИЗАЦИЯ: Перезагрузка браузера каждые 4 страницы убрана
        для ускорения процесса парсинга.
        
        ОПТИМИЗАЦИЯ: Ожидание DOM стабилизации убрано для ускорения.
        Пустые страницы определяются по отсутствию элементов [data-marker="item"].
        ЛОГИКА: При обнаружении пустой страницы парсинг метро завершается,
        так как это означает, что для данного метро больше нет объявлений.
        НОВАЯ ЛОГИКА: Если первая страница пустая, парсинг метро завершается
        немедленно, так как для данного метро нет объявлений вообще.
        
        Args:
            start_page (int): Номер страницы, с которой начать парсинг (по умолчанию 1)
        """
        try:
            all_parsed_cards = []
            page = start_page
            max_attempts = 100  # Защита от бесконечного цикла

            total_pages_known = False  # Флаг, что общее количество страниц известно
            
            if start_page > 1:
                print(f"🚀 Начинаем парсинг с страницы {start_page}")
            
            while True:
                # Защита от бесконечного цикла
                if page > max_attempts:
                    print(f"⚠️ Достигнут максимальный лимит страниц ({max_attempts}), останавливаемся")
                    break
                
                # Если установлен лимит страниц, проверяем его
                if self.max_pages > 0 and page > self.max_pages:
                    print(f"📄 Достигнут лимит страниц ({self.max_pages}), останавливаемся")
                    break
                
                # Парсим текущую страницу
                print(f"📄 Обрабатываем страницу {page}...")
                page_cards = self.parse_metro_page_by_number(page)
                
                # Проверяем, пустая ли первая страница
                if page == start_page and len(page_cards) == 0:
                    print(f"⚠️ Первая страница {page} пустая - для данного метро нет объявлений")
                    print(f"🔄 Завершаем парсинг метро и переходим к следующему")
                    break
                
                # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: если страница помечена как пустая, но карточки найдены
                if len(page_cards) > 0:
                    # print(f"✅ Страница {page} содержит {len(page_cards)} карточек - продолжаем парсинг")  # Убрано из лога
                    
                    # ПРОВЕРЯЕМ на раздел "Вас может заинтересовать" даже если есть карточки
                    try:
                        page_text = self.driver.page_source.lower()
                        if 'вас может заинтересовать' in page_text:
                            print(f"🔍 Обнаружен раздел 'Вас может заинтересовать' на странице с карточками")
                            print(f"📊 Обрабатываем карточки ДО этого раздела")
                            print(f"✅ Всего карточек до раздела: {len(all_parsed_cards) + len(page_cards)}")
                            print(f"🔄 Завершаем парсинг метро и переходим к следующему")
                            
                            # Добавляем карточки с текущей страницы перед завершением
                            all_parsed_cards.extend(page_cards)
                            break
                    except Exception as e:
                        print(f"⚠️ Ошибка проверки на раздел 'Вас может заинтересовать': {e}")
                else:
                    print(f"ℹ️ Страница {page} не содержит карточек")
                
                # На первой странице пытаемся определить общее количество страниц
                if page == start_page and not total_pages_known:
                    try:
                        total_pages = self.get_total_pages_count()
                        if total_pages:
                            # print(f"🎯 Автоматически определено общее количество страниц: {total_pages}")  # Убрано из лога
                            total_pages_known = True
                            # Обновляем max_pages если он не был установлен или был меньше
                            if self.max_pages == 0 or self.max_pages > total_pages:
                                self.max_pages = total_pages
                                print(f"📊 Установлен лимит страниц: {self.max_pages}")
                    except Exception as e:
                        print(f"⚠️ Не удалось определить общее количество страниц: {e}")
                

                
                # Добавляем все карточки с этой страницы
                all_parsed_cards.extend(page_cards)
                # print(f"📄 Страница {page}: {len(page_cards)} карточек")  # Убрано из лога
                
                # НОВАЯ ЛОГИКА: Проверка ограничения по дате
                if self.max_days > 0 and len(page_cards) > 0:
                    oldest_date = self.get_oldest_card_date(page_cards)
                    if oldest_date:
                        # Теперь oldest_date всегда datetime.datetime
                        days_old = (datetime.now() - oldest_date).days
                        print(f"⏰ Самое старое объявление на странице {page}: {days_old} дней назад")
                        
                        if days_old > self.max_days:
                            print(f"⚠️ Обнаружено объявление старше {self.max_days} дней ({days_old} дней)")
                            print(f"🔄 Завершаем парсинг метро - переходим к следующему")
                            break
                    else:
                        print(f"⚠️ Не удалось определить дату объявлений на странице {page}")
                
                # Если страница пустая и это не первая страница, завершаем парсинг
                if len(page_cards) == 0 and page > start_page:
                    print(f"⚠️ Страница {page} пустая - для данного метро больше нет объявлений")
                    print(f"🔄 Завершаем парсинг метро")
                    break
                
                # Сбрасываем флаг ошибки соединения при успешном парсинге
                self.last_connection_error = False
                
                # СОХРАНЯЕМ ДАННЫЕ В БД ПОСЛЕ КАЖДОЙ СТРАНИЦЫ
                if self.enable_db_save and DB_AVAILABLE:
                    print(f"💾 Сохраняем данные страницы {page} в БД...")
                    try:
                        # Создаем таблицу если её нет
                        await create_ads_avito_table()
                        
                        saved_count = 0
                        for i, card in enumerate(page_cards):
                            try:
                                # Подготавливаем данные для БД
                                db_data = await self.prepare_data_for_db(card)
                                if db_data:
                                    # Сохраняем в БД
                                    await save_avito_ad(db_data)
                                    saved_count += 1
                                else:
                                    print(f"⚠️ Карточка {i+1} страницы {page}: prepare_data_for_db вернул None")
                            except Exception as e:
                                print(f"❌ Ошибка сохранения карточки {i+1} страницы {page}: {e}")
                                continue
                        
                        print(f"✅ Страница {page}: сохранено {saved_count} из {len(page_cards)} карточек в БД")
                        
                        # Обновляем статус пагинации в БД
                        try:
                            from parse_todb_avito import update_avito_pagination
                            await update_avito_pagination(self.metro_id, page)
                            print(f"📊 Обновлен статус пагинации: страница {page} для метро {self.metro_id}")
                        except Exception as e:
                            print(f"⚠️ Не удалось обновить статус пагинации для страницы {page}: {e}")
                        
                    except Exception as e:
                        print(f"❌ Ошибка сохранения страницы {page} в БД: {e}")
                else:
                    print(f"ℹ️ Сохранение в БД отключено для страницы {page}")
                
                # Если установлен лимит страниц, останавливаемся после последней
                if self.max_pages > 0 and page >= self.max_pages:
                    print(f"📄 Достигнут лимит страниц ({self.max_pages}), останавливаемся")
                    break
                
                # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: анализируем результат страницы
                if len(page_cards) == 0:
                    print(f"⚠️ Страница {page} не содержит карточек - анализируем причину")
                    try:
                        # Проверяем текущий URL и содержимое страницы
                        current_url = self.driver.current_url
                        page_text = self.driver.page_source.lower()
                        
                        # Ищем индикаторы пустых страниц
                        empty_indicators = [
                            'объявлений не найдено', 'ничего не найдено', 
                            'по вашему запросу ничего не найдено', 'нет объявлений',
                            'объявления не найдены', 'не найдено', 'пустой результат',
                            'поиск не дал результатов', 'ничего не найдено на выбранных станциях метро',
                            'вас может заинтересовать'  # Раздел с объявлениями других метро - НЕ ОБРАБАТЫВАТЬ
                        ]
                        
                        found_indicators = [indicator for indicator in empty_indicators if indicator in page_text]
                        if found_indicators:
                            print(f"   • Найдены индикаторы пустой страницы: {found_indicators}")
                            
                            # ОСОБАЯ ОБРАБОТКА для раздела "Вас может заинтересовать"
                            if 'вас может заинтересовать' in page_text:
                                print(f"   • 🔍 Обнаружен раздел 'Вас может заинтересовать'")
                                print(f"   • 📊 Обрабатываем карточки ДО этого раздела")
                                print(f"   • ✅ Всего карточек до раздела: {len(all_parsed_cards)}")
                                print(f"   • 🔄 Завершаем парсинг метро и переходим к следующему")
                                break
                            else:
                                print(f"   • Завершаем парсинг метро - больше нет объявлений")
                                break
                        else:
                            print(f"   • Индикаторы пустой страницы не найдены")
                            print(f"   • Возможно, проблема с загрузкой карточек")
                            
                    except Exception as e:
                        print(f"   • Ошибка анализа страницы: {e}")
                
                # Переходим к следующей странице
                page += 1
                
                # Задержка между страницами (перезагрузка браузера убрана для ускорения)
                if self.max_pages == 0 or page <= self.max_pages:
                    time.sleep(self.page_delay)
            
            # Выводим итоговую статистику по страницам
            pages_processed = page - 1  # Вычитаем 1, так как page увеличился в конце цикла
            print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА ПО СТРАНИЦАМ:")
            print(f"   • Всего страниц обработано: {pages_processed}")
            print(f"   • Всего карточек спарсено: {len(all_parsed_cards)}")
            
            # Проверяем, была ли первая страница пустой
            if pages_processed == 0 and len(all_parsed_cards) == 0:
                print(f"   • ⚠️ Первая страница была пустой - для данного метро нет объявлений")
                print(f"   • 🔄 Парсинг метро завершен, переходим к следующему")
            elif pages_processed > 0 and len(all_parsed_cards) == 0:
                print(f"   • ⚠️ Обработано {pages_processed} страниц, но карточек не найдено")
                print(f"   • 🔄 Парсинг метро завершен, переходим к следующему")
            
            if self.max_pages > 0:
                print(f"   • Лимит страниц был установлен: {self.max_pages}")
                if pages_processed >= self.max_pages:
                    print(f"   • Лимит страниц достигнут ✅")
                else:
                    print(f"   • Лимит страниц НЕ достигнут (обработано {pages_processed} из {self.max_pages})")
            else:
                print(f"   • Лимит страниц не был установлен (парсили все доступные)")
            
            return all_parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга нескольких страниц: {e}")
            return all_parsed_cards
    
    def print_statistics(self, parsed_cards):
        """Выводит статистику парсинга"""
        try:
            print(f"\n📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПАРСИНГА:")
            print(f"   Метро ID: {self.metro_id}")
            print(f"   Метро avito_id: {self.metro_avito_id}")
            print(f"   Метро название: {self.metro_name if hasattr(self, 'metro_name') else 'Неизвестно'}")
            
            # Рассчитываем количество страниц по карточкам
            cards_per_page = self.max_cards if self.max_cards > 0 else 50  # По умолчанию 50 карточек на странице
            estimated_pages = (len(parsed_cards) + cards_per_page - 1) // cards_per_page
            
            print(f"   Страниц спарсено (расчет): {estimated_pages}")
            print(f"   Карточек спарсено: {len(parsed_cards)}")
            print(f"   Карточек на страницу: {cards_per_page}")
            
            # Информация о лимитах
            if self.max_pages > 0:
                print(f"   Лимит страниц был установлен: {self.max_pages}")
                if estimated_pages >= self.max_pages:
                    print(f"   Лимит страниц достигнут: ✅")
                else:
                    print(f"   Лимит страниц НЕ достигнут: ❌ (обработано {estimated_pages} из {self.max_pages})")
            else:
                print(f"   Лимит страниц не был установлен (парсили все доступные)")
            
            # Информация о БД
            if DB_AVAILABLE:
                print(f"   База данных: ✅ Доступна")
                if self.enable_db_save:
                    print(f"   Сохранение в БД: ✅ Включено")
                else:
                    print(f"   Сохранение в БД: ❌ Отключено")
            else:
                print(f"   База данных: ❌ Недоступна")
            
        except Exception as e:
            print(f"❌ Ошибка вывода статистики: {e}")
    
    async def run_parser(self, start_page=1):
        """Запускает парсер
        
        Args:
            start_page (int): Номер страницы, с которой начать парсинг (по умолчанию 1)
        """
        try:
            # Загружаем словарь тегов из БД
            print("🔄 Инициализация словаря тегов...")
            self.tags_dictionary = await self.load_tags_from_db()
            
            # Получаем avito_id для метро
            if not await self.get_metro_avito_id():
                return False
            
            # Загружаем cookies
            cookies_data = self.load_cookies()
            if not cookies_data:
                return False
            
            # Настраиваем Selenium
            if not self.setup_selenium():
                return False
            
            # Применяем cookies
            if not self.apply_cookies(cookies_data):
                return False
            
            # Выводим настройки парсера
            print("⚙️ Настройки парсера:")
            print(f"   • Страниц для парсинга: {self.max_pages if self.max_pages > 0 else 'все'}")
            print(f"   • Карточек на странице: {self.max_cards if self.max_cards > 0 else 'все'}")
            print(f"   • ID метро: {self.metro_id}")
            
            # Выводим URL общего поискового запроса
            if self.metro_avito_id:
                base_url = f"https://www.avito.ru/moskva/kvartiry/prodam?metro={self.metro_avito_id}&s=104&footWalkingMetro=20"
                print(f"   • URL поиска: {base_url}")
            print()
            
            # Парсим страницы
            parsed_cards = await self.parse_multiple_pages(start_page=start_page)
            
            if parsed_cards:
                # Данные уже сохранены постранично, выводим итоговую статистику
                print(f"\n🎯 ИТОГОВЫЙ РЕЗУЛЬТАТ ПАРСИНГА:")
                print(f"   • Метро: {self.metro_name if hasattr(self, 'metro_name') else 'Неизвестно'}")
                print(f"   • Общее количество карточек: {len(parsed_cards)}")
                
                # Вызываем метод статистики для дополнительной информации
                self.print_statistics(parsed_cards)
                
                return True
            else:
                print("❌ Не удалось спарсить карточки")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка работы парсера: {e}")
            print(f"💡 Для возобновления парсинга с последней успешной страницы используйте:")
            print(f"   • start_page = {self.max_pages if self.max_pages > 0 else 'последняя обработанная страница'}")
            print(f"   • Или укажите конкретную страницу в параметре start_page")
            return False
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
    
    async def parse_single_metro(self, metro_id, max_pages, max_cards=None, start_page=1, max_days=0, multiple_metro_ids=None):
        """
        Парсит одно метро с заданными параметрами
        
        НОВАЯ ЛОГИКА: Если для метро нет объявлений (все страницы пустые),
        парсинг считается успешным и возвращает True, так как это не ошибка.
        
        Args:
            metro_id (int): ID метро из таблицы metro
            max_pages (int): Количество страниц для парсинга
            max_cards (int, optional): Количество карточек на странице (0 = все карточки)
            start_page (int, optional): Номер страницы, с которой начать парсинг (по умолчанию 1)
            max_days (int, optional): Максимальный возраст объявлений в днях (0 = все объявления)
            multiple_metro_ids (list, optional): Список ID метро для парсинга одной ссылкой
        
        Returns:
            tuple: (success: bool, saved_count: int, total_cards: int)
                - success: Успешно ли выполнен парсинг (True даже если нет объявлений)
                - saved_count: Количество сохраненных записей в БД
                - total_cards: Общее количество спарсенных карточек
        """
        try:
            # Устанавливаем параметры для этого запуска
            self.metro_id = metro_id
            self.max_pages = max_pages
            if max_cards is not None:
                self.max_cards = max_cards
            if max_days > 0:
                self.max_days = max_days
                print(f"⏰ Ограничение по дате: только объявления за последние {max_days} дней")
            
            print(f"🚀 Запуск парсинга метро ID={metro_id}, страниц={max_pages}, карточек на странице={self.max_cards}")
            if start_page > 1:
                print(f"🚀 Начинаем с страницы {start_page}")
            
            # Устанавливаем параметры для этого запуска
            self.metro_id = metro_id
            self.max_pages = max_pages
            if max_cards is not None:
                self.max_cards = max_cards
            if max_days > 0:
                self.max_days = max_days
                print(f"⏰ Ограничение по дате: только объявления за последние {max_days} дней")
            
            # НОВАЯ ЛОГИКА: Проверяем множественные метро
            if multiple_metro_ids and len(multiple_metro_ids) > 1:
                print(f"🚀 Парсинг множественных метро: {len(multiple_metro_ids)} метро одной ссылкой")
                if not await self.get_multiple_metro_avito_ids(multiple_metro_ids):
                    print(f"❌ Не удалось получить avito_id для множественных метро")
                    return False, 0, 0
            else:
                # Получаем avito_id для одиночного метро
                if not await self.get_metro_avito_id():
                    print(f"❌ Не удалось получить avito_id для метро {metro_id}")
                    return False, 0, 0
            
            # Загружаем словарь тегов из БД
            if not self.tags_dictionary:
                self.tags_dictionary = await self.load_tags_from_db()
            
            # Загружаем cookies
            cookies_data = self.load_cookies()
            if not cookies_data:
                print("❌ Не удалось загрузить cookies")
                return False, 0, 0
            
            # Настраиваем Selenium
            if not self.setup_selenium():
                print("❌ Не удалось настроить Selenium")
                return False, 0, 0
            
            # Применяем cookies
            if not self.apply_cookies(cookies_data):
                print("❌ Не удалось применить cookies")
                return False, 0, 0
            
            # Парсим страницы
            parsed_cards = await self.parse_multiple_pages(start_page=start_page)
            total_cards = len(parsed_cards)
            
            # Проверяем результат парсинга
            if not parsed_cards:
                print("ℹ️ Для данного метро нет объявлений (все страницы пустые)")
                print("✅ Парсинг метро завершен успешно, переходим к следующему")
                return True, 0, 0  # Возвращаем True, так как это не ошибка
            
            # Данные уже сохранены постранично, подсчитываем общее количество
            saved_count = total_cards  # Все карточки уже сохранены постранично
            
            # Выводим итоговую статистику
            print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
            print(f"   Метро ID: {metro_id}")
            print(f"   Metro avito_id: {self.metro_avito_id}")
            print(f"   Страниц спарсено: {max_pages}")
            print(f"   Карточек спарсено: {total_cards}")
            print(f"   Сохранено в БД: {saved_count} (постранично)")
            print(f"   Metro ID в БД: {metro_id} (связь с таблицей metro)")
            
            return True, saved_count, total_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга метро {metro_id}: {e}")
            return False, 0, 0
        finally:
            # Закрываем браузер
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
    

    
    def get_tags_dictionary(self):
        """Получает кэшированный словарь тегов или стандартный, если кэш не загружен"""
        if self.tags_dictionary is not None:
            return self.tags_dictionary
        
        # Если кэш не загружен, используем стандартный словарь
        print("ℹ️ Кэш тегов не загружен, используем стандартный словарь")
        return self.get_default_tags_dictionary()
    
    def get_default_tags_dictionary(self):
        """Возвращает стандартный словарь тегов, если БД недоступна"""
        return {
            'Возможен торг',
            'Комфортная сделка',
            'Надёжный партнёр',
            'Проверено в Росреестре',
            'Рыночная цена',
            'Свободная продажа',
            'Собственник',
            'Срочная продажа'
        }
    
    async def load_tags_from_db(self):
        """Асинхронно загружает словарь тегов из таблицы system.tags"""
        try:
            if not self.database_url:
                print("⚠️ База данных не подключена, используем стандартный словарь тегов")
                return self.get_default_tags_dictionary()
            
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем все активные теги из таблицы system.tags
            result = await conn.fetch("""
                SELECT tag_name, tag_category, tag_description, usage_count
                FROM system.tags 
                WHERE is_active = true
                ORDER BY tag_category, tag_name
            """)
            
            await conn.close()
            
            # Создаем словарь тегов
            tags_dict = set()
            
            for row in result:
                tag_name = row['tag_name']
                tags_dict.add(tag_name)
            
            # Добавляем стандартные теги для полноты (если их нет в БД)
            default_tags = self.get_default_tags_dictionary()
            tags_dict.update(default_tags)
            
            return tags_dict
            
        except Exception as e:
            print(f"⚠️ Ошибка загрузки тегов из system.tags: {e}, используем стандартный словарь")
            return self.get_default_tags_dictionary()
    
    def analyze_card_content(self, card_data):
        """Анализирует содержимое карточки и возвращает информацию о тегах и адресе"""
        try:
            # Получаем все характеристики карточки
            params_text = card_data.get('params', '')
            if not params_text:
                return "📋 Теги: не найдены", "📍 Адрес: не найден"
            
            # Разбиваем на строки
            lines = [line.strip() for line in params_text.split('\n') if line.strip()]
            
            # Ищем теги и адрес
            tags = []
            address = ""
            
            for line in lines:
                line_lower = line.lower()
                
                # Проверяем, является ли строка тегом
                is_tag = False
                tags_dict = self.get_tags_dictionary()
                for tag in tags_dict:
                    if tag.lower() in line_lower or line_lower in tag.lower():
                        is_tag = True
                        tags.append(line)
                        break
                
                # Дополнительные проверки на теги
                if not is_tag:
                    tag_patterns = [
                        r'^\d+\s*мин',  # Время до метро
                        r'^\d+\s*эт',   # Этаж
                        r'^\d+\s*м²',   # Площадь
                        r'^\d+\s*комнат', # Количество комнат
                        r'^студия$',     # Студия
                        r'^новостройка$', # Новостройка
                        r'^вторичка$',   # Вторичка
                    ]
                    
                    for pattern in tag_patterns:
                        if re.match(pattern, line_lower):
                            is_tag = True
                            tags.append(line)
                            break
                
                # Если это не тег, проверяем на адрес
                if not is_tag and self.is_address_line(line):
                    address = line
                    break
            
            # Формируем информацию для вывода
            tags_info = f"📋 Теги: {', '.join(tags[-3:]) if tags else 'не найдены'}"  # Последние 3 тега
            address_info = f"📍 Адрес: {address if address else 'не найден'}"
            
            return tags_info, address_info
            
        except Exception as e:
            return f"📋 Теги: ошибка анализа", f"📍 Адрес: ошибка анализа"
    
    def cleanup(self):
        """Корректно закрывает браузер и очищает ресурсы"""
        try:
            if self.driver:
                print("🧹 Закрываем браузер...")
                self.driver.quit()
                self.driver = None
                print("✅ Браузер закрыт")
        except Exception as e:
            print(f"⚠️ Ошибка при закрытии браузера: {e}")
    
    def __del__(self):
        """Деструктор для автоматической очистки при удалении объекта"""
        self.cleanup()
    
    def get_oldest_card_date(self, cards):
        """Определяет самую старую дату среди карточек на странице
        
        Args:
            cards (list): Список карточек с данными
            
        Returns:
            datetime: Самая старая дата как datetime.datetime или None, если не удалось определить
        """
        try:
            oldest_date = None
            
            for card in cards:
                if not card:
                    continue
                
                # Пытаемся получить дату из разных полей
                card_date = None
                
                # 1. Пробуем поле creation_time
                if 'creation_time' in card and card['creation_time']:
                    card_date = self.parse_card_date(card['creation_time'])
                
                # 2. Пробуем поле published_time
                if not card_date and 'published_time' in card and card['published_time']:
                    card_date = self.parse_card_date(card['published_time'])
                
                # 3. Пробуем поле seller_info.creation_time
                if not card_date and 'seller_info' in card and card['seller_info']:
                    seller_info = card['seller_info']
                    if 'creation_time' in seller_info and seller_info['creation_time']:
                        card_date = self.parse_card_date(seller_info['creation_time'])
                
                # Если нашли дату, сравниваем с самой старой
                if card_date:
                    # Теперь card_date всегда datetime.datetime
                    if oldest_date is None or card_date < oldest_date:
                        oldest_date = card_date
            
            return oldest_date
            
        except Exception as e:
            print(f"⚠️ Ошибка определения самой старой даты: {e}")
            return None
    
    def parse_card_date(self, date_text):
        """Парсит дату из текста карточки
        
        Args:
            date_text (str): Текст с датой
            
        Returns:
            datetime: Объект datetime.datetime или None, если не удалось распарсить
        """
        try:
            if not date_text:
                return None
            
            # Используем существующую функцию для относительного времени
            parsed_date = self.convert_relative_time_to_date(date_text)
            if parsed_date:
                return parsed_date
            
            # Если это конкретная дата (например, "12 июля 13:35")
            month_names = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
            }
            
            # Ищем формат "12 июля" или "12 июля 13:35"
            for month_name, month_num in month_names.items():
                if month_name in date_text.lower():
                    # Ищем день перед названием месяца
                    day_match = re.search(r'(\d{1,2})\s+' + month_name, date_text.lower())
                    if day_match:
                        day = int(day_match.group(1))
                        current_year = datetime.now().year
                        
                        # Проверяем, есть ли время в строке
                        time_match = re.search(r'(\d{1,2}):(\d{1,2})', date_text.lower())
                        if time_match:
                            # Есть время - используем его
                            hour = int(time_match.group(1))
                            minute = int(time_match.group(2))
                        else:
                            # Время не указано - устанавливаем полдень (12:00)
                            hour = 12
                            minute = 0
                        
                        # Создаем datetime
                        try:
                            card_datetime = datetime(current_year, month_num, day, hour, minute)
                            
                            # Если дата в будущем, значит это прошлый год
                            if card_datetime > datetime.now():
                                card_datetime = datetime(current_year - 1, month_num, day, hour, minute)
                            
                            return card_datetime
                        except ValueError:
                            continue
            
            return None
            
        except Exception as e:
            print(f"⚠️ Ошибка парсинга даты '{date_text}': {e}")
            return None

async def main():
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # SIGTERM
    
    # Загружаем переменные окружения
    load_dotenv()
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        print("💡 Создайте файл .env с содержимым:")
        print("   DATABASE_URL=postgresql://username:password@host:port/database")
        return
    
    # Создаем и запускаем парсер
    global global_parser
    parser = None
    try:
        parser = EnhancedMetroParser()
        global_parser = parser  # Устанавливаем глобальную переменную
        parser.database_url = database_url
        
        # НАСТРОЙКА КОЛИЧЕСТВА СТРАНИЦ И КАРТОЧЕК
        # Измените эти параметры по вашему желанию:
        # parser.max_pages = 3      # Количество страниц для парсинга (1, 2, 3, 5, 10 и т.д.)
        # parser.max_cards = 15     # Количество карточек на странице (0 = все карточки)
        # parser.metro_id = 1       # ID метро из таблицы metro
        
        # НОВЫЕ НАСТРОЙКИ для гибридного парсинга:
        # parser.stream_cards_count = 5   # Первые N карточек парсить потоково (по умолчанию: 5)
        # parser.batch_cards_count = 45   # Остальные M карточек парсить пачками (по умолчанию: 45)
        # parser.batch_size = 10          # Размер пакета для второй части (по умолчанию: 10)
        
        # Раскомментируйте строки ниже, если хотите изменить настройки:
        # parser.max_cards = 15           # Парсить 15 карточек на странице
        # parser.stream_cards_count = 3   # Первые 3 карточки потоково
        # parser.batch_cards_count = 12   # Остальные 12 карточек пачками
        # parser.batch_size = 6           # Размер пакета: 6 карточек
        
        # НАСТРОЙКА НАЧАЛЬНОЙ СТРАНИЦЫ
        # Если хотите начать с определенной страницы, раскомментируйте и измените:
        # start_page = 5  # Начать с 5-й страницы
        start_page = 1   # Начать с 1-й страницы (по умолчанию)
        
        print(f"⚙️ Настройки парсера:")
        print(f"   • Страниц для парсинга: {parser.max_pages}")
        print(f"   • Карточек на странице: {parser.max_cards if parser.max_cards > 0 else 'все'}")
        print(f"   • ID метро: {parser.metro_id}")
        print(f"   • Начальная страница: {start_page}")
        print(f"   • Первые {parser.stream_cards_count} карточек: потоково")
        print(f"   • Остальные {parser.batch_cards_count} карточек: пачками по {parser.batch_size}")
        print("=" * 60)
        
        success = await parser.run_parser(start_page=start_page)
        
        if success:
            print("\n🎉 Парсинг завершен успешно!")
        else:
            print("\n❌ Парсинг завершен с ошибками")
            print("\n💡 Для возобновления парсинга:")
            print(f"   • Измените start_page в коде на нужную страницу")
            print(f"   • Или используйте: start_page = {parser.max_pages if parser.max_pages > 0 else 'последняя обработанная страница'}")
            print(f"   • Текущие настройки: max_pages = {parser.max_pages}, metro_id = {parser.metro_id}")
            
    except KeyboardInterrupt:
        print("\n⚠️ Парсинг прерван пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
    finally:
        # Гарантированная очистка ресурсов
        if parser:
            print("\n🧹 Очищаем ресурсы...")
            parser.cleanup()
            global_parser = None  # Очищаем глобальную переменную
            print("✅ Ресурсы очищены")

if __name__ == "__main__":
    asyncio.run(main())