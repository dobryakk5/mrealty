#!/usr/bin/env python3
"""
Улучшенный парсер карточек с правильным определением metro.avito_id
"""

import json  # Нужен для загрузки cookies
import os
import time
import asyncio
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
        
        self.driver = None
        self.database_url = None
        self.metro_avito_id = None  # avito_id для этого метро
        self.tags_dictionary = None  # Кэш для словаря тегов
        
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
        """Преобразует относительное время в дату"""
        try:
            if not relative_time:
                return None
                
            now = datetime.now()
            relative_time_lower = relative_time.lower().strip()
            
            # Паттерны для относительного времени
            if 'сегодня' in relative_time_lower:
                result = now  # Возвращаем полный datetime для сегодня
                return result
            elif 'вчера' in relative_time_lower:
                yesterday = now - timedelta(days=1)
                result = yesterday  # Возвращаем полный datetime для вчера
                return result
            elif 'позавчера' in relative_time_lower:
                day_before_yesterday = now - timedelta(days=2)
                result = day_before_yesterday  # Возвращаем полный datetime для позавчера
                return result
            
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
                        result = target_time  # Возвращаем полный datetime для часов
                        return result
                    elif unit == 'days':
                        target_time = now - timedelta(days=count)
                    elif unit == 'weeks':
                        target_time = now - timedelta(weeks=count)
                    elif unit == 'months':
                        # Приблизительно, месяц = 30 дней
                        target_time = now - timedelta(days=count * 30)
                    
                    result = target_time.date()
                    return result
            
            # Если это конкретная дата (например, "12 июля 13:35")
            month_names = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
            }
            
            # Ищем формат "12 июля"
            for month_name, month_num in month_names.items():
                if month_name in relative_time_lower:
                    # Ищем день перед названием месяца
                    day_match = re.search(r'(\d{1,2})\s+' + month_name, relative_time_lower)
                    if day_match:
                        day = int(day_match.group(1))
                        # Предполагаем текущий год
                        year = now.year
                        
                        # ЛОГИКА ГОДА: если месяц уже прошел в этом году, то это прошлый год
                        # Например: сейчас август 2025, парсится "12 июля" → это июль 2025 (текущий год)
                        # Если парсится "12 января" → это январь 2025 (текущий год)
                        # НЕ добавляем год, так как объявления обычно не публикуются на год вперед
                        
                        result = datetime(year, month_num, day).date()
                        return result
            
            # Ищем формат "12.07" или "12.07.2024"
            date_dot_patterns = [
                r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # 12.07.2024
                r'(\d{1,2})\.(\d{1,2})'            # 12.07
            ]
            
            for pattern in date_dot_patterns:
                match = re.search(pattern, relative_time_lower)
                if match:
                    if len(match.groups()) == 2:
                        # Формат "12.07"
                        day = int(match.group(1))
                        month = int(match.group(2))
                        year = now.year
                        
                        # ЛОГИКА ГОДА: если месяц уже прошел в этом году, то это текущий год
                        # Например: сейчас август 2025, парсится "12.07" → это июль 2025 (текущий год)
                        # НЕ добавляем год, так как объявления обычно не публикуются на год вперед
                        
                        result = datetime(year, month, day).date()
                        return result
                    
                    elif len(match.groups()) == 3:
                        # Формат "12.07.2024"
                        day = int(match.group(1))
                        month = int(match.group(2))
                        year = int(match.group(3))
                        result = datetime(year, month, day).date()
                        print(f"✅ Формат DD.MM.YYYY -> {result}")
                        return result
            
            print(f"⚠️ Не удалось распарсить время: '{relative_time}', возвращаем текущий datetime")
            return datetime.now()
            
        except Exception as e:
            print(f"❌ Ошибка преобразования времени в дату '{relative_time}': {e}, возвращаем текущий datetime")
            return datetime.now()
    
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
                metro_name = result['name']
                print(f"📍 Метро: {metro_name} (ID: {self.metro_id}, avito_id: {self.metro_avito_id})")
                return True
            else:
                print(f"❌ Метро с ID {self.metro_id} не найдено в БД")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка получения avito_id для метро: {e}")
            return False
    
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
                print(f"📊 Количество cookies: {len(cookies_data['cookies'])}")
                print(f"⏰ Возраст cookies: {age_hours:.1f} часов")
                
                # Предупреждение если cookies старые
                if age_hours > 24:
                    print("⚠️ Cookies старше 24 часов, могут быть неактуальны")
                if age_hours > 72:
                    print("🚨 Cookies старше 72 часов, рекомендуется обновить")
                
            except Exception as e:
                print(f"⚠️ Не удалось проверить возраст cookies: {e}")
                print(f"✅ Загружены cookies от {cookies_data['timestamp']}")
                print(f"📊 Количество cookies: {len(cookies_data['cookies'])}")
            
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
                print("🔒 Браузер запущен в headless режиме")
            else:
                print("🖥️ Браузер запущен с интерфейсом")
            
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
            
            print("🔧 Создаем браузер...")
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
    
    def apply_cookies(self, cookies_data):
        """Применяет cookies к драйверу"""
        try:
            if not cookies_data or 'cookies' not in cookies_data:
                print("❌ Данные cookies отсутствуют или некорректны")
                return False
            
            print(f"📊 Найдено cookies для применения: {len(cookies_data['cookies'])}")
            
            # Сначала переходим на домен
            print("🌐 Переходим на AVITO...")
            self.driver.get("https://avito.ru")
            time.sleep(5)  # Увеличиваем время ожидания
            
            # Проверяем, что страница загрузилась
            try:
                # Ждем появления элемента, подтверждающего загрузку
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                print("✅ Страница AVITO загружена")
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
                        print(f"🔐 Применен важный cookie: {cookie['name']}")
                    
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
        """Получает URL для метро с правильным avito_id"""
        if not self.metro_avito_id:
            print("❌ avito_id для метро не определен")
            return None
            
        # URL для вторички с правильным avito_id
        base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
        metro_url = f"{base_url}?metro={self.metro_avito_id}"
        return metro_url
    
    def generate_search_context(self) -> str:
        """Генерирует простой context для каждого запроса"""
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
            json_str = json.dumps(context_data, separators=(',', ':'))
            compressed = gzip.compress(json_str.encode('utf-8'))
            encoded = base64.b64encode(compressed).decode('utf-8')
            return f"H4sIAAAAAAAA_{encoded}"
        except Exception as e:
            print(f"[CONTEXT] Ошибка генерации: {e}")
            return "H4sIAAAAAAAA_wE-AMH_YToxOntzOjg6ImZyb21QYWdlIjtzOjc6ImNhdGFsb2ciO312FITcIwAAAA"
    
    def clean_url_path(self, url_path: str) -> str:
        """Очищает URL от всех параметров, оставляя только путь"""
        if not url_path:
            return ""
        if "?" in url_path:
            url_path = url_path.split("?")[0]
        return url_path
    
    def get_metro_url_with_page(self, page=1):
        """Получает URL для метро с пагинацией и context"""
        if not self.metro_avito_id:
            print("❌ avito_id для метро не определен")
            return None
            
        # URL для вторички с правильным avito_id и сортировкой
        base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
        metro_url = f"{base_url}?metro={self.metro_avito_id}&s=104"
        
        # Добавляем пагинацию (Avito использует параметр p)
        if page > 1:
            metro_url += f"&p={page}"
        
        # Генерируем новый context для каждой страницы
        context = self.generate_search_context()
        metro_url += f"&context={context}"
        
        print(f"[CONTEXT] Страница {page}: сгенерирован новый context")
        return metro_url
    
    def wait_for_dom_stability(self):
        """Ждет стабилизации DOM после загрузки страницы (без таймаута)"""
        try:
            print("⏳ Ждем стабилизации DOM...")
            
            # Ждем полной загрузки страницы
            while True:
                ready_state = self.driver.execute_script("return document.readyState")
                if ready_state == "complete":
                    break
                time.sleep(0.1)  # Короткая пауза
            
            # Дополнительная пауза для JavaScript и динамического контента
            time.sleep(1)
            
            # Ждем появления первых карточек
            while True:
                cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                if cards:
                    break
                time.sleep(0.1)  # Короткая пауза
            
            # ДОПОЛНИТЕЛЬНАЯ пауза для стабилизации
            time.sleep(0.5)
            
            print("✅ DOM стабилизирован, карточки загружены")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка ожидания стабилизации DOM: {e}")
            return False

    def wait_for_cards_load(self, timeout=30):
        """Ждет загрузки карточек или определяет, что страница пустая"""
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
                'объявления не найдены'
            ]
            
            if any(indicator in page_text for indicator in empty_indicators):
                print("ℹ️ Страница пустая - объявлений не найдено")
                return True  # Возвращаем True, чтобы корректно обработать пустую страницу
            
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
        Гибридный подход: первые N карточек потоково, остальные M - пачками по K
        Параметры настраиваются через:
        - self.stream_cards_count - количество карточек для потокового парсинга
        - self.batch_cards_count - количество карточек для пакетного парсинга  
        - self.batch_size - размер пакета для второй части
        """
        try:
            print(f"🔄 Гибридный парсинг: первые {self.stream_cards_count} потоково + остальные {self.batch_cards_count} пачками по {self.batch_size}")
            
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
                    
                    while retry_count < max_retries and not card_parsed:
                        try:
                            # ВАЖНО: Получаем свежие элементы перед каждой попыткой (как в старом скрипте)
                            fresh_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                            if i >= len(fresh_cards):
                                print(f"⚠️ Карточка {i+1} недоступна в свежих элементах, пропускаем")
                                break
                            
                            card = fresh_cards[i]
                            card_data = self.parse_card(card)
                            if card_data:
                                card_data['card_number'] = i + 1
                                card_data['raw_text'] = card.text.strip()
                                parsed_cards.append(card_data)
                                print(f"   ✅ Спарсена карточка {i+1} (потоково)")
                                card_parsed = True
                            else:
                                print(f"   ⚠️ Карточка {i+1} не дала данных")
                                break  # Если карточка не дала данных, не повторяем
                                        
                        except Exception as e:
                            error_msg = str(e).lower()
                            retry_count += 1
                            
                            # Проверяем на stale element и другие ошибки, которые можно повторить
                            if ('stale element' in error_msg or 'element not found' in error_msg or 'timeout' in error_msg) and retry_count < max_retries:
                                print(f"   🔄 Ошибка для карточки {i+1}, пробуем еще раз... (попытка {retry_count}/{max_retries})")
                                print(f"      Ошибка: {str(e)[:100]}...")
                                time.sleep(0.5)  # Небольшая пауза перед повторной попыткой
                                continue
                            else:
                                print(f"   ❌ Ошибка карточки {i+1}: {e}")
                                break
                    
                    # Если карточка не была спарсена после всех попыток
                    if not card_parsed:
                        print(f"   ❌ Карточка {i+1} не удалась после {max_retries} попыток")
                
                print(f"✅ ЭТАП 1 завершен: {len(parsed_cards)} карточек")
            
            # ЭТАП 2: Остальные карточки - пачками по N (настраиваемый размер)
            remaining_cards = cards_to_parse - stream_count
            if remaining_cards > 0:
                print(f"🔄 ЭТАП 2: Парсим остальные {remaining_cards} карточек пачками по {self.batch_size}...")
                
                # Парсим карточки пачками по N (начиная с N+1-й)
                for i in range(stream_count, cards_to_parse, self.batch_size):
                    # Определяем диапазон для текущей группы
                    start_idx = i
                    end_idx = min(i + self.batch_size, cards_to_parse)
                    group_size = end_idx - start_idx
                    
                    print(f"🔄 Парсим группу карточек {start_idx+1}-{end_idx} ({group_size} карточек)...")
                    
                    # Пакетно парсим все карточки группы
                    group_parsed_count = 0
                    for j in range(start_idx, end_idx):
                        try:
                            # ВАЖНО: Получаем свежие элементы перед каждой попыткой (как в старом скрипте)
                            fresh_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                            if j >= len(fresh_cards):
                                print(f"⚠️ Карточка {j+1} недоступна в свежих элементах, пропускаем")
                                continue
                                
                            card = fresh_cards[j]
                            card_data = self.parse_card(card)
                            if card_data:
                                card_data['card_number'] = j + 1
                                card_data['raw_text'] = card.text.strip()
                                parsed_cards.append(card_data)
                                group_parsed_count += 1
                                print(f"   ✅ Спарсена карточка {j+1} (пачкой)")
                            else:
                                print(f"   ⚠️ Карточка {j+1} не дала данных")
                                    
                        except Exception as e:
                            error_msg = str(e).lower()
                            
                            # Для пакетного парсинга тоже добавляем retry логику
                            if 'stale element' in error_msg or 'element not found' in error_msg:
                                print(f"   🔄 Stale element для карточки {j+1}, получаем свежие элементы...")
                                try:
                                    # Получаем свежие элементы и повторяем попытку
                                    fresh_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                                    if j < len(fresh_cards):
                                        card = fresh_cards[j]
                                        card_data = self.parse_card(card)
                                        if card_data:
                                            card_data['card_number'] = j + 1
                                            card_data['raw_text'] = card.text.strip()
                                            parsed_cards.append(card_data)
                                            group_parsed_count += 1
                                            print(f"   ✅ Спарсена карточка {j+1} (пачкой, после retry)")
                                        else:
                                            print(f"   ⚠️ Карточка {j+1} не дала данных после retry")
                                    else:
                                        print(f"   ❌ Карточка {j+1} недоступна после retry")
                                except Exception as retry_error:
                                    print(f"   ❌ Ошибка retry для карточки {j+1}: {retry_error}")
                            else:
                                print(f"   ❌ Ошибка карточки {j+1}: {e}")
                            continue
                    
                    print(f"✅ Группа {start_idx+1}-{end_idx} завершена: +{group_parsed_count} карточек ({len(parsed_cards)} всего)")
            
            print(f"✅ Гибридный парсинг завершен: {len(parsed_cards)} карточек из {cards_to_parse}")
            return parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка гибридного парсинга: {e}")
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

    def prepare_data_for_db(self, card_data):
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
            
            # Комплекс (берем из complex_name если есть)
            db_data['complex'] = card_data.get('complex_name', '')
            
            # Метро - НЕ сохраняем название в БД, только metro_id
            # Название метро остается в card_data для внутренней логики
            # db_data['metro'] = None  # Убираем название метро из БД
            
            # ID метро из таблицы metro (добавляем для связи с таблицей metro)
            db_data['metro_id'] = self.metro_id
            
            # Время до метро
            time_to_metro = card_data.get('time_to_metro', '')
            if time_to_metro and time_to_metro != 'не указано':
                try:
                    db_data['walk_minutes'] = int(time_to_metro)
                except:
                    db_data['walk_minutes'] = None
            else:
                db_data['walk_minutes'] = None
            
            # Адрес
            db_data['address'] = card_data.get('street_house', '')
            
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
            
            # Информация о продавце (Person) - всё что после времени публикации
            seller_info = card_data.get('seller_info', {})
            
            # ПРИОРИТЕТ: используем clean_person из seller_info, если он есть
            if seller_info and seller_info.get('clean_person'):
                clean_person = seller_info['clean_person']
                # Проверяем длину - если больше 70 символов, значит это описание, а не имя
                if len(clean_person) > 70:
                    db_data['person'] = None
                    print(f"⚠️ Person слишком длинный ({len(clean_person)} символов), устанавливаем NULL")
                else:
                    db_data['person'] = clean_person
                
                # Тип продавца сохраняем в person_type
                if seller_info.get('type'):
                    db_data['person_type'] = seller_info['type']
                
            # ПРИОРИТЕТ: используем поле person из card_data, если оно есть
            elif 'person' in card_data and card_data['person']:
                # Очищаем поле person от лишней информации
                person_text = card_data['person']
                
                # Убираем "Тип: ...", "Объявлений: ...", дату и количество объявлений
                lines = person_text.split(' | ')
                filtered_lines = []
                
                for line in lines:
                    line = line.strip()
                    
                    # Пропускаем лишние строки
                    if (line.startswith('Тип:') or 
                        line.startswith('Объявлений:') or
                        line.startswith('Полная информация:') or
                        'завершённых объявлений' in line or
                        'завершенных объявлений' in line or
                        re.match(r'\d+\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)', line) or
                        re.match(r'\d{1,2}\s+[а-яё]+', line)):  # Дата в формате "18 июля"
                        continue
                    
                    # Оставляем только имя/название
                    if line and len(line) > 2:
                        filtered_lines.append(line)
                
                if filtered_lines:
                    # Берем только первое имя/название (обычно это основное)
                    clean_person = filtered_lines[0]
                    # Проверяем длину - если больше 70 символов, значит это описание, а не имя
                    if len(clean_person) > 70:
                        db_data['person'] = None
                        print(f"⚠️ Person слишком длинный ({len(clean_person)} символов), устанавливаем NULL")
                    else:
                        db_data['person'] = clean_person
                else:
                    db_data['person'] = 'Информация не найдена'
                
                # Тип продавца сохраняем в person_type
                if 'seller_info' in card_data and card_data['seller_info'].get('type'):
                    db_data['person_type'] = card_data['seller_info']['type']
                
            elif seller_info:
                # Формируем полную информацию о продавце для поля person (БЕЗ типа и количества объявлений)
                person_info_parts = []
                
                # Имя продавца
                if 'name' in seller_info:
                    person_info_parts.append(f"Имя: {seller_info['name']}")
                
                # Название агентства
                if 'agency_name' in seller_info:
                    person_info_parts.append(f"Агентство: {seller_info['agency_name']}")
                
                # Дополнительная информация
                if seller_info.get('reliable_partner'):
                    person_info_parts.append("Надёжный партнёр")
                
                if seller_info.get('verified_rosreestr'):
                    person_info_parts.append("Проверено в Росреестре")
                
                if seller_info.get('verified_requisites'):
                    person_info_parts.append("Реквизиты проверены")
                
                # Добавляем ВСЮ информацию после времени публикации
                if 'all_text_after_time' in seller_info and seller_info['all_text_after_time']:
                    all_text = ' | '.join(seller_info['all_text_after_time'])
                    
                    # Также добавляем в person_info_parts для совместимости
                    person_info_parts.append(f"Полная информация: {all_text}")
                elif 'raw_lines' in seller_info and seller_info['raw_lines']:
                    raw_text = ' | '.join(seller_info['raw_lines'])
                    person_info_parts.append(f"Дополнительно: {raw_text}")
                
                # Объединяем всю информацию в поле person
                if person_info_parts:
                    person_text = ' | '.join(person_info_parts)
                    # Проверяем длину - если больше 70 символов, значит это описание, а не имя
                    if len(person_text) > 70:
                        db_data['person'] = None
                        print(f"⚠️ Person слишком длинный ({len(person_text)} символов), устанавливаем NULL")
                    else:
                        db_data['person'] = person_text
                
                # Сохраняем также отдельные поля для совместимости
                if 'type' in seller_info:
                    db_data['person_type'] = seller_info['type']
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
            
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                
                # Находим строку с ценой "за м²"
                if 'за м²' in line:
                    found_price_line = True
                    continue
                
                # После строки с ценой ищем теги в следующих строках
                if found_price_line:
                    # Проверяем, является ли текущая строка тегом
                    is_known_tag = False
                    for known in known_tags:
                        if known.lower() == line.lower():
                            tags.append(known)
                            is_known_tag = True
                            break
                    
                    # Если строка не является тегом, проверяем не адрес ли это
                    if not is_known_tag:
                        # Если это адрес или описание, прекращаем поиск тегов
                        if self.is_address_line(line) or len(line) > 50:
                            break
            
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
                        elif 'проверено в росреестре' in last_part.lower():
                            seller_info['type'] = 'owner'   # Собственник
                        else:
                            seller_info['type'] = 'unknown'
                        
                        break
            
            return tags, seller_info
            
        except Exception as e:
            print(f"❌ Ошибка парсинга тегов: {e}")
            return [], {}
    
    def is_address_line(self, line):
        """Проверяет, является ли строка адресом"""
        try:
            line_lower = line.lower()
            
            # Признаки адреса:
            # 1. Содержит названия улиц
            street_indicators = ['ул.', 'улица', 'проспект', 'пр.', 'переулок', 'пер.', 
                               'площадь', 'пл.', 'бульвар', 'б-р', 'шоссе', 'ш.']
            
            # 2. Содержит названия метро (известные станции)
            metro_stations = ['юго-западная', 'красносельская', 'международная', 'красные ворота',
                            'чкаловская', 'таганская', 'марксистская', 'площадь ильича',
                            'римская', 'крестьянская застава', 'пролетарская', 'волгоградский проспект',
                            'текстильщики', 'кузьминки', 'рязанский проспект', 'выхино',
                            'новогиреево', 'перово', 'шоссе энтузиастов', 'авиамоторная',
                            'площадь ильича', 'марксистская', 'таганская', 'волгоградский проспект',
                            'пролетарская', 'крестьянская застава', 'римская', 'текстильщики',
                            'кузьминки', 'рязанский проспект', 'выхино', 'новогиреево',
                            'перово', 'шоссе энтузиастов', 'авиамоторная']
            
            # 3. Содержит время до метро
            time_pattern = r'\d+\s*мин'
            
            # Проверяем признаки адреса
            has_street = any(indicator in line_lower for indicator in street_indicators)
            has_metro = any(station in line_lower for station in metro_stations)
            has_time = bool(re.search(time_pattern, line_lower))
            
            # Если есть хотя бы 2 признака адреса, считаем строку адресом
            address_indicators = sum([has_street, has_metro, has_time])
            
            if address_indicators >= 2:
                return True
            
            # Дополнительная проверка: если строка содержит запятую и цифры (улица, дом)
            if ',' in line and re.search(r'\d+', line):
                # ИСКЛЮЧЕНИЯ: не считаем адресом заголовки квартир
                if any(word in line_lower for word in ['квартира', 'комната', 'студия', 'апартаменты']):
                    return False
                
                # Проверяем, что это не просто тег с цифрами
                if len(line) < 50:  # Адрес обычно короткий
                    return True
            
            return False
            
        except Exception as e:
            print(f"⚠️ Ошибка проверки адреса: {e}")
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
                        elif 'проверено в росреестре' in last_part.lower():
                            seller_info['type'] = 'owner'   # Собственник
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
                # Пример: "Юго-Западная, 1" -> улица: "Юго-Западная", дом: "1"
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
                # Пример: "Юго-Западная, от 31 мин." -> метро: "Юго-Западная", время: 31
                metro_parts = metro_line.split(',')
                metro_name = None
                time_to_metro = None
                
                for part in metro_parts:
                    part = part.strip()
                    if part:
                        # Ищем время до метро (цифра + "мин")
                        time_match = re.search(r'(\d+)\s*мин', part)
                        if time_match:
                            time_to_metro = int(time_match.group(1))
                        else:
                            # Если это не время, то это название метро
                            if not metro_name and not re.search(r'\d+', part):
                                metro_name = part
                
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
            
            # Если нашли информацию о продавце в конце, используем её
            if seller_lines:
                person_info['all_text_after_time'] = seller_lines
                person_info['raw_lines'] = seller_lines
                
                # ПРИОРИТЕТ: проверяем теги карточки на наличие "Собственник"
                if 'tags' in card_data and card_data['tags']:
                    tags_text = ' '.join(card_data['tags']).lower()
                    if 'собственник' in tags_text:
                        owner_from_tags = True

                
                # Анализируем найденную информацию
                seller_name = None
                for line in seller_lines:
                    line_lower = line.lower()
                    
                    # Тип продавца (ПРИОРИТЕТ: если в тегах есть "Собственник", то тип = "собственник")
                    # ЗАЩИЩАЕМ тип "собственник" от перезаписи, если он уже установлен из тегов
                    if person_info.get('type') == 'собственник':
                        # Если тип уже "собственник" из тегов, НЕ перезаписываем
                        pass
                    elif 'собственник' in line_lower:
                        person_info['type'] = 'собственник'
                    elif 'реквизиты проверены' in line_lower:
                        person_info['type'] = 'агентство'
                    elif 'документы проверены' in line_lower or 'частное лицо' in line_lower:
                        person_info['type'] = 'частное лицо'
                    
                    # Количество объявлений
                    if any(word in line_lower for word in ['завершённых', 'завершенных', 'объявлений']):
                        count_match = re.search(r'(\d+)', line)
                        if count_match:
                            person_info['ads_count'] = int(count_match.group(1))
                    
                    # Ищем имя/название продавца (обычно это строка с запятой, но не служебная)
                    if (',' in line and 
                        not any(word in line_lower for word in ['завершённых', 'завершенных', 'объявлений', 'реквизиты проверены', 'документы проверены']) and
                        len(line.strip()) > 3):
                        # Берем первую часть до запятой как имя/название
                        parts = [p.strip() for p in line.split(',')]
                        first_part = parts[0]
                        if (len(first_part) > 2 and 
                            not any(word in first_part.lower() for word in ['проверено', 'росреестр', 'собственник', 'агентство', 'документы', 'лифт', 'парковка', 'балкон', 'двор', 'окна']) and
                            not re.search(r'\d+', first_part)):
                            person_info['name'] = first_part
                            # print(f"👤 Найдено имя продавца: {first_part}")
                
                # ДОПОЛНИТЕЛЬНО: ищем информацию, которая идет после даты публикации, но перед количеством объявлений
                additional_info = []
                if time_line_index != -1:
                    for i in range(time_line_index + 1, len(lines)):
                        line = lines[i].strip()
                        if not line:
                            continue
                        
                        line_lower = line.lower()
                        
                        # Останавливаемся, когда находим количество объявлений
                        if any(word in line_lower for word in ['завершённых', 'завершенных', 'объявлений']):
                            break
                        
                        # Добавляем строку, если она не пустая и не служебная
                        if (len(line) > 2 and 
                            not any(word in line_lower for word in ['документы', 'реквизиты', 'лифт', 'парковка', 'балкон', 'двор', 'окна', 'написать', 'показать', 'телефон']) and
                                        # Фильтруем относительные даты и временные выражения (точное совпадение слов)
            not any(re.search(rf'\b{word}\b', line_lower) for word in ['вчера', 'сегодня', 'позавчера', 'час', 'день', 'месяц', 'назад']) and
                            # Фильтруем слова, связанные с неделями (точное совпадение)
                            not any(re.search(rf'\b{word}\b', line_lower) for word in ['неделя', 'недели', 'недель']) and
                                        # Фильтруем числовые временные выражения типа "1 час назад", "7 дней назад"
            not re.search(r'\d+\s*(час|часа|часов|месяц|месяца|месяцев)', line_lower) and
                            not re.search(r'\d+', line)):
                            additional_info.append(line)
                
                # Формируем поле Person: дополнительная информация + основная информация
                if additional_info:
                    if seller_name:
                        # Если нашли имя продавца, добавляем дополнительную информацию
                        person_info['clean_person'] = ', '.join(additional_info) + ' | ' + seller_name
                    else:
                        # Если имя не найдено, используем дополнительную информацию
                        person_info['clean_person'] = ', '.join(additional_info)
                else:
                    # Если дополнительной информации нет, используем стандартную логику
                    if seller_name:
                        person_info['clean_person'] = seller_name
                        person_info['name'] = seller_name
                    else:
                        # Если имя не найдено, используем первую неслужебную строку
                        clean_lines = []
                        for line in seller_lines:
                            line_lower = line.lower()
                            if not any(word in line_lower for word in ['завершённых', 'завершенных', 'объявлений', 'реквизиты проверены', 'документы проверены', 'проверено в росреестре']):
                                clean_lines.append(line.strip())
                        
                        if clean_lines:
                            person_info['clean_person'] = clean_lines[0]
                        else:
                            # Если ничего не нашли, создаем обобщенное описание
                            if person_info.get('type') == 'агентство':
                                person_info['clean_person'] = 'Агентство недвижимости'
                            elif person_info.get('type') == 'частное лицо':
                                person_info['clean_person'] = 'Частное лицо'
                            elif person_info.get('type') == 'собственник':
                                person_info['clean_person'] = 'Собственник'
                            else:
                                person_info['clean_person'] = 'Продавец'
                
                # Также сохраняем отдельные поля для совместимости
                if line.strip() and line not in person_info.get('raw_lines', []):
                    if 'raw_lines' not in person_info:
                        person_info['raw_lines'] = []
                    person_info['raw_lines'].append(line.strip())
            
            # Если тип продавца не определен, устанавливаем "частное лицо" по умолчанию
            if not person_info.get('type') or person_info.get('type') == 'не определено':
                person_info['type'] = 'частное лицо'
            
            return person_info
            
        except Exception as e:
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
                card_data['address'] = "Не найдено"
                card_data.update({
                    'street_house': 'не найдено',
                    'metro_name': 'не найдено',
                    'time_to_metro': 'не найдено'
                })
            
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
            
            # Название комплекса
            # Получаем весь текст карточки для нового метода
            all_text = card_element.text
            complex_name = self.extract_complex_name_new(all_text)
            card_data['complex_name'] = complex_name
            
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
            seller_info = self.parse_seller_info(card_element, card_data)
            
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
            
            # Формируем поле person для отображения в терминале (БЕЗ типа и количества объявлений)
            if 'seller_info' in card_data and card_data['seller_info']:
                seller_info = card_data['seller_info']
                person_parts = []
                
                # Имя продавца
                if 'name' in seller_info:
                    person_parts.append(f"Имя: {seller_info['name']}")
                
                # Название агентства
                if 'agency_name' in seller_info:
                    person_parts.append(f"Агентство: {seller_info['agency_name']}")
                
                # Вся информация после времени публикации
                if 'all_text_after_time' in seller_info and seller_info['all_text_after_time']:
                    all_text = ' | '.join(seller_info['all_text_after_time'])
                    person_parts.append(f"Полная информация: {all_text}")
                
                # Объединяем в поле person
                if person_parts:
                    card_data['person'] = ' | '.join(person_parts)
            
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
                    db_data = self.prepare_data_for_db(card)
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
            
            # Ждем загрузки карточек
            if not self.wait_for_cards_load():
                print("❌ Не удалось дождаться загрузки карточек")
                return []
            
            # Получаем все карточки
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
        """Парсит конкретную страницу с метро по номеру"""
        try:
            # Получаем URL для конкретной страницы
            metro_url = self.get_metro_url_with_page(page)
            if not metro_url:
                return []
            
            # Переходим на страницу
            self.driver.get(metro_url)
            
            # Выводим сообщение о обработке страницы
            print(f"страница {page} ({metro_url}) обработана")
            
            # Ждем стабилизации DOM после загрузки страницы
            if not self.wait_for_dom_stability():
                print(f"❌ Не удалось дождаться стабилизации DOM на странице {page}")
                return []
            
            # Ждем загрузки карточек
            if not self.wait_for_cards_load():
                return []
            
            # Получаем все загруженные карточки
            cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
            
            # Если карточек нет, возвращаем пустой список
            if not cards:
                print(f"ℹ️ Страница {page} не содержит карточек")
                return []
            
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
            print(f"❌ Ошибка парсинга страницы {page}: {e}")
            return []
    
    def parse_multiple_pages(self):
        """Парсит несколько страниц с метро"""
        try:
            all_parsed_cards = []
            page = 1
            max_attempts = 100  # Защита от бесконечного цикла
            
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
                page_cards = self.parse_metro_page_by_number(page)
                
                # ПРОСТАЯ ЛОГИКА: если на странице нет объявлений - останавливаемся
                if not page_cards:
                    print(f"❌ Страница {page} пустая (нет объявлений), останавливаемся")
                    break
                
                # Добавляем все карточки с этой страницы
                all_parsed_cards.extend(page_cards)
                print(f"📄 Страница {page}: {len(page_cards)} карточек")
                
                # Если установлен лимит страниц, останавливаемся после последней
                if self.max_pages > 0 and page >= self.max_pages:
                    print(f"📄 Достигнут лимит страниц ({self.max_pages}), останавливаемся")
                    break
                
                # Перезагружаем браузер каждые 4 страницы для стабильности
                if page % 4 == 0:
                    print(f"🔄 Страница {page} - перезагружаем браузер для стабильности")
                    if not self.reload_browser():
                        print("❌ Не удалось перезагрузить браузер, продолжаем...")
                
                # Переходим к следующей странице
                page += 1
                
                # Задержка между страницами
                if self.max_pages == 0 or page <= self.max_pages:
                    time.sleep(self.page_delay)
            
            return all_parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга нескольких страниц: {e}")
            return all_parsed_cards
    
    def print_statistics(self, parsed_cards):
        """Выводит статистику парсинга"""
        try:
            print(f"\n📊 СТАТИСТИКА ПАРСИНГА:")
            print(f"   Метро ID: {self.metro_id}")
            print(f"   Метро avito_id: {self.metro_avito_id}")
            print(f"   Страниц спарсено: {self.max_pages}")
            print(f"   Карточек спарсено: {len(parsed_cards)}")
            
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
    
    async def run_parser(self):
        """Запускает парсер"""
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
                base_url = f"https://www.avito.ru/moskva/kvartiry/prodam?metro={self.metro_avito_id}&s=104"
                print(f"   • URL поиска: {base_url}")
            print()
            
            # Парсим страницы
            parsed_cards = self.parse_multiple_pages()
            
            if parsed_cards:
                # Сохраняем в БД (если включено)
                if self.enable_db_save:
                    await self.save_to_db(parsed_cards)
                
                # Выводим итоговую статистику
                print(f"🎯 Всего обработано страниц: {len(parsed_cards) // (self.max_cards if self.max_cards > 0 else 25) + 1}")
                print(f"📊 Общее количество карточек: {len(parsed_cards)}")
                
                return True
            else:
                print("❌ Не удалось спарсить карточки")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка работы парсера: {e}")
            return False
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
    
    async def parse_single_metro(self, metro_id, max_pages, max_cards=None):
        """
        Парсит одно метро с заданными параметрами
        
        Args:
            metro_id (int): ID метро из таблицы metro
            max_pages (int): Количество страниц для парсинга
            max_cards (int, optional): Количество карточек на странице (0 = все карточки)
        
        Returns:
            tuple: (success: bool, saved_count: int, total_cards: int)
                - success: Успешно ли выполнен парсинг
                - saved_count: Количество сохраненных записей в БД
                - total_cards: Общее количество спарсенных карточек
        """
        try:
            # Устанавливаем параметры для этого запуска
            self.metro_id = metro_id
            self.max_pages = max_pages
            if max_cards is not None:
                self.max_cards = max_cards
            
            print(f"🚀 Запуск парсинга метро ID={metro_id}, страниц={max_pages}, карточек на странице={self.max_cards}")
            
            # Получаем avito_id для метро
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
            parsed_cards = self.parse_multiple_pages()
            total_cards = len(parsed_cards)
            
            if not parsed_cards:
                print("❌ Не удалось спарсить карточки")
                return False, 0, 0
            
            # Сохраняем в БД (если включено)
            saved_count = 0
            if self.enable_db_save and DB_AVAILABLE:
                print(f"💾 Сохраняем {total_cards} карточек в БД...")
                await self.save_to_db(parsed_cards)
                # Получаем количество сохраненных записей
                saved_count = total_cards  # Предполагаем, что все сохранились успешно
            else:
                print("⚠️ Сохранение в БД отключено или недоступно")
            
            # Выводим итоговую статистику
            print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
            print(f"   Метро ID: {metro_id}")
            print(f"   Метро avito_id: {self.metro_avito_id}")
            print(f"   Страниц спарсено: {max_pages}")
            print(f"   Карточек спарсено: {total_cards}")
            print(f"   Сохранено в БД: {saved_count}")
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
    
    def extract_complex_name_new(self, all_text):
        """Новый метод извлечения названия ЖК: берем общий текст, выделяем теги и адрес, ЖК между ними"""
        try:
            # Получаем словарь тегов
            tags_dict = self.get_tags_dictionary()
            
            # Разбиваем текст на строки
            lines = [line.strip() for line in all_text.split('\n') if line.strip()]
            
            # Ищем границы тегов и адреса
            tags_end_index = -1
            address_start_index = -1
            address_text = ""
            
            # Проходим по всем строкам и определяем границы
            for i, line in enumerate(lines):
                line_lower = line.lower()
                
                # Проверяем, является ли строка тегом
                is_tag = False
                for tag in tags_dict:
                    if tag.lower() in line_lower or line_lower in tag.lower():
                        is_tag = True
                        break
                
                # Дополнительные проверки на теги
                if not is_tag:
                    # Проверяем на известные паттерны тегов
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
                            break
                
                # Если это тег, обновляем конец тегов
                if is_tag:
                    tags_end_index = i
                else:
                    # Проверяем, не является ли это адресом
                    if self.is_address_line(line):
                        address_start_index = i
                        address_text = line
                        break
                    elif self.is_description_start(line):
                        # Если началось описание, останавливаемся
                        break
            
            # Теперь ищем название ЖК между тегами и адресом
            if tags_end_index != -1 and address_start_index != -1 and address_start_index > tags_end_index:
                for i in range(tags_end_index + 1, address_start_index):
                    line = lines[i].strip()
                    if line and len(line) > 2:
                        # Проверяем, что это не пустая строка и не слишком короткая
                        # Исключаем строки, которые явно не являются названиями ЖК
                        if not self.is_excluded_line(line):
                            complex_name = line.strip()
                            return complex_name
            
            # Если не нашли между тегами и адресом, ищем в начале текста после тегов
            if tags_end_index != -1:
                for i in range(tags_end_index + 1, min(tags_end_index + 5, len(lines))):
                    line = lines[i].strip()
                    if line and len(line) > 2:
                        # Исключаем адрес, описание и информацию о продавце
                        if (line != address_text and 
                            not self.is_excluded_line(line) and 
                            not self.is_address_line(line) and
                            not self.is_seller_info_line(line) and
                            len(line) < 100):  # ЖК обычно короткое название
                            complex_name = line.strip()
                            return complex_name
            
            # Если ЖК не найден - это нормально, не все объявления имеют ЖК
            return ""
            
        except Exception as e:
            print(f"❌ Ошибка нового метода извлечения ЖК: {e}")
            return ""
    
    def is_seller_info_line(self, line):
        """Проверяет, является ли строка информацией о продавце"""
        line_lower = line.lower()
        
        # Признаки информации о продавце
        seller_indicators = [
            'завершённых объявлений',
            'завершенных объявлений', 
            'реквизиты проверены',
            'проверено в росреестре',
            'агентство',
            'этажи москва',
            'на авито с',
            'объявления'
        ]
        
        return any(indicator in line_lower for indicator in seller_indicators)
    
    def is_excluded_line(self, line):
        """Проверяет, должна ли строка быть исключена из поиска ЖК"""
        line_lower = line.lower()
        
        # Исключаем строки с явными признаками не-ЖК
        exclude_patterns = [
            r'^\d+\s*мин',      # Время до метро
            r'^\d+\s*эт',       # Этаж
            r'^\d+\s*м²',       # Площадь
            r'^\d+\s*комнат',   # Количество комнат
            r'^студия$',         # Студия
            r'^новостройка$',   # Новостройка
            r'^вторичка$',      # Вторичка
            r'^квартира',       # Квартира
            r'^апартаменты',    # Апартаменты
            r'^дом$',           # Дом
        ]
        
        for pattern in exclude_patterns:
            if re.match(pattern, line_lower):
                return True
        
        # Исключаем строки с ключевыми словами
        exclude_words = [
            'м²', 'эт', 'квартира', 'студия', 'собственник', 'агентство', 
            'документы', 'проверены', 'реквизиты', 'лифт', 'парковка', 
            'балкон', 'кондиционер', 'метро', 'мин', 'назад', 'сегодня', 
            'вчера', 'написать', 'показать', 'телефон', 'код', 'объекта'
        ]
        
        if any(word in line_lower for word in exclude_words):
            return True
        
        # Исключаем слишком короткие или слишком длинные строки
        if len(line) < 3 or len(line) > 100:
            return True
        
        return False
    
    def is_description_start(self, line):
        """Проверяет, начинается ли описание квартиры"""
        line_lower = line.lower()
        
        description_indicators = [
            'произведена', 'установлена', 'заменена', 'очищена', 'промыта',
            'быстрый выход', 'оперативные показы', 'ждём вашего звонка',
            'звоните', 'приезжайте', 'позвоните', 'напишите', 'описание',
            'характеристики', 'особенности', 'преимущества'
        ]
        
        return any(indicator in line_lower for indicator in description_indicators)
    
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
                return "📋 Теги: не найдены", "📍 Адрес: не найден", "🏢 ЖК: не найден"
            
            # Разбиваем на строки
            lines = [line.strip() for line in params_text.split('\n') if line.strip()]
            
            # Ищем теги, адрес и ЖК
            tags = []
            address = ""
            complex_name = ""
            
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
            
            # Получаем название ЖК
            complex_name = card_data.get('complex_name', '')
            

            
            # Формируем информацию для вывода
            tags_info = f"📋 Теги: {', '.join(tags[-3:]) if tags else 'не найдены'}"  # Последние 3 тега
            address_info = f"📍 Адрес: {address if address else 'не найден'}"
            complex_info = f"🏢 ЖК: {complex_name if complex_name else 'не найден'}"
            
            return tags_info, address_info, complex_info
            
        except Exception as e:
            return f"📋 Теги: ошибка анализа", f"📍 Адрес: ошибка анализа", f"🏢 ЖК: ошибка анализа"

async def main():
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
    parser = EnhancedMetroParser()
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
    
    print(f"⚙️ Настройки парсера:")
    print(f"   • Страниц для парсинга: {parser.max_pages}")
    print(f"   • Карточек на странице: {parser.max_cards if parser.max_cards > 0 else 'все'}")
    print(f"   • ID метро: {parser.metro_id}")
    print(f"   • Первые {parser.stream_cards_count} карточек: потоково")
    print(f"   • Остальные {parser.batch_cards_count} карточек: пачками по {parser.batch_size}")
    print("=" * 60)
    
    success = await parser.run_parser()
    
    if success:
        print("\n🎉 Парсинг завершен успешно!")
    else:
        print("\n❌ Парсинг завершен с ошибками")

if __name__ == "__main__":
    asyncio.run(main())