#!/usr/bin/env python3
"""
Парсер объявлений Avito по всем метро в Москве
Извлекает данные со страниц поиска по каждой станции метро и сохраняет в БД

ФИЛЬТРЫ ПОИСКА:
1. Тип жилья: вторичка (ASgBAgICAkSSA8YQ5geMUg)
2. Метро: все станции Москвы (metro.is_msk IS NOT FALSE)
3. Сортировка: по дате (s=104)

ИСПОЛЬЗОВАНИЕ:
1. Установите настройки в config_parser.py или используйте значения по умолчанию
2. Запустите скрипт: python parse_avito.py

АРГУМЕНТЫ КОМАНДНОЙ СТРОКИ:
--metro-id ID     - парсить конкретную станцию метро по ID
--max-cards N     - максимальное количество карточек на страницу
--max-pages N     - максимальное количество страниц для каждой станции
--headless        - запуск в headless режиме
--no-db           - не сохранять в БД (только парсинг)
"""

import json
import os
import time
import asyncio
import asyncpg
import argparse
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

class AvitoMetroParser:
    def __init__(self, metro_id=None, max_cards=None, max_pages=None, headless=None, enable_db=None):
        # Настройки из конфигурации или значения по умолчанию
        if CONFIG_LOADED:
            self.max_cards = max_cards or MAX_CARDS_PER_PAGE
            self.max_pages = max_pages or MAX_PAGES
            self.metro_id = metro_id or METRO_ID
            self.page_delay = PAGE_DELAY
            self.page_load_delay = PAGE_LOAD_DELAY
            self.cards_load_timeout = CARDS_LOAD_TIMEOUT
            self.headless_mode = headless if headless is not None else HEADLESS_MODE
            self.cookies_file = COOKIES_FILE
            
            # Настройки плавной прокрутки
            self.enable_smooth_scroll = ENABLE_SMOOTH_SCROLL
            self.scroll_pause = SCROLL_PAUSE
            self.max_scroll_attempts = MAX_SCROLL_ATTEMPTS
            
            # Настройки базы данных
            self.enable_db_save = enable_db if enable_db is not None else ENABLE_DB_SAVE
            
            print("✅ Конфигурация загружена из config_parser.py")
        else:
            self.max_cards = max_cards or 15
            self.max_pages = max_pages or 1
            self.metro_id = metro_id or None  # None означает все метро
            self.page_delay = 5
            self.page_load_delay = 5
            self.cards_load_timeout = 30
            self.headless_mode = headless if headless is not None else False
            self.cookies_file = "avito_cookies.json"
            
            # Настройки плавной прокрутки по умолчанию
            self.enable_smooth_scroll = True
            self.scroll_pause = 1.5
            self.max_scroll_attempts = 10
            
            # Настройки базы данных по умолчанию
            self.enable_db_save = enable_db if enable_db is not None else True
        
        self.driver = None
        self.database_url = None
        self.metro_stations = []  # Список всех станций метро
        
    async def get_all_metro_stations(self):
        """Получает все московские станции метро из БД для обработки (is_msk IS NOT FALSE)"""
        try:
            # Загружаем переменные окружения
            load_dotenv()
            self.database_url = os.getenv('DATABASE_URL')
            
            if not self.database_url:
                print("❌ DATABASE_URL не найден в переменных окружения")
                return False
            
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем все московские станции метро
            stations = await conn.fetch("""
                SELECT id, name, avito_id
                FROM metro
                WHERE is_msk IS NOT FALSE
                ORDER BY id
            """)
            
            await conn.close()
            
            self.metro_stations = [dict(station) for station in stations]
            
            if not self.metro_stations:
                print("❌ Не найдено станций метро в Москве")
                return False
            
            print(f"✅ Загружено {len(self.metro_stations)} станций метро Москвы")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка получения станций метро: {e}")
            return False
    
    def convert_relative_time_to_date(self, relative_time):
        """Преобразует относительное время в дату"""
        try:
            if not relative_time:
                return None
                
            now = datetime.now()
            relative_time_lower = relative_time.lower().strip()
            
            # Паттерны для относительного времени
            if 'сегодня' in relative_time_lower:
                return now.date()
            elif 'вчера' in relative_time_lower:
                yesterday = now - timedelta(days=1)
                return yesterday.date()
            elif 'позавчера' in relative_time_lower:
                day_before_yesterday = now - timedelta(days=2)
                return day_before_yesterday.date()
            
            # Паттерны с количеством времени
            time_patterns = [
                (r'(\d+)\s*(час|часа|часов)\s*назад', 'hours'),
                (r'(\d+)\s*(день|дня|дней)\s*назад', 'days'),
                (r'(\d+)\s*(недел|неделя|недели|недель)\s*назад', 'weeks'),
                (r'(\d+)\s*(месяц|месяца|месяцев)\s*назад', 'months')
            ]
            
            for pattern, unit in time_patterns:
                match = re.search(pattern, relative_time_lower)
                if match:
                    count = int(match.group(1))
                    
                    if unit == 'hours':
                        target_time = now - timedelta(hours=count)
                    elif unit == 'days':
                        target_time = now - timedelta(days=count)
                    elif unit == 'weeks':
                        target_time = now - timedelta(weeks=count)
                    elif unit == 'months':
                        # Приблизительно, месяц = 30 дней
                        target_time = now - timedelta(days=count * 30)
                    
                    return target_time.date()
            
            # Если это конкретная дата (например, "12 июля 13:35")
            month_names = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
            }
            
            # Ищем формат "12 июля"
            for month_name, month_num in month_names.items():
                if month_name in relative_time_lower:
                    day_match = re.search(r'(\d{1,2})\s+' + month_name, relative_time_lower)
                    if day_match:
                        day = int(day_match.group(1))
                        year = now.year
                        return datetime(year, month_num, day).date()
            
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
                        return datetime(year, month, day).date()
                    
                    elif len(match.groups()) == 3:
                        # Формат "12.07.2024"
                        day = int(match.group(1))
                        month = int(match.group(2))
                        year = int(match.group(3))
                        return datetime(year, month, day).date()
            
            return None
            
        except Exception as e:
            print(f"⚠️ Ошибка преобразования времени в дату: {e}")
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
            if self.headless_mode:
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
            time.sleep(5)
            
            # Проверяем, что страница загрузилась
            try:
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
                    
                except Exception as e:
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
    
    def generate_search_context(self) -> str:
        """Генерирует простой context для каждого запроса"""
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
    
    def get_metro_url_with_page(self, metro_avito_id, page=1):
        """Получает URL для метро с пагинацией и context"""
        if not metro_avito_id:
            print("❌ avito_id для метро не определен")
            return None
            
        # URL для вторички с правильным avito_id и сортировкой
        base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
        metro_url = f"{base_url}?metro={metro_avito_id}&s=104"
        
        # Добавляем пагинацию (Avito использует параметр p)
        if page > 1:
            metro_url += f"&p={page}"
        
        # Генерируем новый context для каждой страницы
        context = self.generate_search_context()
        metro_url += f"&context={context}"
        
        return metro_url
    
    def wait_for_cards_load(self, timeout=30):
        """Ждет загрузки карточек"""
        try:
            print("⏳ Ждем загрузки карточек...")
            
            # Используем таймаут из конфигурации или переданный параметр
            actual_timeout = self.cards_load_timeout if hasattr(self, 'cards_load_timeout') else timeout
            
            # Ждем появления карточек
            wait = WebDriverWait(self.driver, actual_timeout)
            cards = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-marker="item"]')))
            
            print(f"✅ Загружено карточек: {len(cards)}")
            return True
            
        except TimeoutException:
            print("❌ Таймаут ожидания карточек")
            return False
        except Exception as e:
            print(f"❌ Ошибка ожидания карточек: {e}")
            return False
    
    def smooth_scroll_and_load_cards(self, target_cards=20, scroll_pause=1.5):
        """Плавно прокручивает страницу и загружает карточки"""
        try:
            # Проверяем, включена ли плавная прокрутка
            if not self.enable_smooth_scroll:
                print("⏭️ Плавная прокрутка отключена, пропускаем...")
                return target_cards
            
            print("🔄 Начинаем плавную прокрутку для загрузки карточек...")
            
            # Используем настройки из конфигурации
            actual_scroll_pause = self.scroll_pause if hasattr(self, 'scroll_pause') else scroll_pause
            max_attempts = self.max_scroll_attempts if hasattr(self, 'max_scroll_attempts') else 10
            
            # Проверяем, сколько карточек уже загружено
            initial_cards = len(self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]'))
            current_cards = initial_cards
            scroll_attempts = 0
            
            # Если уже достаточно карточек, не прокручиваем
            if current_cards >= target_cards:
                print(f"✅ Уже загружено достаточно карточек: {current_cards} (цель: {target_cards})")
                return current_cards
            
            while current_cards < target_cards and scroll_attempts < max_attempts:
                # Получаем текущее количество карточек
                cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                current_cards = len(cards)
                
                if current_cards > initial_cards:
                    print(f"📊 Загружено карточек: {current_cards} (цель: {target_cards})")
                    initial_cards = current_cards
                
                # Если достигли цели, прекращаем СРАЗУ
                if current_cards >= target_cards:
                    print(f"🎯 Достигнута цель: {target_cards} карточек, прекращаем прокрутку")
                    return current_cards  # Возвращаем текущее количество и выходим
                
                # Плавно прокручиваем вниз
                print(f"⬇️ Прокрутка {scroll_attempts + 1}/{max_attempts}...")
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Ждем загрузки новых карточек
                time.sleep(actual_scroll_pause)
                
                # Проверяем, появились ли новые карточки
                new_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                if len(new_cards) <= current_cards:
                    print("⏸️ Новые карточки не загружаются, пробуем еще раз...")
                    time.sleep(actual_scroll_pause * 2)
                
                scroll_attempts += 1
            
            # Финальная проверка
            final_cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
            print(f"✅ Финальная загрузка: {len(final_cards)} карточек")
            
            if scroll_attempts >= max_attempts:
                print("⚠️ Достигнут лимит попыток прокрутки")
            
            return len(final_cards)
            
        except Exception as e:
            print(f"❌ Ошибка плавной прокрутки: {e}")
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                return len(cards)
            except:
                return 0
    
    def parse_card(self, card_element):
        """Парсит одну карточку"""
        try:
            card_data = {}
            
            # ID объявления
            try:
                item_id = card_element.get_attribute('data-item-id')
                if item_id:
                    card_data['item_id'] = item_id
            except:
                pass
            
            # Заголовок
            try:
                title_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-title"]')
                title_text = title_elem.text.strip()
                card_data['title'] = title_text
            except:
                card_data['title'] = "Не найдено"
            
            # Цена
            try:
                price_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-price"]')
                card_data['price'] = price_elem.text.strip()
            except:
                card_data['price'] = "Не найдено"
            
            # Адрес/метро
            try:
                address_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-address"]')
                card_data['address'] = address_elem.text.strip()
            except:
                card_data['address'] = "Не найдено"
            
            # Ссылка на карточку
            try:
                link_elem = card_element.find_element(By.CSS_SELECTOR, 'a[data-marker="item-title"]')
                raw_url = link_elem.get_attribute('href')
                card_data['url'] = self.clean_url_path(raw_url)
            except:
                card_data['url'] = "Не найдено"
            
            # Время публикации
            try:
                time_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-date"]')
                card_data['published_time'] = time_elem.text.strip()
            except:
                card_data['published_time'] = "Не найдено"
            
            # Извлекаем информацию из заголовка (комнаты, площадь, этаж)
            title = card_data.get('title', '')
            if title and title != 'Не найдено':
                # Парсим заголовок на компоненты
                title_components = self.parse_title(title)
                card_data.update(title_components)
            
            # Адрес (улица и дом)
            address = card_data.get('address', '')
            if address and address != 'Не найдено':
                # Парсим адрес на компоненты
                address_components = self.parse_address(address)
                card_data.update(address_components)
            
            # Время до метро (пока не знаем, как извлекать)
            if 'time_to_metro' not in card_data:
                card_data['time_to_metro'] = 'не указано'
            
            # Название ЖК (пока не знаем, как извлекать)
            if 'complex_name' not in card_data:
                card_data['complex_name'] = ''
            
            # Ищем информацию о продавце и теги
            try:
                # Ищем информацию о продавце после времени публикации
                person_info = self.find_person_info_after_time(card_element, card_data)
                if person_info:
                    card_data['seller_info'] = person_info
                    if 'clean_person' in person_info:
                        card_data['person'] = person_info['clean_person']
                    if 'type' in person_info:
                        card_data['person_type'] = person_info['type']
                
                # Ищем теги в характеристиках карточки
                # Сначала пробуем найти характеристики по разным селекторам
                params_text = ""
                params_elem = None
                
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
                
                # Если нашли характеристики, парсим теги
                if params_text:
                    tags, seller_info_from_tags = self.parse_tags_from_params(params_text)
                    card_data['tags'] = tags
                    card_data['params'] = params_text
                    
                    # Если тип продавца не определен из person_info, используем из тегов
                    if not card_data.get('person_type') and seller_info_from_tags.get('type'):
                        card_data['person_type'] = seller_info_from_tags['type']
                else:
                    card_data['tags'] = []
                    card_data['params'] = ''
                
            except Exception as e:
                print(f"❌ Ошибка извлечения информации о продавце/тегов: {e}")
                card_data['tags'] = []
                card_data['params'] = ''
                if 'person' not in card_data:
                    card_data['person'] = 'Информация не найдена'
                if 'person_type' not in card_data:
                    card_data['person_type'] = 'не определено'
            
            return card_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга карточки: {e}")
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
            
            # Если нашли информацию о продавце в конце, используем её
            if seller_lines:
                person_info['all_text_after_time'] = seller_lines
                person_info['raw_lines'] = seller_lines
                
                # Анализируем найденную информацию
                seller_name = None
                for line in seller_lines:
                    line_lower = line.lower()
                    
                    # Тип продавца
                    if 'собственник' in line_lower:
                        person_info['type'] = 'собственник'
                    elif 'реквизиты проверены' in line_lower:
                        person_info['type'] = 'агентство'
                    elif 'документы проверены' in line_lower or 'частное лицо' in line_lower:
                        person_info['type'] = 'частное лицо'
                    
                    # Количество объявлений
                    count_match = re.search(r'(\d+)\s+(?:завершённых|завершенных)\s+объявлений', line_lower)
                    if count_match:
                        person_info['ads_count'] = int(count_match.group(1))
                    
                    # Имя продавца (первая строка с запятой, которая не содержит техническую информацию)
                    if (',' in line and 
                        not any(word in line_lower for word in ['проверено', 'росреестр', 'собственник', 'агентство', 'документы', 'лифт', 'парковка', 'балкон', 'двор', 'окна', 'завершённых', 'завершенных', 'объявлений']) and
                        not re.search(r'\d+', line) and
                        not seller_name):
                        # Разбиваем строку по запятой и берем первую часть как имя
                        parts = line.split(',')
                        if parts:
                            first_part = parts[0].strip()
                            if first_part and len(first_part) > 2:
                                seller_name = first_part
                                person_info['name'] = seller_name
                
                # Формируем clean_person
                if seller_name:
                    additional_info = []
                    for line in seller_lines:
                        line_lower = line.lower()
                        if any(word in line_lower for word in ['проверено', 'росреестр', 'реквизиты проверены', 'документы проверены']):
                            additional_info.append(line)
                    
                    if additional_info:
                        person_info['clean_person'] = ', '.join(additional_info) + ' | ' + seller_name
                    else:
                        person_info['clean_person'] = ', '.join(additional_info)
                else:
                    person_info['clean_person'] = seller_name
                
                # Если имя не найдено, используем первую строку
                if not person_info.get('name'):
                    clean_lines = [line for line in seller_lines if line and len(line) > 2]
                    if clean_lines:
                        person_info['name'] = clean_lines[0]
                        person_info['clean_person'] = clean_lines[0]
                
                # Устанавливаем тип по умолчанию, если не определен
                if not person_info.get('type'):
                    person_info['type'] = 'частное лицо'
                
                # Сохраняем сырые строки для отладки
                person_info['raw_lines'] = []
                for line in seller_lines:
                    if line and len(line) > 2:
                        person_info['raw_lines'].append(line)
            
            return person_info
            
        except Exception as e:
            print(f"❌ Ошибка поиска информации о продавце: {e}")
            return {}
    
    def parse_tags_from_params(self, params_text):
        """Парсит теги из характеристик карточки"""
        try:
            if not params_text or params_text == "Не найдено":
                return [], {}
            
            # Разбиваем текст на строки и ищем теги
            lines = params_text.strip().split('\n')
            tags = []
            seller_info = {}
            
            # Флаг для остановки парсинга тегов
            stop_parsing = False
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Проверяем, не началось ли описание (обычно после адреса)
                # Останавливаем парсинг тегов, если встретили:
                # 1. Описание квартиры/дома
                # 2. Адрес (содержит улицу, дом, метро)
                # 3. Призывы к действию
                if any(stop_word in line.lower() for stop_word in [
                    'произведена', 'установлена', 'заменена', 'очищена', 'промыта',
                    'быстрый выход', 'оперативные показы', 'ждём вашего звонка',
                    'звоните', 'приезжайте', 'позвоните', 'напишите'
                ]):
                    stop_parsing = True
                    break
                
                # Разбиваем строку по запятым для получения отдельных тегов
                line_tags = [tag.strip() for tag in line.split(',') if tag.strip()]
                
                for tag in line_tags:
                    # Очищаем тег от лишних символов
                    clean_tag = tag.strip()
                    if clean_tag and len(clean_tag) > 2:  # Минимальная длина тега
                        # Ищем известные теги
                        known_tags = [
                            'Проверено в Росреестре',
                            'Реквизиты проверены',
                            'Рыночная цена',
                            'Собственник',
                            'Документы проверены',
                            'Документы готовы',
                            'Без отделки',
                            'Студия',
                            'Агентство',
                            'Застройщик',
                            'Частное лицо',
                            'Новостройка',
                            'Вторичка',
                            'С мебелью',
                            'Без мебели',
                            'Евроремонт',
                            'Косметический ремонт',
                            'Капитальный ремонт',
                            'Требует ремонта',
                            'С отделкой',
                            'Без отделки',
                            'С видом',
                            'Балкон',
                            'Лоджия',
                            'Окна во двор',
                            'Окна на улицу',
                            'Парковка',
                            'Лифт',
                            'Консьерж',
                            'Охрана',
                            'Двор',
                            'Детская площадка',
                            'Магазины рядом',
                            'Транспорт рядом',
                            'Надёжный партнёр',
                            'Свободная продажа',
                            'Срочная продажа',
                            'Новое объявление'
                        ]
                        
                        # Проверяем, является ли тег известным
                        is_known = False
                        for known in known_tags:
                            if known.lower() in clean_tag.lower() or clean_tag.lower() in known.lower():
                                tags.append(known)  # Добавляем стандартизированное название
                                is_known = True
                                break
                        
                        # Если тег не известен, но выглядит как валидный, добавляем его
                        if not is_known and not any(char in clean_tag for char in ['₽', 'м²', 'мин', 'эт']):
                            # Дополнительные фильтры для исключения лишней информации
                            if len(clean_tag) < 50:  # Максимальная длина тега
                                tags.append(clean_tag)
            
            # Определяем тип продавца по тегам
            tags_text = ' '.join(tags).lower()
            if 'собственник' in tags_text:
                seller_info['type'] = 'собственник'
            elif 'агентство' in tags_text:
                seller_info['type'] = 'агентство'
            elif 'застройщик' in tags_text:
                seller_info['type'] = 'застройщик'
            elif 'частное лицо' in tags_text:
                seller_info['type'] = 'частное лицо'
            else:
                seller_info['type'] = 'не определено'
            
            return tags, seller_info
            
        except Exception as e:
            print(f"❌ Ошибка парсинга тегов: {e}")
            return [], {}
    
    def prepare_data_for_db(self, card_data, metro_name):
        """Подготавливает данные карточки для сохранения в БД ads_avito"""
        try:
            from datetime import datetime
            
            # Создаем структуру данных для БД
            db_data = {}
            
            # URL
            db_data['URL'] = card_data.get('url', '')
            
            # ID объявления
            db_data['avitoid'] = card_data.get('item_id', '')
            
            # Заголовок
            db_data['title'] = card_data.get('title', '')
            
            # Цена
            price = card_data.get('price', '')
            if price and price != 'Не найдено':
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
                    db_data['area'] = float(area)
                except:
                    db_data['area'] = None
            else:
                db_data['area'] = None
            
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
            
            # Метро - берем только первую часть до запятой
            metro_name_clean = metro_name
            if metro_name and ',' in metro_name:
                db_data['metro'] = metro_name.split(',')[0].strip()
            else:
                db_data['metro'] = metro_name
            
            # Время до метро
            time_to_metro = card_data.get('time_to_metro', '')
            if time_to_metro and time_to_metro != 'не указано':
                try:
                    db_data['min_metro'] = int(time_to_metro)
                except:
                    db_data['min_metro'] = None
            else:
                db_data['min_metro'] = None
            
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
            
            # Информация о продавце (Person) - всё что после времени публикации
            seller_info = card_data.get('seller_info', {})
            
            # ПРИОРИТЕТ: используем clean_person из seller_info, если он есть
            if seller_info and seller_info.get('clean_person'):
                db_data['person'] = seller_info['clean_person']
                
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
                    db_data['person'] = ' | '.join(person_info_parts)
                
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
                if source_created:
                    db_data['source_created'] = source_created
                else:
                    db_data['source_created'] = published_time
            else:
                db_data['source_created'] = None
            
            # Текущее время
            db_data['updated_at'] = datetime.now()
            
            # Тип объекта (1 - квартира, 2 - комната)
            db_data['object_type_id'] = 1  # По умолчанию квартира
            
            return db_data
            
        except Exception as e:
            print(f"❌ Ошибка подготовки данных для БД: {e}")
            return None
    
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
    
    def convert_relative_time_to_date(self, relative_time):
        """Преобразует относительное время в дату"""
        try:
            if not relative_time:
                return None
                
            now = datetime.now()
            relative_time_lower = relative_time.lower().strip()
            
            # Паттерны для относительного времени
            if 'сегодня' in relative_time_lower:
                return now.date()
            elif 'вчера' in relative_time_lower:
                yesterday = now - timedelta(days=1)
                return yesterday.date()
            elif 'позавчера' in relative_time_lower:
                day_before_yesterday = now - timedelta(days=2)
                return day_before_yesterday.date()
            
            # Паттерны с количеством времени
            time_patterns = [
                (r'(\d+)\s*(час|часа|часов)\s*назад', 'hours'),
                (r'(\d+)\s*(день|дня|дней)\s*назад', 'days'),
                (r'(\d+)\s*(недел|неделя|недели|недель)\s*назад', 'weeks'),
                (r'(\d+)\s*(месяц|месяца|месяцев)\s*назад', 'months')
            ]
            
            for pattern, unit in time_patterns:
                match = re.search(pattern, relative_time_lower)
                if match:
                    count = int(match.group(1))
                    
                    if unit == 'hours':
                        target_time = now - timedelta(hours=count)
                    elif unit == 'days':
                        target_time = now - timedelta(days=count)
                    elif unit == 'weeks':
                        target_time = now - timedelta(weeks=count)
                    elif unit == 'months':
                        # Приблизительно, месяц = 30 дней
                        target_time = now - timedelta(days=count * 30)
                    
                    return target_time.date()
            
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
                        
                        return datetime(year, month_num, day).date()
            
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
                        
                        return datetime(year, month, day).date()
                    
                    elif len(match.groups()) == 3:
                        # Формат "12.07.2024"
                        day = int(match.group(1))
                        month = int(match.group(2))
                        year = int(match.group(3))
                        return datetime(year, month, day).date()
            
            return None
            
        except Exception as e:
            print(f"❌ Ошибка конвертации времени: {e}")
            return None
    
    async def save_to_db(self, parsed_cards, metro_name):
        """Сохраняет карточки в БД"""
        if not DB_AVAILABLE or not self.enable_db_save:
            return False
            
        try:
            # Создаем таблицу если её нет
            await create_ads_avito_table()
            
            saved_count = 0
            for i, card in enumerate(parsed_cards):
                try:
                    # Подготавливаем данные для БД
                    db_data = self.prepare_data_for_db(card, metro_name)
                    if db_data:
                        # Сохраняем в БД
                        await save_avito_ad(db_data)
                        saved_count += 1
                except Exception as e:
                    pass
            
            return saved_count > 0
            
        except Exception as e:
            return False
    
    async def parse_metro_station(self, metro_station):
        """Парсит одну станцию метро"""
        try:
            metro_name = metro_station['name']
            metro_avito_id = metro_station['avito_id']
            
            print(f"\n📍 Станция: {metro_name} (avito_id: {metro_avito_id})")
            print("-" * 60)
            
            all_cards = []
            
            # Обрабатываем страницы для этой станции
            # Если max_pages = 0, парсим все страницы (неограниченно)
            if self.max_pages == 0:
                page = 1
                while True:  # Бесконечный цикл для всех страниц
                    try:
                        print(f"📄 Страница {page} (неограниченно)")
                        
                        # Получаем URL для страницы
                        metro_url = self.get_metro_url_with_page(metro_avito_id, page)
                        if not metro_url:
                            print("❌ Не удалось сформировать URL")
                            break
                        
                        print(f"🌐 URL: {metro_url}")
                        
                        # Переходим на страницу
                        self.driver.get(metro_url)
                        time.sleep(self.page_load_delay)
                        
                        # Ждем загрузки карточек
                        if not self.wait_for_cards_load():
                            print("❌ Не удалось дождаться загрузки карточек")
                            break
                        
                        # Плавная прокрутка для загрузки дополнительных карточек
                        if self.enable_smooth_scroll:
                            print("🔄 Вызываем smooth_scroll_and_load_cards...")
                            loaded_cards = self.smooth_scroll_and_load_cards(self.max_cards, self.scroll_pause)
                            print(f"📊 Результат прокрутки: {loaded_cards} карточек")
                        else:
                            print("⏭️ Плавная прокрутка отключена")
                        
                        # Получаем все карточки
                        print("🔍 Ищем карточки на странице...")
                        cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                        print(f"📊 Найдено карточек: {len(cards)}")
                        
                        # Если карточек нет, значит достигли конца
                        if len(cards) == 0:
                            print(f"🏁 Достигнут конец объявлений для станции {metro_name}")
                            break
                        
                        # Парсим карточки
                        print(f"🔄 Начинаем парсинг карточек (максимум {self.max_cards})...")
                        page_cards = []
                        cards_to_parse = min(len(cards), self.max_cards)
                        print(f"📝 Будем парсить {cards_to_parse} карточек из {len(cards)} найденных")
                        
                        for i, card in enumerate(cards[:cards_to_parse]):
                            print(f"   🔍 Парсим карточку {i+1}/{cards_to_parse}...")
                            card_data = self.parse_card(card)
                            if card_data:
                                card_data['card_number'] = i + 1
                                card_data['page'] = page
                                page_cards.append(card_data)
                                print(f"   ✅ Карточка {i+1} спарсена успешно")
                            else:
                                print(f"   ❌ Карточка {i+1} не спарсена")
                        
                        print(f"✅ Парсинг страницы {page}: {len(page_cards)} карточек")
                        
                        # Сохраняем карточки страницы в БД
                        if self.enable_db_save and page_cards:
                            try:
                                print(f"💾 Сохранение {len(page_cards)} карточек страницы {page} в БД...")
                                saved = await self.save_to_db(page_cards, metro_name)
                                if saved:
                                    print(f"✅ Страница {page} сохранена в БД: {len(page_cards)} карточек")
                                else:
                                    print(f"❌ Ошибка сохранения страницы {page} в БД")
                            except Exception as e:
                                print(f"❌ Ошибка при сохранении страницы {page} в БД: {e}")
                        elif not self.enable_db_save:
                            print(f"📝 Сохранение в БД отключено")
                        
                        all_cards.extend(page_cards)
                        
                        # Пауза между страницами
                        print(f"⏳ Пауза {self.page_delay} сек перед следующей страницей...")
                        time.sleep(self.page_delay)
                        
                        page += 1
                        
                    except Exception as e:
                        print(f"❌ Ошибка парсинга страницы {page}: {e}")
                        break
            else:
                # Ограниченное количество страниц
                for page in range(1, self.max_pages + 1):
                    try:
                        print(f"📄 Страница {page}/{self.max_pages}")
                        
                        # Получаем URL для страницы
                        metro_url = self.get_metro_url_with_page(metro_avito_id, page)
                        if not metro_url:
                            print("❌ Не удалось сформировать URL")
                            continue
                        
                        print(f"🌐 URL: {metro_url}")
                        
                        # Переходим на страницу
                        self.driver.get(metro_url)
                        time.sleep(self.page_load_delay)
                        
                        # Ждем загрузки карточек
                        if not self.wait_for_cards_load():
                            print("❌ Не удалось дождаться загрузки карточек")
                            continue
                        
                        # Плавная прокрутка для загрузки дополнительных карточек
                        if self.enable_smooth_scroll:
                            loaded_cards = self.smooth_scroll_and_load_cards(self.max_cards, self.scroll_pause)
                            print(f"📊 Загружено карточек: {loaded_cards} (цель: {self.max_cards})")
                        else:
                            loaded_cards = len(self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]'))
                        
                        # Получаем загруженные карточки
                        cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
                        print(f"📊 Найдено карточек: {len(cards)}")
                        
                        # Парсим карточки (берем только нужное количество)
                        page_cards = []
                        cards_to_parse = min(len(cards), self.max_cards)
                        for i, card in enumerate(cards[:cards_to_parse]):
                            card_data = self.parse_card(card)
                            if card_data:
                                card_data['card_number'] = i + 1
                                card_data['page'] = page
                                page_cards.append(card_data)
                        
                        print(f"✅ Парсинг страницы {page}: {len(page_cards)} карточек")
                        
                        # Сохраняем карточки страницы в БД
                        if self.enable_db_save and page_cards:
                            try:
                                print(f"💾 Сохранение {len(page_cards)} карточек страницы {page} в БД...")
                                saved = await self.save_to_db(page_cards, metro_name)
                                if saved:
                                    print(f"✅ Страница {page} сохранена в БД: {len(page_cards)} карточек")
                                else:
                                    print(f"❌ Ошибка сохранения страницы {page} в БД")
                            except Exception as e:
                                print(f"❌ Ошибка при сохранении страницы {page} в БД: {e}")
                        elif not self.enable_db_save:
                            print(f"📝 Сохранение в БД отключено")
                        
                        all_cards.extend(page_cards)
                        
                        # Пауза между страницами
                        if page < self.max_pages:
                            print(f"⏳ Пауза {self.page_delay} сек перед следующей страницей...")
                            time.sleep(self.page_delay)
                        
                    except Exception as e:
                        print(f"❌ Ошибка парсинга страницы {page}: {e}")
                        continue
            
            print(f"✅ Станция {metro_name} обработана: {len(all_cards)} карточек")
            
            # Сохраняем в БД, если включено
            if self.enable_db_save and all_cards:
                try:
                    print(f"💾 Сохранение {len(all_cards)} карточек в БД...")
                    saved = await self.save_to_db(all_cards, metro_name)
                    if saved:
                        print(f"✅ Успешно сохранено в БД: {len(all_cards)} карточек")
                    else:
                        print(f"❌ Ошибка сохранения в БД")
                except Exception as e:
                    print(f"❌ Ошибка при сохранении в БД: {e}")
            elif not self.enable_db_save:
                print(f"📝 Сохранение в БД отключено")
            else:
                print(f"⚠️ Нет карточек для сохранения")
            
            return all_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга станции {metro_station['name']}: {e}")
            return []
    
    async def parse_all_metro_stations(self):
        """Парсит все станции метро"""
        try:
            print(f"🚇 Начинаем парсинг всех станций метро Москвы")
            print(f"📊 Всего станций: {len(self.metro_stations)}")
            print(f"🎯 Карточек на страницу: {self.max_cards}")
            print(f"📄 Страниц на станцию: {self.max_pages}")
            print("=" * 80)
            
            all_cards = []
            total_saved = 0
            
            # Обрабатываем каждую станцию метро
            for i, station in enumerate(self.metro_stations):
                try:
                    print(f"\n🚇 Станция {i+1}/{len(self.metro_stations)}: {station['name']}")
                    
                    # Парсим станцию
                    station_cards = await self.parse_metro_station(station)
                    
                    if station_cards:
                        # Сохраняем в БД
                        if self.enable_db_save:
                            saved = await self.save_to_db(station_cards, station['name'])
                            if saved:
                                print(f"💾 Сохранено в БД: {len(station_cards)} карточек")
                                total_saved += len(station_cards)
                            else:
                                print("⚠️ Ошибка сохранения в БД")
                        else:
                            print(f"📝 Парсинг без сохранения: {len(station_cards)} карточек")
                        
                        all_cards.extend(station_cards)
                    
                    # Пауза между станциями
                    if i < len(self.metro_stations) - 1:
                        print(f"⏳ Пауза 10 сек перед следующей станцией...")
                        time.sleep(10)
                    
                except Exception as e:
                    print(f"❌ Ошибка обработки станции {station['name']}: {e}")
                    continue
            
            print("\n" + "=" * 80)
            print(f"✅ Парсинг завершен!")
            print(f"📊 Всего карточек: {len(all_cards)}")
            if self.enable_db_save:
                print(f"💾 Сохранено в БД: {total_saved}")
            
            return all_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга всех станций: {e}")
            return []
    
    async def parse_single_metro_station(self, metro_id):
        """Парсит одну конкретную станцию метро"""
        try:
            # Ищем станцию по ID
            station = None
            for s in self.metro_stations:
                if s['id'] == metro_id:
                    station = s
                    break
            
            if not station:
                print(f"❌ Станция метро с ID {metro_id} не найдена")
                return []
            
            print(f"🎯 Парсинг одной станции: {station['name']}")
            print("=" * 60)
            
            # Парсим станцию
            station_cards = await self.parse_metro_station(station)
            
            if station_cards and self.enable_db_save:
                # Сохраняем в БД
                saved = await self.save_to_db(station_cards, station['name'])
                if saved:
                    print(f"💾 Сохранено в БД: {len(station_cards)} карточек")
                else:
                    print("⚠️ Ошибка сохранения в БД")
            
            return station_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга станции: {e}")
            return []
    
    async def parse_metro_by_id(self, metro_id, max_pages=None, max_cards=None):
        """
        Парсит одну станцию метро с заданными параметрами
        
        Args:
            metro_id (int): ID станции метро из таблицы metro
            max_pages (int, optional): Максимальное количество страниц (0 = неограниченно)
            max_cards (int, optional): Максимальное количество карточек на страницу
            
        Returns:
            dict: Результат парсинга с количеством сохраненных записей
            {
                'success': bool,
                'metro_name': str,
                'total_cards': int,
                'saved_cards': int,
                'error': str or None
            }
        """
        try:
            print(f"🎯 Запуск парсинга метро ID: {metro_id}")
            print("=" * 60)
            
            # Сохраняем оригинальные настройки
            original_max_pages = self.max_pages
            original_max_cards = self.max_cards
            
            # Устанавливаем новые параметры
            if max_pages is not None:
                self.max_pages = max_pages
            if max_cards is not None:
                self.max_cards = max_cards
            
            print(f"📊 Параметры парсинга:")
            print(f"   • Максимум страниц: {self.max_pages} {'(неограниченно)' if self.max_pages == 0 else ''}")
            print(f"   • Карточек на страницу: {self.max_cards}")
            
            # Инициализируем парсер если нужно
            if not hasattr(self, 'driver') or not self.driver:
                # Получаем список станций метро
                if not await self.get_all_metro_stations():
                    return {
                        'success': False,
                        'metro_name': None,
                        'total_cards': 0,
                        'saved_cards': 0,
                        'error': 'Не удалось получить список станций метро'
                    }
                
                # Настраиваем Selenium
                if not self.setup_selenium():
                    return {
                        'success': False,
                        'metro_name': None,
                        'total_cards': 0,
                        'saved_cards': 0,
                        'error': 'Не удалось настроить Selenium'
                    }
                
                # Загружаем cookies
                cookies_data = self.load_cookies()
                if not cookies_data:
                    return {
                        'success': False,
                        'metro_name': None,
                        'total_cards': 0,
                        'saved_cards': 0,
                        'error': 'Не удалось загрузить cookies'
                    }
                
                # Применяем cookies
                if not self.apply_cookies(cookies_data):
                    return {
                        'success': False,
                        'metro_name': None,
                        'total_cards': 0,
                        'saved_cards': 0,
                        'error': 'Не удалось применить cookies'
                    }
            
            # Ищем станцию по ID
            station = None
            for s in self.metro_stations:
                if s['id'] == metro_id:
                    station = s
                    break
            
            if not station:
                return {
                    'success': False,
                    'metro_name': None,
                    'total_cards': 0,
                    'saved_cards': 0,
                    'error': f'Станция метро с ID {metro_id} не найдена'
                }
            
            print(f"📍 Станция: {station['name']} (avito_id: {station['avito_id']})")
            print("-" * 60)
            
            # Парсим станцию
            station_cards = await self.parse_metro_station(station)
            
            # Восстанавливаем оригинальные настройки
            self.max_pages = original_max_pages
            self.max_cards = original_max_cards
            
            if not station_cards:
                return {
                    'success': True,
                    'metro_name': station['name'],
                    'total_cards': 0,
                    'saved_cards': 0,
                    'error': None
                }
            
            # Сохраняем в БД если включено
            saved_cards = 0
            if self.enable_db_save and station_cards:
                try:
                    saved = await self.save_to_db(station_cards, station['name'])
                    if saved:
                        saved_cards = len(station_cards)
                        print(f"✅ Успешно сохранено в БД: {saved_cards} карточек")
                    else:
                        print("⚠️ Ошибка сохранения в БД")
                except Exception as e:
                    print(f"❌ Ошибка при сохранении в БД: {e}")
            
            result = {
                'success': True,
                'metro_name': station['name'],
                'total_cards': len(station_cards),
                'saved_cards': saved_cards,
                'error': None
            }
            
            print(f"🎉 Парсинг завершен:")
            print(f"   • Найдено карточек: {result['total_cards']}")
            print(f"   • Сохранено в БД: {result['saved_cards']}")
            
            return result
            
        except Exception as e:
            print(f"❌ Ошибка парсинга метро ID {metro_id}: {e}")
            # Восстанавливаем оригинальные настройки
            if 'original_max_pages' in locals():
                self.max_pages = original_max_pages
            if 'original_max_cards' in locals():
                self.max_cards = original_max_cards
            
            return {
                'success': False,
                'metro_name': None,
                'total_cards': 0,
                'saved_cards': 0,
                'error': str(e)
            }
    
    async def run(self):
        """Основной метод запуска парсера"""
        try:
            print("🚀 Запуск Avito парсера по всем метро Москвы")
            print("=" * 60)
            
            # Получаем список станций метро
            if not await self.get_all_metro_stations():
                print("❌ Не удалось получить список станций метро")
                return
            
            # Настраиваем Selenium
            if not self.setup_selenium():
                print("❌ Не удалось настроить Selenium")
                return
            
            # Загружаем cookies
            cookies_data = self.load_cookies()
            if not cookies_data:
                print("❌ Не удалось загрузить cookies")
                return
            
            # Применяем cookies
            if not self.apply_cookies(cookies_data):
                print("❌ Не удалось применить cookies")
                return
            
            # Запускаем парсинг
            if self.metro_id:
                # Парсим одну конкретную станцию
                await self.parse_single_metro_station(self.metro_id)
            else:
                # Парсим все станции
                await self.parse_all_metro_stations()
            
        except Exception as e:
            print(f"❌ Ошибка выполнения: {e}")
        finally:
            # Закрываем браузер
            if self.driver:
                print("🔒 Закрываем браузер...")
                self.driver.quit()

def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Парсер объявлений Avito по всем метро Москвы')
    
    parser.add_argument(
        '--metro-id',
        type=int,
        help='ID станции метро для парсинга (если не указан - парсим все)'
    )
    
    parser.add_argument(
        '--max-cards',
        type=int,
        help='Максимальное количество карточек на страницу'
    )
    
    parser.add_argument(
        '--max-pages',
        type=int,
        help='Максимальное количество страниц для каждой станции'
    )
    
    parser.add_argument(
        '--headless',
        action='store_true',
        help='Запуск в headless режиме'
    )
    
    parser.add_argument(
        '--no-db',
        action='store_true',
        help='Не сохранять в БД (только парсинг)'
    )
    
    args = parser.parse_args()
    
    # ВАЖНО: если флаг --headless НЕ передан, не переопределяем значение из конфига
    effective_headless = True if args.headless else None

    # Создаем парсер
    parser_instance = AvitoMetroParser(
        metro_id=args.metro_id,
        max_cards=args.max_cards,
        max_pages=args.max_pages,
        headless=effective_headless,
        enable_db=not args.no_db
    )
    
    # Запускаем парсинг
    asyncio.run(parser_instance.run())

if __name__ == "__main__":
    main()
