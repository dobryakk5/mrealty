#!/usr/bin/env python3
"""
Headless парсер карточек с чистой структурой данных и сохранением в БД
"""

import json
import os
import time
import asyncio
import asyncpg
import random
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import re

# Импортируем функции для работы с БД
from parse_todb_avito import create_ads_avito_table, save_avito_ad

# Список User-Agent для ротации
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

# Справочник бейджей/меток Avito
AVITO_BADGES = [
    "Рыночная цена",
    "Собственник", 
    "Проверено в Росреестре",
    "Надежный партнер",
    "Комфортная сделка",
    "Монолитный дом",
    "Срочная продажа",
    "Возможен торг",
    "Без комиссии",
    "Документы готовы",
    "Свободная планировка",
    "Ремонт от застройщика",
    "Ипотека",
    "Рассрочка",
    "Скидка",
    "Акция",
    "Новостройка",
    "Вторичка",
    "Студия",
    "Апартаменты",
    "Пентхаус",
    "Двухуровневая",
    "С видом на город",
    "С мебелью",
    "Без мебели",
    "С отделкой",
    "Без отделки",
    "С балконом",
    "С лоджией",
    "С террасой"
]

class BlockingDetector:
    """Детектор блокировки и система восстановления"""
    
    def __init__(self):
        self.block_count = 0
        self.base_delay = 30  # базовая задержка 30 секунд
        self.max_delay = 480  # максимальная задержка 8 минут
        
    def is_blocked(self, driver):
        """Проверяет наличие блокировки"""
        try:
            current_url = driver.current_url
            page_source = driver.page_source.lower()
            
            # Проверяем URL на редирект
            if "captcha" in current_url.lower() or "check" in current_url.lower():
                print("🚨 Обнаружен редирект на капчу/проверку")
                return True
                
            # Проверяем статус код (если доступен)
            try:
                response = driver.execute_script("return window.performance.getEntries()[0].responseStatus")
                if response == 403:
                    print("🚨 Обнаружен статус 403 (Forbidden)")
                    return True
            except:
                pass
            
            # Проверяем содержимое страницы (более строгие условия)
            blocking_indicators = [
                "captcha", "капча", "verification", "blocked", "заблокирован",
                "доступ временно ограничен", "слишком много запросов", "rate limit",
                "проверьте, что вы не робот", "verify you are human", "security check"
            ]
            
            for indicator in blocking_indicators:
                if indicator in page_source:
                    # Проверяем, что это не просто упоминание в тексте
                    if page_source.count(indicator) > 2:  # Если индикатор встречается много раз
                        print(f"🚨 Обнаружен индикатор блокировки: '{indicator}'")
                        return True
            
            # Проверяем отсутствие ожидаемого контента (более строго)
            if "avito.ru" in current_url and len(page_source) < 500:
                print("🚨 Подозрительно короткая страница")
                return True
                
            return False
            
        except Exception as e:
            print(f"❌ Ошибка проверки блокировки: {e}")
            return False
    
    def get_backoff_delay(self):
        """Вычисляет задержку с exponential backoff и jitter"""
        self.block_count += 1
        
        # Exponential backoff: 30s → 60s → 120s → 240s → 480s
        delay = min(self.base_delay * (2 ** (self.block_count - 1)), self.max_delay)
        
        # Добавляем jitter (±20%)
        jitter = delay * 0.2
        final_delay = delay + random.uniform(-jitter, jitter)
        
        print(f"⏰ Задержка после блокировки #{self.block_count}: {final_delay:.1f} сек")
        return final_delay
    
    def reset_block_count(self):
        """Сбрасывает счетчик блокировок"""
        if self.block_count > 0:
            print(f"✅ Сброс счетчика блокировок (было: {self.block_count})")
            self.block_count = 0

class HeadlessMetroParser:
    def __init__(self, metro_id=2, max_cards=3):
        self.metro_id = metro_id
        self.metro_avito_id = None
        self.max_cards = max_cards
        self.driver = None
        self.blocking_detector = BlockingDetector()  # Добавляем детектор блокировки
        
    async def get_metro_avito_id(self):
        """Получает avito_id для метро из БД"""
        try:
            # Загружаем переменные окружения
            load_dotenv()
            database_url = os.getenv('DATABASE_URL')
            
            if not database_url:
                print("❌ Ошибка: DATABASE_URL не установлен в .env")
                return False
            
            # Подключаемся к БД
            conn = await asyncpg.connect(database_url)
            
            # Получаем avito_id для метро
            result = await conn.fetchrow(
                "SELECT name, avito_id FROM metro WHERE id = $1",
                self.metro_id
            )
            
            if result:
                self.metro_avito_id = result['avito_id']
                print(f"📍 Метро: {result['name']} (ID: {self.metro_id}, avito_id: {self.metro_avito_id})")
                await conn.close()
                return True
            else:
                print(f"❌ Метро с ID {self.metro_id} не найдено в БД")
                await conn.close()
                return False
                
        except Exception as e:
            print(f"❌ Ошибка получения avito_id: {e}")
            return False
    
    async def get_metro_info(self):
        """Получает информацию о метро"""
        try:
            # Загружаем cookies
            cookies_data = self.load_cookies()
            if not cookies_data:
                return None
            
            # Настраиваем Selenium
            if not self.setup_selenium():
                return None
            
            # Применяем cookies
            if not self.apply_cookies(cookies_data):
                return None
            
            # Получаем avito_id для метро
            if not await self.get_metro_avito_id():
                return None
            
            # Получаем название метро из БД (упрощенная версия)
            metro_name = f"Метро ID {self.metro_id}"
            
            return {
                'name': metro_name,
                'id': self.metro_id,
                'avito_id': self.metro_avito_id
            }
            
        except Exception as e:
            print(f"❌ Ошибка получения информации о метро: {e}")
            return None
    
    def load_cookies(self):
        """Загружает cookies из файла"""
        try:
            cookies_file = "avito_cookies.json"
            if os.path.exists(cookies_file):
                with open(cookies_file, 'r', encoding='utf-8') as f:
                    cookies_data = json.load(f)
                
                # Проверяем время создания cookies
                if 'created_at' in cookies_data:
                    created_time = cookies_data['created_at']
                    print(f"✅ Загружены cookies от {created_time}")
                else:
                    print("✅ Загружены cookies")
                
                print(f"📊 Количество cookies: {len(cookies_data.get('cookies', []))}")
                return cookies_data
            else:
                print("❌ Файл cookies не найден")
                return None
        except Exception as e:
            print(f"❌ Ошибка загрузки cookies: {e}")
            return None
    
    def setup_selenium(self):
        """Настраивает Selenium WebDriver в headless режиме"""
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            
            # Используем случайный User-Agent для избежания детекции
            random_user_agent = random.choice(USER_AGENTS)
            options.add_argument(f"--user-agent={random_user_agent}")
            print(f"🔧 Используем User-Agent: {random_user_agent[:50]}...")
            
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            options.add_argument("--disable-features=VizDisplayCompositor")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            print("🔧 Создаем headless браузер...")
            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return True
        except Exception as e:
            print(f"❌ Ошибка создания браузера: {e}")
            return False
    
    def apply_cookies(self, cookies_data):
        """Применяет cookies к браузеру"""
        try:
            if not self.driver:
                return False
            
            # Сначала переходим на Avito
            print("🌐 Переходим на AVITO...")
            self.driver.get("https://avito.ru")
            time.sleep(2)
            
            # Применяем cookies
            cookies = cookies_data.get('cookies', [])
            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except:
                    continue
            
            print(f"✅ Применено cookies: {len(cookies)}")
            
            # Обновляем страницу с cookies
            print("🔄 Обновляем страницу с cookies...")
            self.driver.refresh()
            time.sleep(3)
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка применения cookies: {e}")
            return False
    
    def change_session(self):
        """Сменяет сессию при блокировке"""
        try:
            print("🔄 Смена сессии при блокировке...")
            
            # Закрываем текущий браузер
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            
            # НЕ удаляем cookies - они нужны для авторизации
            # Вместо этого меняем User-Agent и другие параметры
            
            # Создаем новый браузер с новыми настройками
            if not self.setup_selenium():
                print("❌ Не удалось создать новый браузер")
                return False
            
            # Применяем cookies (если есть)
            cookies_data = self.load_cookies()
            if cookies_data:
                if not self.apply_cookies(cookies_data):
                    print("⚠️ Не удалось применить cookies")
            else:
                print("⚠️ Cookies не найдены, продолжаем без них")
            
            print("✅ Сессия успешно сменена")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка смены сессии: {e}")
            return False
    
    def handle_blocking(self):
        """Обрабатывает блокировку с exponential backoff"""
        try:
            print("🚨 ОБНАРУЖЕНА БЛОКИРОВКА!")
            
            # Получаем задержку с exponential backoff
            delay = self.blocking_detector.get_backoff_delay()
            
            print(f"⏳ Ожидание {delay:.1f} секунд...")
            time.sleep(delay)
            
            # Сменяем сессию
            if not self.change_session():
                print("❌ Не удалось сменить сессию")
                return False
            
            print("✅ Восстановление после блокировки завершено")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка обработки блокировки: {e}")
            return False
    
    def get_metro_url(self, page=1):
        """Получает URL для метро с правильным avito_id"""
        if not self.metro_avito_id:
            print("❌ avito_id для метро не определен")
            return None
            
        # URL для вторички с правильным avito_id
        base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
        metro_url = f"{base_url}?metro={self.metro_avito_id}"
        if page > 1:
            metro_url += f"&p={page}"
        return metro_url
    
    def clean_url_from_context(self, url):
        """Очищает URL от context параметра для избежания перегрузки одного контекста"""
        if not url:
            return url
        
        try:
            # Убираем context=... из URL
            if 'context=' in url:
                # Находим позицию context=
                context_start = url.find('context=')
                # Находим следующий & или конец строки
                if '&' in url[context_start:]:
                    next_param = url.find('&', context_start)
                    # Убираем context=...& или context=... в конце
                    if next_param != -1:
                        url = url[:context_start] + url[next_param:]
                    else:
                        url = url[:context_start]
                else:
                    # context в конце URL
                    url = url[:context_start]
                
                # Убираем лишний & в начале если остался
                if url.endswith('&'):
                    url = url[:-1]
                if url.endswith('?'):
                    url = url[:-1]
            
            return url
        except Exception as e:
            print(f"❌ Ошибка очистки URL от context: {e}")
            return url
    
    def wait_for_cards_load(self, timeout=30):
        """Ждет загрузки карточек"""
        try:
            print("⏳ Ждем загрузки карточек...")
            
            # Ждем появления карточек
            wait = WebDriverWait(self.driver, timeout)
            cards = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-marker="item"]')))
            
            print(f"✅ Загружено карточек: {len(cards)}")
            return True
            
        except TimeoutException:
            print("❌ Таймаут ожидания карточек")
            return False
        except Exception as e:
            print(f"❌ Ошибка ожидания карточек: {e}")
            return False
    
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
        """Парсит адрес на компоненты, используя логику из оригинального парсера"""
        try:
            address_data = {}
            
            # Разделяем адрес по строкам
            lines = address_text.strip().split('\n')
            
            if len(lines) >= 2:
                # Первая строка - улица и дом (берем как есть)
                street_line = lines[0].strip()
                address_data['street_house'] = street_line
                
                # Вторая строка - метро и время
                metro_line = lines[1].strip()
                
                # Используем логику из оригинального парсера
                # Разбиваем по запятым и ищем минуты
                parts = [p.strip() for p in metro_line.split(',') if p.strip()]
                
                minutes = None
                metro = None
                minutes_idx = None
                
                # Ищем часть с минутами (берём первую цифру до слова "мин")
                for i, p in enumerate(parts):
                    if re.search(r"мин\.?", p, re.IGNORECASE):
                        nums = re.findall(r"\d+", p)
                        if nums:
                            minutes = int(nums[0])
                            minutes_idx = i
                            break
                
                # Если нашли минуты, метро вероятнее всего в предыдущем сегменте
                if minutes_idx is not None and minutes_idx > 0:
                    # всё, что ДО сегмента с минутами — это адресная часть
                    address_candidates = parts[:minutes_idx]
                else:
                    address_candidates = parts
                
                # Адрес — первые два компонента (улица, дом), если есть
                if address_candidates:
                    street = address_candidates[0]
                    house = None
                    # Попробуем вычленить дом как ведущую цифровую часть во втором сегменте,
                    # а метро — как фразу, начинающуюся с первой пары букв (Заглавная+строчная)
                    if len(address_candidates) >= 2:
                        seg2 = address_candidates[1]
                        # Находим первую позицию заглавной кирил. буквы, за которой идёт строчная — начало названия метро
                        m_cap = re.search(r"[А-ЯЁ][а-яё]", seg2)
                        if m_cap and m_cap.start() > 0:
                            split_idx = m_cap.start()
                            head = seg2[:split_idx].strip()
                            tail = seg2[split_idx:].strip()
                            if head:
                                house = head
                            if not metro and tail:
                                metro = tail
                        else:
                            house = seg2
                    if street and house:
                        address_data['street_house'] = f"{street}, {house}"
                    else:
                        address_data['street_house'] = street
                if metro:
                    address_data['metro_name'] = metro
                else:
                    address_data['metro_name'] = metro_line
                if minutes:
                    address_data['time_to_metro'] = str(minutes)
                else:
                    address_data['time_to_metro'] = 'не указано'
            else:
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
    
    def clean_seller_name(self, seller_text):
        """Очищает название продавца от рекламных фраз (как в оригинальном парсере)"""
        if not seller_text:
            return None
        
        # Убираем рекламные фразы
        ad_phrases = [
            r'звоните, готовы ответить на любые вопросы!?',
            r'звоните!?',
            r'готовы ответить на любые вопросы!?',
            r'обращайтесь!?',
            r'пишите!?',
            r'свяжитесь с нами!?',
            r'консультация бесплатно!?',
            r'работаем без выходных!?',
            r'лучшие цены!?',
            r'актуальные предложения!?'
        ]
        
        cleaned = seller_text
        for phrase in ad_phrases:
            cleaned = re.sub(phrase, '', cleaned, flags=re.IGNORECASE)
        
        # Убираем лишние пробелы
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned if len(cleaned) > 2 else None
    
    def parse_seller_info(self, card_element):
        """Парсит информацию о продавце, используя логику из оригинального парсера"""
        seller_data = {}
        try:
            badge_elements = card_element.find_elements(By.CSS_SELECTOR, '[class*="badge"], [class*="label"], [class*="tag"], [class*="seller"], [class*="owner"]')
            for elem in badge_elements:
                try:
                    text = elem.text.strip()
                    if text and len(text) > 2:
                        if 'собственник' in text.lower():
                            seller_data['type'] = 'собственник'
                            seller_data['from_badge'] = text
                            break
                        elif 'агентство' in text.lower() or 'агент' in text.lower():
                            seller_data['type'] = 'агентство'
                            seller_data['from_badge'] = text
                            break
                        elif 'частн' in text.lower():
                            seller_data['type'] = 'частное лицо'
                            seller_data['from_badge'] = text
                            break
                except:
                    continue
        except:
            pass
        
        if 'type' not in seller_data:
            try:
                all_text = card_element.text.lower()
                if 'собственник' in all_text:
                    seller_data['type'] = 'собственник'
                elif 'агентство' in all_text or 'агент' in all_text:
                    seller_data['type'] = 'агентство'
                elif 'частн' in all_text:
                    seller_data['type'] = 'частное лицо'
                elif 'застройщик' in all_text:
                    seller_data['type'] = 'застройщик'
                else:
                    seller_data['type'] = 'не определено'
                seller_data['full_text'] = 'найдено в общем тексте'
            except:
                seller_data['type'] = 'не найдено'
                seller_data['full_text'] = 'не найдено'
        
        try:
            card_text = card_element.text
            time_seller_pattern = r'(\d+\s*(?:час|часа|часов|день|дня|дней|неделя|недели|недель|месяц|месяца|месяцев)\s*назад)\s*([^0-9\n]+?)(?=\d+\s*завершённых|\d+\s*завершенных)'
            match = re.search(time_seller_pattern, card_text, re.IGNORECASE | re.DOTALL)
            if match:
                time_created = match.group(1).strip()
                seller_name = match.group(2).strip()
                cleaned_seller = self.clean_seller_name(seller_name)
                if cleaned_seller:
                    seller_data['time_created'] = time_created
                    seller_data['seller_name'] = cleaned_seller
                    if 'type' not in seller_data or seller_data['type'] == 'не определено':
                        if 'агентство' in cleaned_seller.lower() or 'агент' in cleaned_seller.lower():
                            seller_data['type'] = 'агентство'
                        elif 'частн' in cleaned_seller.lower():
                            seller_data['type'] = 'частное лицо'
        except:
            pass
        
        try:
            data_attrs = {}
            for attr, value in card_element.get_property('attributes').items():
                if attr.startswith('data-'):
                    data_attrs[attr] = value
            for attr, value in data_attrs.items():
                if 'seller' in attr.lower() or 'owner' in attr.lower():
                    seller_data[f'data_{attr}'] = value
        except:
            pass
        
        return seller_data
    
    def prepare_data_for_db(self, card_data):
        """Подготавливает данные карточки для сохранения в БД ads_avito"""
        try:
            db_data = {}
            
            # URL
            db_data['URL'] = card_data.get('url', '')
            
            # ID объявления
            db_data['offer_id'] = card_data.get('item_id', '')
            
            # Цена
            price_text = card_data.get('price', '')
            if price_text and '₽' in price_text:
                # Извлекаем число из цены "19 500 000 ₽"
                price_match = re.search(r'([\d\s]+)', price_text)
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
            
            # Метро - берем только первую часть до запятой
            metro_name = card_data.get('metro_name', '')
            if metro_name and ',' in metro_name:
                db_data['metro'] = metro_name.split(',')[0].strip()
            else:
                db_data['metro'] = metro_name
            
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
            
            # Теги - сохраняем labels как есть
            db_data['labels'] = card_data.get('labels', [])
            
            # Тип продавца
            seller_type = card_data.get('type', '')
            if seller_type == 'собственник':
                db_data['seller'] = {'type': 'owner'}
            elif seller_type == 'агентство':
                db_data['seller'] = {'type': 'agency'}
            elif seller_type == 'частное лицо':
                db_data['seller'] = {'type': 'user'}
            elif seller_type == 'застройщик':
                db_data['seller'] = {'type': 'developer'}
            else:
                db_data['seller'] = {'type': 'unknown'}
            
            # Название продавца
            if card_data.get('seller_name'):
                db_data['seller']['name'] = card_data['seller_name']
            
            # Тип объекта (вторичка = 2)
            db_data['object_type_id'] = 2
            
            return db_data
            
        except Exception as e:
            print(f"❌ Ошибка подготовки данных для БД: {e}")
            return None
    
    def parse_card(self, card_element):
        """Парсит одну карточку"""
        try:
            card_data = {}
            
            # ID объявления
            try:
                # Ищем ID в data-marker или в URL
                item_id = card_element.get_attribute('data-item-id')
                if not item_id:
                    # Пробуем найти в других атрибутах
                    for attr in ['data-item-id', 'data-avito-item-id', 'id']:
                        item_id = card_element.get_attribute(attr)
                        if item_id:
                            break
                
                if not item_id:
                    # Пробуем найти в тексте карточки
                    card_text = card_element.text
                    id_match = re.search(r'(\d{9,})', card_text)
                    if id_match:
                        item_id = id_match.group(1)
                
                card_data['item_id'] = item_id if item_id else 'не найден'
            except:
                card_data['item_id'] = 'не найден'
            
            # Заголовок
            try:
                title_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-title"], h3, .item-title')
                title_text = title_elem.text.strip()
                card_data['title'] = title_text
                
                # Парсим заголовок на компоненты
                title_data = self.parse_title(title_text)
                card_data.update(title_data)
            except:
                card_data['title'] = 'не найден'
                card_data['rooms'] = 'не определено'
                card_data['area'] = 'не определено'
                card_data['floor'] = 'не определено'
                card_data['total_floors'] = 'не определено'
            
            # Цена
            try:
                price_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-price"], .price, .item-price')
                price_text = price_elem.text.strip()
                card_data['price'] = price_text
            except:
                card_data['price'] = 'не найдена'
            
            # Адрес
            try:
                address_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-address"], .address, .item-address')
                address_text = address_elem.text.strip()
                card_data['address'] = address_text
                
                # Парсим адрес на компоненты
                address_data = self.parse_address(address_text)
                card_data.update(address_data)
            except:
                card_data['address'] = 'не найден'
                card_data['street_house'] = 'не найден'
                card_data['metro_name'] = 'не найден'
                card_data['time_to_metro'] = 'не указано'
            
            # Время публикации
            try:
                time_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-date"], .date, .item-date')
                time_text = time_elem.text.strip()
                card_data['published_time'] = time_text
            except:
                card_data['published_time'] = 'не найдено'
            
            # Информация о продавце
            seller_info = self.parse_seller_info(card_element)
            card_data.update(seller_info)
            
            # Парсим бейджи/метки карточки
            badges = self.parse_badges(card_element)
            card_data['labels'] = badges
            
            # Ссылка на карточку - ИСПРАВЛЕННАЯ ЛОГИКА
            try:
                print(f"🔍 Ищем ссылку для карточки {card_data.get('item_id', 'N/A')}...")
                
                # Пробуем несколько селекторов для поиска ссылки
                link_selectors = [
                    'a[data-marker="item-title"]',
                    'a[href*="/kvartiry/"]',
                    'a[data-marker="item"]',
                    'h3 a',
                    '.item-title a',
                    'a[href*="avito.ru"]',
                    'a'
                ]
                
                link_elem = None
                for i, selector in enumerate(link_selectors):
                    try:
                        elements = card_element.find_elements(By.CSS_SELECTOR, selector)
                        print(f"  Селектор {i+1} '{selector}': найдено {len(elements)} элементов")
                        
                        for elem in elements:
                            href = elem.get_attribute('href')
                            if href and '/kvartiry/' in href:
                                link_elem = elem
                                print(f"  ✅ Найдена ссылка: {href[:80]}...")
                                break
                        
                        if link_elem:
                            break
                    except Exception as e:
                        print(f"  ❌ Ошибка с селектором '{selector}': {e}")
                        continue
                
                if link_elem and link_elem.get_attribute('href'):
                    card_data['url'] = link_elem.get_attribute('href')
                    card_data['url'] = self.clean_url_from_context(card_data['url'])
                    print(f"  ✅ URL сохранен: {card_data['url'][:80]}...")
                else:
                    # Fallback: пытаемся найти любую ссылку в карточке
                    print(f"  🔍 Fallback: ищем любые ссылки...")
                    all_links = card_element.find_elements(By.TAG_NAME, 'a')
                    print(f"  Найдено {len(all_links)} ссылок в карточке")
                    
                    for j, link in enumerate(all_links):
                        try:
                            href = link.get_attribute('href')
                            if href:
                                print(f"    Ссылка {j+1}: {href[:60]}...")
                                if '/kvartiry/' in href:
                                    card_data['url'] = href
                                    card_data['url'] = self.clean_url_from_context(card_data['url'])
                                    print(f"    ✅ Подходящая ссылка найдена: {card_data['url'][:80]}...")
                                    break
                        except Exception as e:
                            print(f"    ❌ Ошибка получения href: {e}")
                            continue
                    else:
                        card_data['url'] = "Не найдено"
                        print(f"    ❌ Подходящая ссылка не найдена")
            except Exception as e:
                print(f"❌ Ошибка поиска ссылки: {e}")
                card_data['url'] = "Не найдено"
            
            return card_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга карточки: {e}")
            return None
    
    def parse_badges(self, card_element):
        """Парсит бейджи/метки карточки"""
        try:
            badges = []
            
            # Ищем бейджи по различным селекторам
            badge_selectors = [
                '[class*="badge"]',
                '[class*="label"]', 
                '[class*="tag"]',
                '[class*="mark"]',
                '[data-marker*="badge"]',
                '[data-marker*="label"]',
                '[data-marker*="tag"]'
            ]
            
            for selector in badge_selectors:
                try:
                    elements = card_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        text = elem.text.strip()
                        if text and len(text) > 1:
                            # Проверяем, есть ли текст в нашем справочнике
                            found_in_dict = False
                            for badge in AVITO_BADGES:
                                if badge.lower() in text.lower():
                                    if badge not in badges:
                                        badges.append(badge)
                                    found_in_dict = True
                                    break
                            # Если не нашли в справочнике, добавляем как есть
                            if not found_in_dict and text not in badges and len(text) < 50:
                                badges.append(text)
                except:
                    continue
            
            # Также ищем в общем тексте карточки
            try:
                card_text = card_element.text
                for badge in AVITO_BADGES:
                    if badge.lower() in card_text.lower() and badge not in badges:
                        badges.append(badge)
            except:
                pass
            
            return badges
            
        except Exception as e:
            print(f"❌ Ошибка парсинга бейджей: {e}")
            return []
    
    def slow_scroll_through_cards(self, cards):
        """Медленный скроллинг через карточки с задержками"""
        try:
            print("🔄 Начинаем медленный скроллинг...")
            
            # Скроллим к первым 2 карточкам
            if len(cards) >= 2:
                print("📜 Скроллим к первым 2 карточкам...")
                self.driver.execute_script("arguments[0].scrollIntoView(true);", cards[1])
                time.sleep(1)  # Задержка 1 секунда после первых 2 объявлений
                print("⏳ Задержка 1 сек после первых 2 объявлений")
            
            # Скроллим к следующим 10 карточкам
            if len(cards) >= 12:
                print("📜 Скроллим к следующим 10 карточкам...")
                self.driver.execute_script("arguments[0].scrollIntoView(true);", cards[11])
                time.sleep(1)  # Задержка 1 секунда после следующих 10 объявлений
                print("⏳ Задержка 1 сек после следующих 10 объявлений")
            
            # Скроллим к концу страницы для загрузки всех карточек
            print("📜 Скроллим к концу страницы...")
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # Ждем загрузки дополнительных карточек
            
            print("✅ Медленный скроллинг завершен")
            
        except Exception as e:
            print(f"❌ Ошибка медленного скроллинга: {e}")
    
    def parse_metro_page(self, page=1):
        """Парсит страницу с метро"""
        try:
            print(f"🎯 Парсим страницу {page} с метро ID = {self.metro_id} (avito_id = {self.metro_avito_id})")
            
            # НЕ очищаем cookies - они нужны для авторизации!
            # Очищаем только context из URL карточек
            
            # Получаем URL с пагинацией
            metro_url = self.get_metro_url(page)
            if not metro_url:
                return []
                
            print(f"🌐 URL: {metro_url}")
            
            # Переходим на страницу
            self.driver.get(metro_url)
            time.sleep(5)
            
            # Проверяем блокировку
            if self.blocking_detector.is_blocked(self.driver):
                if not self.handle_blocking():
                    print("❌ Не удалось восстановиться после блокировки")
                    return []
                # Повторяем переход после восстановления
                self.driver.get(metro_url)
                time.sleep(5)
            
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
            
            # Медленный скроллинг с задержками
            self.slow_scroll_through_cards(cards)
            
            # ПЕРЕПОЛУЧАЕМ карточки после скроллинга (исправляем stale elements)
            print("🔄 Переполучаем карточки после скроллинга...")
            cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
            print(f"📊 Карточек после переполучения: {len(cards)}")
            
            # Парсим первые несколько карточек
            parsed_cards = []
            for i, card in enumerate(cards[:self.max_cards]):
                print(f"\n🔍 Парсим карточку {i+1}/{min(self.max_cards, len(cards))}...")
                
                card_data = self.parse_card(card)
                if card_data:
                    card_data['card_number'] = i + 1
                    card_data['page'] = page
                    parsed_cards.append(card_data)
                    print(f"✅ Карточка {i+1} спарсена")
                else:
                    print(f"❌ Ошибка парсинга карточки {i+1}")
            
            # Сбрасываем счетчик блокировок при успешном парсинге
            self.blocking_detector.reset_block_count()
            
            return parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга страницы: {e}")
            return []
    
    async def parse_multiple_pages(self, max_pages=3):
        """Парсит несколько страниц"""
        try:
            all_parsed_cards = []
            
            for page in range(1, max_pages + 1):
                print(f"\n{'='*60}")
                print(f"📄 СТРАНИЦА {page}/{max_pages}")
                print(f"{'='*60}")
                
                # Парсим текущую страницу
                page_cards = self.parse_metro_page(page)
                
                if not page_cards:
                    print(f"❌ Не удалось спарсить страницу {page}")
                    
                    # Проверяем, не была ли это блокировка
                    if self.blocking_detector.block_count > 0:
                        print(f"🚨 Страница {page} заблокирована, пропускаем остальные страницы")
                        break
                    else:
                        # Если это не блокировка, продолжаем со следующей страницей
                        continue
                
                all_parsed_cards.extend(page_cards)
                print(f"✅ Страница {page} спарсена: {len(page_cards)} карточек")
                
                # Задержка между страницами
                if page < max_pages:
                    print("⏳ Задержка 3 сек между страницами...")
                    time.sleep(3)
            
            return all_parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга нескольких страниц: {e}")
            return all_parsed_cards
    
    async def save_to_db(self, parsed_cards):
        """Сохраняет карточки в БД"""
        try:
            saved_count = 0
            for i, card in enumerate(parsed_cards):
                try:
                    # Подготавливаем данные для БД
                    db_data = self.prepare_data_for_db(card)
                    if db_data:
                        # Сохраняем в БД
                        await save_avito_ad(db_data)
                        saved_count += 1
                        print(f"✅ Карточка {i+1} сохранена в БД")
                    else:
                        print(f"❌ Не удалось подготовить данные карточки {i+1} для БД")
                except Exception as e:
                    print(f"❌ Ошибка сохранения карточки {i+1} в БД: {e}")
            
            print(f"\n📊 Сохранено в БД: {saved_count}/{len(parsed_cards)} карточек")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения в БД: {e}")
    
    def save_results(self, parsed_cards):
        """Сохраняет результаты в файл"""
        try:
            result_data = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'metro_id': self.metro_id,
                'metro_avito_id': self.metro_avito_id,
                'url': self.get_metro_url(),
                'total_cards_found': len(parsed_cards),
                'parsed_cards': parsed_cards
            }
            
            filename = f"headless_metro_{self.metro_id}_cards_{time.strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
            
            print(f"\n💾 Результаты сохранены в {filename}")
            
            # Выводим краткую статистику
            print(f"\n📊 СТАТИСТИКА:")
            print(f"   Метро ID: {self.metro_id}")
            print(f"   Метро avito_id: {self.metro_avito_id}")
            print(f"   Карточек спарсено: {len(parsed_cards)}")
            print(f"   Файл результатов: {filename}")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения результатов: {e}")
    
    def print_clean_results(self, parsed_cards):
        """Выводит чистую структуру данных карточек"""
        print(f"\n📋 ЧИСТАЯ СТРУКТУРА ДАННЫХ КАРТОЧЕК:")
        print("=" * 80)
        
        for i, card in enumerate(parsed_cards):
            print(f"\n🏠 КАРТОЧКА #{i+1}")
            print("-" * 40)
            
            # Основная информация
            print(f"ID объявления: {card.get('item_id', 'Н/Д')}")
            print(f"Заголовок: {card.get('title', 'Н/Д')}")
            print(f"Цена: {card.get('price', 'Н/Д')}")
            print(f"Время публикации: {card.get('published_time', 'Н/Д')}")
            
            # Характеристики
            print(f"\nХарактеристики:")
            print(f"  • Комнаты: {card.get('rooms', 'Н/Д')}")
            print(f"  • Площадь: {card.get('area', 'Н/Д')} м²")
            print(f"  • Этаж: {card.get('floor', 'Н/Д')}/{card.get('total_floors', 'Н/Д')}")
            
            # Адрес и метро
            print(f"\nАдрес и метро:")
            print(f"  • Улица/дом: {card.get('street_house', 'Н/Д')}")
            print(f"  • Метро: {card.get('metro_name', 'Н/Д')}")
            print(f"  • Время до метро: {card.get('time_to_metro', 'Н/Д')} мин")
            
            # Продавец
            print(f"\nПродавец:")
            print(f"  • Тип: {card.get('type', 'Н/Д')}")
            if card.get('seller_name'):
                print(f"  • Название: {card.get('seller_name', 'Н/Д')}")
            if card.get('time_created'):
                print(f"  • Время создания: {card.get('time_created', 'Н/Д')}")
            if card.get('from_badge'):
                print(f"  • Источник: {card.get('from_badge', 'Н/Д')}")
            
            # Ссылка
            print(f"\nСсылка: {card.get('url', 'Н/Д')}")
            
            if i < len(parsed_cards) - 1:
                print("\n" + "=" * 80)
    
    async def run_parser(self):
        """Запускает парсер"""
        try:
            print("🚀 Запуск headless парсера карточек")
            print("=" * 60)
            
            # Создаем таблицу в БД
            print("🗄️ Создаем таблицу ads_avito...")
            await create_ads_avito_table()
            print("✅ Таблица ads_avito готова")
            
            # Получаем информацию о метро
            metro_info = await self.get_metro_info()
            if not metro_info:
                print("❌ Не удалось получить информацию о метро")
                return False
            
            print(f"📍 Метро: {metro_info['name']} (ID: {self.metro_id}, avito_id: {self.metro_avito_id})")
            
            # Настраиваем браузер
            if not self.setup_selenium():
                print("❌ Не удалось настроить браузер")
                return False
            
            try:
                # Парсим несколько страниц
                parsed_cards = await self.parse_multiple_pages(max_pages=2)  # Парсим 2 страницы
                
                if not parsed_cards:
                    print("❌ Не удалось спарсить карточки")
                    return False
                
                # Сохраняем результаты
                self.save_results(parsed_cards)
                
                # Сохраняем в БД
                print("\n💾 Сохраняем данные в БД ads_avito...")
                await self.save_to_db(parsed_cards)
                
                # Выводим чистую структуру
                self.print_clean_results(parsed_cards)
                
                return True
                
            finally:
                # Закрываем браузер
                if self.driver:
                    self.driver.quit()
                    print("🔒 Браузер закрыт")
            
        except Exception as e:
            print(f"❌ Ошибка запуска парсера: {e}")
            return False

async def main():
    """Главная функция"""
    parser = HeadlessMetroParser(metro_id=118, max_cards=3)  # Авиамоторная
    success = await parser.run_parser()
    
    if success:
        print("\n🎉 Парсинг завершен успешно!")
    else:
        print("\n❌ Парсинг завершен с ошибками")

if __name__ == "__main__":
    asyncio.run(main())
