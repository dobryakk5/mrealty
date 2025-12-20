#!/usr/bin/env python3
"""
Минимальный парсер Avito для интеграции с listings_processor

Функции:
- Парсинг карточки объявления Avito
- Извлечение основных атрибутов
- Подготовка данных для БД и Excel
"""

import json
import os
import time
import re
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

class AvitoCardParser:
    """Парсер карточек объявлений Avito для listings_processor"""
    
    def __init__(self, skip_photos=False):
        self.driver = None
        self.cookies_file = "avito_cookies.json"
        self.skip_photos = skip_photos  # Новый параметр для пропуска фото
        self.session = None  # HTTP сессия для легкого парсинга
        
    def load_cookies(self):
        """Загружает cookies для Avito"""
        try:
            if not os.path.exists(self.cookies_file):
                print(f"❌ Файл cookies не найден: {self.cookies_file}")
                return False
            
            with open(self.cookies_file, 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            
            if 'cookies' not in cookies_data:
                print("❌ Неверная структура файла cookies")
                return False
            
            print(f"✅ Загружены cookies: {len(cookies_data['cookies'])}")
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
            
            # Headless режим для быстрой работы (новый headless)
            options.add_argument("--headless=new")
            
            # User-Agent
            options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Размер окна
            options.add_argument("--window-size=1920,1080")
            
            # Дополнительные настройки для обхода блокировок
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)

            # Явно указываем путь к Chrome binary
            if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
                options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            else:
                options.binary_location = "/opt/google/chrome/google-chrome"

            print("🔧 Создаем браузер...")
            self.driver = webdriver.Chrome(options=options)
            
            # Настраиваем таймауты
            self.driver.implicitly_wait(10)  # Неявное ожидание элементов
            self.driver.set_page_load_timeout(30)  # Таймаут загрузки страницы
            self.driver.set_script_timeout(30)  # Таймаут выполнения скриптов
            
            # Убираем webdriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания браузера: {e}")
            return False
    
    def apply_cookies(self, cookies_data):
        """Применяет cookies к драйверу с улучшенной обработкой таймаутов"""
        try:
            if not cookies_data or 'cookies' not in cookies_data:
                print("❌ Данные cookies отсутствуют или некорректны")
                return False
            
            print(f"📊 Применяем cookies: {len(cookies_data['cookies'])}")
            
            # Устанавливаем короткий таймаут для загрузки страницы
            original_timeout = self.driver.timeouts.page_load
            self.driver.set_page_load_timeout(15)  # Сокращаем до 15 секунд
            
            try:
                # Переходим на главную страницу Avito с коротким таймаутом
                print("🌐 Переходим на AVITO...")
                try:
                    self.driver.get("https://avito.ru")
                except Exception as e:
                    print(f"⚠️ Таймаут загрузки главной страницы: {e}")
                    # Продолжаем работу, браузер может уже быть на Avito
                
                # Небольшая пауза для стабилизации
                time.sleep(2)
                
                # Применяем cookies
                applied_count = 0
                for cookie in cookies_data['cookies']:
                    try:
                        if 'name' in cookie and 'value' in cookie:
                            cookie_dict = {
                                'name': cookie['name'],
                                'value': cookie['value'],
                                'domain': cookie.get('domain', ''),
                                'path': cookie.get('path', '/')
                            }
                            
                            if cookie.get('expiry'):
                                cookie_dict['expiry'] = cookie['expiry']
                            if cookie.get('secure'):
                                cookie_dict['secure'] = cookie['secure']
                            if cookie.get('httpOnly'):
                                cookie_dict['httpOnly'] = cookie['httpOnly']
                            
                            self.driver.add_cookie(cookie_dict)
                            applied_count += 1
                            
                    except Exception as e:
                        continue
                
                print(f"✅ Применено cookies: {applied_count}")
                
                # Обновляем страницу с примененными cookies с таймаутом
                try:
                    self.driver.refresh()
                    time.sleep(3)
                except Exception as e:
                    print(f"⚠️ Таймаут обновления страницы: {e}")
                    # Продолжаем работу
                
                return applied_count > 0
                
            finally:
                # Восстанавливаем оригинальный таймаут
                self.driver.set_page_load_timeout(original_timeout)
            
        except Exception as e:
            print(f"❌ Ошибка применения cookies: {e}")
            # Не возвращаем False сразу - даем возможность продолжить без cookies
            print("⚠️ Продолжаем работу без cookies")
            return True  # Возвращаем True чтобы попытаться продолжить работу
    
    def parse_card(self, url):
        """Парсит полную страницу объявления Avito"""
        return self.parse_avito_page(url)
    
    def parse_avito_page_light(self, url):
        """Легкий HTTP парсинг без Selenium. Fallback к Selenium если не работает."""
        try:
            print(f"🌐 Попытка легкого HTTP парсинга: {url}")
            
            # Пробуем HTTP парсинг
            result = self._http_parse(url)
            if result and result.get('success', False):
                print("✅ Легкий HTTP парсинг успешен")
                return result
            
            print("⚠️ HTTP парсинг не удался, переходим на Selenium...")
            
        except Exception as e:
            print(f"⚠️ Ошибка HTTP парсинга: {e}")
            print("🔄 Переходим на Selenium...")
        
        # Fallback к Selenium
        return self.parse_avito_page(url)
    
    def _http_parse(self, url):
        """Пытается распарсить через обычный HTTP запрос"""
        try:
            if not self.session:
                self.session = self._create_http_session()
            
            response = self.session.get(url)
            response.raise_for_status()
            
            # Проверяем не заблокировал ли Avito
            if 'captcha' in response.text.lower() or 'робот' in response.text.lower():
                print("❌ HTTP запрос заблокирован (капча)")
                return None
            
            # Парсим HTML
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Проверяем активность объявления
            if soup.find(string=lambda text: text and 'не найден' in text.lower()):
                print("❌ Объявление не найдено (404)")
                return {'success': False, 'reason': '404'}
            
            # Парсим основные данные
            data = self._extract_basic_data_http(soup, url)
            if data:
                return {'success': True, 'data': data}
            
            return {'success': False, 'reason': 'no_data'}
            
        except Exception as e:
            print(f"❌ HTTP парсинг завершился с ошибкой: {e}")
            return None
    
    def _create_http_session(self):
        """Создает HTTP сессию с нужными заголовками"""
        import requests
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Загружаем cookies если есть
        try:
            if os.path.exists(self.cookies_file):
                with open(self.cookies_file, 'r', encoding='utf-8') as f:
                    cookies_data = json.load(f)
                for cookie in cookies_data.get('cookies', []):
                    session.cookies.set(cookie['name'], cookie['value'], domain=cookie.get('domain'))
                print(f"🍪 Загружено cookies для HTTP: {len(cookies_data.get('cookies', []))}")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки cookies для HTTP: {e}")
        
        return session
    
    def _extract_basic_data_http(self, soup, url):
        """Извлекает базовые данные из HTML через BeautifulSoup"""
        try:
            data = {'url': url}
            
            # Заголовок
            title_elem = soup.find('h1') or soup.find('[data-marker="item-view/title"]')
            if title_elem:
                data['title'] = title_elem.get_text(strip=True)
                # Парсим заголовок
                title_parsed = self.parse_title(data['title'])
                data.update(title_parsed)
            
            # Цена  
            price_selectors = [
                '[data-marker="item-view/item-price"]',
                '.price-value-string',
                '[class*="price"]'
            ]
            
            for selector in price_selectors:
                price_elem = soup.select_one(selector)
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    if price_text and 'цена' not in price_text.lower():
                        data['price'] = price_text
                        break
            
            # Адрес из мета тегов или текста страницы
            # Ищем адрес в различных местах
            address_text = self._find_address_http(soup)
            if address_text:
                data['address'] = address_text
                # Парсим метро из текста адреса  
                metro_data = self._parse_metro_from_text(address_text)
                if metro_data:
                    data.update(metro_data)
            
            return data if data.get('price') else None
            
        except Exception as e:
            print(f"❌ Ошибка извлечения данных HTTP: {e}")
            return None
    
    def _find_address_http(self, soup):
        """Ищет адрес на странице через различные селекторы"""
        try:
            # Поиск по различным селекторам
            selectors = [
                '[itemprop="address"]',
                '[data-marker*="address"]', 
                '.item-address',
                '[class*="address"]'
            ]
            
            for selector in selectors:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 10:
                        return text
            
            return None
            
        except Exception as e:
            print(f"⚠️ Ошибка поиска адреса: {e}")
            return None
    
    def _parse_metro_from_text(self, text):
        """Парсит информацию о метро из текста"""
        try:
            import re
            
            # Ищем паттерны метро
            metro_patterns = [
                r'(.+?)(\d+)\s*мин\.',  # "Сокольникидо 5 мин."
                r'(.+?)\s*-\s*(\d+)\s*мин\.',  # "Сокольники - 5 мин."
            ]
            
            for pattern in metro_patterns:
                match = re.search(pattern, text)
                if match:
                    station = match.group(1).replace('до', '').strip()
                    time_min = int(match.group(2))
                    return {
                        'metro_station': station,
                        'metro_time': time_min,
                        'metro_way': 'пешком'
                    }
            
            return None
            
        except Exception as e:
            print(f"⚠️ Ошибка парсинга метро: {e}")
            return None
    
    def parse_title(self, title_text):
        """Парсит заголовок на компоненты"""
        try:
            title_data = {}
            
            # 1. Количество комнат
            # Ищем "4-к." или "студия"
            rooms_match = re.search(r'(\d+)-к\.', title_text)
            if rooms_match:
                title_data['rooms'] = int(rooms_match.group(1))
            elif 'студия' in title_text.lower():
                title_data['rooms'] = 0
            else:
                title_data['rooms'] = None
            
            # 2. Общая площадь
            # Ищем "95 м²" или "95м²"
            area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', title_text)
            if area_match:
                title_data['total_area'] = float(area_match.group(1).replace(',', '.'))
            else:
                title_data['total_area'] = None
            
            # 3. Этаж и всего этажей
            # Ищем различные форматы: "5/14 эт.", "5 из 14", "5/N/A", "5 из 5/N/A"
            floor_match = re.search(r'(\d+)\s*(?:[\/из]\s*(\d+)|(?:\/N\/A))', title_text)
            if floor_match:
                title_data['floor'] = int(floor_match.group(1))
                # Если есть второй номер (общее количество этажей)
                if floor_match.group(2):
                    title_data['total_floors'] = int(floor_match.group(2))
                else:
                    title_data['total_floors'] = None
            else:
                title_data['floor'] = None
                title_data['total_floors'] = None
            
            return title_data
            
        except Exception as e:
            return {
                'rooms': None,
                'total_area': None,
                'floor': None,
                'total_floors': None
            }
    
    def parse_address(self, address_text):
        """Парсит адрес на компоненты"""
        try:
            address_data = {}
            
            # Разделяем адрес по строкам
            lines = address_text.strip().split('\n')
            
            if len(lines) >= 2:
                # Первая строка - улица и дом
                street_line = lines[0].strip()
                street_parts = street_line.split(',')
                if len(street_parts) >= 2:
                    street = street_parts[0].strip()
                    house = street_parts[1].strip()
                    address_data['street_house'] = f"{street}, {house}"
                else:
                    address_data['street_house'] = street_line
                
                # Вторая строка - метро и время
                metro_line = lines[1].strip()
                metro_parts = metro_line.split(',')
                metro_name = None
                time_to_metro = None
                
                for part in metro_parts:
                    part = part.strip()
                    if part:
                        # Ищем время до метро
                        time_match = re.search(r'(\d+)\s*мин', part)
                        if time_match:
                            time_to_metro = int(time_match.group(1))
                        else:
                            # Если это не время, то это название метро
                            if not metro_name and not re.search(r'\d+', part):
                                metro_name = part
                
                address_data['metro_name'] = metro_name if metro_name else 'не указано'
                address_data['time_to_metro'] = str(time_to_metro) if time_to_metro else 'не указано'
                
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
    
    def parse_tags_from_params(self, params_text):
        """Парсит теги из характеристик карточки"""
        try:
            if not params_text or params_text == "Не найдено":
                return []
            
            # Разбиваем текст на строки
            lines = params_text.strip().split('\n')
            tags = []
            
            # Ищем теги - они идут отдельными строками после цены "за м²"
            found_price_line = False
            
            for line in lines:
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
                    if len(line) < 50 and not self.is_address_line(line):
                        tags.append(line)
                    else:
                        # Если это адрес или описание, прекращаем поиск тегов
                        break
            
            return tags
            
        except Exception as e:
            print(f"❌ Ошибка парсинга тегов: {e}")
            return []
    
    def is_address_line(self, line):
        """Проверяет, является ли строка адресом"""
        try:
            line_lower = line.lower()
            
            # Признаки адреса
            street_indicators = ['ул.', 'улица', 'проспект', 'пр.', 'переулок', 'пер.']
            time_pattern = r'\d+\s*мин'
            
            has_street = any(indicator in line_lower for indicator in street_indicators)
            has_time = bool(re.search(time_pattern, line_lower))
            
            # Если есть хотя бы 2 признака адреса, считаем строку адресом
            if has_street or has_time:
                return True
            
            return False
            
        except Exception as e:
            return False
    
    def parse_seller_info(self, card_element):
        """Парсит информацию о продавце"""
        try:
            seller_data = {}
            
            # Ищем информацию о продавце в характеристиках карточки
            try:
                params_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-specific-params"]')
                if params_elem:
                    params_text = params_elem.text.strip()
                    seller_data['full_text'] = params_text
                    
                    # Определяем тип продавца по тегам
                    if 'собственник' in params_text.lower():
                        seller_data['type'] = 'собственник'
                    elif 'агентство' in params_text.lower() or 'реквизиты проверены' in params_text.lower():
                        seller_data['type'] = 'агентство'
                    else:
                        seller_data['type'] = 'частное лицо'
                    
                else:
                    seller_data['type'] = 'не найдено'
                    seller_data['full_text'] = 'не найдено'
                    
            except:
                seller_data['type'] = 'не найдено'
                seller_data['full_text'] = 'не найдено'
            
            return seller_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга продавца: {e}")
            return {
                'type': 'ошибка парсинга',
                'full_text': 'ошибка парсинга'
            }
    
    def clean_url_path(self, url_path: str) -> str:
        """Очищает URL от всех параметров, оставляя только путь"""
        if not url_path:
            return ""
        if "?" in url_path:
            url_path = url_path.split("?")[0]
        return url_path
    
    def prepare_data_for_db(self, parsed_data):
        """Подготавливает данные для сохранения в БД"""
        try:
            if not parsed_data:
                return None
            
            # Функция для очистки значений "Не найдено" и подобных
            def clean_value(value):
                if value in ['Не найдено', 'не найдено', 'Нет данных', 'нет данных', 'Не указано', 'не указано']:
                    return ''
                return value
            
            # Базовые данные
            db_data = {
                'url': clean_value(parsed_data.get('url', '')),
                'title': clean_value(parsed_data.get('title', '')),
                'price': clean_value(parsed_data.get('price', '')),
                'description': clean_value(parsed_data.get('description', '')),
                'source': 1,  # Avito
            }
            
            # Данные из заголовка
            if 'rooms' in parsed_data:
                db_data['rooms'] = parsed_data['rooms']
            if 'total_area' in parsed_data:
                db_data['total_area'] = parsed_data['total_area']
            if 'floor' in parsed_data:
                db_data['floor'] = parsed_data['floor']
            if 'total_floors' in parsed_data:
                db_data['total_floors'] = parsed_data['total_floors']
            
            # Если этаж не найден в заголовке, пробуем из параметров квартиры
            if 'floor' not in parsed_data or parsed_data['floor'] is None:
                apartment_params = parsed_data.get('apartment_params', {})
                if 'Этаж' in apartment_params:
                    floor_text = apartment_params['Этаж']
                    # Ищем различные форматы: "5 из 5", "5/N/A", "5 из 5/N/A"
                    floor_match = re.search(r'(\d+)\s*(?:из\s*(\d+)|(?:\/N\/A))', floor_text)
                    if floor_match:
                        db_data['floor'] = int(floor_match.group(1))
                        # Если есть второй номер (общее количество этажей)
                        if floor_match.group(2):
                            db_data['total_floors'] = int(floor_match.group(2))
                        else:
                            db_data['total_floors'] = None
                    else:
                        db_data['floor'] = None
                        db_data['total_floors'] = None
            
            # Адрес и метро
            address_data = parsed_data.get('address_data', {})
            if address_data:
                db_data['address'] = clean_value(address_data.get('address', ''))
                db_data['metro_station'] = address_data.get('metro_station')
                db_data['metro_time'] = address_data.get('metro_time')
                db_data['metro_way'] = address_data.get('metro_way')
            
            # Параметры квартиры
            apartment_params = parsed_data.get('apartment_params', {})
            if apartment_params:
                # Основные параметры
                if 'Количество комнат' in apartment_params:
                    db_data['rooms_from_params'] = apartment_params['Количество комнат']
                if 'Общая площадь' in apartment_params:
                    area_text = apartment_params['Общая площадь']
                    if 'м²' in area_text:
                        area_match = re.search(r'(\d+(?:[.,]\d+)?)', area_text)
                        if area_match:
                            db_data['total_area_from_params'] = float(area_match.group(1).replace(',', '.'))
                if 'Площадь кухни' in apartment_params:
                    kitchen_text = apartment_params['Площадь кухни']
                    if 'м²' in kitchen_text:
                        kitchen_match = re.search(r'(\d+(?:[.,]\d+)?)', kitchen_text)
                        if kitchen_match:
                            db_data['kitchen_area'] = float(kitchen_match.group(1).replace(',', '.'))
                if 'Жилая площадь' in apartment_params:
                    living_text = apartment_params['Жилая площадь']
                    if 'м²' in living_text:
                        living_match = re.search(r'(\d+(?:[.,]\d+)?)', living_text)
                        if living_match:
                            db_data['living_area'] = float(living_match.group(1).replace(',', '.'))
                if 'Этаж' in apartment_params:
                    floor_text = apartment_params['Этаж']
                    # Ищем различные форматы: "5 из 5", "5/N/A", "5 из 5/N/A"
                    floor_match = re.search(r'(\d+)\s*(?:из\s*(\d+)|(?:\/N\/A))', floor_text)
                    if floor_match:
                        db_data['floor_from_params'] = int(floor_match.group(1))
                        # Если есть второй номер (общее количество этажей)
                        if floor_match.group(2):
                            db_data['total_floors_from_params'] = int(floor_match.group(2))
                        else:
                            db_data['total_floors_from_params'] = None
                    else:
                        db_data['floor_from_params'] = None
                        db_data['total_floors_from_params'] = None
                if 'Высота потолков' in apartment_params:
                    ceiling_text = apartment_params['Высота потолков']
                    ceiling_match = re.search(r'(\d+(?:[.,]\d+)?)', ceiling_text)
                    if ceiling_match:
                        db_data['ceiling_height'] = float(ceiling_match.group(1).replace(',', '.'))
                
                # Дополнительные параметры
                if 'Балкон или лоджия' in apartment_params:
                    db_data['balcony'] = apartment_params['Балкон или лоджия']
                if 'Тип комнат' in apartment_params:
                    db_data['room_type'] = apartment_params['Тип комнат']
                if 'Санузел' in apartment_params:
                    db_data['bathroom'] = apartment_params['Санузел']
                if 'Окна' in apartment_params:
                    db_data['windows'] = apartment_params['Окна']
                if 'Ремонт' in apartment_params:
                    db_data['renovation'] = apartment_params['Ремонт']
                if 'Мебель' in apartment_params:
                    db_data['furniture'] = apartment_params['Мебель']
                if 'Способ продажи' in apartment_params:
                    db_data['sale_type'] = apartment_params['Способ продажи']
            
            # Параметры дома
            house_params = parsed_data.get('house_params', {})
            if house_params:
                if 'Тип дома' in house_params:
                    db_data['house_type'] = house_params['Тип дома']
                if 'Год постройки' in house_params:
                    year_text = house_params['Год постройки']
                    year_match = re.search(r'(\d{4})', year_text)
                    if year_match:
                        db_data['construction_year'] = int(year_match.group(1))
                if 'Этажей в доме' in house_params:
                    floors_text = house_params['Этажей в доме']
                    # Ищем число или "N/A"
                    if floors_text != 'N/A' and floors_text != 'не указано':
                        floors_match = re.search(r'(\d+)', floors_text)
                        if floors_match:
                            db_data['house_floors'] = int(floors_match.group(1))
                        else:
                            db_data['house_floors'] = None
                    else:
                        db_data['house_floors'] = None
                if 'Пассажирский лифт' in house_params:
                    db_data['passenger_elevator'] = house_params['Пассажирский лифт']
                if 'Грузовой лифт' in house_params:
                    db_data['cargo_elevator'] = house_params['Грузовой лифт']
                if 'В доме' in house_params:
                    db_data['house_amenities'] = house_params['В доме']
                    # Извлекаем информацию о газе из "В доме"
                    house_amenities_text = house_params['В доме'].lower()
                    if 'газ' in house_amenities_text:
                        db_data['gas_supply'] = 'Есть'
                    else:
                        db_data['gas_supply'] = 'Нет'
                if 'Двор' in house_params:
                    db_data['yard_amenities'] = house_params['Двор']
                if 'Парковка' in house_params:
                    db_data['parking'] = house_params['Парковка']
            
            # Информация о публикации
            publication_info = parsed_data.get('publication_info', {})
            if publication_info:
                if 'publication_date' in publication_info:
                    db_data['publication_date'] = publication_info['publication_date']
                if 'today_views' in publication_info:
                    db_data['today_views'] = publication_info['today_views']
            
            # Статус объявления
            status_info = parsed_data.get('status_info', {})
            if status_info:
                status = status_info.get('status', 'active')
                # Преобразуем внутренние значения в формат для пользователя
                if status == 'active':
                    db_data['status'] = 'Активно'
                elif status == 'inactive':
                    db_data['status'] = 'Неактивно'
                else:
                    db_data['status'] = 'Неизвестно'
                
                # Добавляем причину определения статуса для отладки
                if 'reason' in status_info:
                    db_data['status_reason'] = status_info['reason']
            else:
                # Если статус не определен, считаем активным по умолчанию
                db_data['status'] = 'Активно'
            
            # Фотографии - сохраняем для HTML галереи
            photos = parsed_data.get('photos', [])
            if photos:
                photo_urls = [photo[1] for photo in photos if photo[1]]
                db_data['photo_urls'] = photo_urls
                print(f"📸 Сохранено {len(photo_urls)} URL фотографий для HTML")
            
            # Структурированные метки (только для уникальных полей)
            tags = []
            
            # Добавляем только те поля, которых нет в основных колонках Excel
            if apartment_params:
                # Только уникальные поля квартиры
                unique_apartment_fields = {
                    'Тип комнат'  # Это поле есть только в параметрах квартиры
                }
                
                for key, value in apartment_params.items():
                    if (key in unique_apartment_fields and 
                        value and value not in ['нет', 'Нет']):
                        tags.append(f"{key}: {value}")
            
            if house_params:
                # Только уникальные поля дома
                unique_house_fields = {
                    'В доме', 'Двор'  # Эти поля есть только в параметрах дома
                }
                
                for key, value in house_params.items():
                    if (key in unique_house_fields and 
                        value and value not in ['нет', 'Нет']):
                        tags.append(f"{key}: {value}")
            
            if tags:
                # Преобразуем список тегов в строку для БД
                db_data['tags'] = ', '.join(tags)
            
            return db_data
            
        except Exception as e:
            print(f"❌ Ошибка подготовки данных для БД: {e}")
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
                return now
            elif 'вчера' in relative_time_lower:
                return now - timedelta(days=1)
            elif 'позавчера' in relative_time_lower:
                return now - timedelta(days=2)
            
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
                        return now - timedelta(hours=count)
                    elif unit == 'days':
                        return now - timedelta(days=count)
                    elif unit == 'weeks':
                        return now - timedelta(weeks=count)
                    elif unit == 'months':
                        return now - timedelta(days=count * 30)
            
            return now
            
        except Exception as e:
            print(f"❌ Ошибка преобразования времени: {e}")
            return datetime.now()
    
    def cleanup(self):
        """Корректно закрывает браузер"""
        try:
            if self.driver:
                print("🧹 Закрываем браузер...")
                self.driver.quit()
                self.driver = None
                print("✅ Браузер закрыт")
        except Exception as e:
            print(f"⚠️ Ошибка при закрытии браузера: {e}")
    
    def __del__(self):
        """Деструктор для автоматической очистки"""
        self.cleanup()

    def extract_photo_urls(self):
        """
        Извлекает URL фотографий из галереи Avito.
        
        Стратегия: извлекаем только средние preview-версии (1.5x из srcset),
        исключая мелкие дубликаты (1x версии).
        """
        try:
            photos = []
            
            # Ищем главное фото (среднее preview)
            try:
                main_photo_wrapper = self.driver.find_element(By.CSS_SELECTOR, 'div[data-marker="image-frame/image-wrapper"]')
                if main_photo_wrapper:
                    # Берем src из img (это среднее preview)
                    main_img = main_photo_wrapper.find_element(By.TAG_NAME, 'img')
                    if main_img:
                        main_url = main_img.get_attribute('src')
                        if main_url and main_url.startswith('http'):
                            photos.append(('main', main_url))
                            print(f"📸 Главное фото (среднее preview): {main_url}")
            except:
                pass
            
            # Ищем фотографии в галерее - только средние preview (1.5x)
            gallery_items = self.driver.find_elements(By.CSS_SELECTOR, 'li[data-marker="image-preview/item"]')
            for i, item in enumerate(gallery_items):
                try:
                    img = item.find_element(By.TAG_NAME, 'img')
                    if img:
                        # Ищем среднюю версию (1.5x) в srcset
                        srcset = img.get_attribute('srcset')
                        medium_url = None
                        
                        if srcset:
                            # Разбираем srcset и ищем версию 1.5x (средняя)
                            srcset_parts = srcset.split(', ')
                            for part in srcset_parts:
                                if ' ' in part:
                                    url = part.split(' ')[0]
                                    size_indicator = part.split(' ')[1]
                                    
                                    # Берем только 1.5x версии (средние preview)
                                    if size_indicator == '1.5x':
                                        medium_url = url
                                        print(f"📸 Фото {i+1} (среднее 1.5x): {medium_url}")
                                        break
                        
                        # Если не нашли 1.5x, НЕ берем обычный src (это мелкий дубликат)
                        # medium_url остается None
                        
                        # Добавляем фото, если нашли среднюю версию
                        if medium_url and medium_url not in [p[1] for p in photos]:
                            photos.append(('gallery', medium_url))
                            
                except Exception as e:
                    print(f"⚠️ Ошибка извлечения фото {i+1}: {e}")
                    continue
            
            print(f"📸 Всего найдено средних preview-фотографий: {len(photos)}")
            print("ℹ️  Примечание: Извлекаем только средние preview (1.5x), исключая мелкие дубликаты")
            
            return photos
            
        except Exception as e:
            print(f"❌ Ошибка при извлечении фотографий: {e}")
            return []

    def extract_high_quality_photos(self):
        """
        Извлекает все фотографии в высоком качестве через навигацию по слайдеру.
        
        Стратегия:
        1. Находим все preview-изображения
        2. Кликаем по каждому для загрузки в главном слайдере
        3. Извлекаем полноразмерное изображение
        4. Используем стрелки для навигации
        """
        try:
            print("📸 Извлекаем фотографии в высоком качестве...")
            photos = []
            
            # Ждем загрузки галереи
            time.sleep(3)
            
            # Находим все preview-изображения
            preview_items = self.driver.find_elements(By.CSS_SELECTOR, 'li[data-marker="image-preview/item"]')
            if not preview_items:
                print("⚠️ Preview-изображения не найдены, используем базовый метод")
                return self.extract_photo_urls()
            
            print(f"📸 Найдено {len(preview_items)} preview-изображений")
            
            # Начинаем с первого изображения (главное фото)
            try:
                # Извлекаем главное фото
                main_photo = self.extract_current_main_photo()
                if main_photo:
                    photos.append(('main', main_photo))
                    print(f"📸 Главное фото (высокое качество): {main_photo}")
            except Exception as e:
                print(f"⚠️ Ошибка извлечения главного фото: {e}")
            
            # Проходим по всем preview-изображениям
            for i, preview_item in enumerate(preview_items):
                try:
                    print(f"📸 Обрабатываем фото {i+1}/{len(preview_items)}...")
                    
                    # Кликаем по preview для загрузки в главном слайдере
                    self.driver.execute_script("arguments[0].click();", preview_item)
                    time.sleep(2)  # Ждем загрузки изображения
                    
                    # Извлекаем текущее изображение в высоком качестве
                    current_photo = self.extract_current_main_photo()
                    if current_photo and current_photo not in [p[1] for p in photos]:
                        photos.append(('gallery', current_photo))
                        print(f"📸 Фото {i+1} (высокое качество): {current_photo}")
                    
                except Exception as e:
                    print(f"⚠️ Ошибка обработки фото {i+1}: {e}")
                    continue
            
            # Дополнительно используем стрелки для навигации
            photos = self.navigate_with_arrows(photos)
            
            print(f"✅ Всего извлечено фотографий в высоком качестве: {len(photos)}")
            return photos
            
        except Exception as e:
            print(f"❌ Ошибка при извлечении фотографий в высоком качестве: {e}")
            # Возвращаемся к базовому методу
            return self.extract_photo_urls()

    def extract_current_main_photo(self):
        """Извлекает текущее изображение из главного слайдера в высоком качестве"""
        try:
            # Ищем главное изображение
            main_photo_wrapper = self.driver.find_element(By.CSS_SELECTOR, 'div[data-marker="image-frame/image-wrapper"]')
            if not main_photo_wrapper:
                return None
            
            # Ищем img элемент
            img = main_photo_wrapper.find_element(By.TAG_NAME, 'img')
            if not img:
                return None
            
            # Пробуем получить srcset для высокого качества
            srcset = img.get_attribute('srcset')
            if srcset:
                # Ищем версию 2x или 3x (высокое качество)
                srcset_parts = srcset.split(', ')
                high_quality_url = None
                
                for part in srcset_parts:
                    if ' ' in part:
                        url = part.split(' ')[0]
                        size_indicator = part.split(' ')[1]
                        
                        # Приоритет: 3x > 2x > 1.5x
                        if size_indicator == '3x':
                            high_quality_url = url
                            break
                        elif size_indicator == '2x' and not high_quality_url:
                            high_quality_url = url
                        elif size_indicator == '1.5x' and not high_quality_url:
                            high_quality_url = url
                
                if high_quality_url:
                    return high_quality_url
            
            # Если srcset не найден, берем обычный src
            return img.get_attribute('src')
            
        except Exception as e:
            print(f"⚠️ Ошибка извлечения текущего фото: {e}")
            return None

    def navigate_with_arrows(self, existing_photos):
        """
        Использует стрелки для навигации по слайдеру и извлечения дополнительных фото
        """
        try:
            print("🔄 Используем стрелки для навигации...")
            
            # Ищем кнопки навигации
            next_button = None
            prev_button = None
            
            # Ищем разные варианты кнопок навигации
            next_selectors = [
                'button[data-marker="image-frame/next"]',
                'button[class*="next"]',
                'button[class*="arrow"]',
                'div[class*="next"]',
                'div[class*="arrow"]'
            ]
            
            for selector in next_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        if elem.is_displayed() and elem.is_enabled():
                            next_button = elem
                            break
                    if next_button:
                        break
                except:
                    continue
            
            if not next_button:
                print("⚠️ Кнопка 'вперед' не найдена")
                return existing_photos
            
            # Навигация вперед
            max_attempts = 20  # Максимум попыток
            attempts = 0
            
            while attempts < max_attempts:
                try:
                    # Проверяем, можно ли нажать кнопку
                    if not next_button.is_enabled() or not next_button.is_displayed():
                        print("🛑 Кнопка 'вперед' недоступна, останавливаемся")
                        break
                    
                    # Нажимаем кнопку
                    self.driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(2)  # Ждем загрузки
                    
                    # Извлекаем текущее изображение
                    current_photo = self.extract_current_main_photo()
                    if current_photo and current_photo not in [p[1] for p in existing_photos]:
                        existing_photos.append(('gallery', current_photo))
                        print(f"📸 Дополнительное фото (стрелка): {current_photo}")
                    
                    attempts += 1
                    
                    # Проверяем, не зациклились ли мы
                    if len(existing_photos) > 50:  # Максимум 50 фото
                        print("⚠️ Достигнут лимит фотографий, останавливаемся")
                        break
                    
                except Exception as e:
                    print(f"⚠️ Ошибка навигации вперед: {e}")
                    break
            
            print(f"✅ Навигация стрелками завершена, всего фото: {len(existing_photos)}")
            return existing_photos
            
        except Exception as e:
            print(f"❌ Ошибка навигации стрелками: {e}")
            return existing_photos

    def wait_for_image_loading(self, timeout=10):
        """
        Ждет загрузки изображений в галерее
        """
        try:
            print("⏳ Ждем загрузки изображений...")
            
            # Ждем появления preview-изображений
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'li[data-marker="image-preview/item"]'))
            )
            
            # Дополнительная задержка для полной загрузки
            time.sleep(3)
            
            print("✅ Изображения загружены")
            return True
            
        except TimeoutException:
            print("⚠️ Таймаут ожидания изображений")
            return False
        except Exception as e:
            print(f"⚠️ Ошибка ожидания изображений: {e}")
            return False

    def check_image_quality(self, image_url):
        """
        Проверяет качество изображения по URL
        """
        try:
            if not image_url:
                return "unknown"
            
            # Анализируем URL для определения качества
            if "3x" in image_url or "2x" in image_url:
                return "high"
            elif "1.5x" in image_url:
                return "medium"
            elif "1x" in image_url:
                return "low"
            else:
                # Если нет индикатора размера, считаем средним
                return "medium"
                
        except Exception as e:
            return "unknown"

    def log_photo_extraction_summary(self, photos):
        """
        Логирует сводку по извлеченным фотографиям
        """
        try:
            if not photos:
                print("📸 Фотографии не найдены")
                return
            
            print("\n📊 СВОДКА ПО ФОТОГРАФИЯМ:")
            print("=" * 50)
            
            quality_stats = {"high": 0, "medium": 0, "low": 0, "unknown": 0}
            
            for i, (photo_type, photo_url) in enumerate(photos):
                quality = self.check_image_quality(photo_url)
                quality_stats[quality] += 1
                
                print(f"{i+1:2d}. [{photo_type:8s}] [{quality:6s}] {photo_url}")
            
            print("=" * 50)
            print(f"Всего фотографий: {len(photos)}")
            print(f"Высокое качество: {quality_stats['high']}")
            print(f"Среднее качество: {quality_stats['medium']}")
            print(f"Низкое качество: {quality_stats['low']}")
            print(f"Неизвестно: {quality_stats['unknown']}")
            print("=" * 50)
            
        except Exception as e:
            print(f"⚠️ Ошибка логирования сводки: {e}")

    def extract_photos_with_slider_navigation(self):
        """
        Комбинированный метод извлечения фотографий с навигацией по слайдеру.
        Пытается получить максимальное количество фотографий в высоком качестве.
        """
        try:
            print("📸 Запускаем комбинированное извлечение фотографий...")
            
            # Ждем загрузки изображений
            if not self.wait_for_image_loading():
                print("⚠️ Не удалось дождаться загрузки изображений, используем базовый метод")
                return self.extract_photo_urls()
            
            # Сначала пробуем метод с preview-кликами
            photos = self.extract_high_quality_photos()
            
            # Если получили мало фото, пробуем дополнительную навигацию
            if len(photos) < 5:
                print("🔄 Получено мало фото, пробуем дополнительную навигацию...")
                photos = self.navigate_with_arrows(photos)
            
            # Убираем дубликаты
            unique_photos = []
            seen_urls = set()
            
            for photo_type, photo_url in photos:
                if photo_url and photo_url not in seen_urls:
                    unique_photos.append((photo_type, photo_url))
                    seen_urls.add(photo_url)
            
            # Логируем сводку
            self.log_photo_extraction_summary(unique_photos)
            
            print(f"✅ Итоговое количество уникальных фотографий: {len(unique_photos)}")
            return unique_photos
            
        except Exception as e:
            print(f"❌ Ошибка комбинированного извлечения: {e}")
            # Возвращаемся к базовому методу
            return self.extract_photo_urls()

    def extract_apartment_params(self):
        """Извлекает параметры квартиры"""
        try:
            # Ждем загрузки параметров
            import time
            time.sleep(5)
            
            # Используем JavaScript для извлечения параметров
            js_script = """
            function extractApartmentParams() {
                const result = {};
                const paramItems = document.querySelectorAll('li.cHzV4');
                
                paramItems.forEach((item, index) => {
                    try {
                        const fullText = item.textContent.trim();
                        if (fullText) {
                            const nameSpan = item.querySelector('span.Lg7Ax');
                            if (nameSpan) {
                                const paramName = nameSpan.textContent.replace(':', '').trim();
                                const paramValue = fullText.replace(nameSpan.textContent, '').trim();
                                
                                if (paramName && paramValue) {
                                    result[paramName] = paramValue;
                                }
                            } else {
                                if (fullText.includes(':')) {
                                    const parts = fullText.split(':', 1);
                                    const paramName = parts[0].trim();
                                    const paramValue = parts[1].trim();
                                    
                                    if (paramName && paramValue) {
                                        result[paramName] = paramValue;
                                    }
                                }
                            }
                        }
                    } catch (e) {
                        console.log('Ошибка парсинга параметра:', e);
                    }
                });
                
                return result;
            }
            return extractApartmentParams();
            """
            
            apartment_params = self.driver.execute_script(js_script)
            
            if apartment_params and isinstance(apartment_params, dict):
                return apartment_params
            else:
                # Альтернативный метод
                apartment_params = {}
                try:
                    param_items = self.driver.find_elements(By.CSS_SELECTOR, 'li.cHzV4')
                    
                    for item in param_items:
                        try:
                            full_text = item.text.strip()
                            if not full_text:
                                continue
                            
                            param_name = ""
                            param_value = ""
                            
                            try:
                                param_name_elem = item.find_element(By.CSS_SELECTOR, 'span.Lg7Ax')
                                param_name = param_name_elem.text.replace(':', '').strip()
                                
                                if param_name in full_text:
                                    param_value = full_text.replace(param_name_elem.text, '').strip()
                                    if param_value.startswith(':'):
                                        param_value = param_value[1:].strip()
                            except:
                                if ':' in full_text:
                                    parts = full_text.split(':', 1)
                                    param_name = parts[0].strip()
                                    param_value = parts[1].strip()
                            
                            param_value = param_value.replace('\xa0', ' ').strip()
                            
                            if param_name and param_value:
                                apartment_params[param_name] = param_value
                            
                        except Exception as e:
                            continue
                    
                except Exception as e:
                    pass
                
                return apartment_params
            
        except Exception as e:
            return {}

    def extract_house_params(self):
        """Извлекает параметры дома из блока 'О доме'"""
        try:
            # Добавляем задержку для загрузки динамического контента
            import time
            time.sleep(3)
            
            # Ищем все возможные блоки с параметрами (более широкий поиск)
            all_params_blocks = self.driver.find_elements(By.CSS_SELECTOR, 'div[id="bx_item-params"], div[data-marker*="params"], div[class*="params"]')
            
            house_params_block = None
            
            # Ищем блок с заголовком "О доме"
            for i, block in enumerate(all_params_blocks):
                try:
                    # Ищем заголовок h2
                    headers = block.find_elements(By.CSS_SELECTOR, 'h2')
                    for header in headers:
                        header_text = header.text.strip().lower()
                        if 'доме' in header_text:
                            house_params_block = block
                            break
                    if house_params_block:
                        break
                except Exception as e:
                    continue
            
            # Если не нашли по заголовку, попробуем найти по содержимому HTML
            if not house_params_block:
                for i, block in enumerate(all_params_blocks):
                    try:
                        # Получаем HTML содержимое блока
                        html_content = block.get_attribute('outerHTML').lower()
                        if 'о доме' in html_content:
                            house_params_block = block
                            break
                    except Exception as e:
                        continue
            
            # Если не нашли по заголовку, попробуем найти по содержимому
            if not house_params_block:
                for i, block in enumerate(all_params_blocks):
                    try:
                        # Ищем элементы с параметрами дома
                        param_items = block.find_elements(By.CSS_SELECTOR, 'li.cHzV4')
                        if param_items:
                            # Проверяем содержимое на ключевые слова о доме
                            block_text = block.text.lower()
                            house_keywords = ['тип дома', 'год постройки', 'этажей в доме', 'лифт', 'консьерж']
                            
                            keyword_count = sum(1 for keyword in house_keywords if keyword in block_text)
                            if keyword_count >= 2:  # Если найдено минимум 2 ключевых слова
                                house_params_block = block
                                break
                    except Exception as e:
                        continue
            
            # Если все еще не нашли, попробуем детальный анализ содержимого
            if not house_params_block:
                for i, block in enumerate(all_params_blocks):
                    try:
                        # Получаем весь текст блока
                        block_text = block.text.strip()
                        if block_text:
                            # Проверяем на ключевые слова о доме
                            house_keywords = ['тип дома', 'год постройки', 'этажей в доме', 'лифт', 'консьерж', 'мусоропровод', 'пассажирский лифт', 'грузовой лифт']
                            keyword_count = sum(1 for keyword in house_keywords if keyword.lower() in block_text.lower())
                            
                            if keyword_count >= 2:
                                house_params_block = block
                                break
                    except Exception as e:
                        continue
            
            if not house_params_block:
                return {}
            
            # Используем JavaScript для извлечения параметров дома
            js_script = """
            function extractHouseParams() {
                // Ищем блок с параметрами дома
                const allParamsBlocks = document.querySelectorAll('div[id="bx_item-params"], div[data-marker*="params"], div[class*="params"]');
                let houseParamsBlock = null;
                
                for (let i = 0; i < allParamsBlocks.length; i++) {
                    const block = allParamsBlocks[i];
                    
                    // Проверяем заголовок
                    const header = block.querySelector('h2');
                    if (header && header.textContent.toLowerCase().includes('доме')) {
                        houseParamsBlock = block;
                        break;
                    }
                    
                    // Проверяем содержимое на ключевые слова
                    const blockText = block.textContent.toLowerCase();
                    const houseKeywords = ['тип дома', 'год постройки', 'этажей в доме', 'лифт', 'консьерж', 'мусоропровод', 'пассажирский лифт', 'грузовой лифт'];
                    const keywordCount = houseKeywords.filter(keyword => blockText.includes(keyword)).length;
                    
                    if (keywordCount >= 2) {
                        houseParamsBlock = block;
                        break;
                    }
                }
                
                if (!houseParamsBlock) return {};
                
                const result = {};
                
                // Ищем все элементы параметров
                const paramItems = houseParamsBlock.querySelectorAll('li.cHzV4');
                
                paramItems.forEach((item, index) => {
                    try {
                        const fullText = item.textContent.trim();
                        if (fullText) {
                            // Ищем span с названием параметра
                            const nameSpan = item.querySelector('span.Lg7Ax');
                            if (nameSpan) {
                                const paramName = nameSpan.textContent.replace(':', '').trim();
                                const paramValue = fullText.replace(nameSpan.textContent, '').trim();
                                
                                if (paramName && paramValue) {
                                    result[paramName] = paramValue;
                                }
                            } else {
                                // Если span не найден, пытаемся разобрать по двоеточию
                                if (fullText.includes(':')) {
                                    const parts = fullText.split(':', 1);
                                    const paramName = parts[0].trim();
                                    const paramValue = parts[1].trim();
                                    
                                    if (paramName && paramValue) {
                                        result[paramName] = paramValue;
                                    }
                                }
                            }
                        }
                    } catch (e) {
                        console.log('Ошибка парсинга параметра дома:', e);
                    }
                });
                
                return result;
            }
            return extractHouseParams();
            """
            
            house_params = self.driver.execute_script(js_script)
            
            if house_params and isinstance(house_params, dict):
                return house_params
            else:
                # Альтернативный метод: ищем элементы вручную
                house_params = {}
                
                # Ищем все элементы параметров
                try:
                    param_items = house_params_block.find_elements(By.CSS_SELECTOR, 'li.cHzV4')
                    
                    for i, item in enumerate(param_items):
                        try:
                            # Получаем весь текст элемента
                            full_text = item.text.strip()
                            
                            if not full_text:
                                continue
                            
                            # Ищем span с названием параметра
                            param_name = ""
                            param_value = ""
                            
                            try:
                                param_name_elem = item.find_element(By.CSS_SELECTOR, 'span.Lg7Ax')
                                param_name = param_name_elem.text.replace(':', '').strip()
                                
                                # Получаем значение параметра
                                if param_name in full_text:
                                    param_value = full_text.replace(param_name_elem.text, '').strip()
                                    if param_value.startswith(':'):
                                        param_value = param_value[1:].strip()
                            except:
                                # Если span не найден, разбираем по двоеточию
                                if ':' in full_text:
                                    parts = full_text.split(':', 1)
                                    param_name = parts[0].strip()
                                    param_value = parts[1].strip()
                            
                            # Очищаем значение от лишних символов
                            param_value = param_value.replace('\xa0', ' ').strip()
                            
                            if param_name and param_value:
                                house_params[param_name] = param_value
                            
                        except Exception as e:
                            continue
                    
                except Exception as e:
                    pass
                
                return house_params
            
        except Exception as e:
            return {}

    def extract_address_and_metro(self):
        """Извлекает адрес и информацию о метро из секции Расположение"""
        try:
            import re
            print("📍 Извлекаем адрес и метро...")
            
            # Ищем заголовок "Расположение" с улучшенной стратегией
            location_content = None
            
            try:
                # Стратегия 1: Ищем специфичные селекторы для Авито
                location_selectors = [
                    '[data-marker*="location"]',
                    '[class*="location"]', 
                    'section:has(*:contains("Расположение"))',
                    'div:has(h2:contains("Расположение"))',
                    'div:has(h3:contains("Расположение"))'
                ]
                
                for selector in location_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for elem in elements:
                            elem_text = elem.text.strip()
                            if elem_text and ('м.' in elem_text or 'мин' in elem_text or 'ул.' in elem_text or 'проспект' in elem_text):
                                location_content = elem_text
                                print(f"✅ Найден контент через селектор {selector}: {elem_text[:50]}...")
                                break
                        if location_content:
                            break
                    except:
                        continue
                
                # Стратегия 2: Поиск через заголовок "Расположение"
                if not location_content:
                    # Ищем заголовок
                    header_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Расположение')]")
                    for header in header_elements:
                        if header.tag_name.lower() in ['h2', 'h3', 'h4', 'span', 'div']:
                            print(f"🔍 Найден заголовок в {header.tag_name}: {header.text.strip()}")
                            
                            # Ищем контент в различных местах относительно заголовка
                            search_strategies = [
                                # Следующие соседние элементы
                                ('following-sibling', lambda i: f'./following-sibling::*[{i}]'),
                                # Дочерние элементы родителя после заголовка  
                                ('parent-children', lambda i: f'./../*[position()>1][{i}]'),
                                # Элементы внутри родительского контейнера
                                ('parent-content', lambda i: f'./..//div[{i}]'),
                            ]
                            
                            for strategy_name, xpath_func in search_strategies:
                                for i in range(1, 6):  # Проверяем несколько элементов
                                    try:
                                        xpath = xpath_func(i)
                                        candidate = header.find_element(By.XPATH, xpath)
                                        candidate_text = candidate.text.strip()
                                        
                                        # Проверяем, что это похоже на адрес/метро
                                        if (candidate_text and len(candidate_text) > 5 and 
                                            (('м.' in candidate_text and 'мин' in candidate_text) or 
                                             'ул.' in candidate_text or 'проспект' in candidate_text or
                                             any(word in candidate_text.lower() for word in ['москва', 'санкт', 'новосибирск', 'екатеринбург']))):
                                            location_content = candidate_text
                                            print(f"✅ Найден контент ({strategy_name} #{i}): {candidate_text[:50]}...")
                                            break
                                    except Exception as e:
                                        continue
                                if location_content:
                                    break
                            if location_content:
                                break
                
                # Стратегия 3: Широкий поиск по всей странице
                if not location_content:
                    print("🔍 Широкий поиск адреса по странице...")
                    # Ищем элементы, которые могут содержать адрес
                    all_text_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'м.') and contains(text(), 'мин')]")
                    for elem in all_text_elements:
                        elem_text = elem.text.strip()
                        if len(elem_text) > 10 and len(elem_text) < 200:  # Разумная длина для адреса
                            location_content = elem_text
                            print(f"✅ Найден адрес широким поиском: {elem_text[:50]}...")
                            break
                            
            except Exception as e:
                print(f"⚠️ Ошибка поиска секции расположения: {e}")
            
            if not location_content:
                print("❌ Контент расположения не найден")
                print("🔍 Пробуем найти адрес и метро через старые селекторы...")
                return self._extract_address_old_way()
            
            print(f"📍 Извлеченный текст расположения: {location_content}")
            return self._parse_location_section_text(location_content)
        
        except Exception as e:
            print(f"❌ Ошибка извлечения адреса и метро: {e}")
            return {
                'address': 'Ошибка извлечения',
                'metro_stations': []
            }
    
    def _parse_location_section_text(self, section_text):
        """Парсит текст из секции Расположение"""
        try:
            import re
            
            # Разбиваем на строки, включая переносы строк
            lines = []
            for line in section_text.split('\n'):
                line = line.strip()
                if line:
                    lines.append(line)
            
            print(f"🔍 Обработка строк: {lines}")
            
            # Пропускаем заголовок "Расположение" если он есть
            filtered_lines = []
            for line in lines:
                if line.lower() not in ['расположение', 'location']:
                    filtered_lines.append(line)
            
            if len(filtered_lines) >= 2:
                # Первая строка - адрес
                address = filtered_lines[0].strip()
                print(f"📍 Найден адрес: {address}")
                
                # Вторая строка - метро и время (используем старую логику)
                metro_line = filtered_lines[1].strip()
                print(f"🚇 Парсим строку метро: {metro_line}")
                
                # Парсим метро и время (старая проверенная логика)
                metro_name = None
                time_to_metro = None
                
                # Ищем время до метро (расширенные паттерны из старого кода)
                time_patterns = [
                    r'(\d+)\s*[-–—]\s*(\d+)\s*мин\.?',  # "6–10 мин." 
                    r'(\d+)\s*мин\.?',  # "5 мин."
                    r'до\s*(\d+)\s*мин\.?',  # "до 5 мин."
                    r'(\d+)\s*минут',  # "5 минут"
                ]
                
                # Сначала извлекаем время
                for pattern in time_patterns:
                    time_match = re.search(pattern, metro_line)
                    if time_match:
                        if '–' in pattern or '—' in pattern:
                            # Диапазон времени
                            min_time = int(time_match.group(1))
                            max_time = int(time_match.group(2))
                            time_to_metro = (min_time + max_time) // 2
                        else:
                            # Одно время
                            time_to_metro = int(time_match.group(1))
                        break
                
                # Теперь извлекаем название станции
                # "Римская6–10 мин." -> название "Римская"
                station_match = re.match(r'([А-Яа-яёЁ\s]+?)(?=\d)', metro_line)
                if station_match:
                    metro_name = station_match.group(1).strip()
                else:
                    # Если не получилось через регулярку, пробуем разделить по запятой
                    if ',' in metro_line:
                        parts = metro_line.split(',')
                        for part in parts:
                            part = part.strip()
                            if part and not re.search(r'\d+\s*мин', part):
                                # Это часть без времени - потенциальное название
                                clean_name = re.sub(r'\b(до|пешком|мин\.?|минут)\b', '', part).strip()
                                if clean_name and len(clean_name) > 1:
                                    metro_name = clean_name
                                    break
                
                print(f"✅ Извлечено: станция='{metro_name}', время={time_to_metro}")
                
                return {
                    'address': address,
                    'metro_station': metro_name,
                    'metro_time': time_to_metro, 
                    'metro_way': 'пешком'
                }
                
            elif len(filtered_lines) == 1:
                # Только адрес, метро нет
                address = filtered_lines[0].strip()
                print(f"📍 Найден только адрес: {address}")
                return {
                    'address': address,
                    'metro_station': None,
                    'metro_time': None,
                    'metro_way': None
                }
            else:
                # Нет данных
                return {
                    'address': "Не найден",
                    'metro_station': None,
                    'metro_time': None,
                    'metro_way': None
                }
            
        except Exception as e:
            print(f"❌ Ошибка парсинга секции расположения: {e}")
            return {
                'address': 'Ошибка парсинга',
                'metro_station': None,
                'metro_time': None,
                'metro_way': None
            }
    
    def _extract_address_old_way(self):
        """Старый способ извлечения адреса для обратной совместимости"""
        try:
            # Ищем блок с адресом
            address_block = None
            try:
                address_block = self.driver.find_element(By.CSS_SELECTOR, 'div[itemprop="address"]')
                print("✅ Найден блок адреса с itemprop='address'")
            except Exception as e:
                print(f"❌ Блок адреса не найден: {e}")
                print("🔍 Попробуем альтернативные селекторы...")
                
                # Альтернативные селекторы для адреса
                alternative_selectors = [
                    '[data-marker*="address"]',
                    '[class*="address"]',
                    '[class*="Address"]',
                    'div[itemprop*="address"]'
                ]
                
                for selector in alternative_selectors:
                    try:
                        address_block = self.driver.find_element(By.CSS_SELECTOR, selector)
                        print(f"✅ Найден блок адреса с селектором: {selector}")
                        break
                    except:
                        continue
                
                if not address_block:
                    print("❌ Не удалось найти блок с адресом")
                    return {
                        'address': 'Блок адреса не найден',
                        'metro_station': None,
                        'metro_time': None,
                        'metro_way': None
                    }
            
            # Простое извлечение текста из блока адреса
            block_text = address_block.text.strip()
            print(f"📍 Текст блока адреса: {block_text}")
            
            # Используем наш метод парсинга для текста
            if block_text and len(block_text) > 5:
                parsed_data = self._parse_location_section_text(block_text)
                return parsed_data
            else:
                return {
                    'address': 'Текст адреса пуст',
                    'metro_station': None,
                    'metro_time': None,
                    'metro_way': None
                }
        
        except Exception as e:
            print(f"❌ Ошибка в старом способе извлечения: {e}")
            return {
                'address': 'Ошибка извлечения',
                'metro_station': None,
                'metro_time': None,
                'metro_way': None
            }
    
    def _extract_walking_time_minutes(self, time_text):
        """Извлекает минимальное время в минутах из строки вида '6–10 мин.' или '16–20 мин.'"""
        try:
            # Ищем паттерн с числами и диапазоном
            # Паттерн для поиска чисел в строке времени
            time_pattern = r'(\d+)(?:–(\d+))?\s*мин'
            match = re.search(time_pattern, time_text)
            
            if match:
                min_time = int(match.group(1))
                max_time = int(match.group(2)) if match.group(2) else min_time
                # Возвращаем минимальное время
                return min_time
            
            return None
        except Exception as e:
            print(f"⚠️ Ошибка извлечения времени из '{time_text}': {e}")
            return None


    def parse_address_and_metro_from_text(self, address_text):
        """Парсит адрес и метро из обычного текста (например, 'Метро 10 мин пешком')"""
        try:
            address_data = {}
            metro_stations = []
            
            # Разделяем на строки
            lines = [line.strip() for line in address_text.split('\n') if line.strip()]
            
            current_address = ""
            
            for line in lines:
                # Проверяем различные форматы метро
                metro_patterns = [
                    r'Метро\s+(\d+)\s*мин\s*пешком',  # "Метро 10 мин пешком"
                    r'([A-Яа-я\s\d\-№ёЁ]+?)\s*,\s*(\d+)\s*мин',  # "Улица 1905 года, 10 мин"
                    r'м\s*\.\s*([A-Яа-я\s\d\-№ёЁ]+?)\s*,\s*(\d+)\s*мин',  # "м. Улица 1905 года, 10 мин"
                    r'([A-Яа-я\s\d\-№ёЁ]+?)(\d+)(?:–\d+)?\s*мин',  # "Текстильщики6–10 мин" - новый формат
                ]
                
                metro_found = False
                for pattern in metro_patterns:
                    metro_match = re.search(pattern, line, re.IGNORECASE)
                    if metro_match:
                        if len(metro_match.groups()) == 1:  # Первый паттерн - только время
                            minutes = metro_match.group(1)
                            station_name = "Неизвестно"  # Название не указано
                        else:  # Остальные паттерны - название и время
                            station_name = metro_match.group(1).strip()
                            minutes = metro_match.group(2)
                            
                            # Для нового формата "Текстильщики6–10 мин" очищаем название станции
                            # Убираем лишние пробелы и цифры из конца названия станции
                            station_name = re.sub(r'\d+$', '', station_name).strip()
                        
                        metro_stations.append({
                            'name': station_name,
                            'walking_time': f"{minutes}–{minutes} мин.",
                            'line_colors': []
                        })
                        metro_found = True
                        break
                
                if not metro_found:
                    # Это строка адреса
                    if current_address:
                        current_address += f", {line}"
                    else:
                        current_address = line
            
            address_data['address'] = current_address if current_address else "Не найден"
            address_data['metro_stations'] = metro_stations
            
            return address_data
            
        except Exception as e:
            return {}

    def extract_description(self):
        """Извлекает описание объявления"""
        try:
            # Ищем блок с описанием
            description_block = self.driver.find_element(By.CSS_SELECTOR, 'div[data-marker="item-view/item-description"]')
            
            # Используем JavaScript для извлечения описания
            js_script = """
            function extractDescription() {
                const descriptionBlock = document.querySelector('div[data-marker="item-view/item-description"]');
                if (!descriptionBlock) return '';
                
                // Ищем все параграфы
                const paragraphs = descriptionBlock.querySelectorAll('p');
                if (paragraphs.length > 0) {
                    // Собираем текст из параграфов
                    const textParts = [];
                    paragraphs.forEach(p => {
                        const text = p.textContent.trim();
                        if (text) {
                            textParts.push(text);
                        }
                    });
                    return textParts.join('\\n\\n');
                } else {
                    // Если параграфов нет, берем весь текст
                    return descriptionBlock.textContent.trim();
                }
            }
            return extractDescription();
            """
            
            description_text = self.driver.execute_script(js_script)
            
            if description_text:
                return description_text
            else:
                # Альтернативный метод
                try:
                    paragraphs = description_block.find_elements(By.CSS_SELECTOR, 'p')
                    if paragraphs:
                        text_parts = []
                        for p in paragraphs:
                            text = p.text.strip()
                            if text:
                                text_parts.append(text)
                        return '\n\n'.join(text_parts)
                    else:
                        return description_block.text.strip()
                except:
                    return description_block.text.strip()
            
        except Exception as e:
            return ""

    def extract_publication_info(self):
        """Извлекает информацию о публикации: дату и количество просмотров"""
        try:
            # Используем JavaScript для извлечения информации
            js_script = """
            function extractPublicationInfo() {
                const result = {};
                
                // Ищем дату публикации
                const dateElement = document.querySelector('span[data-marker="item-view/item-date"]');
                if (dateElement) {
                    const dateText = dateElement.textContent.trim();
                    // Убираем лишние символы и пробелы
                    result.publication_date = dateText.replace(/^[·\\s]+/, '').trim();
                }
                
                // Ищем просмотры за сегодня с расширенными селекторами
                const viewSelectors = [
                    'span[data-marker="item-view/today-views"]',
                    'span[data-marker="item-view/views"]',
                    'span[class*="today-views"]',
                    'span[class*="views-today"]',
                    'div[data-marker="item-view/today-views"]',
                    '[data-testid="today-views"]'
                ];
                
                let todayViews = null;
                for (const selector of viewSelectors) {
                    const element = document.querySelector(selector);
                    if (element) {
                        const text = element.textContent.trim();
                        // Пробуем разные паттерны извлечения
                        const patterns = [
                            /\+(\d+)/,           // "+20"
                            /(\d+)\s*сегодня/i,   // "20 сегодня"
                            /(\d+)\s*просмотр/i, // "20 просмотров"
                            /сегодня\s+(\d+)/i,  // "сегодня 20"
                            /(\d+)/              // любое число
                        ];
                        
                        for (const pattern of patterns) {
                            const match = text.match(pattern);
                            if (match) {
                                todayViews = parseInt(match[1]);
                                break;
                            }
                        }
                        
                        if (todayViews !== null) {
                            result.today_views = todayViews;
                            break;
                        }
                    }
                }
                
                return result;
            }
            return extractPublicationInfo();
            """
            
            publication_info = self.driver.execute_script(js_script)
            
            if publication_info and isinstance(publication_info, dict):
                return publication_info
            else:
                # Альтернативный метод: ищем элементы вручную с расширенными селекторами
                publication_info = {}
                try:
                    # Ищем дату публикации
                    date_selectors = [
                        'span[data-marker="item-view/item-date"]',
                        'span[class*="item-date"]',
                        'div[data-marker="item-view/item-date"]',
                        '[data-testid="item-date"]'
                    ]
                    
                    for selector in date_selectors:
                        try:
                            date_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if date_element:
                                date_text = date_element.text.strip()
                                # Убираем лишние символы и пробелы
                                publication_info['publication_date'] = date_text.replace('·', '').strip()
                                break
                        except:
                            continue
                    
                    # Ищем просмотры за сегодня с расширенными селекторами
                    today_views_selectors = [
                        'span[data-marker="item-view/today-views"]',
                        'span[data-marker="item-view/views"]',
                        'span[class*="today-views"]',
                        'span[class*="views-today"]',
                        'div[data-marker="item-view/today-views"]',
                        '[data-testid="today-views"]'
                    ]
                    
                    found_views = False
                    for selector in today_views_selectors:
                        try:
                            today_views_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if today_views_element:
                                today_views_text = today_views_element.text.strip()
                                print(f"🔍 Найден элемент просмотров: '{today_views_text}' (селектор: {selector})")
                                
                                # Пробуем разные паттерны извлечения числа
                                patterns = [
                                    r'\+(\d+)',           # "+20"
                                    r'(\d+)\s*сегодня',    # "20 сегодня"
                                    r'(\d+)\s*просмотр',  # "20 просмотров"
                                    r'сегодня\s+(\d+)',   # "сегодня 20"
                                    r'(\d+)'              # любое число
                                ]
                                
                                for pattern in patterns:
                                    today_views_match = re.search(pattern, today_views_text, re.IGNORECASE)
                                    if today_views_match:
                                        publication_info['today_views'] = int(today_views_match.group(1))
                                        print(f"✅ Извлечено просмотров: {publication_info['today_views']}")
                                        found_views = True
                                        break
                                
                                if found_views:
                                    break
                        except:
                            continue
                    
                    # Если не нашли через основные селекторы, ищем во всем тексте страницы
                    if not found_views:
                        try:
                            page_text = self.driver.find_element(By.TAG_NAME, 'body').text
                            # Ищем паттерны в тексте всей страницы
                            view_patterns = [
                                r'\+(\d+)\s*сегодня',
                                r'(\d+)\s*просмотр.*сегодня',
                                r'сегодня.*?(\d+).*?просмотр'
                            ]
                            
                            for pattern in view_patterns:
                                match = re.search(pattern, page_text, re.IGNORECASE)
                                if match:
                                    publication_info['today_views'] = int(match.group(1))
                                    print(f"📄 Найдено в тексте страницы - просмотров: {publication_info['today_views']}")
                                    found_views = True
                                    break
                        except:
                            pass
                    
                    # Если все еще не нашли, выставляем 0 с предупреждением
                    if not found_views:
                        publication_info['today_views'] = 0
                        print("⚠️ Просмотры за сегодня не найдены, устанавливаем 0")
                        
                except Exception as e:
                    print(f"⚠️ Ошибка альтернативного извлечения: {e}")
                    publication_info['today_views'] = 0
                
                return publication_info
            
        except Exception as e:
            print(f"❌ Ошибка извлечения информации о публикации: {e}")
            return {'today_views': 0}

    def quick_status_check(self):
        """Быстрая проверка статуса страницы без глубокого парсинга"""
        try:
            # Проверяем заголовок страницы
            try:
                page_title = self.driver.title.lower()
                if any(marker in page_title for marker in ['объявление снято', 'страница не найдена', 'ошибка']):
                    return {'status': 'inactive', 'reason': f'Неактивный заголовок: {page_title}'}
            except Exception:
                pass
            
            # Проверяем наличие основных элементов
            try:
                # Проверяем наличие заголовка объявления
                title_selectors = [
                    'h1[data-marker="item-view/title-info"]',
                    'h1[class*="title"]',
                    'h1'
                ]
                
                title_found = False
                for selector in title_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements and any(el.is_displayed() and el.text.strip() for el in elements):
                            title_found = True
                            break
                    except Exception:
                        continue
                
                if not title_found:
                    return {'status': 'inactive', 'reason': 'Заголовок объявления не найден'}
            except Exception:
                pass
            
            # Проверяем наличие сообщений об ошибке
            try:
                page_source = self.driver.page_source.lower()
                error_phrases = ['объявление снято', 'страница не найдена', 'ошибка 404']
                for phrase in error_phrases:
                    if phrase in page_source:
                        return {'status': 'inactive', 'reason': f'Найден текст: {phrase}'}
            except Exception:
                pass
            
            # Если все проверки прошли - страница выглядит активной
            return {'status': 'active', 'reason': 'Быстрая проверка пройдена'}
            
        except Exception as e:
            return {'status': 'unknown', 'reason': f'Ошибка быстрой проверки: {str(e)}'}

    def extract_listing_status(self, title_components=None):
        """Извлекает статус объявления на основе наличия данных из заголовка
        
        Args:
            title_components: словарь с данными из заголовка (rooms, floor, etc.)
            
        Returns:
            dict: {'status': 'active'/'inactive', 'reason': 'описание причины'}
        """
        try:
            # Новая логика: если есть основные данные из заголовка (комнаты, этаж), то объявление активно
            if title_components:
                rooms = title_components.get('rooms')
                floor = title_components.get('floor')
                
                # Проверяем наличие ключевых данных
                has_rooms_data = rooms is not None
                has_floor_data = floor is not None
                
                if has_rooms_data and has_floor_data:
                    return {
                        'status': 'active',
                        'reason': f'✅ Найдены основные данные: {rooms} комн., {floor} этаж'
                    }
                elif has_rooms_data or has_floor_data:
                    return {
                        'status': 'active', 
                        'reason': f'✅ Найдены частичные данные: комнаты={rooms}, этаж={floor}'
                    }
                else:
                    return {
                        'status': 'inactive',
                        'reason': '❌ Нет основных данных из заголовка (комнаты, этаж)'
                    }
            
            # Если title_components не передан, пытаемся извлечь заголовок прямо здесь
            try:
                title_element = self.driver.find_element(By.CSS_SELECTOR, 'h1[data-marker="item-view/title-info"]')
                title_text = title_element.text.strip()
                
                if title_text and title_text != "Не найдено":
                    # Парсим заголовок
                    components = self.parse_title(title_text)
                    return self.extract_listing_status(components)  # Рекурсивный вызов с данными
                else:
                    return {
                        'status': 'inactive',
                        'reason': '❌ Заголовок не найден или пустой'
                    }
                    
            except Exception as e:
                return {
                    'status': 'inactive',
                    'reason': f'❌ Ошибка извлечения заголовка: {str(e)}'
                }
                
        except Exception as e:
            return {
                'status': 'unknown',
                'reason': f'❌ Ошибка проверки статуса: {str(e)}'
            }

    def parse_avito_page(self, url):
        """Парсит полную страницу объявления Avito"""
        try:
            print(f"🔄 Парсим страницу Avito: {url}")
            
            # Инициализируем Selenium
            if not self.setup_selenium():
                print("❌ Не удалось настроить Selenium")
                return None
            
            # Загружаем cookies
            cookies_data = self.load_cookies()
            if not cookies_data:
                print("❌ Не удалось загрузить cookies")
                return None
            
            # Применяем cookies
            if not self.apply_cookies(cookies_data):
                print("❌ Не удалось применить cookies")
                return None
            
            # Переходим на страницу объявления
            print(f"🌐 Переходим на страницу: {url}")
            
            # Устанавливаем таймаут для загрузки страницы
            self.driver.set_page_load_timeout(30)  # 30 секунд максимум
            
            try:
                self.driver.get(url)
                # Добавляем задержку для полной загрузки страницы (как в старом парсере)
                print("⏳ Ждем загрузки страницы...")
                time.sleep(5)  # Увеличиваем до 5 секунд для стабильной загрузки
            except Exception as e:
                print(f"⚠️ Таймаут или ошибка загрузки страницы: {e}")
                # Возможно, страница не существует или заблокирована
                return {
                    'url': url,
                    'title': 'Страница недоступна',
                    'price': 'Не найдено',
                    'status_info': {'status': 'inactive', 'reason': f'Ошибка загрузки страницы: {str(e)}'}
                }
            
            # Дополнительная пауза для стабилизации DOM (как в старом парсере)
            print("⏳ Ждем стабилизации DOM...")
            time.sleep(3)
            
            # Пауза перед началом парсинга (как в старом парсере)
            print("⏳ Подготовка к парсингу...")
            time.sleep(2)
            
            # Быстрая проверка статуса страницы перед полным парсингом
            quick_status = self.quick_status_check()
            if quick_status['status'] == 'inactive':
                print(f"🚫 Объявление неактивно: {quick_status['reason']}")
                return {
                    'url': url,
                    'title': 'Объявление неактивно',
                    'price': 'Не найдено',
                    'status_info': quick_status
                }
            
            # Извлекаем заголовок
            try:
                title_element = self.driver.find_element(By.CSS_SELECTOR, 'h1[data-marker="item-view/title-info"]')
                title_text = title_element.text.strip()
                print(f"📝 Заголовок: {title_text}")
                
                # Парсим заголовок на компоненты
                title_components = self.parse_title(title_text)
                print(f"🏠 Комнаты: {title_components.get('rooms')}")
                print(f"📏 Площадь: {title_components.get('total_area')} м²")
                print(f"🏢 Этаж: {title_components.get('floor')}/{title_components.get('total_floors')}")
                
            except Exception as e:
                print(f"❌ Ошибка извлечения заголовка: {e}")
                title_text = "Не найдено"
                title_components = {}
            
            # Извлекаем цену
            try:
                # Пробуем разные селекторы для цены
                price_selectors = [
                    'span[data-marker="price-value"]',
                    'span[data-marker="item-price"]',
                    'span[class*="price-value"]',
                    'span[class*="price"]',
                    'div[data-marker="price-value"]',
                    'div[data-marker="item-price"]',
                    'div[class*="price-value"]',
                    'div[class*="price"]',
                    'span[itemprop="price"]',
                    'span[class*="amount"]'
                ]
                
                price_text = "Не найдено"
                for selector in price_selectors:
                    try:
                        price_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in price_elements:
                            text = element.text.strip()
                            if text and text != "История цены" and len(text) > 3:
                                # Проверяем, что это действительно цена (содержит цифры и символы валюты)
                                if any(char.isdigit() for char in text) and any(char in '₽$€' for char in text):
                                    price_text = text
                                    break
                        if price_text != "Не найдено":
                            break
                    except:
                        continue
                
                print(f"💰 Цена: {price_text}")
            except:
                price_text = "Не найдено"
                print("❌ Цена не найдена")
            
            # Извлекаем фотографии в высоком качестве (только если не отключено)
            if self.skip_photos:
                print("📸 Пропускаем извлечение фотографий (skip_photos=True)")
                photos = []
            else:
                print("📸 Извлекаем фотографии в высоком качестве...")
                photos = self.extract_photos_with_slider_navigation()
                print(f"✅ Найдено фотографий в высоком качестве: {len(photos)}")
            
            # Извлекаем параметры квартиры
            print("🏠 Извлекаем параметры квартиры...")
            apartment_params = self.extract_apartment_params()
            print(f"✅ Параметров квартиры: {len(apartment_params)}")
            
            # Пауза между извлечениями (как в старом парсере)
            time.sleep(2)
            
            # Извлекаем параметры дома
            print("🏢 Извлекаем параметры дома...")
            house_params = self.extract_house_params()
            print(f"✅ Параметров дома: {len(house_params)}")
            
            # Пауза между извлечениями (как в старом парсере)
            time.sleep(2)
            
            # Извлекаем адрес и метро
            print("📍 Извлекаем адрес и метро...")
            address_data = self.extract_address_and_metro()
            print(f"✅ Адрес: {address_data.get('address', 'Не найден')}")
            print(f"✅ Станций метро: {len(address_data.get('metro_stations', []))}")
            
            # Если метро не найдено и мы не для Excel, пробуем еще раз
            if len(address_data.get('metro_stations', [])) == 0 and not self.skip_photos:
                print("🔄 Метро не найдено, пробуем еще раз...")
                time.sleep(2)
                address_data = self.extract_address_and_metro()
                print(f"✅ Станций метро (повторная попытка): {len(address_data.get('metro_stations', []))}")
            
            # Всегда проверяем статус на основе данных заголовка
            print("🔍 Проверяем статус объявления...")
            status_info = self.extract_listing_status(title_components)
            print(f"✅ Статус: {status_info.get('status', 'Неизвестно')} - {status_info.get('reason', 'Нет описания')}")
            
            # Для Excel экспорта пропускаем дополнительные словные данные
            if self.skip_photos:
                print("⚡ Быстрый режим для Excel: пропускаем описание")
                description = ""
                publication_info = {}
            else:
                # Извлекаем описание
                print("📝 Извлекаем описание...")
                description = self.extract_description()
                print(f"✅ Описание: {len(description)} символов")
                
                # Извлекаем информацию о публикации
                print("📅 Извлекаем информацию о публикации...")
                publication_info = self.extract_publication_info()
                print(f"✅ Дата: {publication_info.get('publication_date', 'Не найдена')}")
                print(f"✅ Просмотров сегодня: {publication_info.get('today_views', 'Не найдено')}")
            # Собираем все данные
            result = {
                'url': url,
                'title': title_text,
                'price': price_text,
                'photos': photos,
                'apartment_params': apartment_params,
                'house_params': house_params,
                'address_data': address_data,
                'description': description,
                'publication_info': publication_info,
                'status_info': status_info,
                **title_components  # Добавляем компоненты заголовка
            }
            
            print("✅ Парсинг завершен успешно!")
            return result
            
        except Exception as e:
            print(f"❌ Ошибка парсинга страницы: {e}")
            return None
        finally:
            # Очищаем ресурсы
            self.cleanup()
