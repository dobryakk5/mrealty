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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

class AvitoCardParser:
    """Парсер карточек объявлений Avito для listings_processor"""
    
    def __init__(self):
        self.driver = None
        self.cookies_file = "avito_cookies.json"
        
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
            
            # Headless режим для быстрой работы
            options.add_argument("--headless")
            
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
            
            print("🔧 Создаем браузер...")
            self.driver = webdriver.Chrome(options=options)
            
            # Убираем webdriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
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
            
            print(f"📊 Применяем cookies: {len(cookies_data['cookies'])}")
            
            # Сначала переходим на домен
            print("🌐 Переходим на AVITO...")
            self.driver.get("https://avito.ru")
            time.sleep(3)
            
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
            
            # Обновляем страницу с примененными cookies
            self.driver.refresh()
            time.sleep(3)
            
            return applied_count > 0
            
        except Exception as e:
            print(f"❌ Ошибка применения cookies: {e}")
            return False
    
    def parse_card(self, url):
        """Парсит полную страницу объявления Avito"""
        return self.parse_avito_page(url)
    
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
            # Ищем "5/14 эт." или "5 из 14"
            floor_match = re.search(r'(\d+)\s*[\/из]\s*(\d+)\s*эт', title_text)
            if floor_match:
                title_data['floor'] = int(floor_match.group(1))
                title_data['total_floors'] = int(floor_match.group(2))
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
            
            # Базовые данные
            db_data = {
                'url': parsed_data.get('url', ''),
                'title': parsed_data.get('title', ''),
                'price': parsed_data.get('price', ''),
                'description': parsed_data.get('description', ''),
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
            
            # Адрес и метро
            address_data = parsed_data.get('address_data', {})
            if address_data:
                db_data['address'] = address_data.get('address', '')
                
                # Метро
                metro_stations = address_data.get('metro_stations', [])
                if metro_stations:
                    metro_names = []
                    metro_times = []
                    for station in metro_stations:
                        if station.get('name'):
                            metro_names.append(station['name'])
                        if station.get('walking_time'):
                            metro_times.append(station['walking_time'])
                    
                    db_data['metro_stations'] = ', '.join(metro_names) if metro_names else ''
                    db_data['metro_times'] = ', '.join(metro_times) if metro_times else ''
                    
                    # Извлекаем время до ближайшего метро для Excel
                    if metro_times and metro_names:
                        # Берем первое метро (обычно самое близкое)
                        first_time = metro_times[0]
                        first_station = metro_names[0]
                        
                        # Извлекаем первую цифру из "6–10 мин." или "16–20 мин."
                        time_match = re.search(r'(\d+)', first_time)
                        if time_match:
                            minutes = int(time_match.group(1))
                            db_data['metro_time'] = f"{minutes} {first_station}"
                        else:
                            db_data['metro_time'] = f"0 {first_station}"
                    else:
                        db_data['metro_time'] = None
            
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
                    floor_match = re.search(r'(\d+)\s*из\s*(\d+)', floor_text)
                    if floor_match:
                        db_data['floor_from_params'] = int(floor_match.group(1))
                        db_data['total_floors_from_params'] = int(floor_match.group(2))
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
                    floors_match = re.search(r'(\d+)', floors_text)
                    if floors_match:
                        db_data['house_floors'] = int(floors_match.group(1))
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
        """Извлекает адрес и информацию о метро"""
        try:
            # Ищем блок с адресом
            address_block = self.driver.find_element(By.CSS_SELECTOR, 'div[itemprop="address"]')
            
            # Используем JavaScript для извлечения адреса и метро
            js_script = """
            function extractAddressAndMetro() {
                const addressBlock = document.querySelector('div[itemprop="address"]');
                if (!addressBlock) return {};
                
                const result = {};
                
                // Извлекаем адрес
                const addressSpan = addressBlock.querySelector('span.xLPJ6');
                if (addressSpan) {
                    result.address = addressSpan.textContent.trim();
                }
                
                // Извлекаем информацию о метро
                const metroStations = [];
                const metroSpans = addressBlock.querySelectorAll('span.tAdYM');
                
                metroSpans.forEach((metroSpan, index) => {
                    try {
                        const stationName = '';
                        const walkingTime = '';
                        const lineColors = [];
                        
                        // Ищем название станции (исключаем элементы с определенными классами)
                        const allElements = metroSpan.querySelectorAll('*');
                        for (let elem of allElements) {
                            if (!elem.classList.contains('KIhHC') && 
                                !elem.classList.contains('LHPFZ') && 
                                !elem.classList.contains('dt6FF') &&
                                elem.textContent.trim() &&
                                !elem.textContent.includes('мин') &&
                                !elem.textContent.includes('–')) {
                                stationName = elem.textContent.trim();
                                break;
                            }
                        }
                        
                        // Ищем цвета линий
                        const colorElements = metroSpan.querySelectorAll('i.dJYsT');
                        colorElements.forEach(colorElem => {
                            const style = colorElem.getAttribute('style');
                            if (style && style.includes('background-color:')) {
                                const colorMatch = style.match(/#[0-9A-Fa-f]{6}/);
                                if (colorMatch) {
                                    lineColors.push(colorMatch[0]);
                                }
                            }
                        });
                        
                        // Ищем время в пути
                        for (let elem of allElements) {
                            const text = elem.textContent.trim();
                            if (text.includes('мин') || text.includes('–')) {
                                walkingTime = text;
                                break;
                            }
                        }
                        
                        if (stationName) {
                            metroStations.push({
                                name: stationName,
                                walking_time: walkingTime,
                                line_colors: lineColors
                            });
                        }
                    } catch (e) {
                        console.log('Ошибка парсинга станции метро:', e);
                    }
                });
                
                result.metro_stations = metroStations;
                return result;
            }
            return extractAddressAndMetro();
            """
            
            address_data = self.driver.execute_script(js_script)
            
            if address_data and isinstance(address_data, dict):
                return address_data
            else:
                # Альтернативный метод
                address_data = {}
                try:
                    address_span = address_block.find_element(By.CSS_SELECTOR, 'span.xLPJ6')
                    if address_span:
                        address_data['address'] = address_span.text.strip()
                    
                    metro_stations = []
                    metro_spans = address_block.find_elements(By.CSS_SELECTOR, 'span.tAdYM')
                    
                    for metro_span in metro_spans:
                        try:
                            station_name = ""
                            walking_time = ""
                            line_colors = []
                            
                            # Ищем название станции
                            all_elements = metro_span.find_elements(By.CSS_SELECTOR, '*')
                            for elem in all_elements:
                                elem_text = elem.text.strip()
                                if (elem_text and 
                                    'мин' not in elem_text and 
                                    '–' not in elem_text and
                                    not elem.get_attribute('class') in ['KIhHC', 'LHPFZ', 'dt6FF']):
                                    station_name = elem_text
                                    break
                            
                            # Ищем цвета линий
                            color_elements = metro_span.find_elements(By.CSS_SELECTOR, 'i.dJYsT')
                            for color_elem in color_elements:
                                style = color_elem.get_attribute('style')
                                if style and 'background-color:' in style:
                                    import re
                                    color_match = re.search(r'#[0-9A-Fa-f]{6}', style)
                                    if color_match:
                                        line_colors.append(color_match.group(0))
                            
                            # Ищем время в пути
                            for elem in all_elements:
                                elem_text = elem.text.strip()
                                if 'мин' in elem_text or '–' in elem_text:
                                    walking_time = elem_text
                                    break
                            
                            if station_name:
                                metro_stations.append({
                                    'name': station_name,
                                    'walking_time': walking_time,
                                    'line_colors': line_colors
                                })
                        
                        except Exception as e:
                            continue
                    
                    address_data['metro_stations'] = metro_stations
                    
                except Exception as e:
                    pass
                
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
                
                // Ищем просмотры за сегодня
                const todayViewsElement = document.querySelector('span[data-marker="item-view/today-views"]');
                if (todayViewsElement) {
                    const todayViewsText = todayViewsElement.textContent.trim();
                    // Извлекаем число из текста "(+20 сегодня)"
                    const todayViewsMatch = todayViewsText.match(/\\+(\\d+)/);
                    if (todayViewsMatch) {
                        result.today_views = parseInt(todayViewsMatch[1]);
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
                # Альтернативный метод: ищем элементы вручную
                publication_info = {}
                try:
                    # Ищем дату публикации
                    date_element = self.driver.find_element(By.CSS_SELECTOR, 'span[data-marker="item-view/item-date"]')
                    if date_element:
                        date_text = date_element.text.strip()
                        # Убираем лишние символы и пробелы
                        publication_info['publication_date'] = date_text.replace('·', '').strip()
                    
                    # Ищем просмотры за сегодня
                    today_views_element = self.driver.find_element(By.CSS_SELECTOR, 'span[data-marker="item-view/today-views"]')
                    if today_views_element:
                        today_views_text = today_views_element.text.strip()
                        # Извлекаем число из текста
                        today_views_match = re.search(r'\+(\d+)', today_views_text)
                        if today_views_match:
                            publication_info['today_views'] = int(today_views_match.group(1))
                    
                except Exception as e:
                    pass
                
                return publication_info
            
        except Exception as e:
            return {}

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
            self.driver.get(url)
            
            # Ждем загрузки страницы
            import time
            time.sleep(5)
            
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
            
            # Извлекаем фотографии
            print("📸 Извлекаем фотографии...")
            photos = self.extract_photo_urls()
            print(f"✅ Найдено фотографий: {len(photos)}")
            
            # Извлекаем параметры квартиры
            print("🏠 Извлекаем параметры квартиры...")
            apartment_params = self.extract_apartment_params()
            print(f"✅ Параметров квартиры: {len(apartment_params)}")
            
            # Извлекаем параметры дома
            print("🏢 Извлекаем параметры дома...")
            house_params = self.extract_house_params()
            print(f"✅ Параметров дома: {len(house_params)}")
            
            # Извлекаем адрес и метро
            print("📍 Извлекаем адрес и метро...")
            time.sleep(5)  # Увеличиваем задержку для загрузки динамического контента
            address_data = self.extract_address_and_metro()
            print(f"✅ Адрес: {address_data.get('address', 'Не найден')}")
            print(f"✅ Станций метро: {len(address_data.get('metro_stations', []))}")
            
            # Если метро не найдено, пробуем еще раз с дополнительной задержкой
            if len(address_data.get('metro_stations', [])) == 0:
                print("🔄 Метро не найдено, пробуем еще раз...")
                time.sleep(3)
                address_data = self.extract_address_and_metro()
                print(f"✅ Станций метро (повторная попытка): {len(address_data.get('metro_stations', []))}")
            
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
