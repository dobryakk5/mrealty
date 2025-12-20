#!/usr/bin/env python3
"""
Парсер объявлений Yandex Realty с выводом на экран
Извлекает данные с отдельной страницы объявления и выводит параметры

ПРИМЕРЫ URL:
- Конкретное объявление: https://realty.yandex.ru/offer/4416594170111710645/
- Поиск: https://realty.yandex.ru/moskva/kupit/kvartira/

ИСПОЛЬЗОВАНИЕ:
python parse_yandex_to_db.py <URL>

ПРИМЕРЫ:
python parse_yandex_to_db.py https://realty.yandex.ru/offer/4416594170111710645/
"""

import argparse
import time
import re
from typing import Dict, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

# ========== НАСТРОЙКИ ==========
REQUEST_DELAY = 2.0
TIMEOUT = 10

# ========== УТИЛИТЫ ==========

def _clean(s):
    """Очищает строку от лишних пробелов"""
    if s is None:
        return None
    return re.sub(r'\s+', ' ', s).strip()

def setup_driver():
    """Настраивает Chrome WebDriver"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Запуск в фоновом режиме
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36')
    
    # Отключаем изображения для ускорения
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    
    # На macOS может понадобиться указать путь к Chrome
    import os
    if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
        chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except Exception as e:
        print(f"Ошибка создания драйвера Chrome: {e}")
        print("Убедитесь, что ChromeDriver установлен и находится в PATH")
        print("Или установите его через: brew install chromedriver")
        raise

# ========== ФУНКЦИИ ПАРСИНГА ==========

def parse_yandex_listing(driver, url: str) -> Dict:
    """
    Парсит одно объявление Yandex Realty
    
    Args:
        driver: WebDriver instance
        url: URL объявления
        
    Returns:
        Dict: Данные объявления
    """
    out = {
        'url': url,
        'price': None,
        'rooms': None,
        'area_m2': None,
        'floor': None,
        'floor_total': None,
        'district': None,
        'metro': None,
        'walk_minutes': None,
        'address': None,
        'house_type': None,
        'year_built': None,
        'seller_type': None,
        'seller_name': None,
        'description': None,
        'photos_count': None,
        'renovation': None,
        'bathroom_type': None,
        'balcony': None,
        'window_view': None,
        'status': 'active',  # По умолчанию считаем активным
        'views_count': None,
    }
    
    try:
        print(f"Загружаем страницу: {url}")
        driver.get(url)
        
        # Ждем загрузки основного контента
        try:
            # Попробуем разные селекторы для ожидания загрузки
            selectors_to_wait = [
                "[data-test-id='offer-card']",
                ".OfferSummary",
                ".OfferCardSummary",
                "h1",
                ".OfferTitle"
            ]
            
            for selector in selectors_to_wait:
                try:
                    WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    break
                except TimeoutException:
                    continue
            else:
                print("Не удалось дождаться загрузки страницы с основными селекторами")
                # Все равно продолжаем, может быть страница загрузилась
                
        except Exception as e:
            print(f"Ошибка ожидания загрузки: {e}")
        
        time.sleep(REQUEST_DELAY)
        
        # Получаем HTML и парсим с BeautifulSoup
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Заголовок объявления - парсим area и rooms, но не сохраняем title
        try:
            title_selectors = [
                'h1[data-test-id="offer-title"]',
                '.OfferTitle',
                '.OfferCardTitle',
                'h1',
                '.offer-title'
            ]
            
            for selector in title_selectors:
                title_el = soup.select_one(selector)
                if title_el:
                    title_text = _clean(title_el.get_text())
                    if title_text:
                        # Извлекаем площадь из заголовка если ещё не найдена
                        if out['area_m2'] is None:
                            area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', title_text)
                            if area_match:
                                out['area_m2'] = float(area_match.group(1).replace(',', '.'))
                        
                        # Извлекаем количество комнат если ещё не найдено
                        if out['rooms'] is None:
                            # Проверяем на студию
                            if 'студия' in title_text.lower():
                                out['rooms'] = 0
                            else:
                                # Ищем количество комнат
                                rooms_match = re.search(r'(\d+)[\s-]*комн', title_text, re.IGNORECASE)
                                if rooms_match:
                                    out['rooms'] = int(rooms_match.group(1))
                        break
                    
        except Exception as e:
            print(f"Ошибка парсинга заголовка: {e}")
        
        # Статус объявления (активно / снято)
        try:
            # Ищем бэджи со статусом
            status_selectors = [
                '.OfferCardSummary__tags--QypeB',  # Контейнер с тэгами
                '.OfferCardSummaryTags__tags--380wa',
                '[class*="OfferCardSummary"][class*="tags"]',
                '.Badge__badge--PbgSZ',  # Отдельные бэджи
                '[class*="Badge"][class*="badge"]'
            ]
            
            status_found = False
            for selector in status_selectors:
                status_elements = soup.select(selector)
                for element in status_elements:
                    status_text = _clean(element.get_text().lower())
                    if status_text:
                        # Проверяем на маркеры неактивного объявления
                        inactive_markers = [
                            'снято',
                            'устарело',
                            'неактивно',
                            'закрыто',
                            'отключено',
                            'продано',
                            'архив'
                        ]
                        
                        for marker in inactive_markers:
                            if marker in status_text:
                                out['status'] = 'inactive'
                                status_found = True
                                break
                        
                        if status_found:
                            break
                
                if status_found:
                    break
            
            # Дополнительная проверка по всему тексту страницы
            if not status_found:
                page_text = soup.get_text().lower()
                inactive_phrases = [
                    'объявление снято',
                    'объявление устарело',
                    'объявление неактивно',
                    'объявление закрыто'
                ]
                
                for phrase in inactive_phrases:
                    if phrase in page_text:
                        out['status'] = 'inactive'
                        break
                        
        except Exception as e:
            print(f"Ошибка парсинга статуса: {e}")
        
        # Количество просмотров
        try:
            # Ищем заголовок с информацией о просмотрах
            header_selectors = [
                '.OfferCardSummaryHeader__text--2EMVm',  # Специфичный селектор для Yandex
                '.OfferCardSummary__header--3tUiQ',
                '[class*="OfferCardSummaryHeader"][class*="text"]',
                '[class*="OfferCardSummary"][class*="header"]'
            ]
            
            views_found = False
            for selector in header_selectors:
                header_elements = soup.select(selector)
                for element in header_elements:
                    header_text = _clean(element.get_text())
                    if header_text:
                        # Ищем паттерн для просмотров
                        views_patterns = [
                            r'(\d+)\s*просмотр',  # "32 просмотра"
                            r'(\d+)\s*view',  # на случай английского текста
                        ]
                        
                        for pattern in views_patterns:
                            views_match = re.search(pattern, header_text, re.IGNORECASE)
                            if views_match:
                                out['views_count'] = int(views_match.group(1))
                                views_found = True
                                break
                        
                        if views_found:
                            break
                
                if views_found:
                    break
            
            # Дополнительная проверка по всему тексту страницы
            if not views_found:
                page_text = soup.get_text()
                views_match = re.search(r'(\d+)\s*просмотр', page_text, re.IGNORECASE)
                if views_match:
                    out['views_count'] = int(views_match.group(1))
                    
        except Exception as e:
            print(f"Ошибка парсинга количества просмотров: {e}")
        
        # Цена
        try:
            price_selectors = [
                '.OfferCardSummaryInfo__price--2FD3C',  # Специфичный селектор для Yandex
                '[class*="OfferCardSummaryInfo"][class*="price"]',
                '[data-test-id="price-value"]',
                '.OfferCardPrice__value',
                '.price__value',
                '.OfferCardPrice',
                '[class*="price"][class*="value"]',
                '[class*="Price"]'
            ]
            
            price_found = False
            for selector in price_selectors:
                price_el = soup.select_one(selector)
                if price_el:
                    price_text = _clean(price_el.get_text())
                    if price_text:
                        # Убираем все кроме цифр и пробелов, обрабатываем &nbsp;
                        price_clean = price_text.replace('&nbsp;', ' ').replace('\xa0', ' ')
                        price_digits = re.sub(r'[^\d]', '', price_clean)
                        if price_digits and len(price_digits) >= 6 and len(price_digits) <= 12:  # разумные границы
                            candidate_price = int(price_digits)
                            if 100000 <= candidate_price <= 1000000000:  # от 100тыс до 1млрд
                                out['price'] = candidate_price
                                price_found = True
                                break
            
            if not price_found:
                # Ищем по тексту с рублями в общем тексте страницы
                price_text = soup.get_text()
                price_patterns = [
                    r'(\d{1,3}(?:[\s\u00A0]\d{3}){1,3})\s*₽',  # ограничиваем количество групп
                    r'(\d{1,3}(?:\s\d{3}){1,3})\s*руб',
                    r'Цена[^\d]*(\d{1,3}(?:[\s\u00A0]\d{3}){1,3})'
                ]
                
                for pattern in price_patterns:
                    price_match = re.search(pattern, price_text)
                    if price_match:
                        price_str = re.sub(r'[^\d]', '', price_match.group(1))
                        if price_str and len(price_str) >= 6 and len(price_str) <= 12:
                            candidate_price = int(price_str)
                            if 100000 <= candidate_price <= 1000000000:
                                out['price'] = candidate_price
                                break
                            
        except Exception as e:
            print(f"Ошибка парсинга цены: {e}")
        
        # Характеристики из основной информации
        try:
            # Ищем блоки с характеристиками
            char_selectors = [
                '[data-test-id="offer-summary"]',
                '.OfferSummary',
                '.OfferCardSummary',
                '.offer-summary',
                '.OfferTitle',
                'h1'
            ]
            
            # Собираем весь текст из блоков характеристик
            all_text = ""
            for selector in char_selectors:
                blocks = soup.select(selector)
                for block in blocks:
                    all_text += " " + block.get_text()
            
            # Также добавляем основной текст страницы для поиска
            page_text = soup.get_text()
            combined_text = all_text + " " + page_text
            
            # Комнаты
            if out['rooms'] is None:
                rooms_patterns = [
                    r'(\d+)[\s-]*комн',
                    r'(\d+)-комнатн',
                    r'комнат[^\d]*(\d+)'
                ]
                
                for pattern in rooms_patterns:
                    rooms_match = re.search(pattern, combined_text, re.IGNORECASE)
                    if rooms_match:
                        out['rooms'] = int(rooms_match.group(1))
                        break
                        
                if out['rooms'] is None and 'студия' in combined_text.lower():
                    out['rooms'] = 0
            
            # Площадь
            if out['area_m2'] is None:
                area_patterns = [
                    r'(\d+[.,]?\d*)\s*м²',
                    r'площадь[^\d]*(\d+[.,]?\d*)\s*м',
                    r'(\d+[.,]?\d*)\s*кв\.?\s*м'
                ]
                
                for pattern in area_patterns:
                    area_match = re.search(pattern, combined_text, re.IGNORECASE)
                    if area_match:
                        out['area_m2'] = float(area_match.group(1).replace(',', '.'))
                        break
            
            # Этаж
            if out['floor'] is None:
                floor_patterns = [
                    r'(\d+)\s*/\s*(\d+)\s*эт',
                    r'этаж[^\d]*(\d+)[^\d]*из[^\d]*(\d+)',
                    r'(\d+)\s*этаж[^\d]*из[^\d]*(\d+)',
                    r'(\d+)/(\d+)\s*эт'
                ]
                
                for pattern in floor_patterns:
                    floor_match = re.search(pattern, combined_text, re.IGNORECASE)
                    if floor_match:
                        out['floor'] = int(floor_match.group(1))
                        out['floor_total'] = int(floor_match.group(2))
                        break
                
        except Exception as e:
            print(f"Ошибка парсинга характеристик: {e}")
        
        # Адрес и район
        try:
            address_selectors = [
                '.CardLocation__addressItem--1JYpZ',  # Специфичный селектор для Yandex
                '[class*="CardLocation"][class*="addressItem"]',
                '[data-test-id="offer-location"]',
                '.OfferLocation',
                '.offer-location',
                '[class*="Location"]',
                '[class*="Address"]'
            ]
            
            for selector in address_selectors:
                address_el = soup.select_one(selector)
                if address_el:
                    address_text = _clean(address_el.get_text())
                    if address_text and len(address_text) > 5:  # Минимальная длина адреса
                        out['address'] = address_text
                        break
            
            # Если не нашли через селекторы, ищем по тексту
            if not out['address']:
                page_text = soup.get_text()
                # Ищем адрес по паттернам
                address_patterns = [
                    r'Москва,\s*[^,\n]{10,}',  # Москва, далее минимум 10 символов
                    r'Москва[^\n]*ул[^\n]*',
                    r'Москва[^\n]*пр-т[^\n]*',
                    r'Москва[^\n]*пер[^\n]*',
                    r'Москва[^\n]*бул[^\n]*',
                    r'Москва[^\n]*шоссе[^\n]*'
                ]
                
                for pattern in address_patterns:
                    addr_match = re.search(pattern, page_text, re.IGNORECASE)
                    if addr_match:
                        candidate_addr = _clean(addr_match.group(0))
                        if len(candidate_addr) > 10:  # Фильтруем слишком короткие результаты
                            out['address'] = candidate_addr
                            break
                
            # Извлекаем район из адреса
            if out['address']:
                district_patterns = [
                    r'([^Москва,]+район)',
                    r'([^Москва,]+р-н)',
                    r'([^Москва,]+округ)',
                    r'([А-Яа-я]+ский)\s+район',
                    r'([А-Яа-я]+)\s+р-н'
                ]
                
                for pattern in district_patterns:
                    district_match = re.search(pattern, out['address'], re.IGNORECASE)
                    if district_match:
                        out['district'] = _clean(district_match.group(1))
                        break
                        
        except Exception as e:
            print(f"Ошибка парсинга адреса: {e}")
        
        # Метро
        try:
            metro_selectors = [
                '[data-test-id="metro-station"]',
                '.MetroStation',
                '.metro-station',
                '[class*="Metro"]',
                '[class*="metro"]'
            ]
            
            metro_found = False
            for selector in metro_selectors:
                metro_blocks = soup.select(selector)
                for metro_block in metro_blocks:
                    metro_text = _clean(metro_block.get_text())
                    if metro_text and out['metro'] is None:
                        # Очищаем от лишних слов
                        metro_clean = re.sub(r'(м\.|метро|station)', '', metro_text, flags=re.IGNORECASE).strip()
                        if metro_clean and len(metro_clean) > 2:
                            out['metro'] = metro_clean
                            
                            # Ищем время до метро в том же блоке
                            time_patterns = [
                                r'(\d+)\s*мин',
                                r'(\d+)\s*м\.'
                            ]
                            
                            for time_pattern in time_patterns:
                                time_match = re.search(time_pattern, metro_text)
                                if time_match:
                                    out['walk_minutes'] = int(time_match.group(1))
                                    break
                            
                            metro_found = True
                            break
                if metro_found:
                    break
            
            # Если не нашли через селекторы, ищем по всему тексту
            if not metro_found:
                page_text = soup.get_text()
                metro_patterns = [
                    r'м\.\s*([^н\d][^\n,]{3,20})\s*(\d+)\s*мин',
                    r'метро\s*([^\n,]{3,20})\s*(\d+)\s*мин',
                    r'([^н\d][^\n,]{3,20})\s*м\.\s*(\d+)\s*мин'
                ]
                
                for pattern in metro_patterns:
                    metro_match = re.search(pattern, page_text, re.IGNORECASE)
                    if metro_match:
                        out['metro'] = _clean(metro_match.group(1))
                        if len(metro_match.groups()) > 1:
                            out['walk_minutes'] = int(metro_match.group(2))
                        break
                        
        except Exception as e:
            print(f"Ошибка парсинга метро: {e}")
        
        # Дополнительные характеристики из таблицы или списка
        try:
            # Ищем таблицу или список характеристик
            char_table_selectors = [
                '[data-test-id="object-summary-table"]',
                '.OfferSummaryTable',
                '.object-summary-table',
                '[class*="Summary"][class*="Table"]',
                'table',
                '.characteristics'
            ]
            
            page_text = soup.get_text()
            
            for selector in char_table_selectors:
                tables = soup.select(selector)
                for table in tables:
                    rows = table.select('tr')
                    for row in rows:
                        cells = row.select('td')
                        if len(cells) >= 2:
                            key = _clean(cells[0].get_text()).lower()
                            value = _clean(cells[1].get_text())
                            
                            if ('тип дома' in key or 'дом' in key) and out['house_type'] is None:
                                out['house_type'] = value
                            elif ('год' in key or 'построен' in key) and out['year_built'] is None:
                                year_match = re.search(r'(\d{4})', value)
                                if year_match:
                                    out['year_built'] = int(year_match.group(1))
            
            # Если не нашли в таблицах, ищем по всему тексту
            if out['house_type'] is None:
                house_patterns = [
                    r'кирпичный',
                    r'панельный',
                    r'монолитный',
                    r'сталинка',
                    r'хрущевка',
                    r'брежневка'
                ]
                
                for pattern in house_patterns:
                    if re.search(pattern, page_text, re.IGNORECASE):
                        out['house_type'] = pattern
                        break
            
            if out['year_built'] is None:
                year_match = re.search(r'год[^\d]*(\d{4})', page_text, re.IGNORECASE)
                if not year_match:
                    year_match = re.search(r'(19\d{2}|20\d{2})', page_text)
                if year_match:
                    year = int(year_match.group(1))
                    if 1900 <= year <= 2030:  # разумные границы
                        out['year_built'] = year
                        
        except Exception as e:
            print(f"Ошибка парсинга доп. характеристик: {e}")
        
        # Дополнительные особенности квартиры (отделка, санузел, балкон, вид)
        try:
            # Ищем блок с особенностями
            features_selectors = [
                '.OfferCardDetailsFeatures__container--1IEpT',  # Специфичный селектор для Yandex
                '[class*="OfferCardDetailsFeatures"][class*="container"]',
                '[class*="OfferCardFeature"]',
                '.OfferCardFeature__text--_Hmzv',
                '[class*="feature"][class*="text"]'
            ]
            
            # Собираем все тексты особенностей
            features_text = []
            for selector in features_selectors:
                features_elements = soup.select(selector)
                for element in features_elements:
                    # Ищем текстовые элементы внутри
                    text_elements = element.select('.OfferCardFeature__text--_Hmzv')
                    if not text_elements:
                        text_elements = element.select('[class*="text"]')
                    
                    for text_el in text_elements:
                        feature_text = _clean(text_el.get_text())
                        if feature_text and len(feature_text) > 3:
                            # Пропускаем цену за м²
                            if '₽' not in feature_text and 'за' not in feature_text:
                                features_text.append(feature_text.lower())
            
            # Анализируем особенности
            combined_features = ' '.join(features_text)
            
            # Отделка/ремонт
            if out['renovation'] is None:
                renovation_patterns = [
                    r'(косметический\s*ремонт)',
                    r'(евроремонт)',
                    r'(дизайнерский\s*ремонт)',
                    r'(капитальный\s*ремонт)',
                    r'(без\s*отделки)',
                    r'(чистовая\s*отделка)',
                    r'отделка[^\w]*([\w\s]{5,30})'
                ]
                
                for pattern in renovation_patterns:
                    match = re.search(pattern, combined_features)
                    if match:
                        renovation_text = _clean(match.group(1))
                        # Очищаем от лишних слов
                        if len(renovation_text) <= 50:  # Разумная длина
                            out['renovation'] = renovation_text
                            break
            
            # Санузел
            if out['bathroom_type'] is None:
                if 'санузел раздельный' in combined_features:
                    out['bathroom_type'] = 'separate'
                elif 'санузел совмещенный' in combined_features:
                    out['bathroom_type'] = 'combined'
                elif 'санузел' in combined_features:
                    out['bathroom_type'] = 'unknown'
            
            # Балкон/лоджия
            if out['balcony'] is None:
                if 'лоджия' in combined_features:
                    out['balcony'] = 'loggia'
                elif 'балкон' in combined_features:
                    out['balcony'] = 'balcony'
                elif 'терраса' in combined_features:
                    out['balcony'] = 'terrace'
            
            # Вид из окна
            if out['window_view'] is None:
                # Ищем конкретные варианты сначала
                if 'на улицу' in combined_features:
                    out['window_view'] = 'на улицу'
                elif 'на двор' in combined_features:
                    out['window_view'] = 'на двор'
                elif 'на парк' in combined_features:
                    out['window_view'] = 'на парк'
                elif 'на реку' in combined_features:
                    out['window_view'] = 'на реку'
                elif 'на лес' in combined_features:
                    out['window_view'] = 'на лес'
                else:
                    # Пробуем более общие паттерны
                    view_match = re.search(r'вид\s*из\s*окон\s*(на\s*[\w]{3,15})', combined_features)
                    if view_match:
                        view_text = _clean(view_match.group(1))
                        if view_text and len(view_text) <= 20:
                            out['window_view'] = view_text
                            
        except Exception as e:
            print(f"Ошибка парсинга особенностей квартиры: {e}")
        
        # Описание
        try:
            desc_selectors = [
                '[data-test-id="offer-description"]',
                '.OfferDescription',
                '.offer-description',
                '[class*="Description"]',
                '[class*="description"]'
            ]
            
            for selector in desc_selectors:
                desc_el = soup.select_one(selector)
                if desc_el:
                    desc_text = _clean(desc_el.get_text())
                    if desc_text and len(desc_text) > 10:  # минимальная длина описания
                        out['description'] = desc_text
                        break
                        
        except Exception as e:
            print(f"Ошибка парсинга описания: {e}")
        
        # Продавец и тип продавца
        try:
            # Ищем информацию о продавце
            seller_name_selectors = [
                '.OfferCardAuthorBadge__name--3M271',  # Специфичный селектор для Yandex
                '[class*="OfferCardAuthorBadge"][class*="name"]',
                '.AuthorName__name--3celV',
                '[class*="AuthorName"][class*="name"]',
                '[class*="author"][class*="name"]'
            ]
            
            seller_type_selectors = [
                '.OfferCardAuthorBadge__category--3DrfS',  # Специфичный селектор для Yandex
                '.AuthorCategory__category--3aIvr',
                '[class*="OfferCardAuthorBadge"][class*="category"]',
                '[class*="AuthorCategory"][class*="category"]',
                '[class*="author"][class*="category"]'
            ]
            
            # Извлекаем имя продавца
            for selector in seller_name_selectors:
                seller_el = soup.select_one(selector)
                if seller_el:
                    seller_name = _clean(seller_el.get_text())
                    if seller_name and len(seller_name) > 1:
                        out['seller_name'] = seller_name
                        break
            
            # Извлекаем тип продавца
            for selector in seller_type_selectors:
                seller_type_el = soup.select_one(selector)
                if seller_type_el:
                    seller_type = _clean(seller_type_el.get_text())
                    if seller_type and len(seller_type) > 1:
                        # Нормализуем тип продавца
                        seller_type_lower = seller_type.lower()
                        if 'агентство' in seller_type_lower or 'агент' in seller_type_lower:
                            out['seller_type'] = 'agency'
                        elif 'собственник' in seller_type_lower or 'владелец' in seller_type_lower:
                            out['seller_type'] = 'owner'
                        elif 'застройщик' in seller_type_lower:
                            out['seller_type'] = 'developer'
                        else:
                            out['seller_type'] = seller_type.lower()
                        break
            
            # Если не нашли через селекторы, ищем по тексту страницы
            if not out.get('seller_name') or not out.get('seller_type'):
                page_text = soup.get_text()
                
                # Поиск агентств по известным названиям
                agency_patterns = [
                    r'(Миэль|МИЭЛЬ)',
                    r'(Инком)',
                    r'(Бест)',
                    r'(Этажи)',
                    r'(Сбер)',
                    r'([А-Я][а-я]+\s*[Рр]иэлт)',
                    r'([А-Я][а-я]+\s*[Аа]гентство)'
                ]
                
                for pattern in agency_patterns:
                    match = re.search(pattern, page_text)
                    if match and not out.get('seller_name'):
                        out['seller_name'] = match.group(1)
                        out['seller_type'] = 'agency'
                        break
                
                # Поиск типа продавца по ключевым словам
                if not out.get('seller_type'):
                    if re.search(r'агентство|агент', page_text, re.IGNORECASE):
                        out['seller_type'] = 'agency'
                    elif re.search(r'собственник|владелец', page_text, re.IGNORECASE):
                        out['seller_type'] = 'owner'
                    elif re.search(r'застройщик', page_text, re.IGNORECASE):
                        out['seller_type'] = 'developer'
                        
        except Exception as e:
            print(f"Ошибка парсинга информации о продавце: {e}")
        
        # Количество фотографий
        try:
            photo_selectors = [
                '[data-test-id="photo-thumbnail"]',
                '.PhotoThumbnail',
                '.photo-thumbnail',
                '[class*="Photo"][class*="thumbnail"]',
                '[class*="photo"]',
                'img[src*="photo"]',
                'img[src*="image"]'
            ]
            
            total_photos = 0
            for selector in photo_selectors:
                photo_elements = soup.select(selector)
                if photo_elements:
                    total_photos = max(total_photos, len(photo_elements))
            
            if total_photos > 0:
                out['photos_count'] = total_photos
            else:
                # Попробуем найти в тексте упоминание о количестве фото
                page_text = soup.get_text()
                photo_count_match = re.search(r'(\d+)\s*фото', page_text, re.IGNORECASE)
                if photo_count_match:
                    out['photos_count'] = int(photo_count_match.group(1))
                    
        except Exception as e:
            print(f"Ошибка подсчета фотографий: {e}")
        
        print(f"Успешно спарсено объявление")
        
        # Краткая сводка спарсенных данных
        parsed_fields = sum(1 for v in out.values() if v is not None and v != "")
        print(f"Спарсено полей: {parsed_fields}/{len(out)}")
        
    except Exception as e:
        print(f"Общая ошибка парсинга: {e}")
    
    return out

def print_listing_info(listing: Dict):
    """Выводит информацию об объявлении на экран"""
    print("\n" + "="*80)
    print("ИНФОРМАЦИЯ ОБ ОБЪЯВЛЕНИИ YANDEX REALTY")
    print("="*80)
    
    # Статус объявления
    if listing.get('status') == 'inactive':
        print("⚠️  СТАТУС: ОБЪЯВЛЕНИЕ НЕАКТИВНО (снято/устарело)")
    else:
        print("✅ СТАТУС: ОБЪЯВЛЕНИЕ АКТИВНО")
    print("-" * 80)
    
    if listing.get('price'):
        print(f"💰 Цена: {listing['price']:,} ₽")
    
    if listing.get('rooms') is not None:
        if listing['rooms'] == 0:
            print(f"🏠 Комнаты: студия")
        else:
            print(f"🏠 Комнаты: {listing['rooms']}")
    
    if listing.get('area_m2'):
        print(f"📐 Площадь: {listing['area_m2']} м²")
    
    if listing.get('floor') and listing.get('floor_total'):
        print(f"🏢 Этаж: {listing['floor']}/{listing['floor_total']}")
    elif listing.get('floor'):
        print(f"🏢 Этаж: {listing['floor']}")
    
    if listing.get('address'):
        print(f"📍 Адрес: {listing['address']}")
    
    if listing.get('district'):
        print(f"🗺️  Район: {listing['district']}")
    
    if listing.get('metro'):
        metro_info = f"🚇 Метро: {listing['metro']}"
        if listing.get('walk_minutes'):
            metro_info += f" ({listing['walk_minutes']} мин)"
        print(metro_info)
    
    if listing.get('house_type'):
        print(f"🏗️  Тип дома: {listing['house_type']}")
    
    if listing.get('year_built'):
        print(f"📅 Год постройки: {listing['year_built']}")
    
    # Особенности квартиры
    if listing.get('renovation'):
        print(f"🎨 Отделка: {listing['renovation']}")
    
    if listing.get('bathroom_type'):
        bathroom_names = {
            'separate': 'раздельный',
            'combined': 'совмещенный',
            'unknown': 'неизвестно'
        }
        bathroom_display = bathroom_names.get(listing['bathroom_type'], listing['bathroom_type'])
        print(f"🚽 Санузел: {bathroom_display}")
    
    if listing.get('balcony'):
        balcony_names = {
            'loggia': 'лоджия',
            'balcony': 'балкон',
            'terrace': 'терраса'
        }
        balcony_display = balcony_names.get(listing['balcony'], listing['balcony'])
        print(f"🏠 Балкон/лоджия: {balcony_display}")
    
    if listing.get('window_view'):
        print(f"🌆 Вид из окна: {listing['window_view']}")
    
    if listing.get('photos_count'):
        print(f"📸 Количество фото: {listing['photos_count']}")
    
    if listing.get('views_count'):
        print(f"👁️ Количество просмотров: {listing['views_count']}")
    
    # Информация о продавце
    if listing.get('seller_name') or listing.get('seller_type'):
        seller_info = "👤 Продавец: "
        if listing.get('seller_name'):
            seller_info += listing['seller_name']
        if listing.get('seller_type'):
            if listing.get('seller_name'):
                seller_info += f" ({listing['seller_type']})"
            else:
                seller_info += listing['seller_type']
        print(seller_info)
    
    if listing.get('description'):
        desc = listing['description'][:200] + "..." if len(listing['description']) > 200 else listing['description']
        print(f"📄 Описание: {desc}")
    
    print(f"🔗 URL: {listing['url']}")
    print("="*80)

def parse_arguments():
    """Парсит аргументы командной строки"""
    parser = argparse.ArgumentParser(description='Парсер объявлений Yandex Realty')
    
    parser.add_argument(
        'url',
        type=str,
        help='URL объявления Yandex Realty'
    )
    
    return parser.parse_args()

def main():
    """Основная функция"""
    args = parse_arguments()
    url = args.url
    
    if 'realty.yandex.ru' not in url:
        print("Ошибка: URL должен быть с сайта realty.yandex.ru")
        return
    
    print("ПАРСЕР YANDEX REALTY")
    print(f"URL: {url}")
    print("-" * 80)
    
    driver = None
    try:
        print("Инициализация Chrome WebDriver...")
        driver = setup_driver()
        print("Начинаем парсинг...")
        listing = parse_yandex_listing(driver, url)
        print_listing_info(listing)
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            print("Закрываем браузер...")
            driver.quit()

if __name__ == '__main__':
    main()