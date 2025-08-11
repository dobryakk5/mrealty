#!/usr/bin/env python3
"""
Парсер объявлений CIAN с сохранением в БД
Извлекает данные со страницы поиска и сохраняет в таблицу ads_cian

ФИЛЬТРЫ ПОИСКА:
1. Тип жилья (property_type): 1=вторичка, 2=новостройки
2. Время публикации (time_period): w=неделя, d=день, h=час
3. Станция метро (metro_id): ID станции из ЦИАН (например: 68 для "Маяковская")
4. Время до метро (foot_min): Время в пути пешком в минутах (например: 20)

ПРИМЕРЫ URL:
- Без метро: https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=1&object_type%5B0%5D=2&totime=3600
- С метро: https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=1&object_type%5B0%5D=2&totime=3600&metro%5B0%5D=68
- С метро + временем: https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&foot_min=20&metro%5B0%5D=68&offer_type=flat&only_foot=2

ИСПОЛЬЗОВАНИЕ:
1. Установите METRO_ID = нужный ID станции (из metro_mapping_results.csv)
2. Установите FOOT_MIN = желаемое время в пути (в минутах)
3. Запустите скрипт: python parse_cian_to_db.py

АРГУМЕНТЫ КОМАНДНОЙ СТРОКИ:
[тип][период] - где тип=1(вторичка) или 2(новостройки), период=w(неделя), d(день), h(час)

ПРИМЕРЫ:
python parse_cian_to_db.py          # значения по умолчанию
python parse_cian_to_db.py 2w       # новостройки за неделю
python parse_cian_to_db.py 1d       # вторичка за день
python parse_cian_to_db.py 2h       # новостройки за час
"""

import argparse
import asyncio
import re
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

# Импортируем функции работы с БД
from parse_todb import (
    create_ads_cian_table, 
    save_cian_ad, 
    get_all_metro_stations,
    get_last_parsing_progress,
    create_parsing_session,
    update_parsing_progress,
    complete_parsing_session
)

# ========== ФУНКЦИИ ПАРСИНГА АРГУМЕНТОВ ==========

def parse_arguments():
    """Парсит аргументы командной строки"""
    parser = argparse.ArgumentParser(description='Парсер объявлений CIAN с сохранением в БД')
    
    parser.add_argument(
        'params',
        nargs='?',
        type=str,
        help='Параметры в формате: [тип][период] (например: 2w, 1d, 2h)'
    )
    
    return parser.parse_args()

def parse_params_string(params_str: str) -> tuple[int, str]:
    """Парсит строку параметров в формате [тип][период]"""
    if not params_str:
        return PROPERTY_TYPE, 'w'  # значения по умолчанию
    
    # Первый символ - тип недвижимости
    if params_str[0] in ['1', '2']:
        property_type = int(params_str[0])
        # Остальные символы - период времени
        time_period = params_str[1:] if len(params_str) > 1 else 'w'
    else:
        # Если первый символ не цифра, используем значение по умолчанию
        property_type = PROPERTY_TYPE
        time_period = params_str
    
    # Проверяем корректность периода времени
    if time_period not in ['w', 'd', 'h']:
        time_period = 'w'  # по умолчанию неделя
    
    return property_type, time_period

def convert_time_period(time_period: str) -> int:
    """Конвертирует символьный период времени в секунды"""
    time_mapping = {
        'h': 3600,      # час
        'd': 86400,     # день
        'w': 604800     # неделя
    }
    return time_mapping.get(time_period, 604800)  # по умолчанию неделя

# ========== НАСТРОЙКИ ==========
PROPERTY_TYPE = 1  # 1=вторичка, 2=новостройки
TIME_PERIOD = 604800  # 3600=час, 86400=день, 604800=неделя, -2=сегодня
# Перебираем все страницы до отсутствия карточек
MAX_PAGES = 100  # Увеличено для работы с логикой остановки по дубликатам
# Не ограничиваем кол-во карточек на странице
MAX_URLS = 30
# Пауза между страницами по требованию
REQUEST_DELAY = 3.0
PROXY = "http://qEpxaS:uq2shh@194.67.220.161:9889"

# по метро
METRO_ID = "all"  # "all" для всех станций, или конкретный ID (например: 68 для "Маяковская")
FOOT_MIN = 20  # Время в пути до метро в минутах (например: 20)

# Примеры использования:
# METRO_ID = 68  # Маяковская
# FOOT_MIN = 20  # До 20 минут пешком
# 
# URL будет: https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&foot_min=20&metro%5B0%5D=68&offer_type=flat&only_foot=2

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

# ========== УТИЛИТЫ ==========

def _clean(s):
    if s is None:
        return None
    return re.sub(r'\s+', ' ', s).strip()

MONTHS = {
    'янв': 'января', 'фев': 'февраля', 'мар': 'марта', 'апр': 'апреля',
    'май': 'мая', 'июн': 'июня', 'июл': 'июля', 'авг': 'августа',
    'сен': 'сентября', 'окт': 'октября', 'ноя': 'ноября', 'дек': 'декабря',
}

def parse_time_label(card) -> Optional[str]:
    """Возвращает строку абсолютного времени (например: 'вчера, 15:56' или '8 авг, 13:17'), если есть."""
    try:
        tl = card.select_one('[data-name="TimeLabel"]')
        if not tl:
            return None
        abs_span = tl.select_one('div._93444fe79c--absolute--yut0v span')
        if not abs_span:
            return None
        raw = abs_span.get_text(strip=True)
        return raw
    except Exception:
        return None

def normalize_time_label_to_datetime_str(raw: str) -> Optional[str]:
    """Нормализует 'вчера, 15:56' или '8 авг, 13:17' в строку '%Y-%m-%d %H:%M:%S' (локальное время)."""
    try:
        from datetime import datetime, timedelta
        now = datetime.now()
        raw = raw.strip().lower()
        # вчера, HH:MM
        m = re.match(r'^вчера\s*,\s*(\d{1,2}):(\d{2})$', raw)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
            dt = (now - timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        # сегодня, HH:MM
        m = re.match(r'^сегодня\s*,\s*(\d{1,2}):(\d{2})$', raw)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
            dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        # D мон, HH:MM  (8 авг, 13:17)
        m = re.match(r'^(\d{1,2})\s+([а-я]{3})\s*,\s*(\d{1,2}):(\d{2})$', raw)
        if m:
            d = int(m.group(1))
            mon_short = m.group(2)
            hh, mm = int(m.group(3)), int(m.group(4))
            mon_name = MONTHS.get(mon_short, mon_short)
            # Сформируем через strptime, собрав русский месяц по номеру вручную
            mon_map = {
                'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
                'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12,
            }
            month = mon_map.get(mon_short, now.month)
            dt = now.replace(month=month, day=d, hour=hh, minute=mm, second=0, microsecond=0)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return None
    except Exception:
        return None

def format_price(raw):
    if raw is None:
        return ''
    try:
        return f"{int(round(raw)):,}".replace(',', ' ')
    except Exception:
        return str(raw)

def build_search_url(property_type: int, time_period: int, metro_id: int = None, foot_min: int = None) -> str:
    base_url = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=1&only_foot=2"
    url = f"{base_url}&object_type%5B0%5D={property_type}&totime={time_period}"
    only_foot=2
    if property_type == 2:
        url += "&with_newobject=1"
    
    # Добавляем фильтр по метро
    if metro_id is not None:
        url += f"&metro%5B0%5D={metro_id}"
    
    # Добавляем фильтр по времени в пути до метро
    if foot_min is not None:
        url += f"&foot_min={foot_min}"
    
    return url

# ========== ПАРСЕР ПРОДАВЦА ==========

def parse_card_seller(card_tag):
    """Парсит информацию о продавце из карточки"""
    if card_tag is None:
        return {}

    seller = {
        "type": "unknown",
        "badge": None,
        "name": None,
        "id": None,
        "url": None,
        "all_objects_url": None,
        "avatar": None,
        "phone_text": None,
        "documents_verified": False
    }

    # Branding / contact контейнер
    branding = card_tag.select_one('[data-name="BrandingLevelWrapper"], ._93444fe79c--agent--HG9xn, ._93444fe79c--contact--pa2PA, ._93444fe79c--agent-cont--iStzo')
    if not branding:
        branding = card_tag.select_one('._93444fe79c--aside--ygGB3')

    if branding:
        # avatar
        img = branding.find('img')
        if img and img.get('src'):
            seller['avatar'] = img['src']

        # badge
        badge_el = branding.find(lambda t: t.name in ('span','div') and re.search(r'Собственник|Агент|Агентство|Застройщик|Developer|Собственник', t.get_text() or '', re.I))
        if badge_el:
            badge_text = _clean(badge_el.get_text())
            if badge_text:
                for key in ['Собственник', 'Агентство недвижимости', 'Агентство', 'Застройщик', 'Риелтор', 'Агент']:
                    if key.lower() in badge_text.lower():
                        seller['badge'] = key
                        break
                else:
                    seller['badge'] = badge_text.split()[0] if badge_text else None

        # name
        name_el = branding.select_one('._93444fe79c--name-container--enElO a, ._93444fe79c--name-container--enElO span, ._93444fe79c--name-container--enElO')
        if name_el:
            seller['name'] = _clean(name_el.get_text())

        # sometimes name is plain text "ID 123..."
        if not seller['name']:
            txt = branding.get_text(" ", strip=True)
            mid = re.search(r'\bID\s*(\d+)\b', txt, re.I)
            if mid:
                seller['name'] = "ID " + mid.group(1)
                seller['id'] = mid.group(1)

        # company link
        comp_a = branding.select_one('a[href*="/company/"], a[href*="company/"]')
        if comp_a and comp_a.get('href'):
            href = comp_a['href']
            seller['url'] = href if href.startswith('http') else ("https://www.cian.ru" + href)
            m = re.search(r'/company/(\d+)', href)
            if m:
                seller['id'] = m.group(1)

        # "Посмотреть все объекты" с id_user
        all_a = card_tag.select_one('a[href*="id_user="], a[href*="cat.php?id_user="]')
        if all_a and all_a.get('href'):
            href = all_a['href']
            seller['all_objects_url'] = href if href.startswith('http') else ("https://www.cian.ru" + href)
            mu = re.search(r'id_user=(\d+)', href)
            if mu:
                seller['id'] = seller.get('id') or mu.group(1)

    # infer type using badge/name/id heuristics
    b = (seller['badge'] or "").lower()
    n = (seller['name'] or "").lower()
    if re.search(r'собственник', b) or re.search(r'\bсобственник\b', n):
        seller['type'] = 'owner'
    elif re.search(r'агент|агентство|риэлтор', b) or re.search(r'агент|agency|realtor', n):
        seller['type'] = 'agency'
    elif re.search(r'застройщик|developer', b) or re.search(r'\bзастройщик\b|developer', n):
        seller['type'] = 'developer'
    elif seller['id'] and (n.startswith('id ') or re.match(r'^\d+$', seller['id'])):
        seller['type'] = 'user'
    else:
        seller['type'] = 'private' if seller['name'] else 'unknown'

    return seller

# ========== ПАРСЕР КАРТОЧЕК ==========

def parse_offer_card(card):
    """Безопасный парсер карточки с обработкой ошибок"""
    if card is None:
        return {}
    
    out = {}
    
    try:
        # URL и offer_id
        a_main = card.select_one('a[href*="/sale/flat/"]')
        url = a_main['href'] if a_main and a_main.get('href') else None
        if url and url.startswith('/'):
            url = "https://www.cian.ru" + url
        out['URL'] = _clean(url)
        if url:
            m = re.search(r'/sale/flat/(\d+)', url)
            out['offer_id'] = m.group(1) if m else None
    except:
        pass

    try:
        # Title / Subtitle (не сохраняем в БД, но используем для парсинга)
        title_el = card.select_one('[data-mark="OfferTitle"]')
        subtitle_el = card.select_one('[data-mark="OfferSubtitle"]')
        out['title'] = _clean(title_el.get_text()) if title_el else None
        out['subtitle'] = _clean(subtitle_el.get_text()) if subtitle_el else None
    except:
        pass

    try:
        # Price - ищем агрессивно
        price = None
        
        # Метод 1: стандартные селекторы цены
        price_selectors = [
            '[data-mark="MainPrice"]',
            '[data-testid*="price"]',
            '[class*="price"]'
        ]
        for selector in price_selectors:
            price_el = card.select_one(selector)
            if price_el:
                price_txt = price_el.get_text()
                m = re.search(r'([\d\s\u00A0,]+)', price_txt.replace('₽', ''))
                if m:
                    price_str = m.group(1).replace('\u00A0','').replace(' ','').replace(',','')
                    if price_str.strip():
                        price = int(price_str)
                        break
        
        # Метод 2: поиск по всему тексту карточки с рублями
        if not price:
            full_text = card.get_text()
            patterns = [
                r'(\d{1,3}(?:\s\d{3})*(?:\s\d{3})*)\s*₽',
                r'(\d{1,3}(?:[\s\u00A0]\d{3})*(?:[\s\u00A0]\d{3})*)\s*₽',
                r'(\d{1,3}(?:,\d{3})*(?:,\d{3})*)\s*₽',
                r'(\d{1,3}(?:\s\d{3})*)\s*руб',
            ]
            for pattern in patterns:
                matches = re.findall(pattern, full_text)
                if matches:
                    for match in matches:
                        try:
                            clean_price = match.replace('\u00A0', '').replace(' ', '').replace(',', '')
                            candidate = int(clean_price)
                            if candidate > 1000000:  # минимальная цена квартиры
                                if not price or candidate > price:
                                    price = candidate
                        except:
                            continue
                    if price:
                        break
        
        out['price'] = price
    except:
        out['price'] = None

    try:
        # Парсинг характеристик из title и subtitle с умной логикой
        rooms = None
        area_m2 = None
        floor = None
        floor_total = None
        
        # Получаем оба текста
        title_text = out.get('title') or ""
        subtitle_text = out.get('subtitle') or ""
        
        # Объединяем тексты для более полного поиска
        combined_text = f"{title_text} {subtitle_text}".strip()
        
        # Приоритет: сначала ищем в тексте, где есть "м²" (обычно там полная информация)
        primary_text = None
        if "м²" in title_text or "м2" in title_text:
            primary_text = title_text
        elif "м²" in subtitle_text or "м2" in subtitle_text:
            primary_text = subtitle_text
        else:
            # Если ни в одном нет "м²", используем объединенный текст
            primary_text = combined_text
        
        # Парсим характеристики из приоритетного текста
        if primary_text:
            # Комнаты: число -> int, любая другая строка (например студия) -> 0
            m = re.search(r'(\d+)[^\d\-–—]*-?комн', primary_text, re.IGNORECASE)
            if m:
                rooms = int(m.group(1))
            elif re.search(r'\bстуд', primary_text, re.IGNORECASE):
                rooms = 0

            # Площадь
            m = re.search(r'(\d+[.,]?\d*)\s*(?:м²|м2|м)', primary_text)
            if m:
                area_m2 = float(m.group(1).replace(',', '.'))

            # Этаж
            m = re.search(r'(\d+)\s*/\s*(\d+)', primary_text)
            if m:
                floor = int(m.group(1))
                floor_total = int(m.group(2))
        
        # Если что-то не нашли в приоритетном тексте, ищем в объединенном
        if rooms is None and combined_text:
            m = re.search(r'(\d+)[^\d\-–—]*-?комн', combined_text, re.IGNORECASE)
            if m:
                rooms = int(m.group(1))
            elif re.search(r'\bстуд', combined_text, re.IGNORECASE):
                rooms = 0
        
        if area_m2 is None and combined_text:
            m = re.search(r'(\d+[.,]?\d*)\s*(?:м²|м2|м)', combined_text)
            if m:
                area_m2 = float(m.group(1).replace(',', '.'))
        
        if floor is None and combined_text:
            m = re.search(r'(\d+)\s*/\s*(\d+)', combined_text)
            if m:
                floor = int(m.group(1))
                floor_total = int(m.group(2))

        out['rooms'] = rooms
        out['area_m2'] = area_m2
        out['floor'] = floor
        out['floor_total'] = floor_total
    except:
        pass

    try:
        # ЖК
        complex_a = card.select_one('a[href*="zhk"], a[href^="https://zhk"]')
        out['complex'] = _clean(complex_a.get_text()) if complex_a else None
    except:
        pass

    try:
        # Метро
        metro_els = card.select('a[href*="metro"], [class*="metro"]')
        metro_text = None
        for el in metro_els:
            text = _clean(el.get_text())
            if text and len(text) > 3:
                metro_text = text
                break
        out['metro'] = metro_text
        
        # Время до метро
        walk_minutes = None
        time_patterns = [r'(\d+)\s*мин', r'(\d+)\s*м\.']
        full_text = card.get_text()
        for pattern in time_patterns:
            m = re.search(pattern, full_text, re.IGNORECASE)
            if m:
                try:
                    walk_minutes = int(m.group(1))
                    break
                except:
                    pass
        out['walk_minutes'] = walk_minutes
        
        # metro_id будет заполнен позже в save_cian_ad на основе названия станции
        out['metro_id'] = None
    except:
        pass

    # Время создания (абсолютный лейбл времени)
    try:
        raw_time = parse_time_label(card)
        if raw_time:
            out['created_dt'] = normalize_time_label_to_datetime_str(raw_time)
    except Exception:
        pass

    try:
        # Адрес/гео метки (исключаем метро)
        geo = []
        geo_selectors = [
            '[data-name="GeoLabel"]', 
            '[data-name="AddressItem"]',
            '[class*="address"]', 
            '[class*="location"]'
        ]
        
        # Стоп-слова для исключения метро из адреса
        metro_stop_words = ['м.', 'м ', 'метро', 'станция', 'станции']
        
        # Сначала собираем все элементы из GeoLabel
        all_geo_elements = []
        for selector in geo_selectors:
            elements = card.select(selector)
            for el in elements:
                text = _clean(el.get_text())
                
                if text:  # Убрал проверку длины, оставил только проверку на пустоту
                    
                    # Проверяем, не содержит ли текст метро
                    text_lower = text.lower()
                    is_metro = any(stop_word in text_lower for stop_word in metro_stop_words)
                    
                    if is_metro:
                        continue
                    
                    all_geo_elements.append(text)
        
        # Теперь анализируем собранные элементы
        if all_geo_elements:
            # Ищем район
            district_index = -1
            district_text = None
            
            for i, text in enumerate(all_geo_elements):
                text_lower = text.lower()
                has_district = bool(re.search(r'\bр-?н\b|\bрайон\b|\bр-он\b|\bр\.н\.\b', text_lower))
                if has_district:
                    district_index = i
                    district_text = text
                    break
            
            if district_index >= 0:
                # Район найден - удаляем предыдущие части, последующие идут в address
                geo = [district_text] + all_geo_elements[district_index+1:]
            else:
                # Района нет - все элементы идут в address
                geo = all_geo_elements
        
        out['geo_labels'] = list(dict.fromkeys(geo))
        
        if out['geo_labels']:
            for i, geo_item in enumerate(out['geo_labels']):
                if 'р-н' in str(geo_item).lower():
                    pass
    except:
        pass

    try:
        labels = []
        # сначала более точные селекторы, потом запасные
        label_selectors = [
            '[data-name="LabelsList"] span'
        ]
        #STOP_WORDS = {'м²', '₽', 'этаж', 'м2', 'м'}
        for selector in label_selectors:
            for el in card.select(selector):
                text = _clean(el.get_text())
                if not text:
                    continue

                if len(text) <= 2:
                    continue

                # убираем явные стоп-слова (в нижнем регистре)
                #if text.lower() in STOP_WORDS:
                #    continue

                labels.append(text)

        # убираем дубликаты, сохраняя порядок и тримая пробелы
        seen = set()
        cleaned_labels = []
        for t in labels:
            key = t.strip()
            if key and key not in seen:
                seen.add(key)
                cleaned_labels.append(key)
        out['labels'] = cleaned_labels
    except:
        pass

    try:
        # Информация о продавце
        seller_info = parse_card_seller(card)
        out['seller'] = seller_info
    except:
        out['seller'] = {}

    return out

# ========== ОСНОВНЫЕ ФУНКЦИИ ==========

async def process_single_metro_station(
    search_url: str, 
    station_name: str, 
    station_cian_id: int,
    property_type: int, 
    time_period: int, 
    max_pages: int
) -> List[Dict]:
    """
    Обрабатывает одну станцию метро: парсит страницы поиска и извлекает объявления
    
    :param search_url: базовый URL для поиска
    :param station_name: название станции метро
    :param station_cian_id: CIAN ID станции метро
    :param property_type: тип недвижимости (1=вторичка, 2=новостройки)
    :param time_period: период времени в секундах
    :param max_pages: максимальное количество страниц
    """
    page = 1
    all_cards = []
    duplicate_pages_count = 0  # Счетчик страниц с дубликатами
    
    print(f"🔍 Начинаем парсинг станции {station_name}...")
    
    while page <= max_pages:
        try:
            # Формируем URL для конкретной страницы
            page_url = f"{search_url}&p={page}"
            print(f"   📄 Страница {page}: {page_url}")
            
            # Получаем страницу
            session = requests.Session()
            if PROXY:
                session.proxies = {'http': PROXY, 'https': PROXY}
            
            response = session.get(page_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            # Парсим HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем карточки объявлений
            cards = soup.select('[data-name="CardComponent"]')
            if not cards:
                print(f"   ⚠️ На странице {page} не найдено карточек")
                break
            
            # Ограничиваем количество карточек для обработки
            cards = cards[:MAX_URLS]
            print(f"   📊 Найдено карточек: {len(cards)} (ограничено до {MAX_URLS})")
            
            # Парсим каждую карточку
            page_cards = []
            new_cards_count = 0  # Счетчик новых карточек на странице
            
            for card in cards:
                try:
                    parsed_card = parse_offer_card(card)
                    if parsed_card:
                        # Добавляем информацию о станции только если не перезаписываем существующие данные
                        if 'metro' not in parsed_card or not parsed_card['metro']:
                            parsed_card['metro'] = station_name if station_name != "Без фильтра по метро" else None
                        if 'metro_id' not in parsed_card or not parsed_card['metro_id']:
                            parsed_card['metro_id'] = station_cian_id
                        
                        # Всегда добавляем информацию о станции поиска как дополнительные поля
                        parsed_card['station_name'] = station_name if station_name != "Без фильтра по метро" else None
                        parsed_card['station_cian_id'] = station_cian_id
                        parsed_card['property_type'] = property_type  # Добавляем тип недвижимости
                        
                        # Сохраняем в БД
                        try:
                            saved = await save_cian_ad(parsed_card)
                            parsed_card['saved'] = saved
                            if saved:
                                new_cards_count += 1  # Увеличиваем счетчик новых карточек
                        except Exception as e:
                            print(f"   ❌ Ошибка сохранения в БД: {e}")
                            parsed_card['saved'] = False
                        
                        page_cards.append(parsed_card)
                except Exception as e:
                    print(f"   ❌ Ошибка парсинга карточки: {e}")
                    continue
            
            all_cards.extend(page_cards)
            print(f"   ✅ Обработано карточек: {len(page_cards)}")
            print(f"   🆕 Новых карточек: {new_cards_count}")
            
            # Проверяем, есть ли новые карточки на странице
            if new_cards_count == 0:
                duplicate_pages_count += 1
                print(f"   ⚠️ Страница {page} содержит только дубликаты (счетчик: {duplicate_pages_count})")
                
                # Если 2 страниц подряд содержат только дубликаты - останавливаемся
                if duplicate_pages_count >= 2:
                    print(f"   🛑 Остановка: 2 страниц подряд содержат только дубликаты")
                    break
            else:
                # Сбрасываем счетчик дубликатов при наличии новых карточек
                duplicate_pages_count = 0
                print(f"   ✅ Сброс счетчика дубликатов (найдены новые карточки)")
            
            page += 1
            
            # Пауза между страницами
            if page <= max_pages:
                print(f"   ⏳ Пауза {REQUEST_DELAY} сек перед следующей страницей...")
                time.sleep(REQUEST_DELAY)
                
        except Exception as e:
            print(f"   ❌ Ошибка обработки страницы {page}: {e}")
            break
    
    print(f"🏁 Завершен парсинг станции {station_name}. Всего объявлений: {len(all_cards)}")
    return all_cards

async def fetch_and_save_listings(property_type: int = PROPERTY_TYPE, time_period: int = TIME_PERIOD, max_pages: int = MAX_PAGES, metro_id: int = METRO_ID, foot_min: int = FOOT_MIN) -> List[Dict]:
    """Получает объявления и сохраняет их в БД"""
    
    # Определяем, какие станции метро обрабатывать
    if metro_id == "all":
        # Получаем все станции метро
        metro_stations = await get_all_metro_stations()
        if not metro_stations:
            print("❌ Не удалось получить список станций метро")
            return []
        
        print(f"🚇 Обработка ВСЕХ станций метро ({len(metro_stations)} станций)")
        print("=" * 80)
        
        # Проверяем, есть ли незавершенная сессия
        progress = await get_last_parsing_progress(property_type, time_period)
        
        if progress and progress['status'] == 'active':
            # Продолжаем с места остановки
            print(f"🔄 Продолжаем незавершенную сессию {progress['id']} с метро ID {progress['current_metro_id']}")
            session_id = progress['id']
            
            # Находим следующую станцию по metro.id (не по позиции)
            current_index = None
            print(f"[DEBUG] Ищем следующую станцию после metro.id = {progress['current_metro_id']}")
            
            # Ищем станцию с metro.id максимально близким к текущему, но больше
            target_metro_id = progress['current_metro_id']
            best_match = None
            best_index = None
            
            for i, station in enumerate(metro_stations):
                if station['id'] > target_metro_id:
                    if best_match is None or station['id'] < best_match['id']:
                        best_match = station
                        best_index = i
            
            if best_match:
                current_index = best_index
                print(f"[DEBUG] Найдена следующая станция: metro.id = {best_match['id']}, {best_match['name']} на позиции {best_index}")
            else:
                print(f"⚠️ Следующая станция после metro.id = {progress['current_metro_id']} не найдена, начинаем сначала")
                current_index = 0
                session_id = await create_parsing_session(property_type, time_period, len(metro_stations))
        else:
            # Создаем новую сессию
            print("🆕 Создаем новую сессию парсинга")
            session_id = await create_parsing_session(property_type, time_period, len(metro_stations))
            current_index = 0
        
        all_cards = []
        total_saved = 0
        
        # Обрабатываем станции начиная с текущего индекса
        for i in range(current_index, len(metro_stations)):
            station = metro_stations[i]
            station_cian_id = station['cian_id']
            station_name = station['name']
            
            print(f"\n📍 Станция {i+1}/{len(metro_stations)}: {station_name} (CIAN ID: {station_cian_id})")
            print("-" * 60)
            
            # Создаем URL для конкретной станции
            search_url = build_search_url(property_type, time_period, station_cian_id, foot_min)
            print(f"URL: {search_url}")
            
            # Обрабатываем объявления для этой станции
            station_cards = await process_single_metro_station(
                search_url, station_name, station_cian_id, 
                property_type, time_period, max_pages
            )
            
            # Статистика по текущей станции
            station_saved = len([c for c in station_cards if c.get('saved', False)])
            station_total = len(station_cards)
            
            print(f"Станция {station_name} обработана:")
            print(f"   Всего объявлений: {station_total}")
            print(f"   Сохранено в БД: {station_saved}")
            
            # Обновляем прогресс ПОСЛЕ успешной обработки станции
            await update_parsing_progress(session_id, station['id'], i + 1)
            
            print("-" * 60)
            
            all_cards.extend(station_cards)
            total_saved += station_saved
            
            # Пауза между станциями
            if i < len(metro_stations) - 1:
                print(f"⏳ Пауза 34 сек перед следующей станцией...")
                time.sleep(34)
        
        # Завершаем сессию
        await complete_parsing_session(session_id)
        print(f"✅ Все станции обработаны. Сессия {session_id} завершена.")
        
        return all_cards
    elif metro_id is not None:
        # Обрабатываем одну конкретную станцию
        search_url = build_search_url(property_type, time_period, metro_id, foot_min)
        print(f"URL поиска: {search_url}")
        print(f"Тип: {'вторичка' if property_type == 1 else 'новостройки'}")
        
        period_names = {3600: 'час', 86400: 'день', 604800: 'неделя'}
        print(f"Период: {period_names.get(time_period, str(time_period))}")
        
        print(f"Метро: ID {metro_id}")
        if foot_min is not None:
            print(f"Время до метро: до {foot_min} минут")
        
        print("=" * 80)
        
        return await process_single_metro_station(
            search_url, f"Метро ID {metro_id}", metro_id,
            property_type, time_period, max_pages
        )
    else:
        # Фильтр по метро не применяется - парсим все объявления
        search_url = build_search_url(property_type, time_period, None, foot_min)
        print(f"URL поиска: {search_url}")
        print(f"Тип: {'вторичка' if property_type == 1 else 'новостройки'}")
        
        period_names = {3600: 'час', 86400: 'день', 604800: 'неделя'}
        print(f"Период: {period_names.get(time_period, str(time_period))}")
        print("Метро: фильтр не применяется")
        
        if foot_min is not None:
            print(f"Время до метро: до {foot_min} минут")
        
        print("=" * 80)
        
        return await process_single_metro_station(
            search_url, "Без фильтра по метро", None,
            property_type, time_period, max_pages
        )

def print_summary(cards: List[Dict]):
    """Печатает краткую сводку"""
    if not cards:
        return
        
    print("\nКРАТКАЯ СВОДКА:")
    print("-" * 40)
    
    # Статистика по продавцам
    seller_stats = {}
    for card in cards:
        seller_type = card.get('seller', {}).get('type', 'unknown')
        seller_stats[seller_type] = seller_stats.get(seller_type, 0) + 1
    if seller_stats:
        seller_str = ', '.join([f"{k}: {v}" for k, v in sorted(seller_stats.items())])
        print(f"Продавцы: {seller_str}")

async def main():
    """Основная функция"""
    print("ПАРСЕР CIAN -> БД")
    print(f"Прокси: {'включен' if PROXY else 'выключен'}")
    
    args = parse_arguments()
    
    # Определяем параметры из аргументов командной строки или используем значения по умолчанию
    property_type, time_period = parse_params_string(args.params)
    time_period_seconds = convert_time_period(time_period)
    
    print(f"Тип недвижимости: {'вторичка' if property_type == 1 else 'новостройки'}")
    period_names = {3600: 'час', 86400: 'день', 604800: 'неделя'}
    print(f"Период времени: {period_names.get(time_period_seconds, str(time_period_seconds))}")

    # Создаем таблицы БД если их нет
    await create_ads_cian_table()

    cards = await fetch_and_save_listings(
        property_type=property_type,
        time_period=time_period_seconds,
        max_pages=MAX_PAGES,
        metro_id=METRO_ID,
        foot_min=FOOT_MIN
    )
    print_summary(cards)
    
    return cards

if __name__ == '__main__':
    asyncio.run(main())
