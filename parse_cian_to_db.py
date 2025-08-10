#!/usr/bin/env python3
"""
Парсер объявлений CIAN с сохранением в БД
Извлекает данные со страницы поиска и сохраняет в таблицу ads_cian
"""

import asyncio
import re
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

# Импортируем функции работы с БД
from parse_todb import create_ads_cian_table, save_cian_ad

# ========== НАСТРОЙКИ ==========
PROPERTY_TYPE = 1  # 1=вторичка, 2=новостройки
TIME_PERIOD = 86400  # 3600=час, 86400=день, 604800=неделя, -2=сегодня
MAX_PAGES = 1
MAX_URLS = 50
REQUEST_DELAY = 0.8
PROXY = "http://qEpxaS:uq2shh@194.67.220.161:9889"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

# ========== УТИЛИТЫ ==========

def _clean(s):
    if s is None:
        return None
    return re.sub(r'\s+', ' ', s).strip()

def format_price(raw):
    if raw is None:
        return ''
    try:
        return f"{int(round(raw)):,}".replace(',', ' ')
    except Exception:
        return str(raw)

def build_search_url(property_type: int, time_period: int) -> str:
    base_url = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=1"
    url = f"{base_url}&object_type%5B0%5D={property_type}&totime={time_period}"
    if property_type == 2:
        url += "&with_newobject=1"
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
        # Парсинг характеристик из subtitle или title
        rooms = None
        area_m2 = None
        floor = None
        floor_total = None
        
        # Пробуем сначала subtitle, потом title
        text_to_parse = out.get('subtitle') or out.get('title')
        
        if text_to_parse:
            # Комнаты
            m = re.search(r'(\d+)[^\d\-–—]*-?комн', text_to_parse, re.IGNORECASE)
            if m:
                rooms = int(m.group(1))
            elif re.search(r'\bстуд', text_to_parse, re.IGNORECASE):
                rooms = 'студия'

            # Площадь
            m = re.search(r'(\d+[.,]?\d*)\s*(?:м²|м2|м)', text_to_parse)
            if m:
                area_m2 = float(m.group(1).replace(',', '.'))

            # Этаж
            m = re.search(r'(\d+)\s*/\s*(\d+)', text_to_parse)
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
                walk_minutes = int(m.group(1))
                break
        out['walk_minutes'] = walk_minutes
    except:
        pass

    try:
        # Адрес/гео метки
        geo = []
        geo_selectors = ['[data-name="GeoLabel"]', '[class*="address"]', '[class*="location"]']
        for selector in geo_selectors:
            for el in card.select(selector):
                text = _clean(el.get_text())
                if text and len(text) > 2:
                    geo.append(text)
        out['geo_labels'] = list(dict.fromkeys(geo))
    except:
        pass

    try:
        # Метки/лейблы
        labels = []
        label_selectors = ['[data-name*="Label"]', '[class*="label"]', '[class*="badge"]']
        for selector in label_selectors:
            for el in card.select(selector):
                text = _clean(el.get_text())
                if text and len(text) > 2 and text not in ['м²', '₽', 'этаж']:
                    labels.append(text)
        out['labels'] = list(dict.fromkeys(labels))
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

async def fetch_and_save_listings(property_type: int = PROPERTY_TYPE, time_period: int = TIME_PERIOD, max_pages: int = MAX_PAGES) -> List[Dict]:
    """Получает объявления и сохраняет их в БД"""
    search_url = build_search_url(property_type, time_period)
    
    print(f"URL поиска: {search_url}")
    print(f"Тип: {'вторичка' if property_type == 1 else 'новостройки'}")
    
    period_names = {3600: 'час', 86400: 'день', 604800: 'неделя', -2: 'сегодня'}
    print(f"Период: {period_names.get(time_period, str(time_period))}")
    print("=" * 80)
    
    # Создаем таблицу если её нет
    await create_ads_cian_table()
    
    sess = requests.Session()
    proxies = {"http": PROXY, "https": PROXY} if PROXY else None
    all_cards = []
    saved_count = 0
    
    for page in range(1, max_pages + 1):
        url = search_url + f"&p={page}" if page > 1 else search_url
        print(f"Запрашиваю страницу {page}")
        
        try:
            resp = sess.get(url, headers=HEADERS, timeout=20, proxies=proxies)
            resp.encoding = 'utf-8'
            print(f"Статус ответа: {resp.status_code}")
            
            if resp.status_code != 200:
                print("Ошибка запроса или антибот защита")
                break
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            cards = soup.select('[data-name="CardComponent"]')
            print(f"Найдено карточек: {len(cards)}")
            
            for i, card in enumerate(cards[:MAX_URLS]):
                card_data = parse_offer_card(card)
                card_data['index'] = i + 1
                if card_data.get('URL'):
                    all_cards.append(card_data)
                    
                    # Сохраняем в БД
                    try:
                        await save_cian_ad(card_data)
                        saved_count += 1
                    except Exception as e:
                        print(f"Ошибка сохранения объявления {card_data.get('offer_id', 'неизвестно')}: {e}")
            
            if len(cards) == 0:
                break
                
        except Exception as e:
            print(f"Ошибка при запросе страницы {page}: {e}")
            break
        
        if page < max_pages:
            time.sleep(REQUEST_DELAY)
    
    print(f"\n{'='*80}")
    print(f"ОБРАБОТАНО ОБЪЯВЛЕНИЙ: {len(all_cards)}")
    print(f"СОХРАНЕНО В БД: {saved_count}")
    print(f"{'='*80}")
    
    return all_cards

def print_summary(cards: List[Dict]):
    """Печатает краткую сводку"""
    if not cards:
        return
        
    print("\nКРАТКАЯ СВОДКА:")
    print("-" * 40)
    
    # Статистика по ценам
    prices = [c['price'] for c in cards if c.get('price')]
    if prices:
        print(f"Цены: от {format_price(min(prices))} до {format_price(max(prices))} ₽")
    
    # Статистика по комнатам
    rooms_stats = {}
    for card in cards:
        rooms = card.get('rooms')
        if rooms is not None:
            rooms_stats[rooms] = rooms_stats.get(rooms, 0) + 1
    if rooms_stats:
        # Сортируем безопасно, учитывая что могут быть строки и числа
        sorted_rooms = []
        for k, v in rooms_stats.items():
            if k == 'студия':
                sorted_rooms.append(('студия', v))
            else:
                sorted_rooms.append((k, v))
        sorted_rooms.sort(key=lambda x: (x[0] == 'студия', x[0]))
        rooms_str = ', '.join([f"{k}: {v}" for k, v in sorted_rooms])
        print(f"Комнаты: {rooms_str}")
    
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
    
    cards = await fetch_and_save_listings()
    print_summary(cards)
    
    return cards

if __name__ == '__main__':
    asyncio.run(main())
