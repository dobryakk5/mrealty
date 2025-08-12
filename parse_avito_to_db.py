#!/usr/bin/env python3
"""
Парсер объявлений AVITO (вторичка, Москва) -> БД ads_avito
- Парсит карточки со страниц списка (без перехода в объявления)
- Период: неделя (фильтр в ссылке пользователя)
- Пауза между страницами: 4 секунды
- Пропускает дубликаты по avitoid
"""

import asyncio
import re
import time
import random
import requests
from bs4 import BeautifulSoup
from typing import List, Dict

from parse_todb_avito import create_ads_avito_table, save_avito_ad

# Настройки
BASE_SEARCH_URL = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
REQUEST_DELAY = 4.0
HEADERS_BASE = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6',
    'Connection': 'keep-alive',
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
]

REFERERS = [
    'https://www.avito.ru/',
    'https://www.avito.ru/moskva',
    'https://www.avito.ru/moskva/kvartiry',
]

def build_headers() -> dict:
    ua = random.choice(USER_AGENTS)
    ref = random.choice(REFERERS)
    h = dict(HEADERS_BASE)
    h['User-Agent'] = ua
    h['Referer'] = ref
    return h
PROXY = None  # прокси отключен для Avito


def _clean(s):
    if s is None:
        return None
    return re.sub(r'\s+', ' ', s).strip()


def build_page_url(page: int) -> str:
    # Avito использует параметр p для пагинации
    if page <= 1:
        return BASE_SEARCH_URL
    return f"{BASE_SEARCH_URL}&p={page}"


def parse_price(text: str) -> int | None:
    if not text:
        return None
    m = re.search(r"([\d\s\u00A0,]+)", text)
    if not m:
        return None
    num = m.group(1).replace('\u00A0', '').replace(' ', '').replace(',', '')
    try:
        return int(num)
    except Exception:
        return None

def split_address_and_metro(addr_text: str) -> tuple[str | None, str | None, int | None]:
    """Разбивает строку адреса Avito на (улица,дом), (метро), (минуты до метро).
    Берём первую цифру минут, если указано «6–10 мин» → 6.
    """
    if not addr_text:
        return None, None, None
    # Нормализуем тире диапазона минут: "6–10 мин" -> сохраняем первую цифру
    parts = [p.strip() for p in addr_text.split(',') if p.strip()]
    if not parts:
        return addr_text.strip(), None, None

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
        # всё, что ДО сегмента с минутами — это адресная часть (может включать дом+метро слитно)
        address_candidates = parts[:minutes_idx]
    else:
        address_candidates = parts

    # Адрес — первые два компонента (улица, дом), если есть
    address_value = None
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
                else:
                    house = None
                if not metro and tail:
                    metro = tail
            else:
                # fallback: берём сегмент как дом
                house = seg2
        # Сбор адреса
        if street and house:
            address_value = f"{street}, {house}"
        else:
            address_value = street

    return address_value, metro, minutes


def parse_rooms_area_floor(title: str) -> tuple:
    """Пытаемся из заголовка вытащить (rooms, area_m2, floor, total_floors)."""
    rooms = None
    area_m2 = None
    floor = None
    total_floors = None

    if not title:
        return rooms, area_m2, floor, total_floors

    # Форматы комнат на Avito: "3-к.", "2-к", "3к", "3-комн"
    m = re.search(r"(\d+)\s*[-–—]?\s*к\.?\b", title, re.IGNORECASE)
    if m:
        rooms = int(m.group(1))
    else:
        m = re.search(r"(\d+)[^\d\-–—]*-?комн", title, re.IGNORECASE)
        if m:
            rooms = int(m.group(1))
        elif re.search(r"\bстуд", title, re.IGNORECASE):
            rooms = 0

    m = re.search(r"(\d+[.,]?\d*)\s*(?:м²|м2|м)\b", title)
    if m:
        try:
            area_m2 = float(m.group(1).replace(',', '.'))
        except Exception:
            area_m2 = None

    m = re.search(r"(\d+)\s*/\s*(\d+)\s*эт", title, re.IGNORECASE)
    if not m:
        m = re.search(r"(\d+)\s*/\s*(\d+)", title)
    if m:
        try:
            floor = int(m.group(1))
            total_floors = int(m.group(2))
        except Exception:
            pass

    return rooms, area_m2, floor, total_floors


def parse_avito_card(card) -> Dict:
    """Парсит карточку авито на странице списка."""
    out: Dict = {}

    # URL и id
    a = card.select_one('a[itemprop="url"], a[data-marker="item-title"]')
    url = a.get('href') if a else None
    if url and url.startswith('/'):
        url = 'https://www.avito.ru' + url
    # удаляем параметры (?context=...) и фрагменты
    if url:
        url = url.split('#')[0].split('?')[0]
    out['URL'] = _clean(url)

    # id из data-item-id или из URL
    offer_id = None
    id_holder = card.get('data-item-id')
    if id_holder and id_holder.isdigit():
        offer_id = id_holder
    if not offer_id and url:
        m = re.search(r"_(\d+)(?:$|[?#])", url)
        if not m:
            m = re.search(r"/(\d+)(?:$|[?#])", url)
        if m:
            offer_id = m.group(1)
    out['offer_id'] = offer_id

    # Заголовок
    title_el = card.select_one('[itemprop="name"], h3, a[data-marker="item-title"]')
    title = _clean(title_el.get_text()) if title_el else None
    out['title'] = title

    # Цена
    price_el = card.select_one('[itemprop="price"], meta[itemprop="price"], span[data-marker="item-price"]')
    price = None
    if price_el:
        content = price_el.get('content')
        if content and content.isdigit():
            price = int(content)
        else:
            price = parse_price(price_el.get_text())
    else:
        # Fallback: поиск ₽
        price = parse_price(card.get_text())
    out['price'] = price

    # Характеристики из заголовка
    rooms, area_m2, floor, total_floors = parse_rooms_area_floor(title or '')
    out['rooms'] = rooms
    out['area_m2'] = area_m2
    out['floor'] = floor
    out['floor_total'] = total_floors

    # Адрес и метро из адресной строки
    addr_el = card.select_one('[data-marker="item-address"]')
    addr_text = _clean(addr_el.get_text()) if addr_el else None
    address_value, metro_from_addr, minutes_from_addr = split_address_and_metro(addr_text or '')
    out['address'] = address_value

    # Метро/минуты: сначала берём из адресной строки, при отсутствии — из иных элементов
    metro = metro_from_addr
    walk_minutes = minutes_from_addr
    if metro is None or walk_minutes is None:
        metro_el = card.find(lambda t: t.name in ('span','div') and re.search(r'метро|м\.|мин', (t.get_text() or ''), re.I))
        if metro_el:
            txt = metro_el.get_text(' ', strip=True)
            m = re.search(r'(\d+)', txt)
            if m and walk_minutes is None:
                walk_minutes = int(m.group(1))
            # название метро — первая фраза
            m2 = re.search(r"([^,\(]+)", txt)
            if m2 and not metro:
                metro = _clean(m2.group(0))
    out['metro'] = metro
    out['walk_minutes'] = walk_minutes

    # Продавец (элементарная эвристика)
    seller = {'type': 'unknown', 'name': None}
    seller_el = card.select_one('[data-marker="seller-info/name"], [data-marker="item-contact-bar/call-button"]')
    if seller_el:
        seller_name = _clean(seller_el.get_text())
        if seller_name:
            seller['name'] = seller_name
            if re.search(r'агент|риелтор|компани|agency', seller_name, re.I):
                seller['type'] = 'agency'
            else:
                seller['type'] = 'user'
    out['seller'] = seller

    # Лейблы/теги
    labels = []
    for el in card.select('[data-marker*="badge"], [class*="badge"], [class*="label"]'):
        t = _clean(el.get_text())
        if t:
            labels.append(t)
    out['labels'] = list(dict.fromkeys(labels))

    return out


async def fetch_and_save_avito(max_pages: int = 9999) -> List[Dict]:
    print("AVITO -> БД (вторичка, неделя)")
    await create_ads_avito_table()

    sess = requests.Session()
    proxies = {"http": PROXY, "https": PROXY} if PROXY else None

    all_cards: List[Dict] = []
    for page in range(1, max_pages + 1):
        url = build_page_url(page)
        print(f"Стр. {page}: {url}")
        try:
            resp = sess.get(url, headers=build_headers(), timeout=25, proxies=proxies)
            resp.encoding = 'utf-8'
            if resp.status_code != 200:
                print(f"HTTP {resp.status_code}, остановка")
                break
            soup = BeautifulSoup(resp.text, 'html.parser')

            # Карточки: общий контейнер объявлений
            cards = soup.select('[data-marker="item"]') or soup.select('div.iva-item-content-\w+')
            print(f"Найдено карточек: {len(cards)}")
            if not cards:
                break

            for card in cards:
                try:
                    data = parse_avito_card(card)
                    if data.get('URL') and data.get('offer_id'):
                        # Avito — сохраняем как вторичку (2), по задаче
                        data['object_type_id'] = 2
                        all_cards.append(data)
                        await save_avito_ad(data)
                except Exception as e:
                    print(f"Ошибка парсинга карточки: {e}")
                    continue

            time.sleep(REQUEST_DELAY)
        except Exception as e:
            print(f"Ошибка запроса страницы {page}: {e}")
            break

    print(f"ИТОГО сохранено/обработано: {len(all_cards)}")
    return all_cards


async def main():
    await fetch_and_save_avito()

if __name__ == '__main__':
    asyncio.run(main())
