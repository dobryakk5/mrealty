#!/usr/bin/env python3
"""
dump_first_from_search.py

Ищет первую ссылку на объявление в результатах поиска CIAN (по списку страниц),
скачивает страницу объявления и выводит "всё что есть" (meta, JSON-LD, inline state, текст).
Помечает поля, которые вы уже сохраняете в парсере.

Примеры:
  python dump_first_from_search.py
  python dump_first_from_search.py --metro 68 --foot 20
  python dump_first_from_search.py --proxy "http://user:pass@host:port"

Настройки по умолчанию в верхней части файла — изменяйте их там или через args.
"""
import argparse
import json
import re
import sys
import time
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

# ====== НАСТРОЙКИ (по умолчанию, как вы попросили) ======
PROPERTY_TYPE = 1       # 1=вторичка, 2=новостройки
TIME_PERIOD = 604800    # 3600=час, 86400=день, 604800=неделя, -2=сегодня
MAX_PAGES = 1           # сколько страниц поиска обойти
MAX_URLS = 1            # сколько карточек на странице рассматривать (ограничение)
REQUEST_DELAY = 2.0     # пауза между запросами страниц поиска
PROXY = None            # пример: "http://user:pass@host:port" или None

METRO_ID = "all"        # "all" или конкретный ID станции (можно передать через args)
FOOT_MIN = None         # минуты до метро (int) или None

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

# ПОЛЯ, которые ваш парсер уже сохраняет (для пометки)
SAVED_FIELDS = {
    "URL", "offer_id", "title", "subtitle", "price", "rooms", "area_m2",
    "floor", "floor_total", "complex", "metro", "walk_minutes", "metro_id",
    "created_dt", "geo_labels", "labels", "seller"
}

# ====== УТИЛИТЫ ======

def build_search_url(property_type: int, time_period: int, metro_id: Optional[int] = None, foot_min: Optional[int] = None) -> str:
    base_url = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=1"
    url = f"{base_url}&object_type%5B0%5D={property_type}&totime={time_period}"
    if property_type == 2:
        url += "&with_newobject=1"
    if metro_id is not None and metro_id != "all":
        url += f"&metro%5B0%5D={metro_id}"
    if foot_min is not None:
        url += f"&foot_min={foot_min}"
    return url

def fetch_text(url: str, proxy: Optional[str] = None, timeout: int = 30) -> str:
    s = requests.Session()
    if proxy:
        s.proxies = {'http': proxy, 'https': proxy}
    r = s.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def find_offer_links_on_search_page(html: str, max_urls: int = 5) -> List[str]:
    soup = BeautifulSoup(html, 'html.parser')
    cards = soup.select('[data-name="CardComponent"]')
    links = []
    for c in cards[:max_urls]:
        a = c.select_one('a[href*="/sale/flat/"], a[href*="/object/"]')
        if a and a.get('href'):
            href = a['href']
            if href.startswith('/'):
                href = "https://www.cian.ru" + href
            # normalize: strip parameters after '/'
            links.append(href.split('?')[0])
    return links

def extract_json_ld(soup: BeautifulSoup):
    out = []
    for tag in soup.find_all('script', type='application/ld+json'):
        txt = tag.string or tag.get_text("")
        try:
            parsed = json.loads(txt)
            out.append(parsed)
        except Exception:
            # best-effort split
            try:
                # try to find top-level JSON objects
                objs = re.findall(r'\{.*?\}', txt, flags=re.S)
                for o in objs:
                    try:
                        out.append(json.loads(o))
                    except Exception:
                        continue
            except Exception:
                continue
    return out

def extract_inline_state(html: str):
    out = {}
    patterns = [
        r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
        r'window\.__PRELOADED_STATE__\s*=\s*({.*?});',
        r'window\.__DATA__\s*=\s*({.*?});',
        r'__INITIAL_DATA__\s*=\s*({.*?});',
        r'(?s)var\s+initialState\s*=\s*({.*?});'
    ]
    for p in patterns:
        for m in re.finditer(p, html, flags=re.S):
            js = m.group(1)
            try:
                out[p] = json.loads(js)
            except Exception:
                out[p] = js[:10000]
    return out

def gather_meta(soup: BeautifulSoup):
    metas = {}
    for m in soup.find_all('meta'):
        if m.get('name'):
            metas[f"name:{m['name']}"] = m.get('content', '')
        if m.get('property'):
            metas[f"property:{m['property']}"] = m.get('content', '')
    if soup.title:
        metas['title_tag'] = soup.title.string or ''
    return metas

def find_main_offer_container(soup: BeautifulSoup):
    selectors = [
        '[data-name="CardComponent"]',
        '[data-name="Offer"]',
        '[data-mark="OfferTitle"]',
        '.object-card', '.offer', '.offer-layout', '[data-testid*="object-page"]'
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el
    # fallback: biggest textual block
    candidates = sorted(soup.find_all(['div','section','article']), key=lambda t: len(t.get_text() or ''), reverse=True)
    return candidates[0] if candidates else None

def try_import_local_parser():
    """
    Попытка импортировать parse_offer_card из parse_cian_to_db.py (если он рядом)
    Возвращает функцию или None.
    """
    try:
        import importlib.util, pathlib
        path = pathlib.Path(__file__).resolve().parent / "parse_cian_to_db.py"
        spec = importlib.util.spec_from_file_location("parse_cian_to_db", str(path))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore
        if hasattr(module, 'parse_offer_card'):
            return module.parse_offer_card
    except FileNotFoundError:
        return None
    except Exception as e:
        print("Внимание: ошибка при импорте parse_cian_to_db.py:", e)
        return None

def pretty_print_section(title: str, lines: List[str]):
    print("\n" + "="*80)
    print(title)
    print("-"*80)
    for l in lines:
        print(l)
    print("="*80 + "\n")

# ====== Основная логика: найти ПЕРВУЮ ссылку и выдать dump ======

def find_first_offer(search_url_base: str, max_pages: int, max_urls_per_page: int, proxy: Optional[str]) -> Optional[str]:
    for page in range(1, max_pages + 1):
        page_url = f"{search_url_base}&p={page}"
        print(f"Загружаю страницу поиска: {page_url}")
        try:
            html = fetch_text(page_url, proxy=proxy)
        except Exception as e:
            print("Ошибка при загрузке страницы поиска:", e)
            return None
        links = find_offer_links_on_search_page(html, max_urls=max_urls_per_page)
        if links:
            print(f"Найдено ссылок на странице {page}: {len(links)}. Беру первую: {links[0]}")
            return links[0]
        # пауза перед следующей страницей
        if page < max_pages:
            time.sleep(REQUEST_DELAY)
    print("Не найдено ни одного объявления в пределах заданных параметров.")
    return None

def dump_offer_by_url(url: str, proxy: Optional[str], try_local_parse: bool = True):
    print(f"\n=== Дамп объявления: {url} ===")
    try:
        html = fetch_text(url, proxy=proxy)
    except Exception as e:
        print("Ошибка загрузки объявления:", e)
        return

    soup = BeautifulSoup(html, 'html.parser')

    # meta
    metas = gather_meta(soup)
    pretty_print_section("META-TAGS", [f"{k}: {v}" for k, v in metas.items()])

    # json-ld
    jsonld = extract_json_ld(soup)
    if jsonld:
        pretty_print_section(f"JSON-LD — найдено {len(jsonld)} объектов", [json.dumps(o, ensure_ascii=False, indent=2) for o in jsonld])
    else:
        print("JSON-LD: не найдено")

    # inline state
    states = extract_inline_state(html)
    if states:
        pretty_print_section("Inline JS state (части)", [f"{k}: {str(v)[:3000]}" for k, v in states.items()])
    else:
        print("Inline JS state: не найдено")

    # preview plain text
    full_text = soup.get_text("\n", strip=True)
    pretty_print_section("ПРЕВЬЮ СЫРОГО ТЕКСТА (первые 30к символов)", [full_text[:30000]])

    # main container
    container = find_main_offer_container(soup)
    if container:
        pretty_print_section("ОСНОВНОЙ КОНТЕЙНЕР (текст)", [container.get_text("\n", strip=True)[:20000]])
    else:
        print("Основной контейнер не найден")

    # попытка импортировать и применить локальный парсер (если доступен)
    parsed = None
    if try_local_parse:
        parse_func = try_import_local_parser()
        if parse_func and container:
            try:
                print("Попытка применить локальный parse_offer_card...")
                parsed = parse_func(container)
            except Exception as e:
                print("Ошибка при выполнении parse_offer_card:", e)
                parsed = None
        elif not parse_func:
            print("Локальный parse_offer_card не найден (или ошибка импорта).")

    # fallback простого извлечения, если не получилось
    if parsed is None and container:
        # простой пассивный сбор (title/price/seller/addr)
        parsed = {}
        t = container.select_one('[data-mark="OfferTitle"], h1, h2')
        parsed['title'] = t.get_text(strip=True) if t else None
        st = container.select_one('[data-mark="OfferSubtitle"], .subtitle')
        parsed['subtitle'] = st.get_text(strip=True) if st else None
        price_el = container.select_one('[data-mark="MainPrice"], [data-testid*="price"], .price')
        parsed['price'] = price_el.get_text(" ", strip=True) if price_el else None
        seller = container.select_one('[data-name="BrandingLevelWrapper"], .seller, .agent')
        parsed['seller'] = seller.get_text(" ", strip=True) if seller else None
        geo = container.select_one('[data-name="GeoLabel"], a[href*="geo"], .address, .location')
        parsed['geo_labels'] = [geo.get_text(" ", strip=True)] if geo else []

    # Вывод parsed полей с пометками
    if parsed:
        lines = []
        for k, v in parsed.items():
            mark = "[SAVED]" if k in SAVED_FIELDS else "[NEW]  "
            sval = v if v is None else (v if isinstance(v, (int, float)) else str(v)[:5000])
            lines.append(f"{mark} {k}: {sval}")
        pretty_print_section("ПОЛЯ, ВЫДЕЛЕННЫЕ ПАРСЕРОМ / FALLBACK ( [SAVED]=уже сохраняете )", lines)
    else:
        print("Не удалось извлечь структурированные поля.")

    # дополнительно: попытка найти телефоны и ссылки на компанию
    phones = set(re.findall(r'(\+?\d[\d\-\s\(\)]{6,}\d)', full_text))
    companies = [a.get('href') for a in soup.select('a[href*="/company/"]')][:20]
    extra_lines = []
    if companies:
        extra_lines.append("Company links: " + ", ".join(companies))
    if phones:
        extra_lines.append("Phone-like strings: " + ", ".join(list(phones)[:10]))
    scripts = [s for s in soup.find_all('script') if (s.string or '').strip()]
    big_scripts = [s.string for s in scripts if s.string and len(s.string) > 500][:5]
    for i, b in enumerate(big_scripts, 1):
        extra_lines.append(f"script#{i} len={len(b)} excerpt: {b[:1000]}")

    if extra_lines:
        pretty_print_section("ДОПОЛНИТЕЛЬНЫЕ НАЙДЕННЫЕ БЛОКИ", extra_lines)
    print("\nГотово.")

# ====== CLI и запуск ======

def main():
    ap = argparse.ArgumentParser(description="Найти первое объявление в результатах поиска и вывести всё с его страницы.")
    ap.add_argument('--metro', type=str, default=METRO_ID, help='ID метро или "all"')
    ap.add_argument('--foot', type=int, default=FOOT_MIN, help='минуты до метро (foot_min)')
    ap.add_argument('--pages', type=int, default=MAX_PAGES, help='макс страниц поиска для перебора')
    ap.add_argument('--perpage', type=int, default=MAX_URLS, help='сколько карточек с каждой страницы рассматривать')
    ap.add_argument('--proxy', type=str, default=PROXY, help='HTTP proxy (пример: http://user:pass@host:port)')
    ap.add_argument('--no-local-parse', action='store_true', help='не пытаться импортировать локальный parse_offer_card')
    args = ap.parse_args()

    search_url = build_search_url(PROPERTY_TYPE, TIME_PERIOD, args.metro, args.foot)
    print("Search URL base:", search_url)
    first = find_first_offer(search_url, max_pages=args.pages, max_urls_per_page=args.perpage, proxy=args.proxy)
    if not first:
        print("Не найдено объявлений по заданным критериям.")
        sys.exit(0)
    dump_offer_by_url(first, proxy=args.proxy, try_local_parse=not args.no_local_parse)

if __name__ == "__main__":
    main()
