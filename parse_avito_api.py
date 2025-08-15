#!/usr/bin/env python3
"""
Avito API Парсер с ротацией заголовков
Использует официальный API: https://www.avito.ru/web/1/main/items
Более эффективен и надежен чем парсинг HTML
"""

import asyncio
import requests
import random
import json
import time
from typing import Dict, List, Optional
from parse_todb_avito import create_ads_avito_table, create_avito_api_table, save_avito_api_item


# ==== Настройки API ====
AVITO_API_URL = "https://www.avito.ru/web/1/main/items"
LOCATION_ID = 637640  # Москва
CATEGORY_ID = 24      # Квартиры
SORT_TYPE = "date"    # Сортировка по дате (отключено для разнообразия)
ITEMS_PER_PAGE = 50  # Количество объявлений на странице

# ==== Настройки пагинации ====
MAX_PAGES = 0        # Максимальное количество страниц для парсинга (0 = все страницы)
START_PAGE = 1        # С какой страницы начать
STOP_AFTER_PAGES = 0  # Остановиться после N страниц (0 = не останавливаться)

# ==== Настройки защиты от бана ====
USE_PROXY = False  # Переключатель: True - с прокси, False - без прокси
MY_PROXY = "http://qEpxaS:uq2shh@194.67.220.161:9889"
LONG_PAUSE_EVERY = (3, 7)   # после скольких запросов вставлять длинную паузу
LONG_PAUSE_TIME = (10, 25)  # сек
SHORT_PAUSE = (1.5, 4.0)    # сек между обычными запросами

# ==== User-Agent ротация ====
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
]

# ==== Accept-Language ротация ====
ACCEPT_LANGUAGES = [
    "ru-RU,ru;q=0.9,en;q=0.8",
    "ru-RU,ru;q=0.8,en;q=0.6",
    "ru-RU,ru;q=0.9",
    "en-US,en;q=0.9,ru;q=0.8",
    "ru,en;q=0.9"
]

# ==== Referer ротация ====
REFERERS = [
    "https://www.avito.ru/",
    "https://www.avito.ru/moskva",
    "https://www.avito.ru/moskva/kvartiry",
    "https://www.avito.ru/moskva/kvartiry/prodam",
    "https://www.google.com/",
    "https://yandex.ru/",
    "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
]

# ==== Внутренние счётчики ====
_request_count = 0
_next_long_pause = random.randint(*LONG_PAUSE_EVERY)





def generate_search_context() -> str:
    """Генерирует простой context для каждого запроса"""
    # Простой context без внешних зависимостей
    import base64
    import gzip
    
    # Создаем простую структуру данных
    context_data = {
        "fromPage": "catalog",
        "timestamp": random.randint(1000000000, 9999999999),
        "sessionId": random.randint(100000, 999999)
    }
    
    try:
        # Конвертируем в JSON
        json_str = json.dumps(context_data, separators=(',', ':'))
        
        # Сжимаем gzip
        compressed = gzip.compress(json_str.encode('utf-8'))
        
        # Кодируем в base64
        encoded = base64.b64encode(compressed).decode('utf-8')
        
        # Добавляем префикс Avito
        return f"H4sIAAAAAAAA_{encoded}"
        
    except Exception as e:
        print(f"[CONTEXT] Ошибка генерации: {e}")
        # Fallback context
        return "H4sIAAAAAAAA_wE-AMH_YToxOntzOjg6ImZyb21QYWdlIjtzOjc6ImNhdGFsb2ciO312FITcIwAAAA"

def clean_url_path(url_path: str) -> str:
    """Очищает URL от всех параметров, оставляя только путь"""
    if not url_path:
        return ""
    
    # Убираем все параметры после ? (включая context)
    if "?" in url_path:
        url_path = url_path.split("?")[0]
    
    return url_path

def build_full_url(url_path: str, add_context: bool = True) -> str:
    """Строит полный URL с или без context"""
    if not url_path:
        return ""
    
    # Сначала очищаем от всех параметров
    clean_path = clean_url_path(url_path)
    base_url = f"https://www.avito.ru{clean_path}"
    
    if add_context:
        context = generate_search_context()
        return f"{base_url}?context={context}"
    
    return base_url

def build_rotated_headers() -> Dict[str, str]:
    """Создает заголовки с ротацией параметров"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": random.choice(REFERERS),
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }

def get_random_proxy() -> Optional[Dict[str, str]]:
    """Возвращает случайный прокси или None"""
    if not USE_PROXY:
        return None
    if random.random() < 0.5:
        return {"http": MY_PROXY, "https": MY_PROXY}
    return None

def rotate_cookies(session: requests.Session) -> None:
    """Сбрасывает куки, имитируя нового пользователя"""
    session.cookies.clear()
    # Добавляем случайные куки для имитации реального пользователя
    session.cookies.set("_avito_web_session", str(random.randint(1000000, 9999999)))
    session.cookies.set("_avito_web_session_id", str(random.randint(1000000, 9999999)))

def safe_api_request(session: requests.Session, page: int) -> requests.Response:
    """Безопасный запрос к Avito API с защитой от бана"""
    global _request_count, _next_long_pause

    # Меняем куки иногда
    if random.random() < 0.3:  # 30% случаев
        rotate_cookies(session)

    headers = build_rotated_headers()
    proxies = get_random_proxy()

    # Параметры API с расширенными данными
    params = {
        "locationId": LOCATION_ID,
        "categoryId": CATEGORY_ID,
        "sort": SORT_TYPE,        # Отключено для разнообразия результатов
        "limit": ITEMS_PER_PAGE,
        "page": page,
        "lastStamp": 0,           # Отключено для разнообразия
        # Дополнительные параметры для расширенных данных
        "includeExtended": "true",
        "includeSeller": "true",
        "includeLocation": "true",
        "includeImages": "true",
        "includePrice": "true",
        "includeDescription": "true"
    }

    _request_count += 1
    try:
        print(f"[API] Запрос страницы {page}...")
        resp = session.get(
            AVITO_API_URL, 
            params=params, 
            headers=headers, 
            proxies=proxies, 
            timeout=30
        )
        resp.encoding = "utf-8"
        return resp
    finally:
        # Проверка на длинную паузу
        if _request_count >= _next_long_pause:
            pause_time = random.uniform(*LONG_PAUSE_TIME)
            print(f"[PAUSE] Длинная пауза {pause_time:.1f} сек...")
            time.sleep(pause_time)
            _request_count = 0
            _next_long_pause = random.randint(*LONG_PAUSE_EVERY)
        else:
            pause_time = random.uniform(*SHORT_PAUSE)
            print(f"[PAUSE] Короткая пауза {pause_time:.1f} сек...")
            time.sleep(pause_time)

def parse_avito_item(item: Dict) -> Dict:
    """Парсит одно объявление из API ответа"""
    try:
        # Определяем тип объявления по postfix
        postfix = item.get("priceDetailed", {}).get("postfix", "")
        is_rental = postfix != ""  # Если postfix не пустой - аренда
        
        # Основная информация из базового API
        parsed_item = {
            "offer_id": item.get("id"),
            "URL": build_full_url(item.get('urlPath', ''), add_context=True),  # Полный URL с context
            "url_clean": build_full_url(item.get('urlPath', ''), add_context=False),  # URL без context
            "url_path": clean_url_path(item.get('urlPath', '')),  # Только путь без context
            "title": item.get("title"),
            "price": item.get("priceDetailed", {}).get("value"),
            "price_currency": "RUB",  # По умолчанию рубли
            "area_m2": None,  # Извлекаем из title или imagesAlt
            "rooms": None,    # Извлекаем из title или imagesAlt
            "floor": None,    # Извлекаем из title или imagesAlt
            "floor_total": None,  # Извлекаем из title или imagesAlt
            "metro": None,    # Извлекаем из location.name
            "address": item.get("location", {}).get("name"),
            "description": item.get("imagesAlt"),  # Используем imagesAlt как описание
            "photos_count": item.get("imagesCount", 0),
            "deal_type": "rental" if is_rental else "sale",  # Продажа или аренда
            "price_postfix": postfix  # Сохраняем postfix для информации
        }

        # Извлекаем метро из location.name
        location_name = item.get("location", {}).get("name", "")
        if location_name:
            # Парсим "Москва, Раменки" -> метро: "Раменки"
            import re
            metro_match = re.search(r'Москва,\s*([^,]+)', location_name)
            if metro_match:
                parsed_item["metro"] = metro_match.group(1).strip()
                parsed_item["address"] = f"Москва, метро {parsed_item['metro']}"

        # Извлекаем данные из title или imagesAlt
        title_text = item.get("title", "") or item.get("imagesAlt", "")
        if title_text:
            # Парсим "3-к. квартира, 72 м², 3/9 эт."
            import re
            
            # Площадь
            area_match = re.search(r'(\d+(?:\.\d+)?)\s*м²', title_text)
            if area_match:
                parsed_item["area_m2"] = float(area_match.group(1))
            
            # Комнаты
            rooms_match = re.search(r'(\d+)-к\.', title_text)
            if rooms_match:
                parsed_item["rooms"] = int(rooms_match.group(1))
            
            # Этаж
            floor_match = re.search(r'(\d+)/(\d+)\s*эт', title_text)
            if floor_match:
                parsed_item["floor"] = int(floor_match.group(1))
                parsed_item["floor_total"] = int(floor_match.group(2))

        # Определяем тип продавца по доступным данным
        # В базовом API нет labels, поэтому оставляем unknown
        
        return parsed_item
    except Exception as e:
        print(f"[ERROR] Ошибка парсинга объявления: {e}")
        return {}





async def main():
    """Основная функция"""
    print("🚀 Avito API Парсер запущен!")
    print(f"📍 Регион: Москва (ID: {LOCATION_ID})")
    print(f"🏠 Категория: Квартиры (ID: {CATEGORY_ID})")
    print(f"📊 Объявлений на страницу: {ITEMS_PER_PAGE}")
    
    proxy_status = "С ПРОКСИ" if USE_PROXY else "БЕЗ ПРОКСИ"
    print(f"🛡️ Режим защиты: {proxy_status}")
    print(f"⏱️ Задержки: короткие {SHORT_PAUSE[0]}-{SHORT_PAUSE[1]}с, длинные {LONG_PAUSE_TIME[0]}-{LONG_PAUSE_TIME[1]}с")
    print("="*80)



    # Создаем таблицу в БД
    await create_ads_avito_table()
    await create_avito_api_table()

    session = requests.Session()
    
    # Парсим страницы согласно настройкам
    total_pages = 1  # Начнем с 1, потом обновим
    total_processed = 0
    
    print(f"🔄 НАЧИНАЕМ ПАГИНАЦИЮ:")
    print(f"📄 Начинаем с страницы: {START_PAGE}")
    if MAX_PAGES > 0:
        print(f"📊 Максимум страниц: {MAX_PAGES}")
    if STOP_AFTER_PAGES > 0:
        print(f"⏹️ Остановимся после: {STOP_AFTER_PAGES} страниц")
    print("="*80)
    
    page = START_PAGE
    while True:
        try:
            print(f"\n📄 ОБРАБОТКА СТРАНИЦЫ {page}")
            print("-" * 50)
            
            resp = safe_api_request(session, page)
            
            if resp.status_code != 200:
                print(f"❌ Ошибка API на странице {page}: {resp.status_code}")
                if resp.status_code == 404:  # Страница не найдена
                    print(f"🏁 Достигнут конец - страница {page} не существует")
                    break
                else:
                    print(f"⚠️ Пропускаем страницу {page} и продолжаем")
                    page += 1
                    continue

            # Парсим JSON ответ
            data = resp.json()
            
            if not data or "items" not in data:
                print(f"❌ Неверный формат ответа API на странице {page}")
                break

            items = data["items"]
            
            # Обновляем общую информацию при первой странице
            if page == 1:
                total_items = data.get("total", 0)
                total_pages = data.get("pages", 1)
                print(f"✅ Получено {len(items)} объявлений из {total_items} всего")
                print(f"📊 API говорит страниц: {total_pages}")
                print(f"📄 Размер страницы: {ITEMS_PER_PAGE} объявлений")
                if total_pages <= 1:
                    print(f"⚠️ API показывает мало страниц - будем парсить дальше")
                print("="*80)
            
            if not items:
                print(f"🏁 Страница {page} пустая - достигнут конец")
                break
            
            print(f"📊 Найдено объявлений на странице: {len(items)}")
            
            # Обрабатываем все объявления на странице
            page_processed = 0
            for i, item in enumerate(items, 1):
                parsed = parse_avito_item(item)
                
                if parsed:
                    print(f"URL: {parsed.get('url_clean')}")
                    
                    # Сохраняем в БД
                    try:
                        await save_avito_api_item(parsed)
                        page_processed += 1
                        total_processed += 1
                    except Exception as e:
                        print(f"❌ Ошибка сохранения: {e}")
                else:
                    print("❌ Ошибка парсинга объявления")
            
            print(f"💾 Страница {page}: обработано {page_processed}/{len(items)} объявлений")
            
            # Проверяем ограничения пагинации (приоритет выше естественного конца)
            if MAX_PAGES > 0 and page >= MAX_PAGES:
                print(f"🏁 Достигнут лимит страниц: {MAX_PAGES}")
                break
                
            if STOP_AFTER_PAGES > 0 and page >= START_PAGE + STOP_AFTER_PAGES - 1:
                print(f"🏁 Остановка после {STOP_AFTER_PAGES} страниц")
                break
            
            # Проверяем, достигли ли естественного конца
            # Если API показывает мало страниц, парсим дальше пока есть данные
            if MAX_PAGES == 0 and STOP_AFTER_PAGES == 0:
                if total_pages > 1 and page >= total_pages:
                    print(f"🏁 Достигнут конец по API - страница {page}")
                    break
                elif total_pages <= 1 and len(items) == 0:
                    print(f"🏁 Достигнут конец - страница {page} пустая")
                    break
            
            page += 1
            
        except Exception as e:
            print(f"❌ Критическая ошибка на странице {page}: {e}")
            break
    
    print(f"\n{'='*80}")
    print(f"🎉 ПАГИНАЦИЯ ЗАВЕРШЕНА!")
    print(f"📊 СТАТИСТИКА:")
    print(f"📄 Обработано страниц: {page}")
    print(f"🏠 Всего объявлений: {total_items}")
    print(f"💾 Успешно сохранено: {total_processed}")
    print(f"📈 Эффективность: {(total_processed/total_items*100):.1f}%" if total_items > 0 else "N/A")
    print(f"{'='*80}")

if __name__ == '__main__':
    asyncio.run(main())
