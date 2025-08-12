#!/usr/bin/env python3
"""
Smoke-тест: парсит 1 объявление с первой страницы AVITO (вторичка, Москва)
Тестирует разные способы парсинга: HTML, JSON, и другие методы
Включает защиту от бана: прокси, случайные задержки, ротация User-Agent
"""

import asyncio
import requests
import random
import json
import re
import time
from bs4 import BeautifulSoup

from parse_avito_to_db import build_page_url, parse_avito_card, build_headers
from parse_todb_avito import create_ads_avito_table, save_avito_ad

# ==== Настройки защиты от бана ====
USE_PROXY = False  # Переключатель: True - с прокси, False - без прокси
MY_PROXY = "http://qEpxaS:uq2shh@194.67.220.161:9889"
LONG_PAUSE_EVERY = (5, 10)  # после скольких запросов вставлять длинную паузу (min,max)
LONG_PAUSE_TIME = (15, 35)  # сек
SHORT_PAUSE = (2.5, 6.5)    # сек между обычными запросами

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; rv:128.0) Gecko/20100101 Firefox/128.0"
]

# ==== Внутренние счётчики ====
_request_count = 0
_next_long_pause = random.randint(*LONG_PAUSE_EVERY)

# ==== Генераторы случайных параметров ====
def build_safe_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": random.choice(["ru-RU,ru;q=0.9", "ru-RU,ru;q=0.8,en;q=0.6"]),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive",
        "Referer": "https://www.avito.ru/"
    }

def get_random_proxy():
    if not USE_PROXY:
        return None
    if random.random() < 0.5:
        return {"http": MY_PROXY, "https": MY_PROXY}
    return None

def rotate_cookies(session):
    """Сбрасывает куки, имитируя нового пользователя"""
    session.cookies.clear()
    # Иногда можно положить случайный набор тестовых кук
    # session.cookies.set("some_cookie", str(random.randint(1,10000)))

# ==== Основная безопасная функция ====
def safe_get(session, url):
    global _request_count, _next_long_pause

    # Меняем куки иногда
    if random.random() < 0.2:  # 20% случаев
        rotate_cookies(session)

    headers = build_safe_headers()
    proxies = get_random_proxy()

    _request_count += 1
    try:
        resp = session.get(url, headers=headers, proxies=proxies, timeout=25)
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
            time.sleep(random.uniform(*SHORT_PAUSE))


def extract_json_data(html_content):
    """Извлекает JSON данные из HTML страницы"""
    json_patterns = [
        r'window\._cianConfig\s*=\s*({.*?});',
        r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
        r'<script[^>]*>window\._cianConfig\s*=\s*({.*?})</script>',
        r'<script[^>]*>window\.__INITIAL_STATE__\s*=\s*({.*?})</script>',
        r'data-params="([^"]*)"',
        r'data-item="([^"]*)"'
    ]
    
    extracted_data = {}
    for pattern in json_patterns:
        matches = re.findall(pattern, html_content, re.DOTALL)
        if matches:
            for match in matches:
                try:
                    if pattern.startswith('data-'):
                        # URL decode и parse JSON
                        import urllib.parse
                        decoded = urllib.parse.unquote(match)
                        data = json.loads(decoded)
                    else:
                        data = json.loads(match)
                    extracted_data[f"pattern_{pattern[:20]}"] = data
                except:
                    extracted_data[f"raw_pattern_{pattern[:20]}"] = match[:200]
    
    return extracted_data


def clean_seller_name(seller_text):
    """Очищает название продавца от рекламных фраз и лишнего текста, сохраняя даты"""
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
    
    # НЕ убираем даты - они нужны для времени публикации
    # Даты будут извлечены отдельно в time_info
    
    # Убираем короткие слова-мусор
    noise_words = [
        r'\bнет\b',
        r'\bда\b', 
        r'\bесть\b',
        r'\bвсе\b',
        r'\bновый\b',
        r'\bстарый\b',
        r'\bбольшой\b',
        r'\bмаленький\b',
        r'\bхороший\b',
        r'\bплохой\b',
        r'\bбыстрый\b',
        r'\bмедленный\b'
    ]
    
    for word in noise_words:
        cleaned = re.sub(word, '', cleaned, flags=re.IGNORECASE)
    
    # Убираем лишние пробелы и символы
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = re.sub(r'^[^\wа-яё]+', '', cleaned)  # убираем символы в начале
    cleaned = re.sub(r'[^\wа-яё\s]+$', '', cleaned)  # убираем символы в конце
    
    # Убираем повторяющиеся слова
    words = cleaned.split()
    unique_words = []
    for word in words:
        if word.lower() not in [w.lower() for w in unique_words]:
            unique_words.append(word)
    cleaned = ' '.join(unique_words)
    
    # Если после очистки остался только рекламный текст, возвращаем None
    if len(cleaned) < 2 or cleaned.lower() in ['звоните', 'готовы', 'ответить', 'вопросы']:
        return None
    
    return cleaned


def parse_card_detailed(card):
    """Детальный парсинг карточки разными способами"""
    results = {}
    
    # 1. Стандартный HTML парсинг
    try:
        results['html_parsing'] = parse_avito_card(card)
    except Exception as e:
        results['html_parsing_error'] = str(e)
    
    # 2. Извлечение атрибутов data-*
    data_attrs = {}
    for attr, value in card.attrs.items():
        if attr.startswith('data-'):
            data_attrs[attr] = value
    results['data_attributes'] = data_attrs
    
    # 3. Поиск скрытых полей
    hidden_fields = {}
    hidden_inputs = card.find_all('input', type='hidden')
    for inp in hidden_inputs:
        if inp.get('name') and inp.get('value'):
            hidden_fields[inp.get('name')] = inp.get('value')
    results['hidden_fields'] = hidden_fields
    
    # 4. Поиск мета-информации
    meta_info = {}
    meta_tags = card.find_all('meta')
    for meta in meta_tags:
        if meta.get('name') and meta.get('content'):
            meta_info[meta.get('name')] = meta.get('content')
    results['meta_tags'] = meta_info
    
    # 5. Поиск структурированных данных (JSON-LD)
    json_ld = []
    script_tags = card.find_all('script', type='application/ld+json')
    for script in script_tags:
        try:
            data = json.loads(script.string)
            json_ld.append(data)
        except:
            pass
    results['json_ld'] = json_ld
    
    # 6. Поиск по CSS классам
    css_classes = {}
    for class_name in card.get('class', []):
        if 'price' in class_name.lower():
            price_elem = card.find(class_=class_name)
            if price_elem:
                css_classes['price'] = price_elem.get_text(strip=True)
        elif 'title' in class_name.lower() or 'name' in class_name.lower():
            title_elem = card.find(class_=class_name)
            if title_elem:
                css_classes['title'] = title_elem.get_text(strip=True)
    results['css_classes_data'] = css_classes
    
    # 7. Поиск информации о продавце в разных местах
    seller_info = {}
    
    # Ищем в labels (как в вашем примере)
    labels = []
    label_elements = card.find_all(class_=re.compile(r'label|badge|tag'))
    for elem in label_elements:
        text = elem.get_text(strip=True)
        if text:
            labels.append(text)
            # Определяем тип продавца по меткам
            if text == "Реквизиты проверены":
                seller_info['type'] = 'agency'
                seller_info['verified_type'] = 'company'
            elif text == "Документы проверены":
                seller_info['type'] = 'agent'
                seller_info['verified_type'] = 'agent'
            elif 'проверено в росреестре' in text.lower():
                seller_info['type'] = 'owner'
                seller_info['verified_type'] = 'rosreestr'
            # Дополнительные проверки
            elif 'агентство' in text.lower() or 'агент' in text.lower():
                seller_info['type'] = 'agency'
            elif 'собственник' in text.lower() or 'владелец' in text.lower():
                seller_info['type'] = 'owner'
    
    # Дополнительная проверка: если в labels есть "Собственник", то продавец - собственник
    if any('собственник' in label.lower() for label in labels):
        seller_info['type'] = 'owner'
        seller_info['from_labels_check'] = 'Собственник найден в labels'
    
    # Ищем продавца перед фразами о завершенных объявлениях
    card_text = card.get_text()
    seller_patterns = [
        r'([^.]*?)\s*(?:нет завершенных объявлений|нет завершённых объявлений)',
        r'([^.]*?)\s*(\d+)\s*завершенных объявлений',
        r'([^.]*?)\s*(\d+)\s*завершённых объявлений',
        r'([^.]*?)\s*завершенных объявлений',
        r'([^.]*?)\s*завершённых объявлений'
    ]
    
    for pattern in seller_patterns:
        match = re.search(pattern, card_text, re.IGNORECASE | re.DOTALL)
        if match:
            seller_text = match.group(1).strip()
            if seller_text and len(seller_text) > 3:  # фильтруем слишком короткие совпадения
                seller_info['from_completed_ads'] = seller_text
                # Пытаемся определить тип продавца из текста
                if any(word in seller_text.lower() for word in ['агентство', 'агент', 'риэлтор']):
                    seller_info['type'] = 'agency'
                elif any(word in seller_text.lower() for word in ['собственник', 'владелец', 'хозяин']):
                    seller_info['type'] = 'owner'
                break
    
    # Дополнительный поиск продавца в разных местах
    # Ищем имя продавца после времени размещения
    time_seller_patterns = [
        r'(\d+\s*(?:час|часа|часов|день|дня|дней|неделя|недели|недель|месяц|месяца|месяцев)\s*назад)\s*([^.\n]+?)(?:\n|\.|$|завершённых|завершенных)',
        r'(\d+\s*(?:час|часа|часов|день|дня|дней|неделя|недели|недель|месяц|месяца|месяцев)\s*назад)\s*([^.\n]+?)(?=\d+\s*завершённых|\d+\s*завершенных)',
    ]
    
    for pattern in time_seller_patterns:
        match = re.search(pattern, card_text, re.IGNORECASE | re.DOTALL)
        if match:
            seller_text = match.group(2).strip()
            if seller_text and len(seller_text) > 2:
                # Очищаем от рекламных фраз и оставляем только название компании
                cleaned_seller = clean_seller_name(seller_text)
                if cleaned_seller:
                    seller_info['from_time_pattern'] = cleaned_seller
                    # Определяем тип по ключевым словам
                    if any(word in cleaned_seller.lower() for word in ['агентство', 'агент', 'риэлтор', 'real estate', 'недвижимость']):
                        seller_info['type'] = 'agency'
                    elif any(word in cleaned_seller.lower() for word in ['собственник', 'владелец', 'хозяин', 'артур', 'иван', 'мария']):
                        seller_info['type'] = 'owner'
                break
    
    # Ищем продавца в конце описания (перед завершенными объявлениями)
    end_seller_patterns = [
        r'([^.\n]+?)\s*\n\s*(\d+)\s*завершённых объявлений',
        r'([^.\n]+?)\s*\n\s*(\d+)\s*завершенных объявлений',
        r'([^.\n]+?)\s*(\d+)\s*завершённых объявлений',
        r'([^.\n]+?)\s*(\d+)\s*завершенных объявлений'
    ]
    
    for pattern in end_seller_patterns:
        match = re.search(pattern, card_text, re.IGNORECASE | re.DOTALL)
        if match:
            seller_text = match.group(1).strip()
            if seller_text and len(seller_text) > 2:
                # Очищаем от рекламных фраз и оставляем только название компании
                cleaned_seller = clean_seller_name(seller_text)
                if cleaned_seller:
                    seller_info['from_end_pattern'] = cleaned_seller
                    # Определяем тип по ключевым словам
                    if any(word in cleaned_seller.lower() for word in ['агентство', 'агент', 'риэлтор', 'real estate', 'недвижимость']):
                        seller_info['type'] = 'agency'
                    elif any(word in cleaned_seller.lower() for word in ['собственник', 'владелец', 'хозяин']):
                        seller_info['type'] = 'owner'
                break
    
    # Ищем в бейджах и специальных элементах
    badge_elements = card.find_all(class_=re.compile(r'badge|label|tag|seller|owner'))
    for elem in badge_elements:
        text = elem.get_text(strip=True)
        if text and len(text) > 2:
            if 'собственник' in text.lower():
                seller_info['type'] = 'owner'
                seller_info['from_badge'] = text
            elif 'агентство' in text.lower() or 'агент' in text.lower():
                seller_info['type'] = 'agency'
                seller_info['from_badge'] = text
    
    # 8. Поиск времени до метро в разных форматах
    metro_info = {}
    
    # Ищем текст с минутами до метро
    metro_text = card.get_text()
    metro_patterns = [
        r'(\d+)\s*мин\s*до\s*метро',
        r'до\s*метро\s*(\d+)\s*мин',
        r'(\d+)\s*минут\s*до\s*метро',
        r'метро\s*(\d+)\s*мин',
        r'(\d+)\s*–\s*(\d+)\s*мин',  # формат "6–10 мин"
        r'(\d+)\s*-\s*(\d+)\s*мин'   # формат "6-10 мин"
    ]
    
    for pattern in metro_patterns:
        match = re.search(pattern, metro_text, re.IGNORECASE)
        if match:
            if len(match.groups()) == 2:  # диапазон минут
                metro_info['walk_minutes_min'] = int(match.group(1))
                metro_info['walk_minutes_max'] = int(match.group(2))
                metro_info['walk_minutes'] = f"{match.group(1)}-{match.group(2)}"
            else:
                metro_info['walk_minutes'] = int(match.group(1))
            break
    
    # Ищем в data-атрибутах
    for attr, value in data_attrs.items():
        if 'metro' in attr.lower() or 'walk' in attr.lower():
            metro_info[f'data_{attr}'] = value
    
    results['metro_info'] = metro_info
    
    # 9. Поиск дополнительной информации в тексте карточки
    text_content = card.get_text()
    results['full_text_sample'] = text_content[:500] + "..." if len(text_content) > 500 else text_content
    
    # 10. Поиск количества фото
    photo_info = {}
    photo_patterns = [
        r'(\d+)\s*фото',
        r'(\d+)\s*фотографий',
        r'(\d+)\s*фоток'
    ]
    
    for pattern in photo_patterns:
        match = re.search(pattern, text_content, re.IGNORECASE)
        if match:
            photo_info['count'] = int(match.group(1))
            break
    
    results['photo_info'] = photo_info
    
    # 11. Поиск цены за квадратный метр
    price_per_m2_info = {}
    price_m2_patterns = [
        r'(\d+(?:\s*\d+)*)\s*₽\s*за\s*м²',
        r'(\d+(?:\s*\d+)*)\s*руб\s*за\s*м²',
        r'(\d+(?:\s*\d+)*)\s*рублей\s*за\s*м²'
    ]
    
    for pattern in price_m2_patterns:
        match = re.search(pattern, text_content, re.IGNORECASE)
        if match:
            price_str = match.group(1).replace(' ', '')
            try:
                price_per_m2_info['price_per_m2'] = int(price_str)
            except:
                price_per_m2_info['price_per_m2_raw'] = price_str
            break
    
    results['price_per_m2_info'] = price_per_m2_info
    
    # 12. Поиск статуса проверки документов
    verification_info = {}
    verification_patterns = [
        r'документы проверены',
        r'реквизиты проверены',
        r'проверено в росреестре',
        r'верифицирован'
    ]
    
    for pattern in verification_patterns:
        if re.search(pattern, text_content, re.IGNORECASE):
            verification_info['verified'] = True
            verification_info['verification_text'] = pattern
            # Не определяем тип продавца здесь - это делается в labels
            break
    
    results['verification_info'] = verification_info
    
    # 13. Поиск времени размещения объявления
    time_info = {}
    time_patterns = [
        r'(\d+)\s*(?:час|часа|часов)\s*назад',
        r'(\d+)\s*(?:день|дня|дней)\s*назад',
        r'(\d+)\s*(?:неделя|недели|недель)\s*назад',
        r'(\d+)\s*(?:месяц|месяца|месяцев)\s*назад',
        # Полные даты
        r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{1,2}):(\d{2})',
        r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{1,2})',
        r'(\d{1,2})\.(\d{1,2})\.(\d{4})',
        r'(\d{1,2})\.(\d{1,2})\.(\d{2})'
    ]
    
    for pattern in time_patterns:
        match = re.search(pattern, text_content, re.IGNORECASE)
        if match:
            if 'назад' in pattern:
                time_info['posted_ago'] = match.group(0)
                time_info['posted_value'] = int(match.group(1))
            elif 'января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря' in pattern:
                if len(match.groups()) == 4:  # день месяц час:минута
                    time_info['full_date'] = f"{match.group(1)} {match.group(2)} {match.group(3)}:{match.group(4)}"
                elif len(match.groups()) == 3:  # день месяц час
                    time_info['full_date'] = f"{match.group(1)} {match.group(2)} {match.group(3)}"
            elif '\.' in pattern:  # формат DD.MM.YYYY
                if len(match.groups()) == 3:
                    time_info['date_format'] = f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
            break
    
    results['time_info'] = time_info
    
    results['labels'] = labels
    results['seller_from_labels'] = seller_info
    
    return results


async def main():
    await create_ads_avito_table()

    url = build_page_url(1)
    sess = requests.Session()

    print(f"Тестируем страницу: {url}")
    proxy_status = "С ПРОКСИ" if USE_PROXY else "БЕЗ ПРОКСИ"
    print(f"[SAFETY] Используем защиту от бана: {proxy_status}, случайные задержки, ротация User-Agent")
    
    # Используем безопасную загрузку
    resp = safe_get(sess, url)
    print(f"HTTP: {resp.status_code}")
    if resp.status_code != 200:
        print(resp.text[:400])
        return

    # Извлекаем JSON данные из всей страницы
    print("\n" + "="*80)
    print("АНАЛИЗ JSON ДАННЫХ СТРАНИЦЫ")
    print("="*80)
    json_data = extract_json_data(resp.text)
    for key, data in json_data.items():
        print(f"\n{key}:")
        if isinstance(data, dict):
            print(json.dumps(data, indent=2, ensure_ascii=False)[:500] + "...")
        else:
            print(str(data)[:200] + "...")

    soup = BeautifulSoup(resp.text, 'html.parser')
    cards = soup.select('[data-marker="item"]') or soup.select('div.iva-item-content-\w+')
    cards = cards[:1]  # ограничиваем до 1 карточки для безопасности
    print(f"\nНайдено карточек: {len(cards)}")

    if not cards:
        print("Карточки не найдены!")
        return

    # Детальный анализ одной карточки
    print("\n" + "="*80)
    print("ДЕТАЛЬНЫЙ АНАЛИЗ КАРТОЧКИ")
    print("="*80)
    
    card = cards[0]
    detailed_results = parse_card_detailed(card)
    
    for method, result in detailed_results.items():
        print(f"\n--- {method.upper()} ---")
        if isinstance(result, dict):
            for key, value in result.items():
                if isinstance(value, (dict, list)):
                    print(f"{key}: {json.dumps(value, indent=2, ensure_ascii=False)[:300]}...")
                else:
                    print(f"{key}: {value}")
        else:
            print(result)

    # Сохраняем в БД если есть данные
    if 'html_parsing' in detailed_results and detailed_results['html_parsing']:
        data = detailed_results['html_parsing']
        if data.get('URL') and data.get('offer_id'):
            print(f"\nСохраняем в БД...")
            await save_avito_ad(data)
            print("Сохранено!")
    
    print(f"\n{'='*80}")
    print("АНАЛИЗ ЗАВЕРШЕН")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    asyncio.run(main())
