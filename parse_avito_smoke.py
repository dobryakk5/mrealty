#!/usr/bin/env python3
"""
Smoke-тест: парсит 1 объявление с первой страницы AVITO (вторичка, Москва)
Тестирует разные способы парсинга: HTML, JSON, и другие методы
"""

import asyncio
import requests
import random
import json
import re
from bs4 import BeautifulSoup

from parse_avito_to_db import build_page_url, parse_avito_card, PROXY, build_headers
from parse_todb_avito import create_ads_avito_table, save_avito_ad


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
    
    return results


async def main():
    await create_ads_avito_table()

    url = build_page_url(1)
    sess = requests.Session()
    proxies = {"http": PROXY, "https": PROXY} if PROXY else None

    print(f"Тестируем страницу: {url}")
    resp = sess.get(url, headers=build_headers(), timeout=25, proxies=proxies)
    resp.encoding = 'utf-8'
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
    cards = cards[:1]  # ограничиваем до 1 карточки для детального анализа
    print(f"\nНайдено карточек: {len(cards)}")

    if not cards:
        print("Карточки не найдены!")
        return

    # Детальный анализ первой карточки
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


if __name__ == '__main__':
    asyncio.run(main())
