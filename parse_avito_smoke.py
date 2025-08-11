#!/usr/bin/env python3
"""
Smoke-тест: парсит 2 объявления с первой страницы AVITO (вторичка, Москва)
Печатает ключевые поля и сохраняет в БД ads_avito
"""

import asyncio
import requests
import random
from bs4 import BeautifulSoup

from parse_avito_to_db import build_page_url, parse_avito_card, PROXY, build_headers
from parse_todb_avito import create_ads_avito_table, save_avito_ad


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

    soup = BeautifulSoup(resp.text, 'html.parser')
    cards = soup.select('[data-marker="item"]') or soup.select('div.iva-item-content-\w+')
    print(f"Найдено карточек: {len(cards)}")

    taken = 0
    for card in cards:
        data = parse_avito_card(card)
        if not (data.get('URL') and data.get('offer_id')):
            continue
        taken += 1
        print("-" * 60)
        print(f"URL: {data.get('URL')}")
        print(f"ID: {data.get('offer_id')}")
        print(f"Цена: {data.get('price')}")
        print(f"Комнат: {data.get('rooms')}")
        print(f"Площадь: {data.get('area_m2')}")
        print(f"Этаж: {data.get('floor')}/{data.get('floor_total')}")
        print(f"Метро: {data.get('metro')} ({data.get('walk_minutes')} мин)")
        seller = data.get('seller', {})
        print(f"Продавец: {seller.get('type')} - {seller.get('name')}")
        print(f"Адрес: {data.get('address')}")

        await save_avito_ad(data)

        if taken >= 2:
            break

if __name__ == '__main__':
    asyncio.run(main())
