#!/usr/bin/env python3
"""
Перепарсинг конкретного CIAN объявления для обновления характеристик
"""

import asyncio
import requests
from bs4 import BeautifulSoup
from parse_cian_to_db import parse_offer_card
from parse_todb import save_cian_ad, search_cian_ads

async def reparse_record(offer_id: str):
    """Перепарсит конкретное объявление и обновит БД"""
    print(f"Перепарсинг объявления {offer_id}...")
    
    # URL объявления
    url = f"https://www.cian.ru/sale/flat/{offer_id}/"
    
    try:
        # Получаем страницу
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        print(f"Загружаем страницу: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Парсим HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Ищем основную карточку объявления
        card = soup.find('div', {'data-name': 'ObjectSummary'}) or soup.find('div', {'class': 'object-item'})
        
        if not card:
            # Если не нашли специальную карточку, используем весь body
            card = soup.find('body')
        
        if not card:
            print("Не удалось найти карточку объявления")
            return False
        
        # Парсим карточку
        print("Парсим карточку...")
        parsed_data = parse_offer_card(card)
        
        # Добавляем недостающие поля
        parsed_data['URL'] = url
        parsed_data['offer_id'] = offer_id
        parsed_data['object_type_id'] = 2  # вторичка
        parsed_data['source'] = 'cian_secondary'
        
        print("Результат парсинга:")
        print(f"  rooms: {parsed_data.get('rooms')}")
        print(f"  area: {parsed_data.get('area_m2')}")
        print(f"  floor: {parsed_data.get('floor')}")
        print(f"  total_floors: {parsed_data.get('floor_total')}")
        print(f"  price: {parsed_data.get('price')}")
        
        # Сохраняем в БД
        print("Сохраняем в БД...")
        success = await save_cian_ad(parsed_data)
        
        if success:
            print("✓ Объявление успешно обновлено в БД")
        else:
            print("⚠ Объявление уже существует (ON CONFLICT DO NOTHING)")
            
        return success
        
    except Exception as e:
        print(f"Ошибка при перепарсинге: {e}")
        return False

async def main():
    """Основная функция"""
    offer_id = "317810086"
    
    print(f"Перепарсинг CIAN объявления {offer_id}")
    print("="*50)
    
    # Проверяем текущее состояние
    print("Текущее состояние в БД:")
    search_results = await search_cian_ads(filters={'limit': 1000})
    target_record = None
    for record in search_results:
        if offer_id in record.get('url', ''):
            target_record = record
            break
    
    if target_record:
        print(f"  rooms: {target_record['rooms']}")
        print(f"  area: {target_record['area']}")
        print(f"  floor: {target_record['floor']}")
        print(f"  total_floors: {target_record['total_floors']}")
    else:
        print("  Запись не найдена")
    
    print("\n" + "="*50)
    
    # Перепарсим
    success = await reparse_record(offer_id)
    
    if success:
        print("\nПроверяем результат:")
        search_results = await search_cian_ads(filters={'limit': 1000})
        target_record = None
        for record in search_results:
            if offer_id in record.get('url', ''):
                target_record = record
                break
        
        if target_record:
            print(f"  rooms: {target_record['rooms']}")
            print(f"  area: {target_record['area']}")
            print(f"  floor: {target_record['floor']}")
            print(f"  total_floors: {target_record['total_floors']}")
        else:
            print("  Запись не найдена")

if __name__ == "__main__":
    asyncio.run(main())
