#!/usr/bin/env python3
"""
Тест парсинга CIAN карточек для проверки извлечения характеристик
"""

from parse_cian_to_db import parse_offer_card
from bs4 import BeautifulSoup

def test_cian_parsing():
    # HTML для карточки 317810086 (из примера пользователя)
    html = '''
    <div>
        <a href="/sale/flat/317810086/">Link</a>
        <div data-mark="OfferTitle">5-комн. квартира, 201 м², 3/34 этаж</div>
        <div data-mark="OfferSubtitle">Сдача корпуса 2 кв. 2027</div>
    </div>
    '''
    
    soup = BeautifulSoup(html, 'html.parser')
    card = soup.find('div')
    
    print("Тестируем парсинг CIAN карточки...")
    print("HTML:", html.strip())
    print()
    
    result = parse_offer_card(card)
    
    print("Результат парсинга:")
    print(f"URL: {result.get('URL')}")
    print(f"Title: {result.get('title')}")
    print(f"Subtitle: {result.get('subtitle')}")
    print(f"Rooms: {result.get('rooms')}")
    print(f"Area: {result.get('area_m2')}")
    print(f"Floor: {result.get('floor')}")
    print(f"Total floors: {result.get('floor_total')}")
    print()
    
    # Проверяем, что характеристики извлечены правильно
    expected = {
        'rooms': 5,
        'area_m2': 201.0,
        'floor': 3,
        'floor_total': 34
    }
    
    print("Проверка результатов:")
    for field, expected_value in expected.items():
        actual_value = result.get(field)
        status = "✓" if actual_value == expected_value else "✗"
        print(f"{status} {field}: ожидалось {expected_value}, получено {actual_value}")
    
    return result

if __name__ == "__main__":
    test_cian_parsing()
