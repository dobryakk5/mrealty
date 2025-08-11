#!/usr/bin/env python3
"""
Отладка HTML структуры страницы CIAN объявления
"""

import requests
from bs4 import BeautifulSoup

def debug_cian_page(offer_id: str):
    """Анализирует HTML структуру страницы CIAN объявления"""
    print(f"Анализ HTML структуры для объявления {offer_id}...")
    
    url = f"https://www.cian.ru/sale/flat/{offer_id}/"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        print(f"Загружаем страницу: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print("\n" + "="*60)
        print("ПОИСК ЭЛЕМЕНТОВ TITLE И SUBTITLE:")
        print("="*60)
        
        # Ищем элементы с data-mark="OfferTitle"
        title_elements = soup.find_all(attrs={'data-mark': 'OfferTitle'})
        print(f"Элементы с data-mark='OfferTitle': {len(title_elements)}")
        for i, el in enumerate(title_elements):
            print(f"  {i+1}. {el.get_text()[:100]}...")
        
        # Ищем элементы с data-mark="OfferSubtitle"
        subtitle_elements = soup.find_all(attrs={'data-mark': 'OfferSubtitle'})
        print(f"Элементы с data-mark='OfferSubtitle': {len(subtitle_elements)}")
        for i, el in enumerate(subtitle_elements):
            print(f"  {i+1}. {el.get_text()[:100]}...")
        
        print("\n" + "="*60)
        print("ПОИСК АЛЬТЕРНАТИВНЫХ СЕЛЕКТОРОВ:")
        print("="*60)
        
        # Ищем элементы с классом, содержащим "title"
        title_class_elements = soup.find_all(class_=lambda x: x and 'title' in x.lower())
        print(f"Элементы с классом, содержащим 'title': {len(title_class_elements)}")
        for i, el in enumerate(title_class_elements[:5]):  # Показываем первые 5
            print(f"  {i+1}. Класс: {el.get('class')}")
            print(f"     Текст: {el.get_text()[:100]}...")
        
        # Ищем элементы с классом, содержащим "subtitle"
        subtitle_class_elements = soup.find_all(class_=lambda x: x and 'subtitle' in x.lower())
        print(f"Элементы с классом, содержащим 'subtitle': {len(subtitle_class_elements)}")
        for i, el in enumerate(subtitle_class_elements[:5]):  # Показываем первые 5
            print(f"  {i+1}. Класс: {el.get('class')}")
            print(f"     Текст: {el.get_text()[:100]}...")
        
        print("\n" + "="*60)
        print("ПОИСК ТЕКСТА С ХАРАКТЕРИСТИКАМИ:")
        print("="*60)
        
        # Ищем текст с характеристиками по всему HTML
        full_text = soup.get_text()
        
        # Ищем комнаты
        import re
        rooms_match = re.search(r'(\d+)[^\d\-–—]*-?комн', full_text, re.IGNORECASE)
        if rooms_match:
            print(f"Найдены комнаты: {rooms_match.group(1)}")
        else:
            print("Комнаты не найдены")
        
        # Ищем площадь
        area_match = re.search(r'(\d+[.,]?\d*)\s*(?:м²|м2|м)', full_text)
        if area_match:
            print(f"Найдена площадь: {area_match.group(1)}")
        else:
            print("Площадь не найдена")
        
        # Ищем этаж
        floor_match = re.search(r'(\d+)\s*/\s*(\d+)', full_text)
        if floor_match:
            print(f"Найден этаж: {floor_match.group(1)}/{floor_match.group(2)}")
        else:
            print("Этаж не найден")
        
        print("\n" + "="*60)
        print("СОХРАНЕНИЕ HTML ДЛЯ АНАЛИЗА:")
        print("="*60)
        
        # Сохраняем HTML в файл для анализа
        html_file = f"cian_page_{offer_id}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"HTML сохранен в файл: {html_file}")
        
        return True
        
    except Exception as e:
        print(f"Ошибка при анализе страницы: {e}")
        return False

if __name__ == "__main__":
    debug_cian_page("317810086")
