#!/usr/bin/env python3
"""
Тестовый скрипт для проверки формирования URL поиска
"""

from parse_cian_to_db import (
    parse_params_string, 
    convert_time_period, 
    build_search_url
)

def test_url_formation():
    """Тестирует формирование URL для разных параметров"""
    print("🧪 Тестирование формирования URL поиска")
    print("=" * 60)
    
    test_cases = [
        "1d",           # вторичка за день
        "2w",           # новостройки за неделю
        "1h",           # вторичка за час
        "2none",        # новостройки без ограничений
        "1",            # только тип (вторичка)
        "d",            # только период (день)
    ]
    
    for params in test_cases:
        print(f"\n📋 Параметры: {params}")
        
        # Парсим параметры
        property_type, time_period = parse_params_string(params)
        print(f"   Тип недвижимости: {property_type} ({'вторичка' if property_type == 1 else 'новостройки'})")
        print(f"   Период времени: {time_period}")
        
        # Конвертируем период
        time_period_seconds = convert_time_period(time_period)
        print(f"   Конвертированный период: {time_period_seconds}")
        
        # Формируем URL
        url = build_search_url(property_type, time_period_seconds)
        print(f"   URL: {url}")
        
        # Проверяем наличие параметра totime
        if time_period_seconds is not None:
            if f"&totime={time_period_seconds}" in url:
                print(f"   ✅ Параметр totime={time_period_seconds} найден в URL")
            else:
                print(f"   ❌ Параметр totime={time_period_seconds} НЕ найден в URL")
        else:
            if "&totime=" not in url:
                print(f"   ✅ Параметр totime отсутствует (фильтр по времени отключен)")
            else:
                print(f"   ❌ Параметр totime присутствует, хотя должен отсутствовать")
        
        print("-" * 60)

def test_specific_case_1d():
    """Тестирует конкретный случай с параметром 1d"""
    print("\n🎯 Детальный тест для параметра '1d'")
    print("=" * 60)
    
    # Парсим параметры
    property_type, time_period = parse_params_string("1d")
    print(f"Парсинг '1d':")
    print(f"   property_type = {property_type}")
    print(f"   time_period = {time_period}")
    
    # Конвертируем период
    time_period_seconds = convert_time_period(time_period)
    print(f"Конвертация '{time_period}' -> {time_period_seconds}")
    
    # Формируем URL
    url = build_search_url(property_type, time_period_seconds)
    print(f"Сформированный URL:")
    print(f"   {url}")
    
    # Проверяем наличие totime=-2
    if "&totime=-2" in url:
        print("✅ URL содержит &totime=-2 (фильтр 'за день')")
    else:
        print("❌ URL НЕ содержит &totime=-2")
    
    # Разбираем URL на части
    print(f"\nРазбор URL:")
    parts = url.split("&")
    for part in parts:
        if part.startswith("totime="):
            print(f"   {part} ← это параметр времени")
        elif part.startswith("object_type"):
            print(f"   {part} ← это тип недвижимости")
        else:
            print(f"   {part}")

if __name__ == '__main__':
    test_url_formation()
    test_specific_case_1d()
    print("\n✅ Тестирование завершено!")
