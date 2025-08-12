#!/usr/bin/env python3
"""
Тестовый скрипт для проверки функции build_search_url
"""

def build_search_url(property_type: int, time_period: int = None, metro_id: int = None, foot_min: int = None) -> str:
    """
    Строит URL для поиска на CIAN
    
    Args:
        property_type: Тип недвижимости (1=вторичка, 2=новостройки)
        time_period: Период времени в секундах или None для отключения фильтра
        metro_id: ID станции метро или None
        foot_min: Время до метро в минутах или None
    """
    base_url = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=1&only_foot=2"
    url = f"{base_url}&object_type%5B0%5D={property_type}"
    
    # Добавляем фильтр по времени только если time_period указан
    if time_period is not None:
        url += f"&totime={time_period}"
    
    only_foot=2
    if property_type == 2:
        url += "&with_newobject=1"
    
    # Добавляем фильтр по метро
    if metro_id is not None:
        url += f"&metro%5B0%5D={metro_id}"
    
    # Добавляем фильтр по времени в пути до метро
    if foot_min is not None:
        url += f"&foot_min={foot_min}"
    
    return url

def test_urls():
    """Тестирует различные варианты URL"""
    print("=== ТЕСТИРОВАНИЕ ФУНКЦИИ build_search_url ===\n")
    
    # Тест 1: Вторичка за неделю (с фильтром по времени)
    url1 = build_search_url(1, 604800)
    print(f"1. Вторичка за неделю:")
    print(f"   URL: {url1}")
    print(f"   Содержит totime: {'totime=604800' in url1}")
    print()
    
    # Тест 2: Новостройки за день (с фильтром по времени)
    url2 = build_search_url(2, 86400)
    print(f"2. Новостройки за день:")
    print(f"   URL: {url2}")
    print(f"   Содержит totime: {'totime=86400' in url2}")
    print()
    
    # Тест 3: Вторичка без ограничений по времени
    url3 = build_search_url(1, None)
    print(f"3. Вторичка без ограничений по времени:")
    print(f"   URL: {url3}")
    print(f"   Содержит totime: {'totime=' in url3}")
    print()
    
    # Тест 4: Новостройки без ограничений по времени + метро
    url4 = build_search_url(2, None, 68)
    print(f"4. Новостройки без ограничений + метро ID 68:")
    print(f"   URL: {url4}")
    print(f"   Содержит totime: {'totime=' in url4}")
    print(f"   Содержит metro: {'metro%5B0%5D=68' in url4}")
    print()
    
    # Тест 5: Вторичка без ограничений по времени + время до метро
    url5 = build_search_url(1, None, None, 20)
    print(f"5. Вторичка без ограничений + время до метро 20 мин:")
    print(f"   URL: {url5}")
    print(f"   Содержит totime: {'totime=' in url5}")
    print(f"   Содержит foot_min: {'foot_min=20' in url5}")
    print()
    
    # Тест 6: Полный набор параметров без времени
    url6 = build_search_url(2, None, 68, 20)
    print(f"6. Новостройки без ограничений + метро ID 68 + время до метро 20 мин:")
    print(f"   URL: {url6}")
    print(f"   Содержит totime: {'totime=' in url6}")
    print(f"   Содержит metro: {'metro%5B0%5D=68' in url6}")
    print(f"   Содержит foot_min: {'foot_min=20' in url6}")
    print()

if __name__ == "__main__":
    test_urls()
