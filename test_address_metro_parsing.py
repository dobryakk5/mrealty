#!/usr/bin/env python3
"""
Тест для проверки улучшенного парсинга адреса и метро
без жестко заданных списков станций метро
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from parse_avito_1metro import EnhancedMetroParser

def test_address_metro_parsing():
    """Тестирует парсинг адреса и метро с различными форматами"""
    
    parser = EnhancedMetroParser()
    
    # Тестовые примеры адресов и метро
    test_cases = [
        {
            'name': 'Обычный формат с "мин"',
            'address_text': 'Поварская ул., 8/1к1\nАрбатская, до 5 мин.',
            'expected': {
                'street_house': 'Поварская ул., 8/1к1',
                'metro_name': 'Арбатская',
                'time_to_metro': '5'
            }
        },
        {
            'name': 'Время без "мин" суффикса',
            'address_text': 'Тверская ул., 12\nПушкинская, 3',
            'expected': {
                'street_house': 'Тверская ул., 12',
                'metro_name': 'Pushkinskaya',  # Будет очищено от цифр
                'time_to_metro': '3'
            }
        },
        {
            'name': 'Время с "минут"',
            'address_text': 'Ленинский проспект, 45\nОктябрьская, 7 минут',
            'expected': {
                'street_house': 'Ленинский проспект, 45',
                'metro_name': 'Октябрьская',
                'time_to_metro': '7'
            }
        },
        {
            'name': 'Только одна строка',
            'address_text': 'Садовое кольцо, 15',
            'expected': {
                'street_house': 'Садовое кольцо, 15',
                'metro_name': 'не указано',
                'time_to_metro': 'не указано'
            }
        },
        {
            'name': 'Время с "мин." (с точкой)',
            'address_text': 'Новый Арбат, 22\nСмоленская, 4 мин.',
            'expected': {
                'street_house': 'Новый Арбат, 22',
                'metro_name': 'Смоленская',
                'time_to_metro': '4'
            }
        }
    ]
    
    print("🧪 Тестирование улучшенного парсинга адреса и метро\n")
    print("=" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. {test_case['name']}")
        print(f"   Входные данные: {repr(test_case['address_text'])}")
        
        # Парсим адрес
        result = parser.parse_address(test_case['address_text'])
        
        print(f"   Результат:")
        print(f"     • Улица/дом: {result.get('street_house', 'не найдено')}")
        print(f"     • Метро: {result.get('metro_name', 'не найдено')}")
        print(f"     • Время: {result.get('time_to_metro', 'не найдено')} мин")
        
        # Проверяем базовую корректность
        success = True
        if 'street_house' not in result or not result['street_house']:
            print(f"     ❌ Ошибка: улица/дом не извлечены")
            success = False
        
        if test_case['address_text'].count('\n') > 0:  # Если есть две строки
            if 'metro_name' not in result or result['metro_name'] == 'не указано':
                print(f"     ⚠️ Предупреждение: метро не извлечено")
            if 'time_to_metro' not in result or result['time_to_metro'] == 'не указано':
                print(f"     ⚠️ Предупреждение: время до метро не извлечено")
        
        if success:
            print(f"     ✅ Базовая структура корректна")
        
        print(f"   ---")

def test_metro_line_detection():
    """Тестирует определение строк с метро без жестко заданных списков"""
    
    parser = EnhancedMetroParser()
    
    # Тестовые строки
    test_lines = [
        {'line': 'Поварская ул., 8/1к1', 'expected': True, 'reason': 'адресный формат с улицей'},
        {'line': 'Арбатская, до 5 мин.', 'expected': True, 'reason': 'название + время'},
        {'line': 'Пушкинская', 'expected': True, 'reason': 'потенциальное название станции'},
        {'line': '2-комнатная квартира', 'expected': False, 'reason': 'описание квартиры'},
        {'line': '85 м²', 'expected': False, 'reason': 'площадь'},
        {'line': '12/20 эт', 'expected': False, 'reason': 'этаж'},
        {'line': 'Метро 10 мин пешком', 'expected': True, 'reason': 'содержит метро + время'},
        {'line': 'ул. Тверская, 15', 'expected': True, 'reason': 'улица с домом'},
        {'line': 'Собственник', 'expected': False, 'reason': 'тип продавца'},
        {'line': 'Новостройка', 'expected': False, 'reason': 'тип недвижимости'},
        {'line': 'Соколиная Гора, 3', 'expected': True, 'reason': 'потенциальное название + время'},
    ]
    
    print("\n🧪 Тестирование определения строк с метро/адресом\n")
    print("=" * 60)
    
    for i, test in enumerate(test_lines, 1):
        result = parser.is_metro_line(test['line'])
        status = "✅" if result == test['expected'] else "❌"
        expected_str = "да" if test['expected'] else "нет"
        actual_str = "да" if result else "нет"
        
        print(f"{i:2d}. '{test['line']}'")
        print(f"    Ожидается: {expected_str} ({test['reason']})")
        print(f"    Результат: {actual_str} {status}")
        print()

if __name__ == "__main__":
    print("🏠 Тестирование парсинга адреса и метро (без жестко заданных станций)")
    print("📍 Логика основана на структурных индикаторах, а не на списках станций")
    print()
    
    test_address_metro_parsing()
    test_metro_line_detection()
    
    print("\n" + "=" * 60)
    print("✅ Тестирование завершено!")
    print()
    print("💡 Ключевые улучшения:")
    print("   • Время парсится в различных форматах: 'мин', 'минут', 'мин.', просто цифра")
    print("   • Названия станций не жестко задаются, а определяются структурно")
    print("   • Используются индикаторы: время, улицы, адресный формат, метро-слова")
    print("   • Отфильтровываются служебные слова и технические термины")