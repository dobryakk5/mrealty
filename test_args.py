#!/usr/bin/env python3
"""
Тестовый скрипт для проверки аргументов командной строки
"""

import sys
from parse_cian_to_db import parse_arguments, parse_params_string, convert_time_period

def test_arguments():
    """Тестирует парсинг аргументов командной строки"""
    print("🧪 Тестирование аргументов командной строки")
    print("=" * 50)
    
    # Тест 1: Без аргументов
    print("\n1️⃣ Тест без аргументов:")
    sys.argv = ['test_args.py']
    args = parse_arguments()
    print(f"   args.params: {args.params}")
    print(f"   args.proxy: {args.proxy}")
    print(f"   args.no_proxy: {args.no_proxy}")
    
    # Тест 2: С параметрами
    print("\n2️⃣ Тест с параметрами:")
    sys.argv = ['test_args.py', '2d']
    args = parse_arguments()
    print(f"   args.params: {args.params}")
    print(f"   args.proxy: {args.proxy}")
    print(f"   args.no_proxy: {args.no_proxy}")
    
    # Тест 3: С прокси
    print("\n3️⃣ Тест с прокси:")
    sys.argv = ['test_args.py', '1w', '--proxy']
    args = parse_arguments()
    print(f"   args.params: {args.params}")
    print(f"   args.proxy: {args.proxy}")
    print(f"   args.no_proxy: {args.no_proxy}")
    
    # Тест 4: Без прокси
    print("\n4️⃣ Тест без прокси:")
    sys.argv = ['test_args.py', '2none', '--no-proxy']
    args = parse_arguments()
    print(f"   args.params: {args.params}")
    print(f"   args.proxy: {args.proxy}")
    print(f"   args.no_proxy: {args.no_proxy}")

def test_param_parsing():
    """Тестирует парсинг строки параметров"""
    print("\n🔍 Тестирование парсинга параметров")
    print("=" * 50)
    
    test_cases = [
        None,           # без параметров
        "1d",           # вторичка за день
        "2w",           # новостройки за неделю
        "1h",           # вторичка за час
        "2none",        # новостройки без ограничений
        "1",            # только тип
        "d",            # только период
        "invalid",      # неверный формат
    ]
    
    for params in test_cases:
        print(f"\nПараметры: {params}")
        try:
            property_type, time_period = parse_params_string(params)
            time_period_seconds = convert_time_period(time_period)
            print(f"   Тип: {property_type} ({'вторичка' if property_type == 1 else 'новостройки'})")
            print(f"   Период: {time_period} -> {time_period_seconds}")
        except Exception as e:
            print(f"   Ошибка: {e}")

if __name__ == '__main__':
    test_arguments()
    test_param_parsing()
    print("\n✅ Тестирование завершено!")
