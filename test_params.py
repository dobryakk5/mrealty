#!/usr/bin/env python3
"""
Тестовый скрипт для проверки функции parse_params_string
"""

def parse_params_string(params_str: str) -> tuple[int, str]:
    """
    Парсит строку параметров в формате [тип][период]
    
    Args:
        params_str: Строка параметров (например: "2w", "1d", "2h", "1none")
    
    Returns:
        tuple: (тип_недвижимости, период_времени)
        
    Периоды времени:
        w - неделя
        d - день  
        h - час
        none - без ограничений по времени
    """
    # Настройки для теста
    PROPERTY_TYPE = 1
    TIME_PERIOD = None  # Без ограничений по времени
    
    if not params_str:
        # Используем значения из настроек
        default_time_period = 'none' if TIME_PERIOD is None else 'w'
        return PROPERTY_TYPE, default_time_period
    
    # Первый символ - тип недвижимости
    if params_str[0] in ['1', '2']:
        property_type = int(params_str[0])
        # Остальные символы - период времени
        time_period = params_str[1:] if len(params_str) > 1 else 'w'
    else:
        # Если первый символ не цифра, используем значение по умолчанию
        property_type = 1
        time_period = params_str
    
    # Проверяем корректность периода времени
    if time_period not in ['w', 'd', 'h', 'none']:
        time_period = 'w'  # по умолчанию неделя
    
    return property_type, time_period

def convert_time_period(time_period: str) -> int:
    """Конвертирует символьный период времени в секунды или None для отключения фильтра"""
    if time_period == 'none':
        return None  # отключаем фильтр по времени
    
    time_mapping = {
        'h': 3600,      # час
        'd': 86400,     # день
        'w': 604800     # неделя
    }
    return time_mapping.get(time_period, 604800)  # по умолчанию неделя

def test_params():
    """Тестирует различные варианты параметров"""
    print("=== ТЕСТИРОВАНИЕ ФУНКЦИИ parse_params_string ===\n")
    
    test_cases = [
        "",           # пустая строка
        "1",          # только тип
        "2",          # только тип
        "1w",         # тип + неделя
        "2d",         # тип + день
        "1h",         # тип + час
        "1none",      # тип + без ограничений
        "2none",      # тип + без ограничений
        "w",          # только период
        "d",          # только период
        "h",          # только период
        "none",       # только период без ограничений
        "invalid",    # неверный период
    ]
    
    for params in test_cases:
        property_type, time_period = parse_params_string(params)
        time_period_seconds = convert_time_period(time_period)
        
        print(f"Вход: '{params}' -> Тип: {property_type}, Период: '{time_period}', Секунды: {time_period_seconds}")
        
        # Проверяем корректность
        if property_type not in [1, 2]:
            print("   ❌ ОШИБКА: неверный тип недвижимости")
        if time_period not in ['w', 'd', 'h', 'none']:
            print("   ❌ ОШИБКА: неверный период времени")
        if time_period == 'none' and time_period_seconds is not None:
            print("   ❌ ОШИБКА: 'none' должен возвращать None")
        if time_period != 'none' and time_period_seconds is None:
            print("   ❌ ОШИБКА: валидный период должен возвращать число секунд")
        print()

if __name__ == "__main__":
    test_params()
