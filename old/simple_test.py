#!/usr/bin/env python3
"""
Простой тест импортов и основной логики
"""

import sys
import os

# Добавляем текущую директорию в path для импорта модулей
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Тестирует импорты модулей"""
    print("🔄 Тестируем импорты...")

    try:
        from realty_parser_server import RealtyParserAPI
        print("✅ realty_parser_server импортирован")

        # Создаем экземпляр
        parser = RealtyParserAPI()
        print("✅ RealtyParserAPI создан")

        # Проверяем методы
        test_url = "https://www.avito.ru/moskva/kvartiry/1-k._kvartira_295_m_25_et._7627589190"

        is_avito = parser.is_avito_url(test_url)
        print(f"✅ Проверка Avito URL: {is_avito}")

        source = parser.get_url_source(test_url)
        print(f"✅ Источник URL: {source}")

        print("✅ Все базовые тесты прошли!")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Запуск простого теста")
    print("=" * 50)
    test_imports()
    print("=" * 50)
    print("✅ Тест завершен")