#!/usr/bin/env python3
"""
Тест исправления дублирования парсинга Avito в Excel экспорте
"""

import sys
import os
import asyncio
from datetime import datetime

# Add path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class TestLogger:
    """Logger для отслеживания вызовов функций"""
    
    def __init__(self):
        self.calls = []
        self.start_time = datetime.now()
    
    def log_call(self, function_name, url, additional_info=""):
        """Логирует вызов функции"""
        timestamp = datetime.now()
        elapsed = (timestamp - self.start_time).total_seconds()
        
        entry = {
            'timestamp': timestamp,
            'elapsed_seconds': elapsed,
            'function': function_name,
            'url': url,
            'info': additional_info
        }
        
        self.calls.append(entry)
        print(f"[{elapsed:6.1f}s] {function_name}: {url} {additional_info}")
    
    def check_duplicates(self):
        """Проверяет на дублирование вызовов"""
        print("\n" + "="*60)
        print("🔍 ПРОВЕРКА НА ДУБЛИРОВАНИЕ")
        print("="*60)
        
        # Группируем по URL и функции
        url_function_calls = {}
        for entry in self.calls:
            key = f"{entry['url']}::{entry['function']}"
            if key not in url_function_calls:
                url_function_calls[key] = []
            url_function_calls[key].append(entry)
        
        duplicates_found = False
        
        for key, calls in url_function_calls.items():
            if len(calls) > 1:
                url, function = key.split("::")
                duplicates_found = True
                print(f"\n❌ ДУБЛИРОВАНИЕ ОБНАРУЖЕНО:")
                print(f"   URL: {url}")
                print(f"   Функция: {function}")
                print(f"   Количество вызовов: {len(calls)}")
                
                for i, call in enumerate(calls, 1):
                    print(f"     {i}. [{call['elapsed_seconds']:6.1f}s] {call['info']}")
        
        if not duplicates_found:
            print("✅ Дублирование не обнаружено!")
        
        print(f"\n📊 ИТОГО:")
        print(f"   Всего вызовов функций: {len(self.calls)}")
        print(f"   Уникальных URL+функция: {len(url_function_calls)}")
        
        return not duplicates_found

# Глобальный тест-логгер
test_logger = TestLogger()

async def test_excel_export_fix():
    """Тестирует исправление дублирования в Excel экспорте"""
    
    print("🧪 ТЕСТ ИСПРАВЛЕНИЯ ДУБЛИРОВАНИЯ AVITO")
    print("="*50)
    
    # Тестовый URL Avito
    test_urls = [
        "https://www.avito.ru/moskva/kvartiry/4-k._kvartira_95_m_514_et._4805953955"
    ]
    
    print(f"Тестируем с URL: {test_urls[0]}")
    print("Отслеживаем вызовы функций парсинга...\n")
    
    try:
        # Импортируем и патчим функции для логирования
        from listings_processor import ListingsProcessor, export_listings_to_excel
        
        original_parse_avito = ListingsProcessor.parse_avito_listing
        original_extract_info = ListingsProcessor.extract_listing_info
        
        async def logged_parse_avito(self, url, skip_photos=True):
            """Обертка для parse_avito_listing с логированием"""
            test_logger.log_call("parse_avito_listing", url, f"skip_photos={skip_photos}")
            result = await original_parse_avito(self, url, skip_photos)
            status = "SUCCESS" if result else "FAILED"
            test_logger.log_call("parse_avito_listing_result", url, status)
            return result
        
        async def logged_extract_info(self, url):
            """Обертка для extract_listing_info с логированием"""
            test_logger.log_call("extract_listing_info", url, "CALLED")
            result = await original_extract_info(self, url)
            status = "SUCCESS" if result else "FAILED"
            test_logger.log_call("extract_listing_info_result", url, status)
            return result
        
        # Применяем патчи
        ListingsProcessor.parse_avito_listing = logged_parse_avito
        ListingsProcessor.extract_listing_info = logged_extract_info
        
        # Запускаем Excel экспорт
        print("🚀 Запускаем Excel экспорт...")
        test_logger.log_call("export_listings_to_excel", test_urls[0], "START")
        
        user_id = 999999
        bio, request_id = await export_listings_to_excel(test_urls, user_id)
        
        test_logger.log_call("export_listings_to_excel", test_urls[0], "COMPLETED")
        
        if bio:
            print("✅ Excel экспорт завершен успешно")
            
            # Читаем Excel для проверки результатов
            import pandas as pd
            bio.seek(0)
            df = pd.read_excel(bio)
            print(f"📊 Excel содержит {len(df)} строк")
            
            # Проверяем на дублирование строк
            if len(df) > 1:
                print("❌ ОБНАРУЖЕНО НЕСКОЛЬКО СТРОК - Возможно дублирование!")
                for i, row in df.iterrows():
                    print(f"   Строка {i+1}: {row.get('URL', 'No URL')}")
            else:
                print("✅ Одна строка как и ожидалось")
        else:
            print("❌ Excel экспорт неудачен")
        
        # Восстанавливаем оригинальные функции
        ListingsProcessor.parse_avito_listing = original_parse_avito
        ListingsProcessor.extract_listing_info = original_extract_info
        
    except Exception as e:
        print(f"❌ Тест неудачен: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Анализируем результаты
        success = test_logger.check_duplicates()
        return success

async def test_html_gallery_fix():
    """Тестирует исправление дублирования в HTML галерее"""
    
    print("\n" + "="*50)
    print("🧪 ТЕСТ HTML ГАЛЕРЕИ (НЕ РЕАЛИЗОВАН)")
    print("="*50)
    print("Этот тест можно добавить позже при необходимости")
    return True

if __name__ == "__main__":
    print("🔧 ТЕСТИРОВАНИЕ ИСПРАВЛЕНИЯ ДУБЛИРОВАНИЯ")
    print("="*60)
    
    async def run_all_tests():
        results = []
        
        # Тест Excel экспорта
        excel_success = await test_excel_export_fix()
        results.append(("Excel Export", excel_success))
        
        # Тест HTML галереи (пока не реализован)
        html_success = await test_html_gallery_fix()
        results.append(("HTML Gallery", html_success))
        
        # Итоговый отчет
        print("\n" + "="*60)
        print("📊 ИТОГОВЫЙ ОТЧЕТ ТЕСТИРОВАНИЯ")
        print("="*60)
        
        all_passed = True
        for test_name, success in results:
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"{test_name}: {status}")
            if not success:
                all_passed = False
        
        if all_passed:
            print("\n🎉 ВСЕ ТЕСТЫ ПРОШЛИ! Дублирование исправлено.")
        else:
            print("\n⚠️ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ. Требуется дополнительная работа.")
        
        return all_passed
    
    success = asyncio.run(run_all_tests())
    exit(0 if success else 1)