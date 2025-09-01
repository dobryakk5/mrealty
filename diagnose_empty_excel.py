#!/usr/bin/env python3
"""
Диагностический скрипт для проблемы с пустым Excel файлом для Avito
"""

import sys
import os
import asyncio
from datetime import datetime

# Add path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def test_avito_excel_issue():
    """Тестирует проблему с пустым Excel для Avito"""
    
    print("🧪 ДИАГНОСТИКА ПУСТОГО EXCEL ДЛЯ AVITO")
    print("="*50)
    
    try:
        # Импортируем необходимые модули
        from listings_processor import ListingsProcessor, export_listings_to_excel, AVITO_AVAILABLE
        
        print(f"📋 Статус модуля Avito: {'✅ Доступен' if AVITO_AVAILABLE else '❌ Недоступен'}")
        
        if not AVITO_AVAILABLE:
            print("❌ ПРОБЛЕМА НАЙДЕНА: Модуль Avito недоступен!")
            print("   Это объясняет почему Excel пустой")
            return False
        
        # Тестовый URL Avito
        test_urls = [
            "https://www.avito.ru/moskva/kvartiry/4-k._kvartira_95_m_514_et._4805953955"
        ]
        
        print(f"🔗 Тестируем с URL: {test_urls[0]}")
        
        # Создаем процессор
        processor = ListingsProcessor()
        
        # Проверяем распознавание URL
        is_avito = processor.is_avito_url(test_urls[0])
        print(f"🔍 URL распознан как Avito: {'✅ Да' if is_avito else '❌ Нет'}")
        
        if not is_avito:
            print("❌ ПРОБЛЕМА НАЙДЕНА: URL не распознается как Avito!")
            return False
        
        # Тестируем парсинг одного объявления
        print("\n📋 Тестируем парсинг одного объявления...")
        try:
            listing_data = await processor.parse_avito_listing(test_urls[0], skip_photos=True)
            if listing_data:
                print("✅ Парсинг объявления успешен")
                print(f"📊 Полученные данные: {len(listing_data)} полей")
                
                # Показываем основные поля
                key_fields = ['url', 'title', 'price', 'rooms', 'total_area', 'floor']
                for field in key_fields:
                    value = listing_data.get(field, 'НЕ НАЙДЕНО')
                    print(f"   {field}: {value}")
                    
            else:
                print("❌ ПРОБЛЕМА НАЙДЕНА: Парсинг объявления вернул None!")
                return False
                
        except Exception as e:
            print(f"❌ ПРОБЛЕМА НАЙДЕНА: Ошибка парсинга объявления: {e}")
            return False
        
        # Тестируем Excel экспорт
        print("\n📊 Тестируем Excel экспорт...")
        try:
            user_id = 999999
            bio, request_id = await export_listings_to_excel(test_urls, user_id)
            
            if bio:
                print("✅ Excel экспорт завершен успешно")
                
                # Читаем Excel для проверки содержимого
                import pandas as pd
                bio.seek(0)
                df = pd.read_excel(bio)
                print(f"📊 Excel содержит {len(df)} строк")
                
                if len(df) == 0:
                    print("❌ ПРОБЛЕМА НАЙДЕНА: Excel файл пустой (0 строк)!")
                    print("   Это означает, что данные не попадают в Excel")
                    return False
                else:
                    print("✅ Excel содержит данные")
                    print("📋 Колонки в Excel:")
                    for col in df.columns:
                        print(f"   - {col}")
                    
                    if len(df) > 0:
                        print("📋 Первая строка данных:")
                        for col in df.columns[:5]:  # Показываем первые 5 колонок
                            value = df.iloc[0][col] if len(df) > 0 else 'N/A'
                            print(f"   {col}: {value}")
                    
                    return True
            else:
                print("❌ ПРОБЛЕМА НАЙДЕНА: Excel экспорт вернул None!")
                return False
                
        except Exception as e:
            print(f"❌ ПРОБЛЕМА НАЙДЕНА: Ошибка Excel экспорта: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    except Exception as e:
        print(f"❌ ОБЩАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False

async def check_avito_parser_directly():
    """Проверяет Avito парсер напрямую"""
    print("\n🔧 ПРЯМАЯ ПРОВЕРКА AVITO ПАРСЕРА")
    print("="*40)
    
    try:
        from avito_parser_integration import AvitoCardParser
        
        test_url = "https://www.avito.ru/moskva/kvartiry/4-k._kvartira_95_m_514_et._4805953955"
        
        print(f"🔗 Тестируем URL: {test_url}")
        
        # Создаем парсер
        parser = AvitoCardParser(skip_photos=True)
        
        # Парсим страницу
        print("📋 Парсим страницу...")
        parsed_data = parser.parse_avito_page(test_url)
        
        if parsed_data:
            print("✅ Парсинг страницы успешен")
            print(f"📊 Полученные данные: {len(parsed_data)} полей")
            
            # Показываем основные поля
            key_fields = ['url', 'title', 'price', 'rooms', 'total_area', 'floor']
            for field in key_fields:
                value = parsed_data.get(field, 'НЕ НАЙДЕНО')
                print(f"   {field}: {value}")
            
            # Тестируем подготовку данных для БД
            print("\n📋 Тестируем prepare_data_for_db...")
            db_data = parser.prepare_data_for_db(parsed_data)
            
            if db_data:
                print("✅ Подготовка данных для БД успешна")
                print(f"📊 Данные для БД: {len(db_data)} полей")
                return True
            else:
                print("❌ ПРОБЛЕМА: prepare_data_for_db вернул None!")
                return False
        else:
            print("❌ ПРОБЛЕМА: Парсинг страницы вернул None!")
            return False
            
    except Exception as e:
        print(f"❌ ОШИБКА прямой проверки: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚨 ДИАГНОСТИКА ПРОБЛЕМЫ: АВИТО ВЫВОДИТ ПУСТОЙ EXCEL")
    print("="*60)
    
    async def run_all_diagnostics():
        # Проверка 1: Основная диагностика
        print("🔍 ДИАГНОСТИКА 1: Основной поток")
        main_success = await test_avito_excel_issue()
        
        # Проверка 2: Прямая проверка парсера
        print("\n🔍 ДИАГНОСТИКА 2: Прямая проверка парсера")
        parser_success = await check_avito_parser_directly()
        
        # Итоговый отчет
        print("\n" + "="*60)
        print("📊 ИТОГОВЫЙ ОТЧЕТ ДИАГНОСТИКИ")
        print("="*60)
        
        print(f"Основной поток: {'✅ OK' if main_success else '❌ ПРОБЛЕМА'}")
        print(f"Прямая проверка парсера: {'✅ OK' if parser_success else '❌ ПРОБЛЕМА'}")
        
        if main_success and parser_success:
            print("\n🎉 ВСЕ ТЕСТЫ ПРОШЛИ! Проблема не воспроизводится.")
            print("   Возможно, проблема была временной или уже исправлена.")
        else:
            print("\n⚠️ ПРОБЛЕМА ОБНАРУЖЕНА!")
            
            if not main_success:
                print("   - Основной поток Excel экспорта неисправен")
            if not parser_success:
                print("   - Avito парсер работает некорректно")
            
            print("\n💡 РЕКОМЕНДАЦИИ:")
            print("   1. Проверьте доступность cookies файла avito_cookies.json")
            print("   2. Убедитесь, что Chrome WebDriver совместим с версией Chrome")
            print("   3. Проверьте, что Avito URL активен и доступен")
            print("   4. Проверьте логи выше на предмет конкретных ошибок")
        
        return main_success and parser_success
    
    success = asyncio.run(run_all_diagnostics())
    exit(0 if success else 1)