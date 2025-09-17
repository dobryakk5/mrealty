#!/usr/bin/env python3
"""
Тестовый скрипт для проверки парсера Avito
"""

import asyncio
import sys
import os

# Добавляем текущую директорию в path для импорта модулей
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from realty_parser_server import RealtyParserAPI

async def test_avito_parsing():
    """Тестирует парсинг объявления Avito"""

    # URL для тестирования
    test_url = "https://www.avito.ru/moskva/kvartiry/1-k._kvartira_295_m_25_et._7627589190"

    print(f"🔄 Тестируем парсинг Avito объявления: {test_url}")
    print("-" * 80)

    # Создаем экземпляр парсера
    parser = RealtyParserAPI()

    try:
        # Парсим объявление с таймаутом
        result = await asyncio.wait_for(
            parser.parse_property(test_url, skip_photos=True),
            timeout=60  # 60 секунд таймаут
        )

        if result:
            print("✅ Парсинг успешен!")
            print("-" * 80)
            print("📋 Данные объявления:")
            print("-" * 80)

            # Выводим основную информацию
            if result.rooms:
                print(f"🏠 Комнат: {result.rooms}")
            if result.price:
                print(f"💰 Цена: {result.price:,.0f} ₽")
            if result.total_area:
                print(f"📐 Общая площадь: {result.total_area} м²")
            if result.living_area:
                print(f"🛏️ Жилая площадь: {result.living_area} м²")
            if result.kitchen_area:
                print(f"🍽️ Площадь кухни: {result.kitchen_area} м²")
            if result.floor:
                print(f"🏢 Этаж: {result.floor}")
            if result.total_floors:
                print(f"🏗️ Этажей в доме: {result.total_floors}")

            print("-" * 40)

            # Характеристики
            if result.bathroom:
                print(f"🚿 Санузел: {result.bathroom}")
            if result.balcony:
                print(f"🪟 Балкон: {result.balcony}")
            if result.renovation:
                print(f"🔨 Ремонт: {result.renovation}")
            if result.construction_year:
                print(f"🏗️ Год постройки: {result.construction_year}")
            if result.house_type:
                print(f"🏘️ Тип дома: {result.house_type}")
            if result.ceiling_height:
                print(f"📏 Высота потолков: {result.ceiling_height} м")
            if result.furniture:
                print(f"🪑 Мебель: {result.furniture}")

            print("-" * 40)

            # Расположение
            if result.address:
                print(f"📍 Адрес: {result.address}")
            if result.metro_station:
                print(f"🚇 Станция метро: {result.metro_station}")
            if result.metro_time:
                print(f"⏱️ Время до метро: {result.metro_time}")
            if result.metro_way:
                print(f"🚶 Способ добраться: {result.metro_way}")

            print("-" * 40)

            # Дополнительная информация
            if result.tags:
                print(f"🏷️ Метки: {', '.join(result.tags)}")
            if result.description:
                print(f"📝 Описание: {result.description[:200]}...")
            if result.status is not None:
                print(f"📊 Статус: {'Активно' if result.status else 'Неактивно'}")
            if result.views_today:
                print(f"👁️ Просмотров сегодня: {result.views_today}")

            print("-" * 40)
            print(f"🔗 Источник: {result.source}")
            print(f"🌐 URL: {result.url}")

            print("-" * 80)
            print("📄 JSON данные:")
            print(result.to_json())

        else:
            print("❌ Не удалось спарсить объявление")

    except asyncio.TimeoutError:
        print("⏱️ Таймаут: парсинг занял больше 60 секунд")
    except Exception as e:
        print(f"❌ Ошибка при парсинге: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Очищаем ресурсы
        parser.cleanup()
        print("\n🧹 Ресурсы очищены")

if __name__ == "__main__":
    print("🚀 Запуск тестового парсера Avito")
    print("=" * 80)

    # Запускаем тест
    asyncio.run(test_avito_parsing())

    print("=" * 80)
    print("✅ Тест завершен")