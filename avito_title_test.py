#!/usr/bin/env python3
"""
Простой тест парсинга заголовка с Avito
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os

def test_avito_title():
    """Тестирует получение заголовка страницы Avito"""

    print("🔄 Создаем Chrome driver...")

    options = Options()
    options.add_argument("--headless=new")   # включаем новый headless
    options.add_argument("--no-sandbox")     # полезно на сервере
    options.add_argument("--disable-dev-shm-usage")  # избегает проблем с /dev/shm
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")

    # Путь к Chrome binary для macOS
    if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
        options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    else:
        options.binary_location = "/opt/google/chrome/google-chrome"

    driver = None
    try:
        driver = webdriver.Chrome(options=options)

        print("🌐 Переходим на Avito...")
        url = "https://www.avito.ru/moskva/kvartiry/1-k._kvartira_295_m_25_et._7627589190"
        driver.get(url)

        print("📄 Получаем заголовок страницы...")
        title = driver.title
        print(f"✅ Заголовок: {title}")

        # Также попробуем найти h1
        try:
            h1_element = driver.find_element("tag name", "h1")
            h1_text = h1_element.text
            print(f"📝 H1: {h1_text}")
        except Exception as e:
            print(f"⚠️ Не удалось найти H1: {e}")

        print("✅ Тест успешно завершен!")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            print("🧹 Закрываем браузер...")
            driver.quit()

if __name__ == "__main__":
    print("🚀 Запуск теста парсинга заголовка Avito")
    print("=" * 60)
    test_avito_title()
    print("=" * 60)
    print("✅ Тест завершен")