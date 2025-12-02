#!/usr/bin/env python3
"""
Скрипт для получения cookies с CIAN используя Selenium (headless Chrome)
Используйте этот вариант если cloudscraper не работает
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def get_cian_cookies_selenium():
    """Получает cookies с CIAN используя Selenium headless Chrome"""

    # Настройки Chrome
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Без GUI
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')

    print("Запускаем headless Chrome...")

    try:
        # Создаем драйвер
        driver = webdriver.Chrome(options=chrome_options)

        # URL для получения cookies
        url = "https://www.cian.ru/cat.php?deal_type=sale&offer_type=flat&region=1"

        print(f"Открываем: {url}")
        driver.get(url)

        # Ждем загрузки страницы (Cloudflare может показать challenge)
        print("Ждем загрузки страницы (обход Cloudflare)...")
        time.sleep(10)  # Даем время на обход Cloudflare

        # Проверяем, что страница загрузилась
        print(f"Текущий URL: {driver.current_url}")
        print(f"Заголовок страницы: {driver.title}")

        # Получаем cookies
        cookies = driver.get_cookies()

        if cookies:
            print(f"\n✅ Получено {len(cookies)} cookies!\n")
            print("=" * 80)
            print("Вставьте это в parse_cian_to_db.py в секцию COOKIES:")
            print("=" * 80)

            cookies_dict = {}
            print("\nCOOKIES = {")
            for cookie in cookies:
                name = cookie['name']
                value = cookie['value']
                cookies_dict[name] = value
                print(f"    '{name}': '{value}',")
            print("}\n")

            # Сохраняем в файл
            with open('cookies_cian.txt', 'w') as f:
                f.write("COOKIES = {\n")
                for name, value in cookies_dict.items():
                    f.write(f"    '{name}': '{value}',\n")
                f.write("}\n")

            print("✅ Cookies сохранены в файл: cookies_cian.txt")

            driver.quit()
            return cookies_dict
        else:
            print("❌ Cookies не получены")
            driver.quit()
            return None

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        if 'driver' in locals():
            driver.quit()
        return None

if __name__ == '__main__':
    get_cian_cookies_selenium()
