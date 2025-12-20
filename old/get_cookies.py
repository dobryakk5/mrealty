#!/usr/bin/env python3
"""
Скрипт для получения cookies с CIAN для использования на сервере
Использует cloudscraper для обхода Cloudflare
"""

import cloudscraper

def get_cian_cookies():
    """Получает cookies с CIAN используя cloudscraper"""

    # Создаем scraper с browser emulation
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'linux',
            'desktop': True
        }
    )

    # URL для получения cookies
    url = "https://www.cian.ru/cat.php?deal_type=sale&offer_type=flat&region=1"

    print("Получаем cookies с CIAN...")
    print(f"URL: {url}\n")

    try:
        # Делаем запрос
        response = scraper.get(url, timeout=30)

        print(f"Статус: {response.status_code}")
        print(f"URL после редиректов: {response.url}\n")

        # Получаем cookies
        cookies = scraper.cookies.get_dict()

        if cookies:
            print("✅ Cookies успешно получены!\n")
            print("=" * 80)
            print("Вставьте это в parse_cian_to_db.py в секцию COOKIES:")
            print("=" * 80)
            print("\nCOOKIES = {")
            for key, value in cookies.items():
                print(f"    '{key}': '{value}',")
            print("}\n")

            # Сохраняем в файл
            with open('cookies_cian.txt', 'w') as f:
                f.write("COOKIES = {\n")
                for key, value in cookies.items():
                    f.write(f"    '{key}': '{value}',\n")
                f.write("}\n")

            print("✅ Cookies сохранены в файл: cookies_cian.txt")

            return cookies
        else:
            print("❌ Cookies не получены")
            return None

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

if __name__ == '__main__':
    get_cian_cookies()
