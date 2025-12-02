#!/usr/bin/env python3
"""
Скрипт для получения cookies с CIAN используя curl_cffi
Лучше обходит Cloudflare чем cloudscraper
"""

from curl_cffi import requests

def get_cian_cookies_cffi():
    """Получает cookies с CIAN используя curl_cffi"""

    # URL для получения cookies
    url = "https://www.cian.ru/cat.php?deal_type=sale&offer_type=flat&region=1"

    print("Получаем cookies с CIAN используя curl_cffi...")
    print(f"URL: {url}\n")

    try:
        # Создаем сессию с импресонацией Chrome
        session = requests.Session()

        # Делаем запрос с impersonate Chrome
        response = session.get(
            url,
            impersonate="chrome124",  # Имитируем Chrome 124
            timeout=30
        )

        print(f"Статус: {response.status_code}")
        print(f"URL после редиректов: {response.url}\n")

        # Получаем cookies
        cookies = session.cookies

        if cookies:
            print(f"✅ Получено {len(cookies)} cookies!\n")
            print("=" * 80)
            print("Вставьте это в parse_cian_to_db.py в секцию COOKIES:")
            print("=" * 80)

            cookies_dict = {}
            print("\nCOOKIES = {")
            for cookie in cookies:
                print(f"    '{cookie.name}': '{cookie.value}',")
                cookies_dict[cookie.name] = cookie.value
            print("}\n")

            # Сохраняем в файл
            with open('cookies_cian.txt', 'w') as f:
                f.write("COOKIES = {\n")
                for name, value in cookies_dict.items():
                    f.write(f"    '{name}': '{value}',\n")
                f.write("}\n")

            print("✅ Cookies сохранены в файл: cookies_cian.txt")

            return cookies_dict
        else:
            print("❌ Cookies не получены")
            return None

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

if __name__ == '__main__':
    get_cian_cookies_cffi()
