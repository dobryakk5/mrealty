#!/usr/bin/env python3
"""
Рабочий cookie saver - сразу заходит с сохраненными cookies
"""

import json
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def load_existing_cookies():
    """Загружает существующие cookies"""
    try:
        if not os.path.exists("avito_cookies.json"):
            print("❌ Старые cookies не найдены")
            return None
        
        with open("avito_cookies.json", 'r', encoding='utf-8') as f:
            cookies_data = json.load(f)
        
        print(f"✅ Загружены старые cookies от {cookies_data['timestamp']}")
        print(f"📊 Количество старых cookies: {len(cookies_data['cookies'])}")
        return cookies_data
        
    except Exception as e:
        print(f"❌ Ошибка загрузки старых cookies: {e}")
        return None

def save_cookies_with_existing():
    """Сохраняет cookies, начиная с существующих"""
    try:
        print("🍪 Рабочий cookie saver для AVITO")
        print("=" * 60)
        
        # Загружаем старые cookies
        old_cookies_data = load_existing_cookies()
        
        # Настройка Chrome с агрессивными настройками
        options = Options()
        
        # Базовые настройки
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        
        # User-Agent
        options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Размер окна
        options.add_argument("--window-size=1920,1080")
        
        # Дополнительные настройки для обхода блокировок
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-field-trial-config")
        options.add_argument("--disable-ipc-flooding-protection")
        
        # Убираем признаки автоматизации
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        print("🔧 Создаем браузер...")
        driver = webdriver.Chrome(options=options)
        
        # Убираем webdriver
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Используем рабочий URL (без www)
        target_url = "https://avito.ru"
        print(f"🌐 Переходим на {target_url}...")
        
        # Сначала заходим без cookies
        driver.get(target_url)
        time.sleep(3)
        
        current_url = driver.current_url
        print(f"📍 Текущий URL: {current_url}")
        
        # Применяем старые cookies если есть
        if old_cookies_data and old_cookies_data.get('cookies'):
            print("🔄 Применяем старые cookies...")
            
            applied_count = 0
            for cookie in old_cookies_data['cookies']:
                try:
                    cookie_dict = {
                        'name': cookie['name'],
                        'value': cookie['value'],
                        'domain': cookie.get('domain', ''),
                        'path': cookie.get('path', '/')
                    }
                    
                    if cookie.get('expiry'):
                        cookie_dict['expiry'] = cookie['expiry']
                    if cookie.get('secure'):
                        cookie_dict['secure'] = cookie['secure']
                    if cookie.get('httpOnly'):
                        cookie_dict['httpOnly'] = cookie['httpOnly']
                    
                    driver.add_cookie(cookie_dict)
                    applied_count += 1
                    
                except Exception as e:
                    continue
            
            print(f"✅ Применено старых cookies: {applied_count}")
            
            # Обновляем страницу с примененными cookies
            print("🔄 Обновляем страницу с cookies...")
            driver.refresh()
            time.sleep(5)
            
            current_url = driver.current_url
            page_title = driver.title
            print(f"📍 URL после обновления: {current_url}")
            print(f"📄 Заголовок: {page_title}")
            
            if "Доступ ограничен" in page_title:
                print("⚠️ Страница показывает ограничение доступа")
            else:
                print("✅ Страница загружена успешно")
        
        print("\n⏳ Ждем 60 секунд для добавления новых cookies...")
        print("💡 Теперь вы можете:")
        print("   - Войти в аккаунт (если не вошли)")
        print("   - Просмотреть разные страницы")
        print("   - Добавить новые cookies")
        print("   - Обойти ограничения доступа")
        
        # Обратный отсчет
        for i in range(60, 0, -10):
            print(f"⏰ Осталось: {i} секунд...")
            time.sleep(10)
        
        print("📥 Сохраняем все cookies...")
        all_cookies = driver.get_cookies()
        
        if all_cookies:
            cookies_data = {
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "domain": "avito.ru",
                "cookies": all_cookies
            }
            
            with open("avito_cookies.json", 'w', encoding='utf-8') as f:
                json.dump(cookies_data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Cookies сохранены в avito_cookies.json")
            print(f"📊 Общее количество cookies: {len(all_cookies)}")
            
            # Сравнение с старыми cookies
            if old_cookies_data:
                old_count = len(old_cookies_data['cookies'])
                new_count = len(all_cookies)
                diff = new_count - old_count
                print(f"📈 Изменение: {old_count} → {new_count} (+{diff})")
            
            print("\n📋 Примеры cookies:")
            for i, cookie in enumerate(all_cookies[:5]):
                print(f"  {i+1}. {cookie['name']}: {cookie['value'][:30]}...")
        else:
            print("❌ Cookies не найдены")
        
        driver.quit()
        print("🔒 Браузер закрыт")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        if 'driver' in locals():
            driver.quit()

if __name__ == "__main__":
    save_cookies_with_existing()
    
    print("\n" + "=" * 60)
    print("💡 Теперь можно:")
    print("1. Протестировать cookies: python simple_cookie_saver.py test")
    print("2. Показать информацию: python simple_cookie_saver.py info")
    print("3. Использовать в парсере: python avito_metro_db_parser.py 1 133 3")
