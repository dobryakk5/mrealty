#!/usr/bin/env python3
"""
Скрипт для создания свежих cookies Авито
Браузер остается открытым для ручного создания cookies
"""

import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def create_fresh_cookies():
    """Создает свежие cookies для Авито"""
    
    print("🚀 Запуск браузера для создания cookies...")
    
    # Настройки браузера
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--window-size=1920,1080")
    
    # User-Agent
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Убираем признаки автоматизации
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        # Создаем браузер
        driver = webdriver.Chrome(options=options)
        
        # Убираем webdriver
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print("✅ Браузер запущен!")
        print("🌐 Переходим на Авито...")
        
        # Переходим на Авито
        driver.get("https://avito.ru")
        time.sleep(3)
        
        print("✅ Страница Авито загружена!")
        
        # Автоматически накапливаем cookies
        print("🔄 Автоматически накапливаем cookies...")
        
        # Переходим на страницу с квартирами
        print("🏠 Переходим на страницу с квартирами...")
        driver.get("https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg")
        time.sleep(5)
        
        # Прокручиваем страницу для загрузки контента
        print("📜 Прокручиваем страницу для загрузки контента...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(3)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
        # Проверяем количество cookies
        initial_cookies = driver.get_cookies()
        print(f"📊 Начальное количество cookies: {len(initial_cookies) if initial_cookies else 0}")
        
        print("\n" + "="*60)
        print("🔧 ИНСТРУКЦИЯ ПО СОЗДАНИЮ COOKIES:")
        print("="*60)
        print("1. Войдите в свой аккаунт Авито (если есть)")
        print("2. Просмотрите несколько страниц с квартирами")
        print("3. Поиграйте с фильтрами (метро, цена, комнаты)")
        print("4. Подождите 2-3 минуты для накопления cookies")
        print("5. Нажмите Enter в этом терминале для сохранения")
        print("="*60)
        
        # Ждем команды пользователя
        input("\n⏳ Нажмите Enter когда cookies готовы...")
        
        # Получаем cookies
        print("📊 Получаем cookies...")
        
        # Пробуем несколько раз получить cookies
        cookies = None
        attempts = 3
        
        for attempt in range(attempts):
            print(f"🔄 Попытка {attempt + 1}/{attempts}...")
            cookies = driver.get_cookies()
            
            if cookies and len(cookies) > 0:
                print(f"✅ Получено cookies: {len(cookies)}")
                break
            else:
                print(f"⚠️ Попытка {attempt + 1}: cookies не найдены")
                if attempt < attempts - 1:
                    print("⏳ Ждем 5 секунд...")
                    time.sleep(5)
        
        if not cookies or len(cookies) == 0:
            print("❌ Не удалось получить cookies!")
            print("🔍 Проверяем текущий URL...")
            current_url = driver.current_url
            print(f"📍 Текущий URL: {current_url}")
            
            if 'avito.ru' not in current_url:
                print("⚠️ Вы не на странице Авито!")
                print("🔄 Переходим на Авито...")
                driver.get("https://avito.ru")
                time.sleep(5)
                
                print("📊 Повторная попытка получения cookies...")
                cookies = driver.get_cookies()
        
        # Создаем структуру данных
        cookies_data = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'cookies': cookies or []
        }
        
        # Сохраняем в файл
        filename = "avito_cookies.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(cookies_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Cookies сохранены в {filename}")
        print(f"📊 Количество cookies: {len(cookies)}")
        print(f"⏰ Время создания: {cookies_data['timestamp']}")
        
        # Показываем важные cookies
        important_cookies = ['_avisc', 'srv_id', 'buyer_location_id', 'session_id']
        found_important = []
        
        if cookies:  # Проверяем, что cookies не None
            for cookie in cookies:
                if cookie and 'name' in cookie and cookie['name'] in important_cookies:
                    found_important.append(cookie['name'])
        
        if found_important:
            print(f"🔐 Найдены важные cookies: {', '.join(found_important)}")
        else:
            print("⚠️ Важные cookies не найдены")
        
        # Закрываем браузер
        print("\n🔒 Закрываем браузер...")
        driver.quit()
        
        print("🎉 Cookies успешно созданы!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        if 'driver' in locals():
            driver.quit()

if __name__ == "__main__":
    create_fresh_cookies()
