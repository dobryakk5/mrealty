#!/usr/bin/env python3
"""
Парсер AVITO с поддержкой парсинга по станциям метро
Использует полный список станций метро Москвы
"""

import asyncio
import os
import time
import random
import re
import sys
import json
from typing import Dict, List, Optional
from urllib.parse import urljoin
from dotenv import load_dotenv

# ========== НАСТРОЙКИ ==========
# Режим работы:
# 1 - Вторичка
# 2 - Новостройки
DEFAULT_MODE = 1

# Станция метро (ID из moscow_metro_complete.json)
# Оставьте None для парсинга без фильтра по метро
DEFAULT_METRO_ID = 17  # Например: "55" для Кузьминки

# Количество карточек для парсинга
MAX_CARDS_DEFAULT = 2

# Headless режим
HEADLESS_MODE_DEFAULT = False  # Временно отключено для отладки
# ==============================

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException

# Database imports
from parse_todb_avito import create_ads_avito_table, save_avito_ad

# Load environment
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

class AvitoMetroParser:
    def __init__(self, mode: int = 1, metro_id: Optional[str] = None, max_cards: int = 5):
        self.mode = mode
        self.metro_id = metro_id
        self.max_cards = max_cards
        self.driver = None
        
        # Загружаем данные о метро
        self.metro_data = self.load_metro_data()
        
        # URL для разных режимов
        self.base_urls = {
            1: "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg",
            2: "https://www.avito.ru/moskva/kvartiry/prodam/novostroyka-ASgBAgICAkSSA8YQ5geOUg"
        }
        
        # Названия режимов
        self.mode_names = {
            1: "вторичка",
            2: "новостройки"
        }
    
    def load_metro_data(self) -> Dict:
        """Загружает данные о станциях метро"""
        try:
            if os.path.exists("moscow_metro_complete.json"):
                with open("moscow_metro_complete.json", 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                print("⚠️ Файл moscow_metro_complete.json не найден")
                return {}
        except Exception as e:
            print(f"❌ Ошибка загрузки данных о метро: {e}")
            return {}
    
    def get_metro_name(self, metro_id: str) -> str:
        """Получает название станции метро по ID"""
        if self.metro_data and "stations" in self.metro_data:
            return self.metro_data["stations"].get(metro_id, f"Метро {metro_id}")
        return f"Метро {metro_id}"
    
    def get_target_url(self) -> str:
        """Формирует целевой URL с учетом метро"""
        base_url = self.base_urls.get(self.mode, self.base_urls[1])
        
        if self.metro_id:
            url = f"{base_url}?metro={self.metro_id}"
            metro_name = self.get_metro_name(self.metro_id)
            print(f"📍 Парсим {self.mode_names[self.mode]} у метро: {metro_name} (ID: {self.metro_id})")
        else:
            url = base_url
            print(f"📍 Парсим {self.mode_names[self.mode]} без фильтра по метро")
        
        return url
    
    def setup_selenium(self) -> webdriver.Chrome:
        """Настройка Selenium с stealth-техниками"""
        options = Options()
        
        # Stealth настройки
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # User-Agent
        options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Размер окна
        options.add_argument("--window-size=1920,1080")
        
        # Headless режим
        if HEADLESS_MODE_DEFAULT:
            options.add_argument("--headless")
            print("🔧 Настраиваем Selenium WebDriver с stealth-техниками в headless режиме...")
        else:
            print("🔧 Настраиваем Selenium WebDriver с stealth-техниками...")
        
        # Отключение изображений для ускорения
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2
        }
        options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=options)
        
        # Убираем признаки автоматизации
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return self.driver
    
    async def parse_avito_page(self, url: str) -> List[Dict]:
        """Парсит страницу AVITO"""
        try:
            print(f"📄 Загружаем страницу...")
            self.driver.get(url)
            
            # Случайная задержка для имитации человеческого поведения
            delay = random.uniform(3, 6)
            print(f"⏳ Ждем загрузки {delay:.1f} сек...")
            await asyncio.sleep(delay)
            
            # Ждем загрузки контента
            print("⏳ Ждем загрузки контента...")
            await asyncio.sleep(2)
            
            # Ищем заголовки страницы для проверки загрузки
            try:
                headers = self.driver.find_elements(By.CSS_SELECTOR, "h1, h2, h3")
                if headers:
                    print(f"✅ Найдено заголовков: {len(headers)}")
                else:
                    print("⚠️ Заголовки не найдены")
            except Exception as e:
                print(f"⚠️ Ошибка поиска заголовков: {e}")
            
            # Ищем карточки объявлений
            print("🔍 Ищем карточки объявлений...")
            
            # Попробуем разные селекторы
            selectors = [
                '[data-marker="item"]',
                '.iva-item-root',
                '.item',
                '.listing-item'
            ]
            
            cards = []
            used_selector = None
            
            for selector in selectors:
                try:
                    cards = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if cards:
                        used_selector = selector
                        print(f"✅ Используем селектор: {selector}")
                        break
                except Exception as e:
                    continue
            
            if not cards:
                print("❌ Не удалось найти карточки объявлений")
                return []
            
            print(f"📊 Найдено карточек: {len(cards)}")
            print(f"📋 Обрабатываем карточек: {min(len(cards), self.max_cards)}")
            
            # Обрабатываем карточки
            parsed_ads = []
            for i, card in enumerate(cards[:self.max_cards]):
                try:
                    ad_data = self.parse_card(card, i + 1)
                    if ad_data:
                        parsed_ads.append(ad_data)
                        print(f"  ✅ Спарсено: {ad_data['avitoid']}")
                except Exception as e:
                    print(f"  ❌ Ошибка парсинга карточки {i + 1}: {e}")
            
            return parsed_ads
            
        except Exception as e:
            print(f"❌ Ошибка при парсинге страницы: {e}")
            return []
    
    def parse_card(self, card, card_number: int) -> Optional[Dict]:
        """Парсит отдельную карточку объявления"""
        try:
            print(f"\n📦 Карточка {card_number}/{self.max_cards}:")
            
            # ID объявления
            avitoid = self.extract_avitoid(card)
            if not avitoid:
                print("    ❌ ID не найден")
                return None
            
            # Заголовок
            title = self.extract_text(card, '[data-marker="item-title"]', '.iva-item-title')
            if title:
                print(f"    Заголовок: {title[:50]}...")
            
            # Цена
            price = self.extract_price(card)
            if price:
                print(f"    Цена: {price:,} ₽")
            
            # Адрес
            address = self.extract_text(card, '[data-marker="item-address"]', '.iva-item-address')
            if address:
                print(f"    Адрес: {address}")
            
            # Создаем объект объявления
            ad_data = {
                'avitoid': avitoid,
                'title': title or '',
                'price': price or 0,
                'address': address or '',
                'metro_id': int(self.metro_id) if self.metro_id else None,
                'metro_name': self.get_metro_name(self.metro_id) if self.metro_id else None,
                'mode': self.mode,
                'mode_name': self.mode_names[self.mode],
                'url': self.get_target_url(),
                'parsed_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return ad_data
            
        except Exception as e:
            print(f"    ❌ Ошибка парсинга: {e}")
            return None
    
    def extract_avitoid(self, card) -> Optional[str]:
        """Извлекает ID объявления"""
        try:
            # Пробуем разные способы
            href = card.get_attribute('href')
            if href:
                # Ищем ID в href
                match = re.search(r'/(\d+)$', href)
                if match:
                    return match.group(1)
            
            # Пробуем data-атрибуты
            avitoid = card.get_attribute('data-item-id') or card.get_attribute('data-avito-id')
            if avitoid:
                return avitoid
            
            # Пробуем найти в дочерних элементах
            id_elements = card.find_elements(By.CSS_SELECTOR, '[data-item-id], [data-avito-id]')
            for elem in id_elements:
                avitoid = elem.get_attribute('data-item-id') or elem.get_attribute('data-avito-id')
                if avitoid:
                    return avitoid
            
            return None
        except:
            return None
    
    def extract_text(self, card, *selectors) -> Optional[str]:
        """Извлекает текст по разным селекторам"""
        for selector in selectors:
            try:
                element = card.find_element(By.CSS_SELECTOR, selector)
                text = element.text.strip()
                if text:
                    return text
            except:
                continue
        return None
    
    def extract_price(self, card) -> Optional[int]:
        """Извлекает цену"""
        try:
            # Пробуем разные селекторы для цены
            price_selectors = [
                '[data-marker="item-price"]',
                '.iva-item-price',
                '.price',
                '.price-value'
            ]
            
            for selector in price_selectors:
                try:
                    element = card.find_element(By.CSS_SELECTOR, selector)
                    price_text = element.text.strip()
                    
                    # Извлекаем числа из текста
                    price_match = re.search(r'[\d\s]+', price_text)
                    if price_match:
                        price_str = price_match.group().replace(' ', '')
                        return int(price_str)
                except:
                    continue
            
            return None
        except:
            return None
    
    async def run_parser(self):
        """Запуск парсера"""
        try:
            print("🚀 Запуск парсера AVITO с поддержкой метро")
            print("=" * 60)
            
            # Настройки
            print(f"📋 Настройки:")
            print(f"  Режим: {self.mode} ({self.mode_names[self.mode]})")
            print(f"  Метро: {self.metro_id or 'Не указано'}")
            if self.metro_id:
                print(f"  Название: {self.get_metro_name(self.metro_id)}")
            print(f"  Максимум карточек: {self.max_cards}")
            print(f"  Headless режим: {'Да' if HEADLESS_MODE_DEFAULT else 'Нет'}")
            
            # Настраиваем базу данных
            print("\n💾 Настраиваем базу данных...")
            await create_ads_avito_table()
            print("✅ База данных настроена")
            
            # Получаем целевой URL
            target_url = self.get_target_url()
            print(f"\n🔍 Начинаем парсинг...")
            print(f"🚀 Начинаем парсинг {self.mode_names[self.mode]} с stealth-техниками: {target_url}")
            
            # Настраиваем Selenium
            self.setup_selenium()
            
            if not self.driver:
                print("❌ Не удалось создать WebDriver")
                return
            
            print(f"📋 Максимум карточек: {self.max_cards}")
            
            # Парсим страницу
            parsed_ads = await self.parse_avito_page(target_url)
            
            if not parsed_ads:
                print("⚠️ Нет данных для сохранения")
                return
            
            print(f"\n🎉 Парсинг {self.mode_names[self.mode]} завершен! Спарсено: {len(parsed_ads)} объявлений")
            
            # Сохраняем в БД
            print(f"\n💾 Сохраняем {len(parsed_ads)} объявлений в БД...")
            saved_count = 0
            
            for ad in parsed_ads:
                try:
                    result = await save_avito_ad(ad)
                    if result:
                        print(f"  ✅ Сохранено: {ad['avitoid']} ({self.mode_names[self.mode]})")
                        saved_count += 1
                    else:
                        print(f"  ⚠️ Пропущено (дубликат) AVITO {ad['avitoid']}: {result}")
                except Exception as e:
                    print(f"  ❌ Ошибка сохранения {ad['avitoid']}: {e}")
            
            print(f"✅ Сохранено в БД: {saved_count} из {len(parsed_ads)} объявлений")
            
        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                print("🔒 WebDriver закрыт")
    
    def print_metro_info(self):
        """Выводит информацию о доступных станциях метро"""
        if not self.metro_data or "stations" not in self.metro_data:
            print("❌ Данные о метро не загружены")
            return
        
        print(f"🚇 Доступно станций метро: {len(self.metro_data['stations'])}")
        print("\n📋 Примеры станций:")
        
        stations = list(self.metro_data['stations'].items())[:10]
        for metro_id, name in stations:
            print(f"  {metro_id:3s} - {name}")
        
        if self.metro_id:
            metro_name = self.get_metro_name(self.metro_id)
            print(f"\n📍 Выбранная станция: {metro_id} - {metro_name}")

async def main():
    """Основная функция"""
    print("🚀 Запуск парсера AVITO с поддержкой метро")
    
    # Получаем параметры
    mode = DEFAULT_MODE
    metro_id = DEFAULT_METRO_ID
    max_cards = MAX_CARDS_DEFAULT
    
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        try:
            mode = int(sys.argv[1])
            if mode not in [1, 2]:
                print("❌ Неверный режим. Используйте 1 для вторички или 2 для новостроек")
                return
        except ValueError:
            print("❌ Неверный режим. Введите число 1 или 2")
            return
    
    if len(sys.argv) > 2:
        metro_id = sys.argv[2]
        print(f"📍 Указана станция метро: {metro_id}")
    
    if len(sys.argv) > 3:
        try:
            max_cards = int(sys.argv[3])
        except ValueError:
            print("❌ Неверное количество карточек")
            return
    
    # Создаем парсер
    parser = AvitoMetroParser(mode=mode, metro_id=metro_id, max_cards=max_cards)
    
    # Показываем информацию о метро
    parser.print_metro_info()
    
    # Запускаем парсинг
    await parser.run_parser()
    
    print("\n🎉 Парсинг завершен успешно!")

if __name__ == "__main__":
    asyncio.run(main())
