#!/usr/bin/env python3
"""
Парсер AVITO с поддержкой парсинга по станциям метро
Использует таблицу metro из БД
"""

import asyncio
import os
import time
import random
import re
import sys
from typing import Dict, List, Optional
from urllib.parse import urljoin
from dotenv import load_dotenv
import json

# ========== НАСТРОЙКИ ==========
# Режим работы:
# 1 - Вторичка
# 2 - Новостройки
DEFAULT_MODE = 1

# Станция метро (ID из таблицы metro в БД)
# Оставьте None для парсинга без фильтра по метро
DEFAULT_METRO_ID = None  # Например: 55 для Кузьминки

# Количество карточек для парсинга
MAX_CARDS_DEFAULT = 5

# Headless режим
HEADLESS_MODE_DEFAULT = False  # Отключено для отладки
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
import asyncpg

# Load environment
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

class AvitoMetroDBParser:
    def __init__(self, mode: int = 1, metro_id: Optional[int] = None, max_cards: int = 5):
        self.mode = mode
        self.metro_id = metro_id
        self.max_cards = max_cards
        self.driver = None
        self.db_conn = None
        
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
    
    async def connect_db(self):
        """Подключается к БД"""
        try:
            self.db_conn = await asyncpg.connect(DATABASE_URL)
            print("✅ Подключение к БД установлено")
        except Exception as e:
            print(f"❌ Ошибка подключения к БД: {e}")
            return False
        return True
    
    async def close_db(self):
        """Закрывает соединение с БД"""
        if self.db_conn:
            await self.db_conn.close()
            print("🔒 Соединение с БД закрыто")
    
    async def get_metro_info(self, metro_id: int) -> Optional[Dict]:
        """Получает информацию о станции метро из БД"""
        try:
            if not self.db_conn:
                return None
            
            # Ищем по системному ID
            metro = await self.db_conn.fetchrow("""
                SELECT id, name, line_id, lat, lon, cian_id, avito_id
                FROM metro 
                WHERE id = $1
            """, metro_id)
            
            if metro:
                return dict(metro)
            
            # Если не найдено по системному ID, пробуем по avito_id
            metro = await self.db_conn.fetchrow("""
                SELECT id, name, line_id, lat, lon, cian_id, avito_id
                FROM metro 
                WHERE avito_id = $1
            """, metro_id)
            
            if metro:
                return dict(metro)
            
            # Если не найдено по avito_id, пробуем по cian_id
            metro = await self.db_conn.fetchrow("""
                SELECT id, name, line_id, lat, lon, cian_id, avito_id
                FROM metro 
                WHERE cian_id = $1
            """, metro_id)
            
            if metro:
                return dict(metro)
            
            return None
            
        except Exception as e:
            print(f"❌ Ошибка получения информации о метро: {e}")
            return None
    
    async def search_metro_by_name(self, name: str) -> Optional[Dict]:
        """Ищет станцию метро по названию"""
        try:
            if not self.db_conn:
                return None
            
            # Поиск по точному названию
            metro = await self.db_conn.fetchrow("""
                SELECT id, name, line_id, lat, lon, cian_id
                FROM metro 
                WHERE LOWER(name) = LOWER($1)
            """, name)
            
            if metro:
                return dict(metro)
            
            # Поиск по частичному совпадению
            metro = await self.db_conn.fetchrow("""
                SELECT id, name, line_id, lat, lon, cian_id
                FROM metro 
                WHERE LOWER(name) LIKE LOWER($1)
                LIMIT 1
            """, f"%{name}%")
            
            if metro:
                return dict(metro)
            
            return None
            
        except Exception as e:
            print(f"❌ Ошибка поиска метро по названию: {e}")
            return None
    
    async def get_metro_stats(self) -> Dict:
        """Получает статистику по метро"""
        try:
            if not self.db_conn:
                return {}
            
            # Общее количество станций
            total_count = await self.db_conn.fetchval("SELECT COUNT(*) FROM metro")
            
            # Количество линий
            lines_count = await self.db_conn.fetchval("SELECT COUNT(DISTINCT line_id) FROM metro")
            
            # Станции с cian_id
            cian_count = await self.db_conn.fetchval("SELECT COUNT(*) FROM metro WHERE cian_id IS NOT NULL")
            
            # Примеры станций
            sample_stations = await self.db_conn.fetch("""
                SELECT id, name, line_id, cian_id
                FROM metro 
                ORDER BY id 
                LIMIT 10
            """)
            
            return {
                "total_stations": total_count,
                "lines_count": lines_count,
                "cian_id_count": cian_count,
                "sample_stations": [dict(station) for station in sample_stations]
            }
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики метро: {e}")
            return {}
    
    def get_target_url(self) -> str:
        """Формирует целевой URL с учетом метро"""
        base_url = self.base_urls.get(self.mode, self.base_urls[1])
        
        if self.metro_id:
            # Используем avito_id для URL, если он есть
            url = f"{base_url}?metro={self.metro_id}"
            print(f"📍 Парсим {self.mode_names[self.mode]} у метро ID: {self.metro_id}")
        else:
            url = base_url
            print(f"📍 Парсим {self.mode_names[self.mode]} без фильтра по метро")
        
        return url
    
    async def get_target_url_with_metro_info(self) -> str:
        """Формирует целевой URL с учетом информации о метро"""
        base_url = self.base_urls.get(self.mode, self.base_urls[1])
        
        if self.metro_id:
            # Получаем информацию о метро
            metro_info = await self.get_metro_info(self.metro_id)
            if metro_info and metro_info.get('avito_id'):
                # Используем avito_id для URL
                url = f"{base_url}?metro={metro_info['avito_id']}"
                print(f"📍 Парсим {self.mode_names[self.mode]} у метро: {metro_info['name']} (avito_id: {metro_info['avito_id']})")
                return url
            else:
                # Fallback: используем переданный ID
                url = f"{base_url}?metro={self.metro_id}"
                print(f"📍 Парсим {self.mode_names[self.mode]} у метро ID: {self.metro_id} (avito_id не найден)")
                return url
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
    
    def load_cookies(self) -> bool:
        """Загружает и применяет cookies для обхода антибот-защиты"""
        try:
            cookies_file = "avito_cookies.json"
            if not os.path.exists(cookies_file):
                print("⚠️ Файл cookies не найден, работаем без авторизации")
                return False
            
            # Загружаем cookies
            with open(cookies_file, 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            
            print(f"🍪 Загружены cookies от {cookies_data['timestamp']}")
            print(f"📊 Количество cookies: {len(cookies_data['cookies'])}")
            
            # Сначала переходим на домен
            self.driver.get("https://www.avito.ru")
            time.sleep(2)
            
            # Применяем cookies
            applied_count = 0
            for cookie in cookies_data['cookies']:
                try:
                    # Убираем лишние поля
                    cookie_dict = {
                        'name': cookie['name'],
                        'value': cookie['value'],
                        'domain': cookie.get('domain', ''),
                        'path': cookie.get('path', '/')
                    }
                    
                    # Добавляем только если есть
                    if cookie.get('expiry'):
                        cookie_dict['expiry'] = cookie['expiry']
                    if cookie.get('secure'):
                        cookie_dict['secure'] = cookie['secure']
                    if cookie.get('httpOnly'):
                        cookie_dict['httpOnly'] = cookie['httpOnly']
                    
                    self.driver.add_cookie(cookie_dict)
                    applied_count += 1
                    
                except Exception as e:
                    continue
            
            print(f"✅ Применено cookies: {applied_count}")
            
            # Проверяем авторизацию
            self.driver.refresh()
            time.sleep(2)
            
            try:
                # Ищем признаки авторизации
                auth_elements = [
                    '[data-marker="header/create-button"]',
                    '.create-button',
                    '.post-button',
                    '[data-marker="header/profile"]',
                    '.profile-link'
                ]
                
                for selector in auth_elements:
                    try:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        print("✅ Авторизация успешна (найден элемент авторизации)")
                        return True
                    except:
                        continue
                
                print("⚠️ Авторизация не подтверждена, но cookies применены")
                return True
                
            except Exception as e:
                print(f"⚠️ Ошибка проверки авторизации: {e}")
                return True
            
        except Exception as e:
            print(f"❌ Ошибка загрузки cookies: {e}")
            return False
    
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
                    for header in headers[:3]:  # Показываем первые 3 заголовка
                        print(f"    📝 {header.text[:100]}...")
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
                '.listing-item',
                'a[href*="/item/"]'  # Ссылки на объявления
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
                
                # Попробуем найти любые ссылки
                all_links = self.driver.find_elements(By.TAG_NAME, "a")
                print(f"🔍 Всего ссылок на странице: {len(all_links)}")
                
                # Ищем ссылки, которые могут быть объявлениями
                item_links = [link for link in all_links if '/item/' in (link.get_attribute('href') or '')]
                if item_links:
                    print(f"✅ Найдено {len(item_links)} ссылок на объявления")
                    cards = item_links[:self.max_cards]
                    used_selector = "a[href*='/item/']"
                
                if not cards:
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
            title = self.extract_text(card, '[data-marker="item-title"]', '.iva-item-title', 'h3', 'h4')
            if title:
                print(f"    Заголовок: {title[:50]}...")
            
            # Цена
            price = self.extract_price(card)
            if price:
                print(f"    Цена: {price:,} ₽")
            
            # Адрес
            address = self.extract_text(card, '[data-marker="item-address"]', '.iva-item-address', '.address')
            if address:
                print(f"    Адрес: {address}")
            
            # Создаем объект объявления
            ad_data = {
                'avitoid': avitoid,
                'title': title or '',
                'price': price or 0,
                'address': address or '',
                'metro_id': self.metro_id,
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
                '.price-value',
                '.price-text'
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
            print("🚀 Запуск парсера AVITO с поддержкой метро (БД)")
            print("=" * 60)
            
            # Подключаемся к БД
            if not await self.connect_db():
                return
            
            # Получаем статистику метро
            metro_stats = await self.get_metro_stats()
            if metro_stats:
                print(f"🚇 Статистика метро:")
                print(f"  Всего станций: {metro_stats['total_stations']}")
                print(f"  Линий: {metro_stats['lines_count']}")
                print(f"  С cian_id: {metro_stats['cian_id_count']}")
            
            # Настройки
            print(f"\n📋 Настройки:")
            print(f"  Режим: {self.mode} ({self.mode_names[self.mode]})")
            print(f"  Метро: {self.metro_id or 'Не указано'}")
            
            if self.metro_id:
                metro_info = await self.get_metro_info(self.metro_id)
                if metro_info:
                    print(f"  Название: {metro_info['name']}")
                    print(f"  Линия: {metro_info['line_id']}")
                    print(f"  Координаты: {metro_info['lat']:.6f}, {metro_info['lon']:.6f}")
                    if metro_info['cian_id']:
                        print(f"  Cian ID: {metro_info['cian_id']}")
                else:
                    print(f"  ⚠️ Станция с ID {self.metro_id} не найдена в БД")
            
            print(f"  Максимум карточек: {self.max_cards}")
            print(f"  Headless режим: {'Да' if HEADLESS_MODE_DEFAULT else 'Нет'}")
            
            # Настраиваем базу данных
            print("\n💾 Настраиваем базу данных...")
            await create_ads_avito_table()
            print("✅ База данных настроена")
            
            # Получаем целевой URL
            target_url = await self.get_target_url_with_metro_info()
            print(f"\n🔍 Начинаем парсинг...")
            print(f"🚀 Начинаем парсинг {self.mode_names[self.mode]} с stealth-техниками: {target_url}")
            
            # Настраиваем Selenium
            self.setup_selenium()
            
            if not self.driver:
                print("❌ Не удалось создать WebDriver")
                return
            
            # Загружаем cookies для обхода антибот-защиты
            print("\n🍪 Загружаем cookies для обхода антибот-защиты...")
            cookies_loaded = self.load_cookies()
            
            if cookies_loaded:
                print("✅ Cookies загружены и применены")
            else:
                print("⚠️ Работаем без cookies (может быть заблокирован)")
            
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
            await self.close_db()
    
    async def print_metro_info(self):
        """Выводит информацию о доступных станциях метро"""
        if not self.db_conn:
            print("❌ Нет подключения к БД")
            return
        
        try:
            # Получаем примеры станций
            stations = await self.db_conn.fetch("""
                SELECT id, name, line_id, cian_id, avito_id
                FROM metro 
                ORDER BY id 
                LIMIT 15
            """)
            
            print(f"🚇 Доступно станций метро: {len(stations)}")
            print("\n📋 Примеры станций:")
            
            for station in stations:
                cian_info = f" (cian_id: {station['cian_id']})" if station['cian_id'] else ""
                avito_info = f" (avito_id: {station['avito_id']})" if station['avito_id'] else ""
                print(f"  {station['id']:3d} - {station['name']:<25} Линия {station['line_id']}{cian_info}{avito_info}")
            
            if self.metro_id:
                metro_info = await self.get_metro_info(self.metro_id)
                if metro_info:
                    print(f"\n📍 Выбранная станция: {metro_info['id']} - {metro_info['name']} (Линия {metro_info['line_id']})")
                    if metro_info.get('avito_id'):
                        print(f"  🎯 Avito ID: {metro_info['avito_id']} (для URL)")
                    if metro_info.get('cian_id'):
                        print(f"  🏠 Cian ID: {metro_info['cian_id']}")
                else:
                    print(f"\n⚠️ Станция с ID {self.metro_id} не найдена")
        
        except Exception as e:
            print(f"❌ Ошибка получения информации о метро: {e}")

async def main():
    """Основная функция"""
    print("🚀 Запуск парсера AVITO с поддержкой метро (БД)")
    
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
        try:
            metro_id = int(sys.argv[2])
            print(f"📍 Указана станция метро: {metro_id}")
        except ValueError:
            print("❌ Неверный ID метро. Введите число")
            return
    
    if len(sys.argv) > 3:
        try:
            max_cards = int(sys.argv[3])
        except ValueError:
            print("❌ Неверное количество карточек")
            return
    
    # Создаем парсер
    parser = AvitoMetroDBParser(mode=mode, metro_id=metro_id, max_cards=max_cards)
    
    # Показываем информацию о метро
    await parser.print_metro_info()
    
    # Запускаем парсинг
    await parser.run_parser()
    
    print("\n🎉 Парсинг завершен успешно!")

if __name__ == "__main__":
    asyncio.run(main())
