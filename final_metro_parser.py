#!/usr/bin/env python3
"""
Финальная версия парсера карточек с правильным определением metro.avito_id
"""

import json
import os
import time
import asyncio
import asyncpg
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import re

class FinalMetroParser:
    def __init__(self):
        self.cookies_file = "avito_cookies_fixed.json"
        self.driver = None
        self.database_url = None
        self.max_cards = 3  # Количество карточек для парсинга
        self.metro_id = 1  # ID метро из таблицы metro
        self.metro_avito_id = None  # avito_id для этого метро
        
    async def get_metro_avito_id(self):
        """Получает avito_id для метро из БД"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем avito_id для метро
            result = await conn.fetchrow("""
                SELECT id, name, avito_id 
                FROM metro 
                WHERE id = $1
            """, self.metro_id)
            
            await conn.close()
            
            if result:
                self.metro_avito_id = result['avito_id']
                metro_name = result['name']
                print(f"📍 Метро: {metro_name} (ID: {self.metro_id}, avito_id: {self.metro_avito_id})")
                return True
            else:
                print(f"❌ Метро с ID {self.metro_id} не найдено в БД")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка получения avito_id для метро: {e}")
            return False
    
    def load_cookies(self):
        """Загружает зафиксированные cookies"""
        try:
            if not os.path.exists(self.cookies_file):
                print(f"❌ Файл cookies не найден: {self.cookies_file}")
                return False
            
            with open(self.cookies_file, 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            
            print(f"✅ Загружены cookies от {cookies_data['timestamp']}")
            print(f"📊 Количество cookies: {len(cookies_data['cookies'])}")
            return cookies_data
            
        except Exception as e:
            print(f"❌ Ошибка загрузки cookies: {e}")
            return False
    
    def setup_selenium(self):
        """Настраивает Selenium WebDriver"""
        try:
            options = Options()
            
            # Базовые настройки
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            # options.add_argument("--headless")  # Убираем headless для отладки
            
            # User-Agent
            options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Размер окна
            options.add_argument("--window-size=1920,1080")
            
            # Дополнительные настройки для обхода блокировок
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            options.add_argument("--disable-features=VizDisplayCompositor")
            
            # Убираем признаки автоматизации
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            print("🔧 Создаем браузер...")
            self.driver = webdriver.Chrome(options=options)
            
            # Убираем webdriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания браузера: {e}")
            return False
    
    def apply_cookies(self, cookies_data):
        """Применяет cookies к драйверу"""
        try:
            if not cookies_data or 'cookies' not in cookies_data:
                return False
            
            # Сначала переходим на домен
            print("🌐 Переходим на AVITO...")
            self.driver.get("https://avito.ru")
            time.sleep(3)
            
            # Применяем cookies
            applied_count = 0
            for cookie in cookies_data['cookies']:
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
                    
                    self.driver.add_cookie(cookie_dict)
                    applied_count += 1
                    
                except Exception as e:
                    continue
            
            print(f"✅ Применено cookies: {applied_count}")
            
            # Обновляем страницу с примененными cookies
            print("🔄 Обновляем страницу с cookies...")
            self.driver.refresh()
            time.sleep(5)
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка применения cookies: {e}")
            return False
    
    def get_metro_url(self):
        """Получает URL для метро с правильным avito_id"""
        if not self.metro_avito_id:
            print("❌ avito_id для метро не определен")
            return None
            
        # URL для вторички с правильным avito_id
        base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
        metro_url = f"{base_url}?metro={self.metro_avito_id}"
        return metro_url
    
    def wait_for_cards_load(self, timeout=30):
        """Ждет загрузки карточек"""
        try:
            print("⏳ Ждем загрузки карточек...")
            
            # Ждем появления карточек
            wait = WebDriverWait(self.driver, timeout)
            cards = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-marker="item"]')))
            
            print(f"✅ Загружено карточек: {len(cards)}")
            return True
            
        except TimeoutException:
            print("❌ Таймаут ожидания карточек")
            return False
        except Exception as e:
            print(f"❌ Ошибка ожидания карточек: {e}")
            return False
    
    def parse_title(self, title_text):
        """Парсит заголовок на компоненты"""
        try:
            title_data = {}
            
            # Паттерн для количества комнат
            rooms_pattern = r'(\d+)-к\.|(\d+)\s*комнат|студия|Студия'
            rooms_match = re.search(rooms_pattern, title_text)
            if rooms_match:
                if 'студия' in rooms_match.group(0).lower():
                    title_data['rooms'] = 'студия'
                else:
                    title_data['rooms'] = rooms_match.group(1) if rooms_match.group(1) else rooms_match.group(2)
            else:
                title_data['rooms'] = 'не определено'
            
            # Паттерн для площади
            area_pattern = r'(\d+(?:[,\d]+)?)\s*м²'
            area_match = re.search(area_pattern, title_text)
            if area_match:
                title_data['area'] = area_match.group(1).replace(',', '.')
            else:
                title_data['area'] = 'не определено'
            
            # Паттерн для этажа и этажности
            floor_pattern = r'(\d+)/(\d+)\s*эт'
            floor_match = re.search(floor_pattern, title_text)
            if floor_match:
                title_data['floor'] = floor_match.group(1)
                title_data['total_floors'] = floor_match.group(2)
            else:
                title_data['floor'] = 'не определено'
                title_data['total_floors'] = 'не определено'
            
            return title_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга заголовка: {e}")
            return {
                'rooms': 'ошибка парсинга',
                'area': 'ошибка парсинга',
                'floor': 'ошибка парсинга',
                'total_floors': 'ошибка парсинга'
            }
    
    def parse_address(self, address_text):
        """Парсит адрес на компоненты, используя логику из оригинального парсера"""
        try:
            address_data = {}
            
            # Разделяем адрес по строкам
            lines = address_text.strip().split('\n')
            
            if len(lines) >= 2:
                # Первая строка - улица и дом (берем как есть)
                street_line = lines[0].strip()
                address_data['street_house'] = street_line
                
                # Вторая строка - метро и время
                metro_line = lines[1].strip()
                
                # Используем логику из оригинального парсера
                # Разбиваем по запятым и ищем минуты
                parts = [p.strip() for p in metro_line.split(',') if p.strip()]
                
                minutes = None
                metro = None
                minutes_idx = None
                
                # Ищем часть с минутами (берём первую цифру до слова "мин")
                for i, p in enumerate(parts):
                    if re.search(r"мин\.?", p, re.IGNORECASE):
                        nums = re.findall(r"\d+", p)
                        if nums:
                            minutes = int(nums[0])
                            minutes_idx = i
                            break
                
                # Если нашли минуты, метро вероятнее всего в предыдущем сегменте
                if minutes_idx is not None and minutes_idx > 0:
                    # всё, что ДО сегмента с минутами — это адресная часть
                    address_candidates = parts[:minutes_idx]
                else:
                    address_candidates = parts
                
                # Адрес — первые два компонента (улица, дом), если есть
                if address_candidates:
                    street = address_candidates[0]
                    house = None
                    # Попробуем вычленить дом как ведущую цифровую часть во втором сегменте,
                    # а метро — как фразу, начинающуюся с первой пары букв (Заглавная+строчная)
                    if len(address_candidates) >= 2:
                        seg2 = address_candidates[1]
                        # Находим первую позицию заглавной кирил. буквы, за которой идёт строчная — начало названия метро
                        m_cap = re.search(r"[А-ЯЁ][а-яё]", seg2)
                        if m_cap and m_cap.start() > 0:
                            split_idx = m_cap.start()
                            head = seg2[:split_idx].strip()
                            tail = seg2[split_idx:].strip()
                            if head:
                                house = head
                            if not metro and tail:
                                metro = tail
                        else:
                            # fallback: берём сегмент как дом
                            house = seg2
                    
                    # Сбор адреса
                    if street and house:
                        address_data['street_house'] = f"{street}, {house}"
                    else:
                        address_data['street_house'] = street
                
                # Сохраняем метро и минуты
                if metro:
                    address_data['metro_name'] = metro
                else:
                    # Если метро не выделили, берём всю строку
                    address_data['metro_name'] = metro_line
                
                if minutes:
                    address_data['time_to_metro'] = str(minutes)
                else:
                    address_data['time_to_metro'] = 'не указано'
                    
            else:
                # Если только одна строка
                address_data['street_house'] = address_text.strip()
                address_data['metro_name'] = 'не указано'
                address_data['time_to_metro'] = 'не указано'
            
            return address_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга адреса: {e}")
            return {
                'street_house': 'ошибка парсинга',
                'metro_name': 'ошибка парсинга',
                'time_to_metro': 'ошибка парсинга'
            }
    
    def parse_seller_info(self, card_element):
        """Парсит информацию о продавце, используя логику из оригинального парсера"""
        try:
            seller_data = {}
            
            # Ищем информацию о продавце в разных местах
            try:
                # Пробуем найти по data-marker для характеристик
                params_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-specific-params"]')
                if params_elem:
                    params_text = params_elem.text.strip()
                    
                    # Ищем тип продавца в тексте
                    if 'частн' in params_text.lower():
                        seller_data['type'] = 'частное лицо'
                    elif 'агентств' in params_text.lower():
                        seller_data['type'] = 'агентство'
                    elif 'застройщик' in params_text.lower():
                        seller_data['type'] = 'застройщик'
                    else:
                        seller_data['type'] = 'не определено'
                    
                    seller_data['full_text'] = params_text
                else:
                    seller_data['type'] = 'не найдено'
                    seller_data['full_text'] = 'не найдено'
                    
            except:
                # Если не нашли по data-marker, пробуем найти по тексту
                try:
                    # Ищем любой текст, содержащий информацию о продавце
                    all_text = card_element.text.lower()
                    if 'частн' in all_text:
                        seller_data['type'] = 'частное лицо'
                    elif 'агентств' in all_text:
                        seller_data['type'] = 'агентство'
                    elif 'застройщик' in all_text:
                        seller_data['type'] = 'застройщик'
                    else:
                        seller_data['type'] = 'не определено'
                    
                    seller_data['full_text'] = 'найдено в общем тексте'
                except:
                    seller_data['type'] = 'не найдено'
                    seller_data['full_text'] = 'не найдено'
            
            return seller_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга продавца: {e}")
            return {
                'type': 'ошибка парсинга',
                'full_text': 'ошибка парсинга'
            }
    
    def parse_card(self, card_element):
        """Парсит одну карточку"""
        try:
            card_data = {}
            
            # ID объявления
            try:
                item_id = card_element.get_attribute('data-item-id')
                if item_id:
                    card_data['item_id'] = item_id
            except:
                pass
            
            # Заголовок
            try:
                title_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-title"]')
                title_text = title_elem.text.strip()
                card_data['title'] = title_text
                
                # Парсим заголовок на компоненты
                title_components = self.parse_title(title_text)
                card_data.update(title_components)
                
            except:
                card_data['title'] = "Не найдено"
                card_data.update({
                    'rooms': 'не найдено',
                    'area': 'не найдено',
                    'floor': 'не найдено',
                    'total_floors': 'не найдено'
                })
            
            # Цена
            try:
                price_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-price"]')
                card_data['price'] = price_elem.text.strip()
            except:
                card_data['price'] = "Не найдено"
            
            # Адрес/метро
            try:
                address_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-address"]')
                address_text = address_elem.text.strip()
                card_data['address'] = address_text
                
                # Парсим адрес на компоненты
                address_components = self.parse_address(address_text)
                card_data.update(address_components)
                
            except:
                card_data['address'] = "Не найдено"
                card_data.update({
                    'street_house': 'не найдено',
                    'metro_name': 'не найдено',
                    'time_to_metro': 'не найдено'
                })
            
            # Характеристики
            try:
                params_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-specific-params"]')
                card_data['params'] = params_elem.text.strip()
            except:
                card_data['params'] = "Не найдено"
            
            # Описание
            try:
                description_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-description"]')
                card_data['description'] = description_elem.text.strip()
            except:
                card_data['description'] = "Не найдено"
            
            # Ссылка на карточку
            try:
                link_elem = card_element.find_element(By.CSS_SELECTOR, 'a[data-marker="item-title"]')
                card_data['url'] = link_elem.get_attribute('href')
            except:
                card_data['url'] = "Не найдено"
            
            # Время публикации
            try:
                time_elem = card_element.find_element(By.CSS_SELECTOR, '[data-marker="item-date"]')
                card_data['published_time'] = time_elem.text.strip()
            except:
                card_data['published_time'] = "Не найдено"
            
            # Информация о продавце
            seller_info = self.parse_seller_info(card_element)
            card_data.update(seller_info)
            
            return card_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга карточки: {e}")
            return None
    
    def parse_metro_page(self):
        """Парсит страницу с метро"""
        try:
            print(f"🎯 Парсим страницу с метро ID = {self.metro_id} (avito_id = {self.metro_avito_id})")
            
            # Получаем URL
            metro_url = self.get_metro_url()
            if not metro_url:
                return []
                
            print(f"🌐 URL: {metro_url}")
            
            # Переходим на страницу
            self.driver.get(metro_url)
            time.sleep(5)
            
            # Проверяем текущий URL
            current_url = self.driver.current_url
            print(f"📍 Текущий URL: {current_url}")
            
            # Ждем загрузки карточек
            if not self.wait_for_cards_load():
                print("❌ Не удалось дождаться загрузки карточек")
                return []
            
            # Получаем все карточки
            cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
            print(f"📊 Найдено карточек: {len(cards)}")
            
            # Парсим первые несколько карточек
            parsed_cards = []
            for i, card in enumerate(cards[:self.max_cards]):
                print(f"\n🔍 Парсим карточку {i+1}/{min(self.max_cards, len(cards))}...")
                
                card_data = self.parse_card(card)
                if card_data:
                    card_data['card_number'] = i + 1
                    parsed_cards.append(card_data)
                    print(f"✅ Карточка {i+1} спарсена")
                else:
                    print(f"❌ Ошибка парсинга карточки {i+1}")
            
            return parsed_cards
            
        except Exception as e:
            print(f"❌ Ошибка парсинга страницы: {e}")
            return []
    
    def save_results(self, parsed_cards):
        """Сохраняет результаты в файл"""
        try:
            result_data = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'metro_id': self.metro_id,
                'metro_avito_id': self.metro_avito_id,
                'url': self.get_metro_url(),
                'total_cards_found': len(parsed_cards),
                'parsed_cards': parsed_cards
            }
            
            filename = f"final_metro_{self.metro_id}_cards_{time.strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
            
            print(f"\n💾 Результаты сохранены в {filename}")
            
            # Выводим краткую статистику
            print(f"\n📊 СТАТИСТИКА:")
            print(f"   Метро ID: {self.metro_id}")
            print(f"   Метро avito_id: {self.metro_avito_id}")
            print(f"   Карточек спарсено: {len(parsed_cards)}")
            print(f"   Файл результатов: {filename}")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения результатов: {e}")
    
    async def run_parser(self):
        """Запускает парсер"""
        try:
            print("🚀 Запуск финального парсера карточек")
            print("=" * 60)
            
            # Получаем avito_id для метро
            if not await self.get_metro_avito_id():
                return False
            
            # Загружаем cookies
            cookies_data = self.load_cookies()
            if not cookies_data:
                return False
            
            # Настраиваем Selenium
            if not self.setup_selenium():
                return False
            
            # Применяем cookies
            if not self.apply_cookies(cookies_data):
                return False
            
            # Парсим страницу
            parsed_cards = self.parse_metro_page()
            
            if parsed_cards:
                # Сохраняем результаты
                self.save_results(parsed_cards)
                
                # Выводим детальную информацию о карточках
                print(f"\n📋 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О КАРТОЧКАХ:")
                for i, card in enumerate(parsed_cards):
                    print(f"\n   🏠 Карточка {i+1}:")
                    print(f"     ID: {card.get('item_id', 'Н/Д')}")
                    print(f"     Заголовок: {card.get('title', 'Н/Д')}")
                    print(f"     Комнаты: {card.get('rooms', 'Н/Д')}")
                    print(f"     Площадь: {card.get('area', 'Н/Д')} м²")
                    print(f"     Этаж: {card.get('floor', 'Н/Д')}/{card.get('total_floors', 'Н/Д')}")
                    print(f"     Цена: {card.get('price', 'Н/Д')}")
                    print(f"     Улица/дом: {card.get('street_house', 'Н/Д')}")
                    print(f"     Метро: {card.get('metro_name', 'Н/Д')}")
                    print(f"     Время до метро: {card.get('time_to_metro', 'Н/Д')}")
                    print(f"     Продавец: {card.get('type', 'Н/Д')}")
                    print(f"     Время публикации: {card.get('published_time', 'Н/Д')}")
                
                return True
            else:
                print("❌ Не удалось спарсить карточки")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка работы парсера: {e}")
            return False
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    print("🔒 Браузер закрыт")
                except:
                    pass

async def main():
    # Загружаем переменные окружения
    load_dotenv()
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        print("💡 Создайте файл .env с содержимым:")
        print("   DATABASE_URL=postgresql://username:password@host:port/database")
        return
    
    # Создаем и запускаем парсер
    parser = FinalMetroParser()
    parser.database_url = database_url
    
    success = await parser.run_parser()
    
    if success:
        print("\n🎉 Парсинг завершен успешно!")
    else:
        print("\n❌ Парсинг завершен с ошибками")

if __name__ == "__main__":
    asyncio.run(main())
