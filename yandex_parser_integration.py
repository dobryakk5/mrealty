#!/usr/bin/env python3
"""
Парсер объявлений Yandex Realty для интеграции с listings_processor

Функции:
- Парсинг карточки объявления Yandex Realty
- Извлечение основных атрибутов
- Подготовка данных для БД и Excel
"""

import os
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

class YandexCardParser:
    """Парсер карточек объявлений Yandex Realty для listings_processor"""
    
    def __init__(self):
        self.driver = None
        self.timeout = 10
        self.request_delay = 2.0
        
    def setup_selenium(self):
        """Настраивает Chrome WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.6904.127 Safari/537.36')
            
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Явно указываем путь к Chrome binary
            if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
                chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            else:
                chrome_options.binary_location = "/opt/google/chrome/google-chrome"
            
            print("🔧 Создаем браузер...")
            self.driver = webdriver.Chrome(options=chrome_options)

            # Антидетект скрипты как в Avito парсере
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
                window.chrome = {runtime: {}};
            """)
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания браузера Chrome: {e}")
            return False

    def _clean(self, s):
        """Очищает строку от лишних пробелов"""
        if s is None:
            return None
        return re.sub(r'\s+', ' ', s).strip()

    def parse_card(self, url):
        """Парсит полную страницу объявления Yandex Realty"""
        return self.parse_yandex_page(url)

    def extract_quick_data(self, html):
        """Быстро извлекает только цену и статус из HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        result = {}

        # Цена
        try:
            price_selectors = [
                # Устойчивые селекторы (менее вероятно изменятся)
                '[data-test-id="price-value"]',          # data-test-id обычно стабильны
                'span[class*="price"][class*="Price"]',  # Комбинация классов с Price
                'span[class*="SummaryInfo"][class*="price"]',  # SummaryInfo + price
                # Текущие точные селекторы (могут измениться)
                '.OfferCardSummaryInfo__price--2FD3C',
                # Общие селекторы как fallback
                'span[class*="price"]',                   # Любой спан с price в классе
                '.price__value',                         # Общий селектор
                'h1 + span'                              # Спан после h1 (цена обычно идет после заголовка)
            ]
            for selector in price_selectors:
                price_el = soup.select_one(selector)
                if price_el:
                    price_text = self._clean(price_el.get_text())
                    if price_text:
                        price_clean = price_text.replace('&nbsp;', ' ').replace('\xa0', ' ')
                        price_digits = re.sub(r'[^\d]', '', price_clean)
                        if price_digits and len(price_digits) >= 6 and len(price_digits) <= 12:
                            candidate_price = int(price_digits)
                            if 100000 <= candidate_price <= 1000000000:
                                result['price'] = candidate_price
                                break
        except Exception:
            pass

        # Статус - упрощенная версия
        try:
            result['status'] = 'active'  # По умолчанию активно

            # 1. Проверяем красную метку "снято или устарело" рядом с ценой
            status_tag_selectors = [
                # Устойчивые селекторы (менее вероятно изменятся)
                '[data-test="Badge"]',                                      # data-test стабилен
                'div[class*="red"][class*="Badge"]',                        # Красный бейдж
                'div[class*="Badge"][class*="view_red"]',                   # Бейдж с красным видом
                '*[class*="badgeText"]',                                    # Любой текст бейджа
                '*[class*="tags"] *[class*="Badge"]',                       # Бейдж в контейнере тегов
                # Текущие точные селекторы (могут измениться)
                '.OfferCardSummary__tags--QypeB .Badge__badgeText--GkeO3',  # Точный путь к тексту бейджа
                '.Badge__view_red--oJExh .Badge__badgeText--GkeO3',         # Красный бейдж с текстом
                '.Badge__badgeText--GkeO3',                                 # Любой текст бейджа
                '.OfferCardSummary__tags--QypeB',                           # Контейнер тегов
                # Общие селекторы как fallback
                '[class*="Badge"]',
                '[class*="badge"]',
                '[class*="Tag"]',
                '[class*="tag"]'
            ]

            for selector in status_tag_selectors:
                status_elements = soup.select(selector)
                for element in status_elements:
                    status_text = self._clean(element.get_text().lower())
                    if status_text and any(marker in status_text for marker in ['снято', 'устарело', 'неактуально']):
                        result['status'] = 'inactive'
                        print(f"🔴 Найдена метка статуса: '{status_text}' в селекторе: {selector}")
                        break
                if result['status'] == 'inactive':
                    break

            # Дополнительный поиск по тексту всей страницы
            if result['status'] == 'active':
                page_text = soup.get_text()
                if 'объявление снято или устарело' in page_text.lower():
                    result['status'] = 'inactive'
                    print("🔴 Найден текст 'объявление снято или устарело' на странице")

            # 2. Проверяем заголовок страницы
            if result['status'] == 'active':
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text().lower()
                    if any(word in title_text for word in ['снято', 'устарело', 'недоступно']):
                        result['status'] = 'inactive'

            # 3. Быстрая проверка структуры - если нет фото, вероятно неактивно
            if result['status'] == 'active':
                page_html = str(soup)
                has_photos = any(indicator in page_html for indicator in [
                    'data-test-id="photo-thumbnail"',
                    'data-test-id="gallery"'
                ])
                if not has_photos and 'похожие объявления' in page_html.lower():
                    result['status'] = 'inactive'

        except Exception:
            result['status'] = 'active'

        return result

    def extract_all_data(self, html):
        """Извлекает все данные из HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        result = {}
        
        # Заголовок - извлекаем area и rooms
        try:
            title_selectors = ['h1[data-test-id="offer-title"]', '.OfferTitle', 'h1']
            for selector in title_selectors:
                title_el = soup.select_one(selector)
                if title_el:
                    title_text = self._clean(title_el.get_text())
                    if title_text:
                        area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', title_text)
                        if area_match:
                            result['area_m2'] = float(area_match.group(1).replace(',', '.'))
                        
                        if 'студия' in title_text.lower():
                            result['rooms'] = 0
                        else:
                            rooms_match = re.search(r'(\d+)[\s-]*комн', title_text, re.IGNORECASE)
                            if rooms_match:
                                result['rooms'] = int(rooms_match.group(1))
                        break
        except Exception:
            pass

        # Технические характеристики из OfferCard__techFeatures
        try:
            tech_features = soup.select_one('.OfferCard__techFeatures--3Zoaa')
            if tech_features:
                highlights = tech_features.select('.OfferCardHighlight__container--2gZn2')
                for highlight in highlights:
                    try:
                        value_el = highlight.select_one('.OfferCardHighlight__value--HMVgP')
                        label_el = highlight.select_one('.OfferCardHighlight__label--2uMCy')
                        
                        if value_el and label_el:
                            value_text = self._clean(value_el.get_text())
                            label_text = self._clean(label_el.get_text()).lower()
                            
                            if 'общая' in label_text and value_text:
                                area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', value_text)
                                if area_match:
                                    result['area_m2'] = float(area_match.group(1).replace(',', '.'))
                            
                            elif 'жилая' in label_text and value_text:
                                area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', value_text)
                                if area_match:
                                    result['living_area'] = float(area_match.group(1).replace(',', '.'))
                            
                            elif 'кухня' in label_text and value_text:
                                area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', value_text)
                                if area_match:
                                    result['kitchen_area'] = float(area_match.group(1).replace(',', '.'))
                            
                            elif 'год постройки' in label_text and value_text:
                                year_match = re.search(r'(\d{4})', value_text)
                                if year_match:
                                    year = int(year_match.group(1))
                                    if 1800 <= year <= 2030:
                                        result['year_built'] = year
                            
                            elif 'этаж' in label_text and value_text:
                                floor_match = re.search(r'(\d+)\s*этаж', value_text)
                                if floor_match:
                                    result['floor'] = int(floor_match.group(1))
                            
                            elif 'из' in label_text and value_text:
                                total_floors_match = re.search(r'(\d+)', value_text)
                                if total_floors_match:
                                    result['floor_total'] = int(total_floors_match.group(1))
                    
                    except Exception:
                        continue
        except Exception:
            pass

        # Цена
        try:
            price_selectors = [
                # Устойчивые селекторы (менее вероятно изменятся)
                '[data-test-id="price-value"]',          # data-test-id обычно стабильны
                'span[class*="price"][class*="Price"]',  # Комбинация классов с Price
                'span[class*="SummaryInfo"][class*="price"]',  # SummaryInfo + price
                # Текущие точные селекторы (могут измениться)
                '.OfferCardSummaryInfo__price--2FD3C',
                # Общие селекторы как fallback
                'span[class*="price"]',                   # Любой спан с price в классе
                '.price__value',                         # Общий селектор
                'h1 + span'                              # Спан после h1 (цена обычно идет после заголовка)
            ]
            for selector in price_selectors:
                price_el = soup.select_one(selector)
                if price_el:
                    price_text = self._clean(price_el.get_text())
                    if price_text:
                        price_clean = price_text.replace('&nbsp;', ' ').replace('\xa0', ' ')
                        price_digits = re.sub(r'[^\d]', '', price_clean)
                        if price_digits and len(price_digits) >= 6 and len(price_digits) <= 12:
                            candidate_price = int(price_digits)
                            if 100000 <= candidate_price <= 1000000000:
                                result['price'] = candidate_price
                                break
        except Exception:
            pass

        # Этаж
        try:
            page_text = soup.get_text()
            floor_match = re.search(r'(\d+)\s*/\s*(\d+)', page_text)
            if floor_match:
                result['floor'] = int(floor_match.group(1))
                result['floor_total'] = int(floor_match.group(2))
        except Exception:
            pass

        # Адрес
        try:
            address_selectors = ['.CardLocation__addressItem--1JYpZ', '[data-test-id="offer-location"]']
            for selector in address_selectors:
                address_el = soup.select_one(selector)
                if address_el:
                    address_text = self._clean(address_el.get_text())
                    if address_text and len(address_text) > 5:
                        result['address'] = address_text
                        break
        except Exception:
            pass

        # Метро - расширенная логика как в Avito
        try:
            metro_selectors = [
                '[data-test-id="metro-station"]', 
                '.MetroStation',
                '.OfferCardMetro',
                '.CardLocation__metro',
                '.Metro__station'
            ]
            
            for selector in metro_selectors:
                metro_blocks = soup.select(selector)
                for metro_block in metro_blocks:
                    metro_text = self._clean(metro_block.get_text())
                    if metro_text and result.get('metro') is None:
                        # Очищаем от префиксов "м." и "метро"
                        metro_clean = re.sub(r'(^|\s+)(м\.|^m\.|^метро)', '', metro_text, flags=re.IGNORECASE).strip()
                        
                        # Парсим время до метро различными способами
                        time_patterns = [
                            r'(\d+)\s*мин',  # "10 мин"
                            r'(\d+)\s*min',   # "10 min"
                            r'(\d+)\s*м',     # "10 м"
                            r'(\d+)-\d+\s*мин',  # "5-10 мин" - берем первое число
                            r'(\d+)–\d+\s*мин',  # "em dash" variant
                        ]
                        
                        walk_minutes = None
                        for time_pattern in time_patterns:
                            time_match = re.search(time_pattern, metro_text)
                            if time_match:
                                walk_minutes = int(time_match.group(1))
                                break
                        
                        # Удаляем время из названия станции
                        if walk_minutes:
                            # Удаляем все варианты времени из названия
                            for time_pattern in time_patterns:
                                metro_clean = re.sub(time_pattern, '', metro_clean, flags=re.IGNORECASE).strip()
                            
                            result['walk_minutes'] = walk_minutes
                        
                        # Окончательно очищаем название станции
                        metro_clean = re.sub(r'[,\-\s\.]+$', '', metro_clean).strip()
                        
                        if metro_clean and len(metro_clean) > 2:
                            result['metro'] = metro_clean
                            break
                if result.get('metro'):
                    break
                    
            # Дополнительный поиск метро в тексте страницы (как в Avito)
            if not result.get('metro'):
                try:
                    page_text = soup.get_text()
                    # Поиск паттернов вида "Метро 10 мин пешком" или "Улица 1905 года, 10 мин"
                    metro_patterns = [
                        r'Метро\s+(\d+)\s*мин\s*пешком',  # "Метро 10 мин пешком"
                        r'([A-Яа-я\s\d\-№ёЁ]+?)\s*,\s*(\d+)\s*мин',  # "Улица 1905 года, 10 мин"
                        r'м\s*\.\s*([A-Яа-я\s\d\-№ёЁ]+?)\s*,\s*(\d+)\s*мин',  # "м. Улица 1905 года, 10 мин"
                    ]
                    
                    for pattern in metro_patterns:
                        metro_match = re.search(pattern, page_text, re.IGNORECASE)
                        if metro_match:
                            if len(metro_match.groups()) == 1:  # Первый паттерн - только время
                                result['walk_minutes'] = int(metro_match.group(1))
                            else:  # Остальные паттерны - станция и время
                                result['metro'] = metro_match.group(1).strip()
                                result['walk_minutes'] = int(metro_match.group(2))
                            break
                except Exception:
                    pass
                    
        except Exception:
            pass

        # Тип дома и год (только если год не найден в техблоке)
        try:
            page_text = soup.get_text()
            house_patterns = ['кирпичный', 'панельный', 'монолитный']
            for pattern in house_patterns:
                if re.search(pattern, page_text, re.IGNORECASE):
                    result['house_type'] = pattern
                    break
            
            # Ищем год только если он не был найден в техническом блоке
            if 'year_built' not in result or result.get('year_built') is None:
                year_match = re.search(r'(19\d{2}|20\d{2})', page_text)
                if year_match:
                    year = int(year_match.group(1))
                    if 1900 <= year <= 2030:
                        result['year_built'] = year
        except Exception:
            pass

        # Особенности квартиры
        try:
            features_text = []
            features_elements = soup.select('.OfferCardFeature__text--_Hmzv')
            for text_el in features_elements:
                feature_text = self._clean(text_el.get_text())
                if feature_text and len(feature_text) > 3 and '₽' not in feature_text:
                    features_text.append(feature_text.lower())
            
            combined_features = ' '.join(features_text)
            
            if 'косметический ремонт' in combined_features:
                result['renovation'] = 'косметический ремонт'
            
            if 'санузел раздельный' in combined_features:
                result['bathroom_type'] = 'separate'
            elif 'санузел совмещенный' in combined_features:
                result['bathroom_type'] = 'combined'
            
            if 'лоджия' in combined_features:
                result['balcony'] = 'loggia'
            elif 'балкон' in combined_features:
                result['balcony'] = 'balcony'
            
            if 'на улицу' in combined_features:
                result['window_view'] = 'на улицу'
        except Exception:
            pass

        # Статус
        try:
            result['status'] = 'active'  # По умолчанию активно

            # 1. Проверяем красную метку "снято или устарело" рядом с ценой
            status_tag_selectors = [
                # Устойчивые селекторы (менее вероятно изменятся)
                '[data-test="Badge"]',                                      # data-test стабилен
                'div[class*="red"][class*="Badge"]',                        # Красный бейдж
                'div[class*="Badge"][class*="view_red"]',                   # Бейдж с красным видом
                '*[class*="badgeText"]',                                    # Любой текст бейджа
                '*[class*="tags"] *[class*="Badge"]',                       # Бейдж в контейнере тегов
                # Текущие точные селекторы (могут измениться)
                '.OfferCardSummary__tags--QypeB .Badge__badgeText--GkeO3',  # Точный путь к тексту бейджа
                '.Badge__view_red--oJExh .Badge__badgeText--GkeO3',         # Красный бейдж с текстом
                '.Badge__badgeText--GkeO3',                                 # Любой текст бейджа
                '.OfferCardSummary__tags--QypeB',                           # Контейнер тегов
                # Общие селекторы как fallback
                '[class*="Badge"]',
                '[class*="badge"]',
                '[class*="Tag"]',
                '[class*="tag"]'
            ]

            for selector in status_tag_selectors:
                status_elements = soup.select(selector)
                for element in status_elements:
                    status_text = self._clean(element.get_text().lower())
                    if status_text and any(marker in status_text for marker in ['снято', 'устарело', 'неактуально']):
                        result['status'] = 'inactive'
                        print(f"🔴 Найдена метка статуса: '{status_text}' в селекторе: {selector}")
                        break
                if result['status'] == 'inactive':
                    break

            # Дополнительный поиск по тексту всей страницы
            if result['status'] == 'active':
                page_text = soup.get_text()
                if 'объявление снято или устарело' in page_text.lower():
                    result['status'] = 'inactive'
                    print("🔴 Найден текст 'объявление снято или устарело' на странице")

            # 2. Если статус не определен по меткам, проверяем структуру страницы
            if result['status'] == 'active':
                page_html = str(soup)

                # Проверяем, есть ли фото галерея - если нет, то объявление неактивно
                photo_indicators = [
                    'data-test-id="photo-thumbnail"',
                    'data-test-id="gallery"',
                    'class="Gallery"',
                    'фото квартиры',
                    'фотография'
                ]

                has_photos = any(indicator in page_html for indicator in photo_indicators)

                # Если нет фото И есть "Похожие объявления" в начале - объявление неактивно
                if not has_photos and 'похожие объявления' in page_html.lower():
                    result['status'] = 'inactive'

                # Дополнительно: проверяем заголовок страницы
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text().lower()
                    if any(word in title_text for word in ['снято', 'устарело', 'недоступно']):
                        result['status'] = 'inactive'


        except Exception as e:
            result['status'] = 'active'

        # Просмотры
        try:
            header_elements = soup.select('.OfferCardSummaryHeader__text--2EMVm')
            for element in header_elements:
                header_text = self._clean(element.get_text())
                if header_text:
                    views_match = re.search(r'(\d+)\s*просмотр', header_text, re.IGNORECASE)
                    if views_match:
                        result['views_count'] = int(views_match.group(1))
                        break
        except Exception:
            pass

        # Продавец
        try:
            seller_name_el = soup.select_one('.OfferCardAuthorBadge__name--3M271')
            if seller_name_el:
                result['seller_name'] = self._clean(seller_name_el.get_text())
            
            seller_type_el = soup.select_one('.OfferCardAuthorBadge__category--3DrfS')
            if seller_type_el:
                seller_type = self._clean(seller_type_el.get_text()).lower()
                if 'агентство' in seller_type:
                    result['seller_type'] = 'agency'
                elif 'собственник' in seller_type:
                    result['seller_type'] = 'owner'
                else:
                    result['seller_type'] = seller_type
        except Exception:
            pass

        # Описание
        try:
            desc_el = soup.select_one('[data-test-id="offer-description"]')
            if desc_el:
                desc_text = self._clean(desc_el.get_text())
                if desc_text and len(desc_text) > 10:
                    result['description'] = desc_text
        except Exception:
            pass

        # Фотографии
        try:
            photo_elements = soup.select('[data-test-id="photo-thumbnail"]')
            if photo_elements:
                result['photos_count'] = len(photo_elements)
        except Exception:
            pass

        return result

    def parse_yandex_quick(self, url):
        """Быстрый парсинг только цены и статуса Yandex Realty"""
        try:
            print(f"⚡ Быстрый парсинг Yandex Realty: {url}")

            if not self.setup_selenium():
                print("❌ Не удалось настроить Selenium")
                return None

            print(f"🌐 Переходим на страницу: {url}")
            self.driver.get(url)

            # Минимальное ожидание
            time.sleep(1.5)

            html = self.driver.page_source
            print("📄 Получен HTML страницы")

            print("⚡ Быстро извлекаем цену и статус...")
            result = self.extract_quick_data(html)
            result['url'] = url

            print("✅ Быстрый парсинг завершен!")
            return result

        except Exception as e:
            print(f"❌ Ошибка быстрого парсинга: {e}")
            return None
        finally:
            self.cleanup()

    def parse_yandex_page(self, url):
        """Парсит полную страницу объявления Yandex Realty"""
        try:
            print(f"🔄 Парсим страницу Yandex Realty: {url}")
            
            if not self.setup_selenium():
                print("❌ Не удалось настроить Selenium")
                return None
            
            print(f"🌐 Переходим на страницу: {url}")
            self.driver.get(url)
            
            # Ждем загрузки
            try:
                for selector in ["[data-test-id='offer-card']", ".OfferSummary", "h1"]:
                    try:
                        WebDriverWait(self.driver, 3).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        break
                    except TimeoutException:
                        continue
            except Exception as e:
                print(f"Ошибка ожидания загрузки: {e}")
            
            time.sleep(self.request_delay)
            
            html = self.driver.page_source
            print("📄 Получен HTML страницы")
            
            print("🔍 Извлекаем все данные...")
            result = self.extract_all_data(html)
            result['url'] = url
            
            print("✅ Парсинг завершен успешно!")
            return result
            
        except Exception as e:
            print(f"❌ Ошибка парсинга страницы: {e}")
            return None
        finally:
            self.cleanup()

    def cleanup(self):
        """Очищает ресурсы"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
        except Exception:
            pass

    def prepare_quick_data_for_db(self, parsed_data):
        """Подготавливает быстрые данные (цена + статус) для БД"""
        if not parsed_data:
            return None

        db_data = {
            'url': parsed_data.get('url'),
            'price': parsed_data.get('price'),
            'status': parsed_data.get('status'),
            'source': 'yandex'
        }

        return db_data

    def prepare_data_for_db(self, parsed_data):
        """Подготавливает данные для БД в формате, совместимом с listings_processor"""
        if not parsed_data:
            return None
        
        # Формируем metro_time в формате Avito: "минуты название_станции"
        metro_time_formatted = None
        metro_station = parsed_data.get('metro')
        walk_minutes = parsed_data.get('walk_minutes')
        
        if metro_station and walk_minutes:
            # Очищаем название станции от лишних символов
            clean_station = re.sub(r'[,\-\s\.]+$', '', metro_station).strip()
            metro_time_formatted = f"{walk_minutes} {clean_station}"
        elif metro_station:
            # Если есть станция, но нет времени - ставим 0
            clean_station = re.sub(r'[,\-\s\.]+$', '', metro_station).strip()
            metro_time_formatted = f"0 {clean_station}"
        
        # Маппинг полей в формат БД
        db_data = {
            'url': parsed_data.get('url'),
            'price': parsed_data.get('price'),
            'rooms': parsed_data.get('rooms'),
            'area_total': parsed_data.get('area_m2'),
            'living_area': parsed_data.get('living_area'),
            'kitchen_area': parsed_data.get('kitchen_area'),
            'floor': parsed_data.get('floor'),
            'floor_total': parsed_data.get('floor_total'),
            'address': parsed_data.get('address'),
            'metro_station': metro_station,  # Оставляем для совместимости
            'metro_time': metro_time_formatted,  # Новый формат как в Avito
            'house_type': parsed_data.get('house_type'),
            'year_built': parsed_data.get('year_built'),
            'renovation': parsed_data.get('renovation'),
            'bathroom': parsed_data.get('bathroom_type'),
            'balcony': parsed_data.get('balcony'),
            'view': parsed_data.get('window_view'),
            'status': parsed_data.get('status'),
            'views': parsed_data.get('views_count'),
            'seller_name': parsed_data.get('seller_name'),
            'seller_type': parsed_data.get('seller_type'),
            'description': parsed_data.get('description'),
            'photos_count': parsed_data.get('photos_count'),
            'source': 'yandex'
        }
        
        return db_data

def test_parser():
    """Тестирует парсер"""
    parser = YandexCardParser()
    test_url = "https://realty.yandex.ru/offer/4416594170111710645/"
    
    try:
        result = parser.parse_card(test_url)
        if result:
            print("\n🎉 РЕЗУЛЬТАТ ПАРСИНГА:")
            for key, value in result.items():
                if value is not None:
                    print(f"  {key}: {value}")
        else:
            print("❌ Парсинг не удался")
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")

if __name__ == '__main__':
    test_parser()