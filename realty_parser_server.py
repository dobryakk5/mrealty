"""
HTTP API сервер для парсинга недвижимости
Предназначен для использования с Fastify (Node.js)
"""

import re
import json
import asyncio
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os
import threading
import signal
import atexit

# Импортируем парсер Avito
try:
    from avito_parser_integration import AvitoCardParser
    AVITO_AVAILABLE = True
except ImportError:
    AVITO_AVAILABLE = False
    print("⚠️ Модуль avito_parser_integration не найден, парсинг Avito недоступен")

# Импортируем парсер Yandex
try:
    from yandex_parser_integration import YandexCardParser
    YANDEX_AVAILABLE = True
except ImportError:
    YANDEX_AVAILABLE = False
    print("⚠️ Модуль yandex_parser_integration не найден, парсинг Yandex недоступен")

# Импортируем парсер Baza Winner
try:
    from baza_winner_parser import BazaWinnerParser
    BAZA_WINNER_AVAILABLE = True
except ImportError:
    BAZA_WINNER_AVAILABLE = False
    print("⚠️ Модуль baza_winner_parser не найден, парсинг Baza Winner недоступен")

# Заголовки для HTTP-запросов
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

@dataclass
class PropertyData:
    """Структурированные данные о недвижимости"""
    # Основная информация
    rooms: Optional[int] = None
    price: Optional[float] = None
    total_area: Optional[float] = None
    living_area: Optional[float] = None
    kitchen_area: Optional[float] = None
    floor: Optional[str] = None
    total_floors: Optional[int] = None
    
    # Характеристики
    bathroom: Optional[str] = None
    balcony: Optional[str] = None
    renovation: Optional[str] = None
    construction_year: Optional[int] = None
    house_type: Optional[str] = None
    ceiling_height: Optional[float] = None
    furniture: Optional[str] = None
    
    # Расположение
    address: Optional[str] = None
    metro_station: Optional[str] = None
    metro_time: Optional[Union[int, str]] = None  # Can be int (minutes) or str (formatted like "6 Текстильщики")
    metro_way: Optional[str] = None  # Способ добраться до метро (пешком, транспорт)
    
    # Дополнительно
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    photo_urls: Optional[List[str]] = None
    
    # Метаданные
    source: Optional[str] = None  # 'avito', 'cian', 'yandex'
    url: Optional[str] = None
    status: Optional[bool] = None  # Статус активности объявления
    views_today: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует в словарь для JSON"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Преобразует в JSON строку"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

# Pydantic модели для FastAPI
class ParseUrlsRequest(BaseModel):
    urls: List[str]

class ParseTextRequest(BaseModel):
    text: str

class BazaWinnerAuthRequest(BaseModel):
    username: str
    password: str

class BazaWinnerSearchRequest(BaseModel):
    username: str
    password: str
    search_params: Dict[str, Any] = {}

class ParseResponse(BaseModel):
    success: bool
    data: List[PropertyData]
    total: int
    message: str
    timestamp: str

class RealtyParserAPI:
    """API класс для парсинга недвижимости"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def is_avito_url(self, url: str) -> bool:
        """Определяет, является ли ссылка ссылкой на Avito"""
        return 'avito.ru' in url.lower()
    
    def is_cian_url(self, url: str) -> bool:
        """Определяет, является ли ссылка ссылкой на Cian"""
        return 'cian.ru' in url.lower()
    
    def is_yandex_url(self, url: str) -> bool:
        """Определяет, является ли ссылка ссылкой на Yandex Realty"""
        return 'realty.yandex.ru' in url.lower()
    
    def _extract_station_from_metro_time(self, metro_time: str) -> Optional[str]:
        """Извлекает название станции из формата '6 Текстильщики'"""
        if not metro_time or not isinstance(metro_time, str):
            return None
        
        parts = metro_time.strip().split(' ', 1)
        if len(parts) >= 2 and parts[0].isdigit():
            return parts[1]  # Название станции
        return None
    
    def _extract_minutes_from_metro_time(self, metro_time: str) -> Optional[int]:
        """Извлекает минуты из формата '6 Текстильщики'"""
        if not metro_time or not isinstance(metro_time, str):
            return None
        
        parts = metro_time.strip().split(' ', 1)
        if len(parts) >= 1 and parts[0].isdigit():
            try:
                return int(parts[0])  # Количество минут
            except ValueError:
                return None
        return None
    
    def get_url_source(self, url: str) -> str:
        """Возвращает источник ссылки"""
        if self.is_avito_url(url):
            return 'avito'
        elif self.is_cian_url(url):
            return 'cian'
        elif self.is_yandex_url(url):
            return 'yandex'
        else:
            return 'unknown'
    
    def _determine_status(self, status_str: Optional[str]) -> bool:
        """Определяет активность объявления по строковому статусу"""
        if not status_str:
            return True  # По умолчанию считаем активным
        
        status_lower = status_str.lower().strip()
        inactive_statuses = [
            'снято', 'неактивно', 'архив', 'удалено', 
            'продано', 'сдано', 'неактуальное', 'заблокировано'
        ]
        
        return not any(inactive_status in status_lower for inactive_status in inactive_statuses)
    
    async def parse_property(self, url: str, skip_photos: bool = True) -> Optional[PropertyData]:
        """
        Универсальный метод для парсинга объявлений
        Возвращает структурированные данные PropertyData
        """
        try:
            if self.is_avito_url(url):
                return await self._parse_avito_property(url, skip_photos=skip_photos)
            elif self.is_cian_url(url):
                return await self._parse_cian_property(url)
            elif self.is_yandex_url(url):
                return await self._parse_yandex_property(url)
            else:
                print(f"⚠️ Неизвестный источник ссылки: {url}")
                return None
        except Exception as e:
            print(f"❌ Ошибка парсинга {url}: {e}")
            return None

    async def parse_property_extended(self, url: str, skip_photos: bool = True) -> Optional[PropertyData]:
        """
        Расширенный парсинг объявления с полными данными
        Возвращает структурированные данные PropertyData
        """
        try:
            if self.is_avito_url(url):
                return await self._parse_avito_extended(url, skip_photos=skip_photos)
            elif self.is_cian_url(url):
                return await self._parse_cian_property(url)
            elif self.is_yandex_url(url):
                return await self._parse_yandex_property(url)
            else:
                print(f"⚠️ Неизвестный источник ссылки: {url}")
                return None
        except Exception as e:
            print(f"❌ Ошибка расширенного парсинга {url}: {e}")
            return None

    async def parse_properties_batch(self, urls: List[str], skip_photos: bool = True) -> List[PropertyData]:
        """
        Пакетный парсинг множественных объявлений
        Возвращает список PropertyData объектов
        """
        results = []
        for i, url in enumerate(urls, 1):
            try:
                print(f"🔄 Парсим объявление {i}/{len(urls)}: {url}")
                property_data = await self.parse_property(url, skip_photos=skip_photos)
                if property_data:
                    results.append(property_data)
                    print(f"✅ Объявление успешно спарсено")
                else:
                    print(f"❌ Не удалось спарсить объявление")
            except Exception as e:
                print(f"❌ Ошибка при парсинге {url}: {e}")
                continue
        
        print(f"📊 Всего успешно спарсено: {len(results)} из {len(urls)}")
        return results

    async def _parse_avito_light(self, url: str) -> Optional[PropertyData]:
        """Легкий парсер Avito через persistent браузер"""
        try:
            print(f"🔍 Легкий парсинг Avito (persistent): {url}")

            # Используем persistent браузер
            data = parse_avito_fast(url)

            if data:
                # Извлекаем данные
                title = data.get('title', '')
                h1 = data.get('h1', '')
                price_text = data.get('price', '')

                # Парсим данные из заголовка
                text = h1 if h1 else title
                parsed_data = self._extract_data_from_title(title, h1)

                # Добавляем данные, полученные напрямую из парсера
                if 'rooms' in data:
                    parsed_data['rooms'] = data['rooms']
                if 'total_area' in data:
                    parsed_data['total_area'] = data['total_area']
                if 'floor' in data:
                    parsed_data['floor'] = data['floor']
                if 'total_floors' in data:
                    parsed_data['total_floors'] = data['total_floors']

                # Парсим цену
                price = None
                if price_text:
                    price_match = re.search(r'(\d[\d\s]*)', price_text.replace('\u00a0', ' '))
                    if price_match:
                        price_str = price_match.group(1).replace(' ', '')
                        try:
                            price = float(price_str)
                        except:
                            pass

                if parsed_data:
                    # Определяем статус: активно если есть информация о комнатах
                    has_rooms = parsed_data.get('rooms') is not None
                    status = has_rooms

                    print(f"📊 Статус объявления: {'активно' if status else 'неактивно'} (комнаты: {parsed_data.get('rooms')})")

                    # Создаем объект PropertyData
                    property_data = PropertyData(
                        rooms=parsed_data.get('rooms'),
                        price=price,
                        total_area=parsed_data.get('total_area'),
                        floor=parsed_data.get('floor'),
                        total_floors=parsed_data.get('total_floors'),
                        source='avito',
                        url=url,
                        status=status
                    )

                    print("✅ Легкий парсинг (persistent) успешен")
                    return property_data

            return None

        except Exception as e:
            print(f"❌ Ошибка легкого парсинга (persistent): {e}")
            return None

    def _extract_data_from_title(self, title: str, h1: str) -> Optional[Dict[str, Any]]:
        """Извлекает данные из заголовка и H1 Avito"""
        try:
            # Используем H1 как основной источник, если он есть, иначе title
            text = h1 if h1 else title
            print(f"🔍 Анализируем текст: {text}")

            data = {}

            # Извлекаем количество комнат: "1-к.", "2-к.", "3-к." и т.д.
            rooms_match = re.search(r'(\d+)-к\.', text)
            if rooms_match:
                data['rooms'] = int(rooms_match.group(1))
                print(f"🏠 Комнат: {data['rooms']}")

            # Проверяем студии и апартаменты (0 комнат)
            if re.search(r'\bстудия\b|\bапартаменты\b', text.lower()):
                data['rooms'] = 0
                print(f"🏠 Тип жилья: {'студия' if 'студия' in text.lower() else 'апартаменты'} (комнат: 0)")

            # Извлекаем площадь: "29,5 м²", "45.2 м²"
            area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', text)
            if area_match:
                area_str = area_match.group(1).replace(',', '.')
                data['total_area'] = float(area_str)
                print(f"📐 Площадь: {data['total_area']} м²")

            # Извлекаем этаж: "3/5 эт.", "12/25 эт."
            floor_match = re.search(r'(\d+)/(\d+)\s*эт\.', text)
            if floor_match:
                data['floor'] = floor_match.group(1)
                data['total_floors'] = int(floor_match.group(2))
                print(f"🏢 Этаж: {data['floor']}/{data['total_floors']}")

            return data if data else None

        except Exception as e:
            print(f"❌ Ошибка извлечения данных: {e}")
            return None

    async def _parse_avito_property(self, url: str, skip_photos: bool = True) -> Optional[PropertyData]:
        """Парсит объявление с Avito (только легкий парсер)"""
        return await self._parse_avito_light(url)

    async def _parse_avito_extended(self, url: str, skip_photos: bool = True) -> Optional[PropertyData]:
        """Расширенный парсинг объявления с Avito (полный парсер)"""
        if not AVITO_AVAILABLE:
            print("❌ Парсер Avito недоступен")
            return None

        try:
            print(f"🏠 Парсим объявление Avito (расширенный парсер): {url}")

            # Создаем парсер Avito с параметром skip_photos
            parser = AvitoCardParser(skip_photos=skip_photos)

            # Используем только Selenium парсинг (HTTP вариант блокируется)
            parsed_data = parser.parse_avito_page(url)
            if not parsed_data:
                print("❌ Не удалось спарсить данные объявления Avito")
                return None

            # Подготавливаем данные для БД
            db_data = parser.prepare_data_for_db(parsed_data)
            print("✅ Использованы данные Selenium парсинга")
            if not db_data:
                print("❌ Не удалось подготовить данные для БД")
                return None
            
            # Создаем структурированный объект
            # Если нет цены в Avito, то объявление неактивно
            price = db_data.get('price')
            is_active = price is not None and price != "" and price != 0
            
            property_data = PropertyData(
                rooms=db_data.get('rooms'),
                price=price,
                total_area=db_data.get('total_area'),
                living_area=db_data.get('living_area'),
                kitchen_area=db_data.get('kitchen_area'),
                floor=db_data.get('floor'),
                total_floors=db_data.get('total_floors'),
                bathroom=db_data.get('bathroom'),
                balcony=db_data.get('balcony'),
                renovation=db_data.get('renovation'),
                construction_year=db_data.get('construction_year'),
                house_type=db_data.get('house_type'),
                ceiling_height=db_data.get('ceiling_height'),
                furniture=db_data.get('furniture'),
                address=db_data.get('address'),
                metro_station=db_data.get('metro_station'),
                metro_time=db_data.get('metro_time'),
                metro_way=db_data.get('metro_way'),
                tags=db_data.get('tags'),
                description=db_data.get('description'),
                photo_urls=db_data.get('photo_urls'),
                source='avito',
                url=url,
                status=is_active,
                views_today=db_data.get('today_views')
            )
            
            print(f"✅ Объявление Avito успешно спарсено")
            return property_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга объявления Avito: {e}")
            return None
        finally:
            # Закрываем браузер
            if 'parser' in locals() and parser.driver:
                try:
                    parser.cleanup()
                except:
                    pass
    
    async def _parse_yandex_property(self, url: str) -> Optional[PropertyData]:
        """Парсит объявление с Yandex Realty"""
        if not YANDEX_AVAILABLE:
            print("❌ Парсер Yandex недоступен")
            return None
        
        try:
            print(f"🏠 Парсим объявление Yandex Realty: {url}")
            
            # Создаем парсер Yandex
            parser = YandexCardParser()
            
            # Парсим полную страницу объявления
            parsed_data = parser.parse_yandex_page(url)
            if not parsed_data:
                print("❌ Не удалось спарсить данные объявления Yandex Realty")
                return None
            
            # Преобразуем данные в формат для БД
            db_data = parser.prepare_data_for_db(parsed_data)
            if not db_data:
                print("❌ Не удалось подготовить данные для БД")
                return None
            
            # Создаем структурированный объект
            property_data = PropertyData(
                rooms=db_data.get('rooms'),
                price=db_data.get('price'),
                total_area=db_data.get('area_total'),
                living_area=db_data.get('living_area'),
                kitchen_area=db_data.get('kitchen_area'),
                floor=db_data.get('floor'),
                total_floors=db_data.get('floor_total'),
                bathroom=db_data.get('bathroom'),
                balcony=db_data.get('balcony'),
                renovation=db_data.get('renovation'),
                construction_year=db_data.get('year_built'),
                house_type=db_data.get('house_type'),
                address=db_data.get('address'),
                metro_station=self._extract_station_from_metro_time(db_data.get('metro_time')),  # Extract station name from metro_time
                metro_time=self._extract_minutes_from_metro_time(db_data.get('metro_time')),      # Extract minutes from metro_time
                description=db_data.get('description'),
                source='yandex',
                url=url,
                status=self._determine_status(db_data.get('status', 'active')),
                views_today=db_data.get('views')  # Yandex views are today's views, not total
            )
            
            print(f"✅ Объявление Yandex Realty успешно спарсено")
            return property_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга объявления Yandex Realty: {e}")
            return None
        finally:
            # Закрываем браузер
            if 'parser' in locals():
                try:
                    parser.cleanup()
                except:
                    pass
    
    async def _parse_cian_property(self, url: str) -> Optional[PropertyData]:
        """Парсит объявление с Cian"""
        try:
            print(f"🏠 Парсим объявление Cian: {url}")
            
            # Получаем страницу
            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.session.get(url)
            )
            response.raise_for_status()
            
            # Парсим HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Извлекаем данные
            data = self._extract_cian_data(soup, url)
            
            # Создаем структурированный объект
            property_data = PropertyData(
                rooms=data.get('Комнат'),
                price=data.get('Цена_raw'),
                total_area=data.get('Общая площадь'),
                living_area=data.get('Жилая площадь'),
                kitchen_area=data.get('Площадь кухни'),
                floor=data.get('Этаж'),
                total_floors=data.get('Всего этажей'),
                bathroom=data.get('Санузел'),
                balcony=data.get('Балкон/лоджия'),
                renovation=data.get('Ремонт'),
                construction_year=data.get('Год постройки'),
                house_type=data.get('Тип дома'),
                ceiling_height=data.get('Высота потолков'),
                furniture=data.get('Мебель'),
                address=data.get('Адрес'),
                metro_station=self._extract_station_from_metro_time(data.get('Минут метро')),  # Extract station name from metro_time format
                metro_time=self._extract_minutes_from_metro_time(data.get('Минут метро')),      # Extract minutes from metro_time format
                tags=data.get('Метки'),
                description=data.get('Описание'),
                photo_urls=data.get('photo_urls', []),
                source='cian',
                url=url,
                status=self._determine_status(data.get('Статус', 'active')),
                views_today=data.get('Просмотров сегодня')
            )
            
            print(f"✅ Объявление Cian успешно спарсено")
            return property_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга объявления Cian: {e}")
            return None
    
    def _extract_cian_data(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Извлекает данные из HTML страницы Cian"""
        data = {'URL': url}
        
        # Определяем возможную блокировку
        page_text = soup.get_text(" ", strip=True).lower()
        is_blocked = bool(re.search(r"подтвердите, что запросы.*не робот|похожи на автоматические", page_text))
        if is_blocked:
            data['Статус'] = None
        elif soup.find(string=re.compile(r"Объявление снято", re.IGNORECASE)):
            data['Статус'] = 'Снято'
        
        # Метки (улучшенный селектор для предотвращения дубликатов)
        labels = []
        # Пробуем разные селекторы для меток
        label_selectors = [
            'div[data-name="LabelsLayoutNew"] > span[class]',  # Прямые дочерние спаны с классами
            'div[data-name="LabelsLayoutNew"] span[data-testid]',  # Спаны с data-testid
            'div[data-name="LabelsLayoutNew"] span:not(:has(span))'  # Листовые спаны (без вложенных)
        ]
        
        for selector in label_selectors:
            try:
                spans = soup.select(selector)
                if spans:
                    labels = [span.get_text(strip=True) for span in spans if span.get_text(strip=True)]
                    break  # Используем первый работающий селектор
            except Exception:
                continue
                
        data['Метки'] = '; '.join(labels) if labels else None
        
        # Количество комнат
        h1 = soup.find('h1')
        if h1:
            m = re.search(r"(\d+)[^\d]*[-–]?комн", h1.get_text())
            if m:
                data['Комнат'] = self._extract_number(m.group(1))
        
        # Цена
        price_el = (
            soup.select_one('[data-name="NewbuildingPriceInfo"] [data-testid="price-amount"] span')
            or soup.select_one('[data-name="AsideGroup"] [data-testid="price-amount"] span')
        )
        if price_el:
            data['Цена_raw'] = self._extract_number(price_el.get_text())
            if 'Статус' not in data or data['Статус'] is None:
                data['Статус'] = 'Активно'
        
        # Основная информация
        summary = soup.select_one('[data-name="OfferSummaryInfoLayout"]')
        if summary:
            for item in summary.select('[data-name="OfferSummaryInfoItem"]'):
                ps = item.find_all('p')
                if len(ps) < 2:
                    continue
                key = ps[0].get_text(strip=True)
                val = ps[1].get_text(strip=True)
                kl = key.lower().strip()
                if key == 'Строительная серия':
                    data[key] = val
                    continue
                if kl == 'этаж': 
                    # Разбираем этаж на текущий и общий
                    floor_info = self._parse_floor_info(val)
                    data['Этаж'] = floor_info['current_floor']
                    data['Всего этажей'] = floor_info['total_floors']
                    continue
                if kl in ['санузел', 'балкон/лоджия', 'количество лифтов']:
                    data[key] = val
                    continue
                data[key] = self._extract_number(val) if re.search(r"\d", val) else val
        
        # Дополнительная информация
        cont = soup.find('div', {'data-name': 'ObjectFactoids'})
        if cont:
            lines = cont.get_text(separator='\n', strip=True).split('\n')
            for i in range(0, len(lines)-1, 2):
                key, val = lines[i].strip(), lines[i+1].strip()
                kl = key.lower().strip()
                if key == 'Строительная серия': 
                    data[key] = val
                    continue
                if kl == 'этаж' and 'Этаж' not in data: 
                    # Разбираем этаж на текущий и общий
                    floor_info = self._parse_floor_info(val)
                    data['Этаж'] = floor_info['current_floor']
                    data['Всего этажей'] = floor_info['total_floors']
                elif kl in ['санузел', 'балкон/лоджия', 'количество лифтов']: 
                    data[key] = val
                else: 
                    data[key] = self._extract_number(val) if re.search(r"\d", val) else val
        
        # Статистика просмотров
        stats_re = re.compile(r"([\d\s]+)\sпросмотр\S*,\s*(\d+)\sза сегодня,\s*(\d+)\sуникаль", re.IGNORECASE)
        st = soup.find(string=stats_re)
        if st:
            m = stats_re.search(st)
            data['Всего просмотров'], data['Просмотров сегодня'], data['Уникальных просмотров'] = (
                self._extract_number(m.group(1)), self._extract_number(m.group(2)), self._extract_number(m.group(3))
            )
        
        # География и метро
        geo = soup.select_one('div[data-name="Geo"]')
        if geo:
            span = geo.find('span', itemprop='name')
            addr = span['content'] if span and span.get('content') else ', '.join(
                a.get_text(strip=True) for a in geo.select('a[data-name="AddressItem"]')
            )
            parts = [s.strip() for s in addr.split(',') if s.strip()]
            data['Адрес'] = ', '.join(parts[-2:]) if len(parts) > 1 else addr
            
            # Метро
            stations = []
            for li in geo.select('ul[data-name="UndergroundList"] li[data-name="UndergroundItem"]'):
                st_el = li.find('a', href=True)
                tm_el = li.find('span', class_=re.compile(r".*underground_time.*"))
                if st_el and tm_el:
                    name = st_el.get_text(strip=True)
                    m = re.search(r"(\d+)", tm_el.get_text(strip=True))
                    stations.append((name, int(m.group(1)) if m else None))
            if stations:
                station, time_to = min(stations, key=lambda x: x[1] or float('inf'))
                data['Минут метро'] = f"{time_to} {station}"
        
        # Фотографии
        data['photo_urls'] = self._extract_cian_photos(soup)
        
        return data
    
    def _parse_floor_info(self, text: str) -> Dict[str, Optional[int]]:
        """Разбирает информацию об этаже на текущий и общий"""
        if not text:
            return {'current_floor': None, 'total_floors': None}
        
        s = str(text).replace('\u00A0', ' ').strip().lower()
        
        # Паттерн "13 из 37" или "13/37"
        m = re.search(r"(\d+)\s*(?:из|/)\s*(\d+)", s)
        if m:
            return {
                'current_floor': int(m.group(1)),
                'total_floors': int(m.group(2))
            }
        
        # Только текущий этаж
        m2 = re.search(r"(\d+)\b", s)
        if m2:
            return {
                'current_floor': int(m2.group(1)),
                'total_floors': None
            }
        
        return {'current_floor': None, 'total_floors': None}
    
    def _extract_cian_photos(self, soup: BeautifulSoup) -> List[str]:
        """Извлекает ссылки на фотографии с Cian"""
        photo_urls = []
        
        try:
            # Ищем галерею
            gallery = soup.find('div', {'data-name': 'GalleryInnerComponent'})
            if not gallery:
                return photo_urls
            
            # Основные изображения
            images = gallery.find_all('img', src=True)
            for img in images:
                src = img.get('src')
                if src and src.startswith('http') and 'cdn-cian.ru' in src:
                    photo_urls.append(src)
            
            # Изображения в background-image
            elements_with_bg = gallery.find_all(style=re.compile(r'background-image'))
            for elem in elements_with_bg:
                style = elem.get('style', '')
                bg_match = re.search(r'background-image:\s*url\(["\']?([^"\')\s]+)["\']?\)', style)
                if bg_match:
                    bg_url = bg_match.group(1)
                    if bg_url.startswith('http') and ('cdn-cian.ru' in bg_url or 'kinescopecdn.net' in bg_url):
                        photo_urls.append(bg_url)
            
            # Убираем дубликаты, сохраняя порядок
            seen = set()
            unique_photos = []
            for url in photo_urls:
                if url not in seen:
                    seen.add(url)
                    unique_photos.append(url)
            
            return unique_photos
            
        except Exception as e:
            print(f"Ошибка при извлечении фотографий Cian: {e}")
            return []
    
    def _extract_number(self, text: str) -> Optional[Union[int, float]]:
        """Извлекает число из текста"""
        if not text or text == '—':
            return None
        cleaned = re.sub(r"[^\d.,]", "", text)
        cleaned = cleaned.replace('\u00A0', '').replace(' ', '').replace(',', '.')
        try:
            return float(cleaned) if '.' in cleaned else int(cleaned)
        except ValueError:
            return None
    
    def cleanup(self):
        """Корректно закрывает ресурсы"""
        try:
            self.session.close()
        except Exception as e:
            print(f"⚠️ Ошибка при очистке ресурсов: {e}")
    
    def __del__(self):
        """Деструктор для автоматической очистки"""
        self.cleanup()


# ===============================================
# PERSISTENT BROWSER CLASS (встроенный)
# ===============================================

class PersistentAvitoBrowser:
    """Persistent браузер для Avito с cookies"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Singleton pattern для единственного браузера"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, 'initialized'):
            return

        self.driver = None
        self.cookies_file = "avito_cookies.json"
        self.initialized = False
        self.last_activity = time.time()
        self.session_timeout = 86400  # 24 часа без активности (практически навсегда)

        # НЕ регистрируем cleanup при выходе для "навсегда" режима
        # atexit.register(self.cleanup)  # Закомментировано для постоянной работы
        print("🔄 Persistent браузер инициализирован для постоянной работы")

    def setup_browser(self):
        """Настраивает и запускает браузер"""
        if self.driver and self._is_browser_alive():
            print("✅ Браузер уже запущен")
            return True

        try:
            print("🔧 Запускаем persistent браузер...")

            options = Options()

            # Проверяем есть ли cookies для headless режима
            has_cookies = os.path.exists(self.cookies_file)

            if has_cookies:
                # С cookies можем использовать headless для экономии памяти
                options.add_argument("--headless=new")
                print("🔒 Режим headless (есть cookies)")
            else:
                # Без cookies лучше обычный режим для обхода блокировок
                print("👁️ Обычный режим (нет cookies)")

            # Базовые настройки
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-images")  # Не загружаем изображения
            options.add_argument("--memory-pressure-off")  # Отключаем сборку мусора по памяти
            options.add_argument("--max_old_space_size=512")  # Ограничиваем память V8
            options.add_argument("--window-size=1280,720")

            # User-Agent для обхода блокировок
            options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            # Отключаем webdriver флаги
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)

            # Chrome binary path
            if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
                options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            else:
                options.binary_location = "/opt/google/chrome/google-chrome"

            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(5)

            # Убираем webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # Загружаем cookies
            self._load_and_apply_cookies()

            self.initialized = True
            self.last_activity = time.time()

            print("✅ Persistent браузер готов к работе")
            return True

        except Exception as e:
            print(f"❌ Ошибка запуска браузера: {e}")
            return False

    def _is_browser_alive(self):
        """Проверяет, жив ли браузер"""
        try:
            if not self.driver:
                return False
            # Простая проверка - получаем текущий URL
            _ = self.driver.current_url
            return True
        except:
            return False

    def _load_and_apply_cookies(self):
        """Загружает и применяет cookies"""
        try:
            if not os.path.exists(self.cookies_file):
                print("⚠️ Файл cookies не найден, создайте его вручно")
                return

            # Сначала идем на главную Avito для установки cookies
            print("🍪 Загружаем главную страницу для cookies...")
            self.driver.get("https://www.avito.ru/")
            time.sleep(2)

            # Загружаем cookies из файла
            with open(self.cookies_file, 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)

            # Применяем cookies
            if 'cookies' in cookies_data:
                cookies_list = cookies_data['cookies']
            else:
                cookies_list = cookies_data

            for cookie in cookies_list:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    print(f"⚠️ Не удалось добавить cookie: {e}")

            # Обновляем страницу для применения cookies
            self.driver.refresh()
            time.sleep(1)

            print("✅ Cookies применены")
            print("🏠 Остаемся на главной Avito для постоянной сессии")

        except Exception as e:
            print(f"❌ Ошибка загрузки cookies: {e}")

    def parse_url(self, url):
        """Быстро парсит URL с уже открытым браузером"""
        if not self.setup_browser():
            return None

        try:
            self.last_activity = time.time()

            print(f"🔄 Парсим: {url}")
            start_time = time.time()

            # Переходим на страницу
            self.driver.get(url)

            # Минимальная задержка
            time.sleep(1)

            # Получаем данные
            data = {}

            # Заголовок
            try:
                data['title'] = self.driver.title
            except:
                pass

            # H1
            try:
                h1_element = self.driver.find_element("tag name", "h1")
                data['h1'] = h1_element.text.strip()
            except:
                pass

            # Цена
            try:
                price_selectors = [
                    '[data-marker="item-view/item-price"]',
                    '[class*="price"]',
                    '[data-testid*="price"]'
                ]

                for selector in price_selectors:
                    try:
                        price_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for el in price_elements:
                            if el.is_displayed() and el.text.strip():
                                data['price'] = el.text.strip()
                                break
                        if 'price' in data:
                            break
                    except:
                        continue
            except:
                pass

            # Парсим из текста
            text = data.get('h1', '') or data.get('title', '')
            if text:
                parsed_data = self._extract_from_text(text)
                data.update(parsed_data)

            parse_time = time.time() - start_time
            print(f"⏱️ Парсинг занял: {parse_time:.2f} сек")

            return data

        except Exception as e:
            print(f"❌ Ошибка парсинга: {e}")
            return None

    def _extract_from_text(self, text):
        """Извлекает данные из текста"""
        data = {}

        # Комнаты
        rooms_match = re.search(r'(\d+)-к\.', text)
        if rooms_match:
            data['rooms'] = int(rooms_match.group(1))

        # Студии/апартаменты
        if re.search(r'\bстудия\b|\bапартаменты\b', text.lower()):
            data['rooms'] = 0

        # Площадь
        area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', text)
        if area_match:
            area_str = area_match.group(1).replace(',', '.')
            data['total_area'] = float(area_str)

        # Этаж
        floor_match = re.search(r'(\d+)/(\d+)\s*эт\.', text)
        if floor_match:
            data['floor'] = floor_match.group(1)
            data['total_floors'] = int(floor_match.group(2))

        return data

    def is_session_expired(self):
        """Проверяет, истекла ли сессия"""
        return time.time() - self.last_activity > self.session_timeout

    def get_session_info(self):
        """Возвращает информацию о текущей сессии"""
        if not self.driver:
            return {"status": "not_started", "message": "Браузер не запущен"}

        try:
            current_url = self.driver.current_url
            title = self.driver.title
            session_age = time.time() - self.last_activity

            return {
                "status": "active",
                "url": current_url,
                "title": title,
                "session_age_minutes": round(session_age / 60, 1),
                "is_on_avito": 'avito.ru' in current_url,
                "last_activity": self.last_activity
            }
        except:
            return {"status": "error", "message": "Ошибка получения информации о сессии"}

    def refresh_session(self):
        """Обновляет сессию"""
        if self.is_session_expired() or not self._is_browser_alive():
            print("🔄 Обновляем браузер сессию...")
            self.cleanup()
            return self.setup_browser()

        # Проверяем, что мы все еще на Avito
        try:
            current_url = self.driver.current_url
            if not ('avito.ru' in current_url):
                print("🔄 Возвращаемся на главную Avito...")
                self.driver.get("https://www.avito.ru/")
                time.sleep(1)
        except:
            pass

        return True

    def cleanup(self):
        """Закрывает браузер"""
        try:
            if self.driver:
                print("🧹 Закрываем persistent браузер...")
                self.driver.quit()
                self.driver = None
                print("✅ Браузер закрыт")
        except Exception as e:
            print(f"⚠️ Ошибка закрытия браузера: {e}")

    def __del__(self):
        self.cleanup()


# Глобальный экземпляр
_browser = None

def get_persistent_browser():
    """Получает экземпляр persistent браузера"""
    global _browser
    if _browser is None:
        _browser = PersistentAvitoBrowser()
    return _browser

def parse_avito_fast(url):
    """Быстрый парсинг через persistent браузер"""
    browser = get_persistent_browser()
    if not browser.refresh_session():
        return None
    return browser.parse_url(url)

# Создаем экземпляр парсера
parser = RealtyParserAPI()

# Функция для инициализации persistent браузера в фоне
def init_persistent_browser():
    """Инициализирует persistent браузер в фоновом потоке"""
    try:
        print("🔄 Инициализация persistent браузера...")
        browser = get_persistent_browser()
        if browser.setup_browser():
            print("✅ Persistent браузер запущен и готов к работе")
            print("🏠 Браузер находится на Avito с активными cookies")
        else:
            print("❌ Не удалось запустить persistent браузер")
    except Exception as e:
        print(f"❌ Ошибка инициализации persistent браузера: {e}")

# Запускаем инициализацию браузера в фоновом потоке
browser_thread = threading.Thread(target=init_persistent_browser, daemon=True)
browser_thread.start()

# Создаем FastAPI приложение
app = FastAPI(
    title="Realty Parser HTTP API",
    description="API для парсинга объявлений недвижимости с Avito и Cian",
    version="1.0.0"
)

# Настройка CORS для Fastify
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене укажите конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API endpoints
@app.post("/api/parse/urls", response_model=ParseResponse)
async def parse_by_urls(request: ParseUrlsRequest):
    """Парсинг по списку URL"""
    try:
        properties = await parser.parse_properties_batch(request.urls)
        
        return ParseResponse(
            success=True,
            data=properties,
            total=len(properties),
            message=f"Успешно спарсено {len(properties)} из {len(request.urls)} объявлений",
            timestamp=str(datetime.now())
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")

@app.post("/api/parse/text", response_model=ParseResponse)
async def parse_from_text(request: ParseTextRequest):
    """Парсинг объявлений из текста"""
    try:
        # Извлекаем URL из текста
        urls = extract_urls(request.text)
        if not urls:
            raise HTTPException(status_code=400, detail="URL не найдены в тексте")
        
        # Парсим объявления
        properties = await parser.parse_properties_batch(urls)
        
        return ParseResponse(
            success=True,
            data=properties,
            total=len(properties),
            message=f"Извлечено {len(urls)} URL, успешно спарсено {len(properties)} объявлений",
            timestamp=str(datetime.now())
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга из текста: {str(e)}")

@app.get("/api/parse/single")
async def parse_single_property(url: str):
    """Парсинг одного объявления по URL (быстрый режим)"""
    try:
        property_data = await parser.parse_property(url)
        if property_data:
            return {
                "success": True,
                "data": property_data.to_dict(),
                "message": "Объявление успешно спарсено (быстрый режим)"
            }
        else:
            raise HTTPException(status_code=400, detail="Не удалось спарсить объявление")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")

@app.get("/api/parse/extended")
async def parse_extended_property(url: str):
    """Расширенный парсинг одного объявления по URL (полные данные)"""
    try:
        property_data = await parser.parse_property_extended(url)
        if property_data:
            return {
                "success": True,
                "data": property_data.to_dict(),
                "message": "Объявление успешно спарсено (расширенный режим)"
            }
        else:
            raise HTTPException(status_code=400, detail="Не удалось спарсить объявление")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка расширенного парсинга: {str(e)}")

@app.get("/api/health")
async def health_check():
    """Проверка состояния API"""
    # Проверяем статус persistent браузера
    browser_status = "unknown"
    try:
        browser = get_persistent_browser()
        session_info = browser.get_session_info()
        browser_status = session_info.get('status', 'unknown')
    except:
        browser_status = "error"

    return {
        "status": "healthy",
        "service": "realty-parser-api",
        "avito_available": AVITO_AVAILABLE,
        "cian_available": True,
        "yandex_available": YANDEX_AVAILABLE,
        "persistent_browser": browser_status
    }

@app.get("/api/browser/status")
async def browser_status():
    """Проверка статуса persistent браузера"""
    try:
        browser = get_persistent_browser()
        session_info = browser.get_session_info()
        return {
            "success": True,
            "browser_session": session_info,
            "message": "Статус браузера получен"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Ошибка получения статуса браузера"
        }

@app.post("/api/browser/init")
async def init_browser():
    """Принудительная инициализация persistent браузера"""
    try:
        browser = get_persistent_browser()
        if browser.setup_browser():
            session_info = browser.get_session_info()
            return {
                "success": True,
                "browser_session": session_info,
                "message": "Браузер успешно инициализирован"
            }
        else:
            return {
                "success": False,
                "message": "Не удалось инициализировать браузер"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Ошибка инициализации браузера"
        }

@app.post("/api/browser/refresh")
async def refresh_browser():
    """Обновление сессии persistent браузера"""
    try:
        browser = get_persistent_browser()
        if browser.refresh_session():
            session_info = browser.get_session_info()
            return {
                "success": True,
                "browser_session": session_info,
                "message": "Сессия браузера обновлена"
            }
        else:
            return {
                "success": False,
                "message": "Не удалось обновить сессию браузера"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Ошибка обновления сессии браузера"
        }

@app.get("/api/sources")
async def get_supported_sources():
    """Получение списка поддерживаемых источников"""
    return {
        "sources": [
            {
                "name": "Avito",
                "domain": "avito.ru",
                "available": AVITO_AVAILABLE,
                "source_id": "avito"
            },
            {
                "name": "Cian",
                "domain": "cian.ru",
                "available": True,
                "source_id": "cian"
            },
            {
                "name": "Yandex Realty",
                "domain": "realty.yandex.ru",
                "available": YANDEX_AVAILABLE,
                "source_id": "yandex"
            }
        ]
    }

@app.get("/api/docs")
async def api_documentation():
    """Документация API"""
    return {
        "title": "Realty Parser HTTP API",
        "version": "1.0.0",
        "description": "API для парсинга объявлений недвижимости с Avito, Cian и Yandex Realty",
        "base_url": "http://localhost:8008",
        "endpoints": {
            "monitoring": {
                "GET /api/health": {
                    "description": "Проверка состояния API и persistent браузера",
                    "response": {
                        "status": "healthy",
                        "service": "realty-parser-api",
                        "avito_available": True,
                        "cian_available": True,
                        "yandex_available": True,
                        "persistent_browser": "active"
                    }
                },
                "GET /api/sources": {
                    "description": "Список поддерживаемых источников",
                    "response": {
                        "sources": [
                            {"name": "Avito", "domain": "avito.ru", "available": True}
                        ]
                    }
                }
            },
            "parsing": {
                "GET /api/parse/single": {
                    "description": "Быстрый парсинг одного объявления (из заголовка)",
                    "parameters": {
                        "url": "URL объявления (обязательный)"
                    },
                    "example": "/api/parse/single?url=https://www.avito.ru/moskva/kvartiry/1-k._kvartira_295_m_25_et._7627589190",
                    "response": {
                        "success": True,
                        "data": {
                            "rooms": 1,
                            "total_area": 29.5,
                            "floor": "3",
                            "total_floors": 5,
                            "price": 9000000,
                            "source": "avito",
                            "status": True
                        },
                        "message": "Объявление успешно спарсено (быстрый режим)"
                    }
                },
                "GET /api/parse/extended": {
                    "description": "Расширенный парсинг с полными данными",
                    "parameters": {
                        "url": "URL объявления (обязательный)"
                    },
                    "example": "/api/parse/extended?url=https://www.avito.ru/moskva/kvartiry/1-k._kvartira_295_m_25_et._7627589190",
                    "response": {
                        "success": True,
                        "data": {
                            "rooms": 1,
                            "price": 9000000,
                            "total_area": 29.5,
                            "address": "Москва, 3-я Рыбинская ул., 30",
                            "metro_station": "Митьково",
                            "description": "...",
                            "source": "avito"
                        },
                        "message": "Объявление успешно спарсено (расширенный режим)"
                    }
                },
                "POST /api/parse/urls": {
                    "description": "Пакетный парсинг списка URL",
                    "body": {
                        "urls": ["url1", "url2", "..."]
                    },
                    "response": {
                        "success": True,
                        "data": ["array of PropertyData objects"],
                        "total": 2,
                        "message": "Успешно спарсено 2 из 2 объявлений"
                    }
                },
                "POST /api/parse/text": {
                    "description": "Парсинг URL из текста",
                    "body": {
                        "text": "Текст с URL объявлений"
                    },
                    "response": {
                        "success": True,
                        "data": ["array of PropertyData objects"],
                        "total": 1,
                        "message": "Извлечено 1 URL, успешно спарсено 1 объявлений"
                    }
                }
            },
            "browser_management": {
                "GET /api/browser/status": {
                    "description": "Подробный статус persistent браузера",
                    "response": {
                        "success": True,
                        "browser_session": {
                            "status": "active",
                            "url": "https://www.avito.ru/",
                            "title": "Авито",
                            "session_age_minutes": 15.5,
                            "is_on_avito": True
                        },
                        "message": "Статус браузера получен"
                    }
                },
                "POST /api/browser/init": {
                    "description": "Принудительная инициализация браузера",
                    "response": {
                        "success": True,
                        "browser_session": {"status": "active"},
                        "message": "Браузер успешно инициализирован"
                    }
                },
                "POST /api/browser/refresh": {
                    "description": "Обновление сессии браузера",
                    "response": {
                        "success": True,
                        "browser_session": {"status": "active"},
                        "message": "Сессия браузера обновлена"
                    }
                }
            }
        },
        "data_structure": {
            "PropertyData": {
                "description": "Структура данных объявления",
                "fields": {
                    "rooms": "Количество комнат (int, 0 для студий)",
                    "price": "Цена в рублях (float)",
                    "total_area": "Общая площадь в м² (float)",
                    "living_area": "Жилая площадь в м² (float)",
                    "kitchen_area": "Площадь кухни в м² (float)",
                    "floor": "Этаж (string)",
                    "total_floors": "Этажей в доме (int)",
                    "bathroom": "Тип санузла (string)",
                    "balcony": "Балкон/лоджия (string)",
                    "renovation": "Тип ремонта (string)",
                    "construction_year": "Год постройки (int)",
                    "house_type": "Тип дома (string)",
                    "ceiling_height": "Высота потолков в м (float)",
                    "furniture": "Мебель (string)",
                    "address": "Адрес (string)",
                    "metro_station": "Ближайшая станция метро (string)",
                    "metro_time": "Время до метро в минутах (int)",
                    "metro_way": "Способ добраться до метро (string)",
                    "tags": "Метки объявления (array of strings)",
                    "description": "Описание (string)",
                    "photo_urls": "Ссылки на фото (array of strings)",
                    "source": "Источник: avito/cian/yandex (string)",
                    "url": "URL объявления (string)",
                    "status": "Активность объявления (boolean)",
                    "views_today": "Просмотров сегодня (int)"
                }
            }
        },
        "features": {
            "fast_parsing": "Быстрый парсинг из заголовка (3-5 сек)",
            "persistent_browser": "Постоянный браузер с cookies для обхода блокировок",
            "multiple_sources": "Поддержка Avito, Cian, Yandex Realty",
            "batch_processing": "Пакетная обработка множества URL",
            "auto_extraction": "Автоматическое извлечение URL из текста"
        },
        "performance": {
            "fast_mode": "3-5 секунд (только заголовок)",
            "extended_mode": "10-30 секунд (полные данные)",
            "memory_usage": "~435 MB (persistent браузер)",
            "concurrent_requests": "Поддерживается"
        }
    }

@app.get("/", response_class=HTMLResponse)
async def api_docs_html():
    """HTML документация API"""
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Realty Parser API - Документация</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 1200px; margin: 0 auto; padding: 20px; }
            h1, h2, h3 { color: #2c3e50; }
            .endpoint { background: #f8f9fa; border-left: 4px solid #007bff; padding: 15px; margin: 10px 0; border-radius: 5px; }
            .method { display: inline-block; padding: 4px 8px; border-radius: 3px; font-weight: bold; color: white; font-size: 12px; }
            .get { background: #28a745; }
            .post { background: #007bff; }
            code { background: #f1f1f1; padding: 2px 4px; border-radius: 3px; font-family: 'Monaco', 'Consolas', monospace; }
            .example { background: #e9ecef; padding: 10px; border-radius: 5px; overflow-x: auto; }
            .status { display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 11px; }
            .active { background: #d4edda; color: #155724; }
            .fast { background: #fff3cd; color: #856404; }
            .extended { background: #f8d7da; color: #721c24; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; text-align: center; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }
            .card { background: white; border: 1px solid #e1e5e9; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .badge { display: inline-block; padding: 3px 8px; background: #e9ecef; border-radius: 12px; font-size: 12px; margin: 2px; }
            .success { color: #28a745; }
            .warning { color: #ffc107; }
            .error { color: #dc3545; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🏠 Realty Parser API</h1>
            <p>HTTP API для парсинга объявлений недвижимости</p>
            <p><strong>Поддерживает:</strong> Avito, Cian, Yandex Realty</p>
        </div>

        <div class="grid">
            <div class="card">
                <h3>🚀 Быстрый старт</h3>
                <div class="example">
                    <strong>Проверка статуса:</strong><br>
                    <code>GET /api/health</code><br><br>
                    <strong>Быстрый парсинг:</strong><br>
                    <code>GET /api/parse/single?url=https://www.avito.ru/...</code><br><br>
                    <strong>Статус браузера:</strong><br>
                    <code>GET /api/browser/status</code>
                </div>
            </div>

            <div class="card">
                <h3>📊 Производительность</h3>
                <span class="badge fast">Быстрый режим: 3-5 сек</span><br>
                <span class="badge extended">Расширенный: 10-30 сек</span><br>
                <span class="badge">Память: ~435 MB</span><br>
                <span class="badge active">Persistent браузер</span>
            </div>
        </div>

        <h2>📡 Endpoints</h2>

        <h3>🔍 Мониторинг</h3>
        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/health</code>
            <p>Проверка состояния API и persistent браузера</p>
        </div>

        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/sources</code>
            <p>Список поддерживаемых источников</p>
        </div>

        <h3>🏠 Парсинг объявлений</h3>
        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/parse/single?url={URL}</code>
            <p><strong>Быстрый парсинг</strong> - извлекает основные данные из заголовка (3-5 сек)</p>
            <div class="example">
                <strong>Возвращает:</strong> комнаты, площадь, этаж, цену (для Avito)
            </div>
        </div>

        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/parse/extended?url={URL}</code>
            <p><strong>Расширенный парсинг</strong> - полные данные со страницы (10-30 сек)</p>
            <div class="example">
                <strong>Возвращает:</strong> все поля + адрес, метро, описание, фото
            </div>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/parse/urls</code>
            <p><strong>Пакетный парсинг</strong> списка URL</p>
            <div class="example">
                <strong>Body:</strong> <code>{"urls": ["url1", "url2", ...]}</code>
            </div>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/parse/text</code>
            <p><strong>Парсинг из текста</strong> - автоматически извлекает URL</p>
            <div class="example">
                <strong>Body:</strong> <code>{"text": "Текст с URL объявлений"}</code>
            </div>
        </div>

        <h3>🔧 Управление браузером</h3>
        <div class="endpoint">
            <span class="method get">GET</span> <code>/api/browser/status</code>
            <p>Подробный статус persistent браузера</p>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/browser/init</code>
            <p>Принудительная инициализация браузера</p>
        </div>

        <div class="endpoint">
            <span class="method post">POST</span> <code>/api/browser/refresh</code>
            <p>Обновление сессии браузера</p>
        </div>

        <h2>📋 Структура данных</h2>
        <div class="card">
            <h3>PropertyData</h3>
            <div class="grid">
                <div>
                    <strong>Основные поля:</strong><br>
                    <span class="badge">rooms</span> - количество комнат<br>
                    <span class="badge">price</span> - цена в рублях<br>
                    <span class="badge">total_area</span> - общая площадь<br>
                    <span class="badge">floor</span> - этаж<br>
                    <span class="badge">source</span> - avito/cian/yandex<br>
                    <span class="badge">status</span> - активность объявления
                </div>
                <div>
                    <strong>Дополнительные:</strong><br>
                    <span class="badge">address</span> - адрес<br>
                    <span class="badge">metro_station</span> - метро<br>
                    <span class="badge">description</span> - описание<br>
                    <span class="badge">photo_urls</span> - фотографии<br>
                    <span class="badge">renovation</span> - ремонт<br>
                    <span class="badge">house_type</span> - тип дома
                </div>
            </div>
        </div>

        <h2>🌟 Особенности</h2>
        <div class="grid">
            <div class="card">
                <h3>⚡ Быстрый парсинг</h3>
                <p>Извлечение данных из заголовка страницы за 3-5 секунд</p>
            </div>
            <div class="card">
                <h3>🍪 Persistent браузер</h3>
                <p>Постоянный браузер с cookies для обхода блокировок</p>
            </div>
            <div class="card">
                <h3>🔄 Автообновление</h3>
                <p>Автоматическое обновление сессии и возврат на Avito</p>
            </div>
            <div class="card">
                <h3>📦 Пакетная обработка</h3>
                <p>Обработка множества URL одним запросом</p>
            </div>
        </div>

        <h2>🔗 Полезные ссылки</h2>
        <div class="example">
            📊 <a href="/api/health">Статус API</a><br>
            🔍 <a href="/api/browser/status">Статус браузера</a><br>
            📚 <a href="/api/docs">JSON документация</a><br>
            🎯 <a href="/api/sources">Поддерживаемые источники</a>
        </div>

        <footer style="margin-top: 50px; text-align: center; color: #6c757d; border-top: 1px solid #e1e5e9; padding-top: 20px;">
            <p>Realty Parser API v1.0.0 | Поддержка: Avito, Cian, Yandex Realty</p>
        </footer>
    </body>
    </html>
    """

# Функции для быстрого доступа (для использования в других Python модулях)
async def parse_property(url: str) -> Optional[PropertyData]:
    """Быстрый парсинг одного объявления"""
    return await parser.parse_property(url)

async def parse_property_extended(url: str) -> Optional[PropertyData]:
    """Расширенный парсинг одного объявления"""
    return await parser.parse_property_extended(url)

async def parse_properties_batch(urls: List[str]) -> List[PropertyData]:
    """Быстрый пакетный парсинг"""
    return await parser.parse_properties_batch(urls)

def extract_urls(raw_input: str) -> List[str]:
    """Извлекает URL из текста"""
    return re.findall(r'https?://[^\s,;]+', raw_input)

# Создаем экземпляр класса для использования в других модулях
realty_parser = RealtyParserAPI()

if __name__ == "__main__":
    print("🚀 Запуск Realty Parser Server")
    print("=" * 60)
    print("🌐 HTTP API: http://localhost:8008")
    print("📚 Документация: http://localhost:8008/")
    print("📊 Статус API: http://localhost:8008/api/health")
    print("🔍 Статус браузера: http://localhost:8008/api/browser/status")
    print("🔄 Persistent браузер инициализируется в фоне...")
    print("=" * 60)

    # Запуск HTTP сервера
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8008,
        log_level="info"
    )
