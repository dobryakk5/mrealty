"""Core parsing logic for the realty parser server."""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Union

import requests
from bs4 import BeautifulSoup

from cian_http_client import fetch_cian_page
from models import PropertyData
from persistent_browser import parse_avito_fast

# Импортируем парсер Avito
try:
    from avito_parser_integration import AvitoCardParser

    AVITO_AVAILABLE = True
except ImportError:
    AvitoCardParser = None
    AVITO_AVAILABLE = False
    print("⚠️ Модуль avito_parser_integration не найден, парсинг Avito недоступен")

# Импортируем парсер Yandex
try:
    from yandex_parser_integration import YandexCardParser

    YANDEX_AVAILABLE = True
except ImportError:
    YandexCardParser = None
    YANDEX_AVAILABLE = False
    print("⚠️ Модуль yandex_parser_integration не найден, парсинг Yandex недоступен")

# Импортируем парсер Baza Winner (для совместимости)
try:
    from baza_winner_parser import BazaWinnerParser  # noqa: F401

    BAZA_WINNER_AVAILABLE = True
except ImportError:
    BazaWinnerParser = None
    BAZA_WINNER_AVAILABLE = False
    print("⚠️ Модуль baza_winner_parser не найден, парсинг Baza Winner недоступен")

# Импортируем расширенный сборщик данных
try:
    from extended_data_collector import get_property_by_guid

    EXTENDED_COLLECTOR_AVAILABLE = True
except ImportError:
    get_property_by_guid = None
    EXTENDED_COLLECTOR_AVAILABLE = False
    print("⚠️ Модуль extended_data_collector не найден, получение данных по GUID недоступно")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
}


class RealtyParserAPI:
    """API класс для парсинга недвижимости."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def is_avito_url(self, url: str) -> bool:
        """Определяет, является ли ссылка ссылкой на Avito."""

        return "avito.ru" in url.lower()

    def is_cian_url(self, url: str) -> bool:
        """Определяет, является ли ссылка ссылкой на Cian."""

        return "cian.ru" in url.lower()

    def is_yandex_url(self, url: str) -> bool:
        """Определяет, является ли ссылка ссылкой на Yandex Realty."""

        return "realty.yandex.ru" in url.lower() or "realty.ya.ru" in url.lower()

    def _extract_station_from_metro_time(self, metro_time: str) -> Optional[str]:
        """Извлекает название станции из формата '6 Текстильщики'."""

        if not metro_time or not isinstance(metro_time, str):
            return None

        parts = metro_time.strip().split(" ", 1)
        if len(parts) >= 2 and parts[0].isdigit():
            return parts[1]
        return None

    def _extract_minutes_from_metro_time(self, metro_time: str) -> Optional[int]:
        """Извлекает минуты из формата '6 Текстильщики'."""

        if not metro_time or not isinstance(metro_time, str):
            return None

        parts = metro_time.strip().split(" ", 1)
        if len(parts) >= 1 and parts[0].isdigit():
            try:
                return int(parts[0])
            except ValueError:
                return None
        return None

    def get_url_source(self, url: str) -> str:
        """Возвращает источник ссылки."""

        if self.is_avito_url(url):
            return "avito"
        if self.is_cian_url(url):
            return "cian"
        if self.is_yandex_url(url):
            return "yandex"
        return "unknown"

    def _determine_status(self, status_str: Optional[str]) -> bool:
        """Определяет активность объявления по строковому статусу."""

        if not status_str:
            return True

        status_lower = status_str.lower().strip()

        if status_lower == "inactive":
            return False
        if status_lower == "active":
            return True

        inactive_statuses = [
            "снято",
            "неактивно",
            "архив",
            "удалено",
            "продано",
            "сдано",
            "неактуальное",
            "заблокировано",
            "устарело",
            "inactive",
        ]

        return not any(inactive_status in status_lower for inactive_status in inactive_statuses)

    async def parse_property(self, url: str, skip_photos: bool = True) -> Optional[PropertyData]:
        """Быстрый парсинг объявления."""

        try:
            if self.is_avito_url(url):
                return await self._parse_avito_property(url, skip_photos=skip_photos)
            if self.is_cian_url(url):
                return await self._parse_cian_property(url)
            if self.is_yandex_url(url):
                return await self._parse_yandex_property_quick(url)
            print(f"⚠️ Неизвестный источник ссылки: {url}")
            return None
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка парсинга {url}: {exc}")
            return None

    async def parse_property_extended(self, url: str, skip_photos: bool = True) -> Optional[PropertyData]:
        """Расширенный парсинг объявления."""

        try:
            if self.is_avito_url(url):
                return await self._parse_avito_extended(url, skip_photos=skip_photos)
            if self.is_cian_url(url):
                return await self._parse_cian_property(url)
            if self.is_yandex_url(url):
                return await self._parse_yandex_property(url)
            print(f"⚠️ Неизвестный источник ссылки: {url}")
            return None
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка расширенного парсинга {url}: {exc}")
            return None

    async def parse_property_flat_state(self, url: str) -> Optional[PropertyData]:
        """Парсит только цену, статус и просмотры объявления."""

        try:
            if self.is_avito_url(url):
                data = await self._parse_avito_extended(url, skip_photos=True)
            elif self.is_cian_url(url):
                data = await self._parse_cian_flat_state(url)
            elif self.is_yandex_url(url):
                data = await self._parse_yandex_property(url)
            else:
                print(f"⚠️ Неизвестный источник ссылки (flat_state): {url}")
                return None

            if data:
                return PropertyData(
                    price=data.price,
                    status=data.status,
                    views_today=data.views_today,
                    url=url,
                )
            return None
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка flat_state парсинга {url}: {exc}")
            return None

    async def parse_properties_batch(self, urls: List[str], skip_photos: bool = True) -> List[PropertyData]:
        """Пакетный парсинг множественных объявлений."""

        results: List[PropertyData] = []
        for i, url in enumerate(urls, 1):
            try:
                print(f"🔄 Парсим объявление {i}/{len(urls)}: {url}")
                property_data = await self.parse_property(url, skip_photos=skip_photos)
                if property_data:
                    results.append(property_data)
                    print("✅ Объявление успешно спарсено")
                else:
                    print("❌ Не удалось спарсить объявление")
            except Exception as exc:  # noqa: BLE001
                print(f"❌ Ошибка при парсинге {url}: {exc}")
                continue

        print(f"📊 Всего успешно спарсено: {len(results)} из {len(urls)}")
        return results

    async def _parse_avito_light(self, url: str) -> Optional[PropertyData]:
        """Легкий парсер Avito через persistent браузер."""

        try:
            print(f"🔍 Легкий парсинг Avito (persistent): {url}")
            data = parse_avito_fast(url)

            if data:
                title = data.get("title", "")
                h1 = data.get("h1", "")
                price_text = data.get("price", "")
                parsed_data = self._extract_data_from_title(title, h1)

                if "rooms" in data:
                    parsed_data["rooms"] = data["rooms"]
                if "total_area" in data:
                    parsed_data["total_area"] = data["total_area"]
                if "floor" in data:
                    parsed_data["floor"] = data["floor"]
                if "total_floors" in data:
                    parsed_data["total_floors"] = data["total_floors"]

                price = None
                if price_text:
                    price_match = re.search(r"(\d[\d\s]*)", price_text.replace("\u00a0", " "))
                    if price_match:
                        price_str = price_match.group(1).replace(" ", "")
                        try:
                            price = float(price_str)
                        except Exception:
                            pass

                if parsed_data:
                    has_rooms = parsed_data.get("rooms") is not None
                    status = has_rooms

                    print(f"📊 Статус объявления: {'активно' if status else 'неактивно'} (комнаты: {parsed_data.get('rooms')})")

                    return PropertyData(
                        rooms=parsed_data.get("rooms"),
                        price=price,
                        total_area=parsed_data.get("total_area"),
                        floor=parsed_data.get("floor"),
                        total_floors=parsed_data.get("total_floors"),
                        source="avito",
                        url=url,
                        status=status,
                    )
            return None

        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка легкого парсинга (persistent): {exc}")
            return None

    def _extract_data_from_title(self, title: str, h1: str) -> Dict[str, Any]:
        """Извлекает данные из заголовка и H1 Avito."""

        try:
            text = h1 if h1 else title
            print(f"🔍 Анализируем текст: {text}")

            data: Dict[str, Any] = {}

            rooms_match = re.search(r"(\d+)-к\.", text)
            if rooms_match:
                data["rooms"] = int(rooms_match.group(1))
                print(f"🏠 Комнат: {data['rooms']}")

            if re.search(r"\bстудия\b|\bапартаменты\b", text.lower()):
                data["rooms"] = 0
                print("🏠 Тип жилья: студия/апартаменты (комнат: 0)")

            area_match = re.search(r"(\d+(?:[.,]\d+)?)\s*м²", text)
            if area_match:
                area_str = area_match.group(1).replace(",", ".")
                data["total_area"] = float(area_str)
                print(f"📐 Площадь: {data['total_area']} м²")

            floor_match = re.search(r"(\d+)/(\d+)\s*эт\.", text)
            if floor_match:
                data["floor"] = floor_match.group(1)
                data["total_floors"] = int(floor_match.group(2))
                print(f"🏢 Этаж: {data['floor']}/{data['total_floors']}")

            return data

        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка извлечения данных: {exc}")
            return {}

    async def _parse_avito_property(self, url: str, skip_photos: bool = True) -> Optional[PropertyData]:
        """Парсит объявление с Avito (только легкий парсер)."""

        return await self._parse_avito_light(url)

    async def _parse_avito_extended(self, url: str, skip_photos: bool = True) -> Optional[PropertyData]:
        """Расширенный парсинг объявления с Avito (полный парсер)."""

        if not AVITO_AVAILABLE or AvitoCardParser is None:
            print("❌ Парсер Avito недоступен")
            return None

        try:
            print(f"🏠 Парсим объявление Avito (расширенный парсер): {url}")
            parser = AvitoCardParser(skip_photos=skip_photos)
            parsed_data = parser.parse_avito_page(url)
            if not parsed_data:
                print("❌ Не удалось спарсить данные объявления Avito")
                return None

            db_data = parser.prepare_data_for_db(parsed_data)
            print("✅ Использованы данные Selenium парсинга")
            if not db_data:
                print("❌ Не удалось подготовить данные для БД")
                return None

            price = db_data.get("price")
            is_active = price not in (None, "", 0)

            property_data = PropertyData(
                rooms=db_data.get("rooms"),
                price=price,
                total_area=db_data.get("total_area"),
                living_area=db_data.get("living_area"),
                kitchen_area=db_data.get("kitchen_area"),
                floor=db_data.get("floor"),
                total_floors=db_data.get("total_floors"),
                bathroom=db_data.get("bathroom"),
                balcony=db_data.get("balcony"),
                renovation=db_data.get("renovation"),
                construction_year=db_data.get("construction_year"),
                house_type=db_data.get("house_type"),
                ceiling_height=db_data.get("ceiling_height"),
                furniture=db_data.get("furniture"),
                address=db_data.get("address"),
                metro_station=db_data.get("metro_station"),
                metro_time=db_data.get("metro_time"),
                metro_way=db_data.get("metro_way"),
                tags=db_data.get("tags"),
                description=db_data.get("description"),
                photo_urls=db_data.get("photo_urls"),
                source="avito",
                url=url,
                status=is_active,
                views_today=db_data.get("today_views"),
            )

            print("✅ Объявление Avito успешно спарсено")
            return property_data

        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка парсинга объявления Avito: {exc}")
            return None
        finally:
            if "parser" in locals() and getattr(parser, "driver", None):
                try:
                    parser.cleanup()
                except Exception:
                    pass

    async def _parse_yandex_property_quick(self, url: str) -> Optional[PropertyData]:
        """Быстро парсит цену и статус объявления с Yandex Realty."""

        if not YANDEX_AVAILABLE or YandexCardParser is None:
            print("❌ Парсер Yandex недоступен")
            return None

        try:
            print(f"⚡ Быстрый парсинг Yandex Realty: {url}")
            parser = YandexCardParser()
            parsed_data = parser.parse_yandex_quick(url)
            if not parsed_data:
                print("❌ Не удалось быстро спарсить данные объявления Yandex Realty")
                return None

            db_data = parser.prepare_quick_data_for_db(parsed_data)
            if not db_data:
                print("❌ Не удалось подготовить быстрые данные для БД")
                return None

            yandex_status = self._determine_status(db_data.get("status"))
            price = db_data.get("price")

            return PropertyData(
                price=price,
                source="yandex",
                url=url,
                status=yandex_status,
            )

        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка быстрого парсинга объявления Yandex Realty: {exc}")
            return None
        finally:
            if "parser" in locals():
                try:
                    parser.cleanup()
                except Exception:
                    pass

    async def _parse_yandex_property(self, url: str) -> Optional[PropertyData]:
        """Парсит объявление с Yandex Realty (полные данные)."""

        if not YANDEX_AVAILABLE or YandexCardParser is None:
            print("❌ Парсер Yandex недоступен")
            return None

        try:
            print(f"🏠 Парсим объявление Yandex Realty: {url}")
            parser = YandexCardParser()
            parsed_data = parser.parse_yandex_page(url)
            if not parsed_data:
                print("❌ Не удалось спарсить данные объявления Yandex Realty")
                return None

            db_data = parser.prepare_data_for_db(parsed_data)
            if not db_data:
                print("❌ Не удалось подготовить данные для БД")
                return None

            yandex_status = self._determine_status(db_data.get("status"))
            price = db_data.get("price")

            property_data = PropertyData(
                rooms=db_data.get("rooms"),
                price=price,
                total_area=db_data.get("area_total"),
                living_area=db_data.get("living_area"),
                kitchen_area=db_data.get("kitchen_area"),
                floor=db_data.get("floor"),
                total_floors=db_data.get("floor_total"),
                bathroom=db_data.get("bathroom"),
                balcony=db_data.get("balcony"),
                renovation=db_data.get("renovation"),
                construction_year=db_data.get("year_built"),
                house_type=db_data.get("house_type"),
                address=db_data.get("address"),
                metro_station=self._extract_station_from_metro_time(db_data.get("metro_time")),
                metro_time=self._extract_minutes_from_metro_time(db_data.get("metro_time")),
                description=db_data.get("description"),
                source="yandex",
                url=url,
                status=yandex_status,
                views_today=db_data.get("views"),
            )

            print("✅ Объявление Yandex Realty успешно спарсено")
            return property_data

        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка парсинга объявления Yandex Realty: {exc}")
            return None
        finally:
            if "parser" in locals():
                try:
                    parser.cleanup()
                except Exception:
                    pass

    async def _parse_cian_property(self, url: str) -> Optional[PropertyData]:
        """Парсит объявление с Cian."""

        try:
            print(f"🏠 Парсим объявление Cian: {url}")

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: fetch_cian_page(
                    url,
                    headers=dict(self.session.headers),
                    cookies=None,
                    proxy=None,
                    timeout=30,
                ),
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            data = self._extract_cian_data(soup, url)

            property_data = PropertyData(
                rooms=data.get("Комнат"),
                price=data.get("Цена_raw"),
                total_area=data.get("Общая площадь"),
                living_area=data.get("Жилая площадь"),
                kitchen_area=data.get("Площадь кухни"),
                floor=data.get("Этаж"),
                total_floors=data.get("Всего этажей"),
                bathroom=data.get("Санузел"),
                balcony=data.get("Балкон/лоджия"),
                renovation=data.get("Ремонт"),
                construction_year=data.get("Год постройки"),
                house_type=data.get("Тип дома"),
                ceiling_height=data.get("Высота потолков"),
                furniture=data.get("Мебель"),
                address=data.get("Адрес"),
                metro_station=self._extract_station_from_metro_time(data.get("Минут метро")),
                metro_time=self._extract_minutes_from_metro_time(data.get("Минут метро")),
                tags=data.get("Метки"),
                description=data.get("Описание"),
                photo_urls=data.get("photo_urls", []),
                source="cian",
                url=url,
                status=self._determine_status(data.get("Статус", "active")),
                views_today=data.get("Просмотров сегодня"),
            )

            print("✅ Объявление Cian успешно спарсено")
            return property_data

        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка парсинга объявления Cian: {exc}")
            return None

    async def _parse_cian_flat_state(self, url: str) -> Optional[PropertyData]:
        """Парсит только цену/статус/просмотры с Cian."""

        try:
            print(f"🏠 Парсим flat_state Cian: {url}")
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: fetch_cian_page(
                    url,
                    headers=dict(self.session.headers),
                    cookies=None,
                    proxy=None,
                    timeout=30,
                ),
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            data = self._extract_cian_data(soup, url)
            price = data.get("Цена_raw")
            status = self._determine_status(data.get("Статус", "active"))
            views_today = data.get("Просмотров сегодня")

            return PropertyData(
                price=price,
                status=status,
                views_today=views_today,
                source="cian",
                url=url,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка flat_state парсинга Cian: {exc}")
            return None

    def _extract_cian_data(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Извлекает данные из HTML страницы Cian."""

        data: Dict[str, Any] = {"URL": url}

        page_text = soup.get_text(" ", strip=True).lower()
        is_blocked = bool(re.search(r"подтвердите, что запросы.*не робот|похожи на автоматические", page_text))
        if is_blocked:
            data["Статус"] = None
        elif soup.find(string=re.compile(r"Объявление снято", re.IGNORECASE)):
            data["Статус"] = "Снято"

        labels: List[str] = []
        label_selectors = [
            'div[data-name="LabelsLayoutNew"] > span[class]',
            'div[data-name="LabelsLayoutNew"] span[data-testid]',
            'div[data-name="LabelsLayoutNew"] span:not(:has(span))',
        ]

        for selector in label_selectors:
            try:
                spans = soup.select(selector)
                if spans:
                    labels = [span.get_text(strip=True) for span in spans if span.get_text(strip=True)]
                    break
            except Exception:
                continue

        data["Метки"] = "; ".join(labels) if labels else None

        h1 = soup.find("h1")
        if h1:
            match = re.search(r"(\d+)[^\d]*[-–]?комн", h1.get_text())
            if match:
                data["Комнат"] = self._extract_number(match.group(1))

        price_el = (
            soup.select_one('[data-name="NewbuildingPriceInfo"] [data-testid="price-amount"] span')
            or soup.select_one('[data-name="AsideGroup"] [data-testid="price-amount"] span')
        )
        if price_el:
            data["Цена_raw"] = self._extract_number(price_el.get_text())
            if "Статус" not in data or data["Статус"] is None:
                data["Статус"] = "Активно"

        summary = soup.select_one('[data-name="OfferSummaryInfoLayout"]')
        if summary:
            for item in summary.select('[data-name="OfferSummaryInfoItem"]'):
                ps = item.find_all("p")
                if len(ps) < 2:
                    continue
                key = ps[0].get_text(strip=True)
                val = ps[1].get_text(strip=True)
                kl = key.lower().strip()
                if key == "Строительная серия":
                    data[key] = val
                    continue
                if kl == "этаж":
                    floor_info = self._parse_floor_info(val)
                    data["Этаж"] = floor_info["current_floor"]
                    data["Всего этажей"] = floor_info["total_floors"]
                    continue
                if kl in ["санузел", "балкон/лоджия", "количество лифтов"]:
                    data[key] = val
                    continue
                data[key] = self._extract_number(val) if re.search(r"\d", val) else val

        cont = soup.find("div", {"data-name": "ObjectFactoids"})
        if cont:
            lines = cont.get_text(separator="\n", strip=True).split("\n")
            for i in range(0, len(lines) - 1, 2):
                key, val = lines[i].strip(), lines[i + 1].strip()
                kl = key.lower().strip()
                if key == "Строительная серия":
                    data[key] = val
                    continue
                if kl == "этаж" and "Этаж" not in data:
                    floor_info = self._parse_floor_info(val)
                    data["Этаж"] = floor_info["current_floor"]
                    data["Всего этажей"] = floor_info["total_floors"]
                elif kl in ["санузел", "балкон/лоджия", "количество лифтов"]:
                    data[key] = val
                else:
                    data[key] = self._extract_number(val) if re.search(r"\d", val) else val

        stats_re = re.compile(r"([\d\s]+)\sпросмотр\S*,\s*(\d+)\sза сегодня,\s*(\d+)\sуникаль", re.IGNORECASE)
        stats_text = soup.find(string=stats_re)
        if stats_text:
            match = stats_re.search(stats_text)
            data["Всего просмотров"], data["Просмотров сегодня"], data["Уникальных просмотров"] = (
                self._extract_number(match.group(1)),
                self._extract_number(match.group(2)),
                self._extract_number(match.group(3)),
            )

        geo = soup.select_one('div[data-name="Geo"]')
        if geo:
            span = geo.find("span", itemprop="name")
            addr = span["content"] if span and span.get("content") else ", ".join(
                a.get_text(strip=True) for a in geo.select('a[data-name="AddressItem"]')
            )
            parts = [s.strip() for s in addr.split(",") if s.strip()]
            data["Адрес"] = ", ".join(parts[-2:]) if len(parts) > 1 else addr

            stations = []
            for li in geo.select('ul[data-name="UndergroundList"] li[data-name="UndergroundItem"]'):
                station_el = li.find("a", href=True)
                time_el = li.find("span", class_=re.compile(r".*underground_time.*"))
                if station_el and time_el:
                    name = station_el.get_text(strip=True)
                    match = re.search(r"(\d+)", time_el.get_text(strip=True))
                    stations.append((name, int(match.group(1)) if match else None))
            if stations:
                station, time_to = min(stations, key=lambda value: value[1] or float("inf"))
                data["Минут метро"] = f"{time_to} {station}"

        data["photo_urls"] = self._extract_cian_photos(soup)
        return data

    def _parse_floor_info(self, text: str) -> Dict[str, Optional[int]]:
        """Разбирает информацию об этаже на текущий и общий."""

        if not text:
            return {"current_floor": None, "total_floors": None}

        normalized = str(text).replace("\u00A0", " ").strip().lower()
        match = re.search(r"(\d+)\s*(?:из|/)\s*(\d+)", normalized)
        if match:
            return {"current_floor": int(match.group(1)), "total_floors": int(match.group(2))}

        match = re.search(r"(\d+)\b", normalized)
        if match:
            return {"current_floor": int(match.group(1)), "total_floors": None}

        return {"current_floor": None, "total_floors": None}

    def _extract_cian_photos(self, soup: BeautifulSoup) -> List[str]:
        """Извлекает ссылки на фотографии с Cian."""

        photo_urls: List[str] = []

        try:
            gallery = soup.find("div", {"data-name": "GalleryInnerComponent"})
            if not gallery:
                return photo_urls

            images = gallery.find_all("img", src=True)
            for img in images:
                src = img.get("src")
                if src and src.startswith("http") and "cdn-cian.ru" in src:
                    photo_urls.append(src)

            elements_with_bg = gallery.find_all(style=re.compile(r"background-image"))
            for element in elements_with_bg:
                style = element.get("style", "")
                bg_match = re.search(r'background-image:\s*url\(["\']?([^"\')\s]+)["\']?\)', style)
                if bg_match:
                    bg_url = bg_match.group(1)
                    if bg_url.startswith("http") and ("cdn-cian.ru" in bg_url or "kinescopecdn.net" in bg_url):
                        photo_urls.append(bg_url)

            seen = set()
            unique_photos: List[str] = []
            for photo_url in photo_urls:
                if photo_url not in seen:
                    seen.add(photo_url)
                    unique_photos.append(photo_url)

            return unique_photos

        except Exception as exc:  # noqa: BLE001
            print(f"Ошибка при извлечении фотографий Cian: {exc}")
            return []

    def _extract_number(self, text: str) -> Optional[Union[int, float]]:
        """Извлекает число из текста."""

        if not text or text == "—":
            return None
        cleaned = re.sub(r"[^\d.,]", "", text)
        cleaned = cleaned.replace("\u00A0", "").replace(" ", "").replace(",", ".")
        try:
            return float(cleaned) if "." in cleaned else int(cleaned)
        except ValueError:
            return None

    def cleanup(self) -> None:
        """Корректно закрывает ресурсы."""

        try:
            self.session.close()
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ Ошибка при очистке ресурсов: {exc}")

    def __del__(self):
        self.cleanup()


parser = RealtyParserAPI()


async def parse_property(url: str, skip_photos: bool = True) -> Optional[PropertyData]:
    """Быстрый парсинг одного объявления."""

    return await parser.parse_property(url, skip_photos=skip_photos)


async def parse_property_extended(url: str, skip_photos: bool = True) -> Optional[PropertyData]:
    """Расширенный парсинг одного объявления."""

    return await parser.parse_property_extended(url, skip_photos=skip_photos)


async def parse_property_flat_state(url: str) -> Optional[PropertyData]:
    """Минимальный парсинг объявления: цена + статус + просмотры."""

    return await parser.parse_property_flat_state(url)


async def parse_properties_batch(urls: List[str], skip_photos: bool = True) -> List[PropertyData]:
    """Быстрый пакетный парсинг."""

    return await parser.parse_properties_batch(urls, skip_photos=skip_photos)


def extract_urls(raw_input: str) -> List[str]:
    """Извлекает URL из текста."""

    return re.findall(r"https?://[^\s,;]+", raw_input)


__all__ = [
    "RealtyParserAPI",
    "AVITO_AVAILABLE",
    "YANDEX_AVAILABLE",
    "BAZA_WINNER_AVAILABLE",
    "EXTENDED_COLLECTOR_AVAILABLE",
    "parser",
    "parse_property",
    "parse_property_extended",
    "parse_properties_batch",
    "extract_urls",
    "get_property_by_guid",
    "parse_property_flat_state",
]
