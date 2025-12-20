"""Persistent browser helpers for Avito parsing."""

import json
import os
import re
import threading
import time
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


class PersistentAvitoBrowser:
    """Persistent браузер для Avito с cookies."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Singleton pattern для единственного браузера."""

        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "initialized"):
            return

        self.driver = None
        self.cookies_file = "avito_cookies.json"
        self.initialized = False
        self.last_activity = time.time()
        self.session_timeout = 86400  # 24 часа без активности
        print("🔄 Persistent браузер инициализирован для постоянной работы")

    def setup_browser(self) -> bool:
        """Настраивает и запускает браузер."""

        if self.driver and self._is_browser_alive():
            print("✅ Браузер уже запущен")
            return True

        try:
            print("🔧 Запускаем persistent браузер...")

            options = Options()
            has_cookies = os.path.exists(self.cookies_file)

            if has_cookies:
                options.add_argument("--headless=new")
                print("🔒 Режим headless (есть cookies)")
            else:
                print("👁️ Обычный режим (нет cookies)")

            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-images")
            options.add_argument("--memory-pressure-off")
            options.add_argument("--max_old_space_size=512")
            options.add_argument("--window-size=1280,720")
            options.add_argument(
                "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.6904.127 Safari/537.36"
            )
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-features=VizDisplayCompositor")
            options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            options.add_experimental_option("useAutomationExtension", False)
            options.add_experimental_option(
                "prefs",
                {
                    "profile.default_content_setting_values.notifications": 2,
                    "profile.default_content_settings.popups": 0,
                    "profile.managed_default_content_settings.images": 2,
                },
            )

            if os.path.exists("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"):
                options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            else:
                options.binary_location = "/opt/google/chrome/google-chrome"

            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(5)

            self.driver.execute_script(
                """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
                window.chrome = {runtime: {}};
            """
            )

            self._load_and_apply_cookies()

            self.initialized = True
            self.last_activity = time.time()

            print("✅ Persistent браузер готов к работе")
            return True

        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка запуска браузера: {exc}")
            return False

    def _is_browser_alive(self) -> bool:
        """Проверяет, жив ли браузер."""

        try:
            if not self.driver:
                return False
            _ = self.driver.current_url
            return True
        except Exception:  # noqa: BLE001
            return False

    def _load_and_apply_cookies(self) -> None:
        """Загружает и применяет cookies."""

        try:
            if not os.path.exists(self.cookies_file):
                print("⚠️ Файл cookies не найден, создайте его вручную")
                return

            print("🍪 Загружаем главную страницу для cookies...")
            self.driver.get("https://www.avito.ru/")
            time.sleep(2)

            with open(self.cookies_file, "r", encoding="utf-8") as file:
                cookies_data = json.load(file)

            cookies_list = cookies_data["cookies"] if "cookies" in cookies_data else cookies_data

            for cookie in cookies_list:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as exc:  # noqa: BLE001
                    print(f"⚠️ Не удалось добавить cookie: {exc}")

            self.driver.refresh()
            time.sleep(1)

            print("✅ Cookies применены")
            print("🏠 Остаемся на главной Avito для постоянной сессии")
            time.sleep(3)

        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка загрузки cookies: {exc}")

    def parse_url(self, url: str, max_retries: int = 2) -> Optional[dict]:
        """Быстро парсит URL с уже открытым браузером."""

        if not self.setup_browser():
            return None

        for attempt in range(max_retries + 1):
            try:
                self.last_activity = time.time()

                print(f"🔄 Парсим: {url}")
                start_time = time.time()

                if not self._is_browser_ready():
                    print("⚠️ Браузер не готов, ждем...")
                    time.sleep(3)
                    continue

                self.driver.set_page_load_timeout(15)
                self.driver.get(url)
                time.sleep(1)

                data = {}

                try:
                    data["title"] = self.driver.title
                except Exception:
                    pass

                try:
                    h1_element = self.driver.find_element("tag name", "h1")
                    data["h1"] = h1_element.text.strip()
                except Exception:
                    pass

                try:
                    price_selectors = [
                        '[data-marker="item-view/item-price"]',
                        '[class*="price"]',
                        '[data-testid*="price"]',
                    ]

                    for selector in price_selectors:
                        try:
                            price_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            for element in price_elements:
                                if element.is_displayed() and element.text.strip():
                                    data["price"] = element.text.strip()
                                    break
                            if "price" in data:
                                break
                        except Exception:
                            continue
                except Exception:
                    pass

                text = data.get("h1", "") or data.get("title", "")
                if text:
                    data.update(self._extract_from_text(text))

                parse_time = time.time() - start_time
                print(f"⏱️ Парсинг занял: {parse_time:.2f} сек")

                return data

            except Exception as exc:  # noqa: BLE001
                print(f"❌ Ошибка парсинга (попытка {attempt + 1}/{max_retries + 1}): {exc}")
                if attempt < max_retries:
                    print("🔄 Повторяем через 2 секунды...")
                    time.sleep(2)
                    continue
                return None

        return None

    def _is_browser_ready(self) -> bool:
        """Проверяет готовность браузера к работе."""

        try:
            self.driver.current_url
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ Браузер не готов: {exc}")
            return False

    def _extract_from_text(self, text: str) -> dict:
        """Извлекает данные из текста."""

        data: dict = {}

        rooms_match = re.search(r"(\d+)-к\.", text)
        if rooms_match:
            data["rooms"] = int(rooms_match.group(1))

        if re.search(r"\bстудия\b|\bапартаменты\b", text.lower()):
            data["rooms"] = 0

        area_match = re.search(r"(\d+(?:[.,]\d+)?)\s*м²", text)
        if area_match:
            area_str = area_match.group(1).replace(",", ".")
            data["total_area"] = float(area_str)

        floor_match = re.search(r"(\d+)/(\d+)\s*эт\.", text)
        if floor_match:
            data["floor"] = floor_match.group(1)
            data["total_floors"] = int(floor_match.group(2))

        return data

    def is_session_expired(self) -> bool:
        """Проверяет, истекла ли сессия."""

        return time.time() - self.last_activity > self.session_timeout

    def get_session_info(self) -> dict:
        """Возвращает информацию о текущей сессии."""

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
                "is_on_avito": "avito.ru" in current_url,
                "last_activity": self.last_activity,
            }
        except Exception:  # noqa: BLE001
            return {"status": "error", "message": "Ошибка получения информации о сессии"}

    def refresh_session(self) -> bool:
        """Обновляет сессию."""

        if self.is_session_expired() or not self._is_browser_alive():
            print("🔄 Обновляем браузер сессию...")
            self.cleanup()
            return self.setup_browser()

        try:
            current_url = self.driver.current_url
            if "avito.ru" not in current_url:
                print("🔄 Возвращаемся на главную Avito...")
                self.driver.get("https://www.avito.ru/")
                time.sleep(1)
        except Exception:
            pass

        return True

    def cleanup(self) -> None:
        """Закрывает браузер."""

        try:
            if self.driver:
                print("🧹 Закрываем persistent браузер...")
                self.driver.quit()
                self.driver = None
                print("✅ Браузер закрыт")
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️ Ошибка закрытия браузера: {exc}")

    def __del__(self):
        self.cleanup()


_browser: Optional[PersistentAvitoBrowser] = None


def get_persistent_browser() -> PersistentAvitoBrowser:
    """Возвращает глобальный экземпляр браузера."""

    global _browser
    if _browser is None:
        _browser = PersistentAvitoBrowser()
    return _browser


def parse_avito_fast(url: str) -> Optional[dict]:
    """Быстрый парсинг через persistent браузер."""

    browser = get_persistent_browser()
    if not browser.refresh_session():
        return None
    return browser.parse_url(url)


def init_persistent_browser() -> None:
    """Инициализирует браузер в текущем потоке."""

    try:
        print("🔄 Инициализация persistent браузера...")
        browser = get_persistent_browser()
        if browser.setup_browser():
            print("✅ Persistent браузер запущен и готов к работе")
            print("🏠 Браузер находится на Avito с активными cookies")
        else:
            print("❌ Не удалось запустить persistent браузер")
    except Exception as exc:  # noqa: BLE001
        print(f"❌ Ошибка инициализации persistent браузера: {exc}")


def start_persistent_browser_thread() -> threading.Thread:
    """Запускает поток инициализации браузера."""

    thread = threading.Thread(target=init_persistent_browser, daemon=True)
    thread.start()
    return thread


__all__ = [
    "PersistentAvitoBrowser",
    "get_persistent_browser",
    "parse_avito_fast",
    "init_persistent_browser",
    "start_persistent_browser_thread",
]
