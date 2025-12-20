"""
Унифицированный HTTP-клиент для запросов к Cian.
Поддерживает curl_cffi (impersonate), cloudscraper и обычный requests в качестве фолбэка.
"""

from typing import Dict, Optional

import requests as standard_requests

try:
    from curl_cffi import requests as curl_requests

    CURL_CFFI_AVAILABLE = True
    print("[INFO] Используется curl_cffi для подключения к CIAN")
except ImportError:
    curl_requests = None
    CURL_CFFI_AVAILABLE = False

_scraper = None
try:
    import cloudscraper

    CLOUDSCRAPER_AVAILABLE = True
    _scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "linux", "desktop": True}
    )
    if not CURL_CFFI_AVAILABLE:
        print("[INFO] Используется cloudscraper для подключения к CIAN")
except ImportError:
    cloudscraper = None
    CLOUDSCRAPER_AVAILABLE = False

if not CURL_CFFI_AVAILABLE and not CLOUDSCRAPER_AVAILABLE:
    print("[WARNING] curl_cffi и cloudscraper не установлены, используется обычный requests")
    print("[WARNING] Установите: pip install curl_cffi  или  pip install cloudscraper")


DEFAULT_CIAN_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


def fetch_cian_page(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    proxy: Optional[str] = None,
    timeout: int = 30,
    allow_redirects: bool = True,
    impersonate: str = "chrome124",
):
    """
    Выполняет HTTP-запрос к странице Cian, выбирая лучший доступный транспорт.

    Args:
        url: Полный URL страницы Cian.
        headers: Заголовки HTTP.
        cookies: Дополнительные cookies.
        proxy: Строка прокси формата http://user:pass@host:port.
        timeout: Таймаут запроса в секундах.
        allow_redirects: Разрешать ли редиректы.
        impersonate: Браузер для curl_cffi.
    """
    headers = headers or DEFAULT_CIAN_HEADERS
    cookies = cookies or {}

    if CURL_CFFI_AVAILABLE and curl_requests:
        session = curl_requests.Session()
        request_params = {
            "headers": headers,
            "timeout": timeout,
            "allow_redirects": allow_redirects,
            "impersonate": impersonate,
        }
        if proxy:
            request_params["proxies"] = {"http": proxy, "https": proxy}
        if cookies:
            request_params["cookies"] = cookies
        return session.get(url, **request_params)

    if CLOUDSCRAPER_AVAILABLE and _scraper:
        request_params = {
            "headers": headers,
            "timeout": timeout,
            "allow_redirects": allow_redirects,
        }
        if proxy:
            request_params["proxies"] = {"http": proxy, "https": proxy}
        if cookies:
            _scraper.cookies.update(cookies)
        return _scraper.get(url, **request_params)

    session = standard_requests.Session()
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}
    if cookies:
        session.cookies.update(cookies)
    return session.get(url, headers=headers, timeout=timeout, allow_redirects=allow_redirects)
