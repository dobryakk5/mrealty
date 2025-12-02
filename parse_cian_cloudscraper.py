#!/usr/bin/env python3
"""
Версия парсера CIAN с использованием cloudscraper
Используйте этот файл если curl_cffi не работает
"""

# Копируем весь код из parse_cian_to_db.py, но меняем импорт
import argparse
import asyncio
import re
import time
import cloudscraper
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

# Создаем scraper с browser emulation
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'linux',
        'desktop': True
    }
)

# Остальной код такой же, но вместо requests.Session() используется scraper
# Все вызовы session.get() заменяются на scraper.get()

print("[INFO] Используется cloudscraper для обхода Cloudflare")
print("[INFO] Скопируйте код из parse_cian_to_db.py и замените все session.get() на scraper.get()")
