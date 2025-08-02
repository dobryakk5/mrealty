import time
import re
from seleniumbase import Driver
from bs4 import BeautifulSoup

# === настройки ===
LISTING_URL = "https://www.cian.ru/sale/flat/309583631/"

# === главный запуск ===
def main():
    driver = Driver(uc=True, headless=True)
    try:
        driver.open(LISTING_URL)
        time.sleep(5)
        soup = BeautifulSoup(driver.get_page_source(), 'html.parser')

        # 1. Заголовок объявления
        title_tag = soup.find('h1')
        title = title_tag.get_text(strip=True) if title_tag else '—'
        print(f"Заголовок: {title}\n")

        # 2. Цена (из блока AsideGroup -> PriceInfo)
        price_span = soup.select_one(
            'div[data-name="AsideGroup"] div[data-name="PriceInfo"] [data-testid="price-amount"] span'
        )
        price = price_span.get_text(' ', strip=True) if price_span else '—'
        print(f"Цена: {price}\n")

        # 3. Адрес объявления
        address = '—'
        geo_div = soup.select_one('div[data-name="Geo"]')
        if geo_div:
            # сначала пробуем взять атрибут content у span[itemprop=name]
            addr_span = geo_div.find('span', attrs={'itemprop': 'name'})
            if addr_span and addr_span.get('content'):
                address = addr_span['content'].strip()
            else:
                # иначе собираем все части адреса из ссылок AddressItem
                parts = [a.get_text(strip=True) for a in geo_div.select('a[data-name="AddressItem"]')]
                if parts:
                    address = ', '.join(parts)
        print(f"Адрес: {address}\n")

        # 4. Метки
        labels = []
        label_div = soup.select_one('div[data-name="LabelsLayoutNew"]')
        if label_div:
            for child in label_div.find_all('span', recursive=False):
                text_spans = child.find_all('span')
                if text_spans:
                    lbl = text_spans[-1].get_text(strip=True)
                    if lbl:
                        labels.append(lbl)
        if labels:
            print("Метки:")
            for lbl in labels:
                print(f"  - {lbl}")
            print()

        # 5. Обновлено (date/time)
        updated = '—'
        if (ut := soup.find(string=re.compile(r"Обновлено:"))):
            if m := re.search(r"Обновлено:\s*(\d{1,2}\s\w+\s*,\s*\d{1,2}:\d{2})", ut):
                updated = m.group(1)
        print(f"Обновлено: {updated}")

        # 6. Статистика просмотров
        stats_pattern = re.compile(
            r"\d[\d\s]*\sпросмотр\S*,\s*\d+\sза сегодня,\s*\d+\sуникаль\S*",
            re.IGNORECASE
        )
        stats = soup.find(string=stats_pattern)
        print(stats.strip() if stats else '—')

        # 7. Сводная информация: "О квартире" и "О доме"
        floor = None
        for item in soup.select('[data-name="ObjectFactoidsItem"]'):
            spans = item.find_all('span')
            if len(spans) >= 2 and 'Этаж' in spans[0].get_text():
                floor = spans[1].get_text(strip=True)
                break

        summary = soup.select_one('[data-name="OfferSummaryInfoLayout"]')
        if summary:
            for group in summary.select('[data-name="OfferSummaryInfoGroup"]'):
                header = group.select_one('h2')
                title_group = header.get_text(strip=True) if header else '—'
                print(f"\n{title_group}:")
                for item in group.select('[data-name="OfferSummaryInfoItem"]'):
                    labels_p = item.find_all('p')
                    if len(labels_p) >= 2:
                        key = labels_p[0].get_text(strip=True)
                        val = labels_p[1].get_text(strip=True)
                        print(f"  {key}: {val}")
                if title_group == 'О квартире' and floor:
                    print(f"  Этаж: {floor}")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
