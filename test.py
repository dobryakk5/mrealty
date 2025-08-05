import requests
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

URL = "https://www.cian.ru/sale/flat/318826533/"

def dump_offer_groups(url):
    resp = requests.get(url, headers=HEADERS)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'html.parser')

    groups = soup.select('div[data-name="OfferSummaryInfoGroup"]')
    if not groups:
        print("Группы OfferSummaryInfoGroup не найдены.")
        return

    for gi, grp in enumerate(groups, 1):
        header = grp.find('h2')
        header_text = header.get_text(strip=True) if header else f"Group {gi} (без заголовка)"
        print(f"\n=== Группа {gi}: {header_text} ===")

        items = grp.select('div[data-name="OfferSummaryInfoItem"]')
        if not items:
            print("  (нет элементов OfferSummaryInfoItem)")
            continue

        for ii, item in enumerate(items, 1):
            ps = item.find_all('p')
            texts = [p.get_text(strip=True) for p in ps if p.get_text(strip=True)]
            if len(texts) >= 2:
                print(f"  {ii}. {texts[0]} → {texts[1]}")
            else:
                print(f"  {ii}. {' | '.join(texts)}")

if __name__ == '__main__':
    print(f"Сканируем: {URL}")
    dump_offer_groups(URL)
