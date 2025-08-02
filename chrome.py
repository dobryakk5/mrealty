from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By

service = ChromeService(executable_path="/usr/bin/chromedriver")
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-blink-features=AutomationControlled")
driver = webdriver.Chrome(service=service, options=options)

try:
    # 1) Загрузим простую страницу
    driver.get("https://photos.google.com/")
    # 2) Выведем title
    print("Title:", driver.title)
    # 3) Выведем первые 200 символов HTML
    print("HTML snippet:", driver.page_source[:200])
    
    # 4) Попробуем найти элемент <h1> на example.com
    h1 = driver.find_element(By.TAG_NAME, "h1")
    print("Found <h1>:", h1.text)
finally:
    driver.quit()
