
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

service = Service("/usr/bin/chromedriver")
opts = webdriver.ChromeOptions()
opts.add_argument("--headless=new")
driver = webdriver.Chrome(service=service, options=opts)

try:
    driver.get("https://photos.google.com/")
    print("Example.com title:", driver.title)
    print("Example.com h1 text:", driver.find_element(By.TAG_NAME, "h1").text)
finally:
    driver.quit()
