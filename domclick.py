import time
import shutil
import tempfile

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Параметры
ADDRESS    = "ул Малышева, 3к2"
ROOMS_ITEM = 3      # 0=Студия,1=1-комн,2=2-комн,3=3-комн
AREA_SQM   = "54"

# Настройка опций
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--window-size=1920,1080")

# Если chromedriver не в PATH, укажите полный путь:
service = ChromeService(executable_path="/usr/bin/chromedriver")

driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 30)

try:
    # 1) Открываем сайт
    driver.get("https://price.domclick.ru/")
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

    # 2) Адрес
    addr = wait.until(EC.visibility_of_element_located((By.ID, "full_address_input")))
    addr.clear()
    addr.send_keys(ADDRESS)
    time.sleep(2)
    addr.send_keys(Keys.ARROW_DOWN)
    addr.send_keys(Keys.ENTER)

    # 3) Комнаты
    rooms = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-rooms_input"]')))
    driver.execute_script("arguments[0].click()", rooms)
    time.sleep(0.5)
    opt = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, f'[data-e2e-id="appraisal-rooms_item_{ROOMS_ITEM}"]')))
    driver.execute_script("arguments[0].click()", opt)

    # 4) Площадь (React-controlled)
    area_el = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-area"]//input')))
    driver.execute_script("""
        const el = arguments[0], val = arguments[1];
        const d = Object.getOwnPropertyDescriptor(el, 'value');
        const p = Object.getPrototypeOf(el);
        const s = d?.set, ps = Object.getOwnPropertyDescriptor(p, 'value')?.set;
        if (s && ps && s!==ps) ps.call(el,val);
        else if (s) s.call(el,val);
        el.dispatchEvent(new Event('input',{bubbles:true}));
        el.dispatchEvent(new Event('change',{bubbles:true}));
    """, area_el, AREA_SQM)
    time.sleep(0.2)

    # 5) Сабмит
    btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//button[@data-e2e-id="appraisal-submit"]')))
    driver.execute_script("arguments[0].click()", btn)

    # 6) Результат
    wait.until(EC.visibility_of_element_located((By.XPATH, '//h2[text()="Результат оценки"]')))
    price = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, 'div.WR8hRawFuZ.DUx5fOdLmA p.O42sTBiVLr')))
    print("Рыночная цена квартиры:", price.text)

finally:
    driver.quit()
