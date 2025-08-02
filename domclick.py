import tempfile
import shutil
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Параметры для оценки
ADDRESS    = "ул Малышева, 3к2"
ROOMS_ITEM = 3        # 0=Студия, 1=1-комн, 2=2-комн, 3=3-комн
AREA_SQM   = "54"

# 1) создаём временную папку для профиля
profile_dir = tempfile.mkdtemp(prefix="chrome-profile-")

# 2) настраиваем ChromeOptions
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument(f"--user-data-dir={profile_dir}")  # вот тут
options.add_argument("--window-size=1920,1080")

# 3) укажите ваш путь к chromedriver, если не в PATH
service = ChromeService(executable_path="/usr/bin/chromedriver")

driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 30)

try:
    # Открываем сайт
    driver.get("https://price.domclick.ru/")
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

    # Вводим адрес
    addr = wait.until(EC.visibility_of_element_located((By.ID, "full_address_input")))
    addr.clear()
    addr.send_keys(ADDRESS)
    time.sleep(2)
    addr.send_keys(Keys.ARROW_DOWN)
    addr.send_keys(Keys.ENTER)

    # Открываем выбор комнат
    rooms_wrapper = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-rooms_input"]')))
    driver.execute_script("arguments[0].click()", rooms_wrapper)
    time.sleep(0.5)

    # Выбираем комнаты
    opt = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, f'[data-e2e-id="appraisal-rooms_item_{ROOMS_ITEM}"]')))
    driver.execute_script("arguments[0].click()", opt)

    # Устанавливаем площадь (React-controlled)
    area_el = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-area"]//input')))
    driver.execute_script("""
        const el = arguments[0], val = arguments[1];
        const desc = Object.getOwnPropertyDescriptor(el, 'value');
        const proto = Object.getPrototypeOf(el);
        const setter = desc?.set;
        const protoSetter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
        if (setter && protoSetter && setter !== protoSetter) protoSetter.call(el, val);
        else if (setter) setter.call(el, val);
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
    """, area_el, AREA_SQM)
    time.sleep(0.2)

    # Нажимаем «Узнать цену»
    submit_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//button[@data-e2e-id="appraisal-submit"]')))
    driver.execute_script("arguments[0].click()", submit_btn)

    # Ждём и выводим цену
    wait.until(EC.visibility_of_element_located((By.XPATH, '//h2[text()="Результат оценки"]')))
    price_elem = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, 'div.WR8hRawFuZ.DUx5fOdLmA p.O42sTBiVLr')))
    print("Рыночная цена квартиры:", price_elem.text)

finally:
    driver.quit()
    # 4) удаляем временный профиль
    shutil.rmtree(profile_dir, ignore_errors=True)
