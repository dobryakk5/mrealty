import time
import tempfile
import shutil

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Параметры для оценки
ADDRESS    = "ул Малышева, 3к2"
ROOMS_ITEM = 3      # 0=Студия,1=1-комн,2=2-комн,3=3-комн
AREA_SQM   = "54"

# 1) Создаём временный каталог для user-data-dir
profile_dir = tempfile.mkdtemp(prefix="chrome-profile-")

# 2) Опции Chrome
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--window-size=1920,1080")
options.add_argument(f"--user-data-dir={profile_dir}")

# 3) Сервис с путём до chromedriver
service = ChromeService(executable_path="/usr/bin/chromedriver")

driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 20)

try:
    # 1) Открываем сайт
    driver.get("https://price.domclick.ru/")
    print("CURRENT URL:", driver.current_url)
    html = driver.page_source
    print("PAGE SOURCE SNIPPET:", html[:500])  # первые 500 символов

    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

    # 2) Заполняем адрес
    addr = wait.until(EC.visibility_of_element_located((By.ID, "full_address_input")))
    addr.clear()
    addr.send_keys(ADDRESS)
    time.sleep(2)
    addr.send_keys(Keys.ARROW_DOWN)
    addr.send_keys(Keys.ENTER)

    # 3) Раскрываем выбор комнат
    rooms = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-rooms_input"]')))
    driver.execute_script("arguments[0].click()", rooms)
    time.sleep(0.5)
    opt = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, f'[data-e2e-id="appraisal-rooms_item_{ROOMS_ITEM}"]')))
    driver.execute_script("arguments[0].click()", opt)

    # 4) Ставим площадь (React-controlled)
    area_el = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-area"]//input')))
    driver.execute_script("""
        const el=arguments[0], v=arguments[1];
        const d=Object.getOwnPropertyDescriptor(el,'value'),
              p=Object.getPrototypeOf(el),
              s=d?.set,
              ps=Object.getOwnPropertyDescriptor(p,'value')?.set;
        if(s&&ps&&s!==ps) ps.call(el,v); else if(s) s.call(el,v);
        el.dispatchEvent(new Event('input',{bubbles:true}));
        el.dispatchEvent(new Event('change',{bubbles:true}));
    """, area_el, AREA_SQM)
    time.sleep(0.2)

    # 5) Сабмит
    submit_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//button[@data-e2e-id="appraisal-submit"]')))
    driver.execute_script("arguments[0].click()", submit_btn)

    # 6) Ждём результат
    wait.until(EC.visibility_of_element_located((By.XPATH, '//h2[text()="Результат оценки"]')))
    price_elem = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, 'div.WR8hRawFuZ.DUx5fOdLmA p.O42sTBiVLr')))
    print("Рыночная цена квартиры:", price_elem.text)

finally:
    driver.quit()
    # 7) Удаляем временный профиль
    shutil.rmtree(profile_dir, ignore_errors=True)
