import time
import undetected_chromedriver as uc

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Параметры
ADDRESS = "ул Малышева, 3к2"
ROOMS_ITEM = 3
AREA_SQM = "54"

# ——————————————————————————————————
#  Настройка Headless ChromeDriver
# ——————————————————————————————————
options = uc.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--window-size=1920,1080")
options.add_argument(
    "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
    " (KHTML, like Gecko) Chrome/138.0.7204.184 Safari/537.36"
)

driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 20)

try:
    # 1) Открываем сайт и ждём полной загрузки
    driver.get("https://price.domclick.ru/")
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

    # 2) Ждём появления поля адреса и вводим его
    addr = wait.until(EC.visibility_of_element_located((By.ID, "full_address_input")))
    addr.clear()
    addr.send_keys(ADDRESS)
    time.sleep(2)  # ждём автокомплит
    addr.send_keys(Keys.ARROW_DOWN)
    addr.send_keys(Keys.ENTER)

    # 3) Открываем список комнат
    rooms_wrapper = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-rooms_input"]')))
    driver.execute_script("arguments[0].scrollIntoView(true);", rooms_wrapper)
    driver.execute_script("arguments[0].click()", rooms_wrapper)
    time.sleep(0.5)

    # 4) Выбираем нужную опцию
    opt = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, f'[data-e2e-id="appraisal-rooms_item_{ROOMS_ITEM}"]')))
    driver.execute_script("arguments[0].scrollIntoView(true);", opt)
    opt.click()

    # 5) Ставим площадь через React-native setter
    area_el = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-area"]//input')))
    driver.execute_script("arguments[0].scrollIntoView(true);", area_el)
    time.sleep(0.2)
    driver.execute_script("""
        const el = arguments[0], val = arguments[1];
        const desc = Object.getOwnPropertyDescriptor(el, 'value');
        const proto = Object.getPrototypeOf(el);
        const setter = desc.set;
        const protoSetter = Object.getOwnPropertyDescriptor(proto, 'value').set;
        if (setter !== protoSetter) protoSetter.call(el, val);
        else setter.call(el, val);
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
    """, area_el, AREA_SQM)
    time.sleep(0.2)

    # 6) Нажимаем «Узнать цену»
    submit_btn = wait.until(EC.visibility_of_element_located(
        (By.XPATH, '//button[@data-e2e-id="appraisal-submit"]')))
    driver.execute_script("arguments[0].scrollIntoView(true);", submit_btn)
    driver.execute_script("arguments[0].click()", submit_btn)

    # 7) Ждём и выводим рыночную цену
    wait.until(EC.visibility_of_element_located((By.XPATH, '//h2[text()="Результат оценки"]')))
    price_elem = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, 'div.WR8hRawFuZ.DUx5fOdLmA p.O42sTBiVLr')))
    print("Рыночная цена квартиры:", price_elem.text)

finally:
    driver.quit()
