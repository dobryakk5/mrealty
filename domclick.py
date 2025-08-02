import time
import undetected_chromedriver as uc

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Параметры для оценки
ADDRESS = "ул Малышева, 3к2"
ROOMS_ITEM = 3        # номер опции: 0=Студия, 1=1-комн, 2=2-комн, 3=3-комн
AREA_SQM = "54"       # площадь в м²

# —————————————————————————
#  Настройки и инициализация
# —————————————————————————
options = uc.ChromeOptions()
options.add_argument("--headless=new")
options.binary_location = "/usr/bin/chromium-browser"

driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 20)

try:
    # 1) Открываем страницу
    driver.get("https://price.domclick.ru/")

    # 2) Вводим адрес и выбираем автокомплит
    addr = wait.until(EC.element_to_be_clickable((By.ID, "full_address_input")))
    addr.clear()
    addr.send_keys(ADDRESS)
    time.sleep(2)  # ждём подгрузки подсказок
    addr.send_keys(Keys.ARROW_DOWN)
    addr.send_keys(Keys.ENTER)

    # 3) Раскрываем дропдаун «Жилых комнат»
    rooms_wrapper = wait.until(EC.presence_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-rooms_input"]')))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'})", rooms_wrapper)
    driver.execute_script("arguments[0].click()", rooms_wrapper)
    time.sleep(0.5)

    # 4) Выбираем нужное количество комнат
    selector = f'[data-e2e-id="appraisal-rooms_item_{ROOMS_ITEM}"]'
    opt = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
    driver.execute_script("arguments[0].scrollIntoView({block:'nearest'})", opt)
    opt.click()

    # 5) Устанавливаем площадь через React-native setter
    area_el = wait.until(EC.presence_of_element_located(
        (By.XPATH, '//div[@data-e2e-id="appraisal-area"]//input')))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'})", area_el)
    time.sleep(0.2)
    js = '''
    function setNative(el, val){
      const desc = Object.getOwnPropertyDescriptor(el, "value");
      const setter = desc?.set;
      const proto = Object.getPrototypeOf(el);
      const protoSetter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
      if (setter && protoSetter && setter !== protoSetter) protoSetter.call(el,val);
      else if (setter) setter.call(el,val);
      el.dispatchEvent(new Event("input",{bubbles:true}));
      el.dispatchEvent(new Event("change",{bubbles:true}));
    }
    setNative(arguments[0], arguments[1]);
    '''
    driver.execute_script(js, area_el, AREA_SQM)
    time.sleep(0.2)

    # 6) Жмём «Узнать цену»
    submit_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//button[@data-e2e-id="appraisal-submit"]')))
    driver.execute_script("arguments[0].scrollIntoView({block:'nearest'})", submit_btn)
    driver.execute_script("arguments[0].click()", submit_btn)

    # 7) Ожидаем и получаем «Рыночная цену»
    wait.until(EC.visibility_of_element_located((By.XPATH, '//h2[text()="Результат оценки"]')))
    price_elem = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, 'div.WR8hRawFuZ.DUx5fOdLmA p.O42sTBiVLr')))
    print("Рыночная цена квартиры:", price_elem.text)

finally:
    driver.quit()
