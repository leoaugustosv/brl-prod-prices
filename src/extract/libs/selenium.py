from selenium import webdriver
from selenium.webdriver.common.by import By


def init_browser(url="about:blank", headless = False):

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36"

    # args
    config_params = webdriver.ChromeOptions()
    config_params.add_argument('--incognito')
    config_params.add_argument('--disable-blink-features=AutomationControlled')
    config_params.add_argument(f'user-agent={user_agent}')
    config_params.add_experimental_option("excludeSwitches", ["enable-automation"])
    config_params.add_experimental_option('useAutomationExtension', False)
    # config_params.add_argument("--log-level=1")
    
    # specific args for google-chrome .deb package 
    config_params.add_argument("--no-sandbox")
    config_params.add_argument("--disable-gpu")
    config_params.add_argument("--disable-default-apps")
    config_params.add_argument("--no-first-run")
    config_params.add_argument("--no-default-browser-check")
    config_params.add_argument("--disable-extensions")
    config_params.add_argument("--user-data-dir=/tmp/chrome-profile")


    # service
    service = webdriver.ChromeService(executable_path='/usr/bin/chromedriver')

    if headless:
        config_params.add_argument("--headless=new")
        browser = webdriver.Chrome(options=config_params, service=service)
    else:
        browser = webdriver.Chrome(options=config_params, service=service)
        browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    browser.get(url)

    return browser


def close_browser(browser):
    browser.quit()
