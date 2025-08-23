import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import time
from model.products_model import Product
from math import floor

seller_name = "Kabum"

def get_html_categories(browser, url):

    categories = []
    

    print(f"Obtaining categories for {seller_name}...")
    browser.get(url)
    time.sleep(1)

    body = browser.find_element("tag name", "body")
    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)
    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)

    # MAINTENANCE - Categories Area
    elem = browser.find_element(By.XPATH, "//section[@id='carrosselCategorias']")
    inner_html = elem.get_attribute('innerHTML')
    soup = BeautifulSoup(inner_html, "html.parser")
    filtered_html = soup.find_all('a', {'class': re.compile(r'linkCategoria')})
    

    for tag in filtered_html:
        try:
            category = {}
            category[tag.get_text()] = f'https://www.kabum.com.br{tag.attrs.get("href")}'
            categories.append(category)
        except Exception as e:
            print(f"{seller_name} Appending Categories Tag Error: {e}")
    
    return categories


def get_product_prices(browser, category_list, endpoint_dict, product_id, limit:int = None):

    print(f"Obtaining products for {seller_name}...")
    product_instances = []

    for category_dict in category_list:

        for category, cat_url in category_dict.items():
            
            category_endpoint = str(endpoint_dict["public"]["predicate"]+endpoint_dict["public"]["category"]).replace("PLACEHOLDER_STR","")
            route = f'{cat_url.split("/")[-1]}'
            most_searched_filter = f'{endpoint_dict["public"]["filters"]["predicate"]}{endpoint_dict["public"]["filters"]["most_sold"]}'

            url = f"{category_endpoint}{route}{most_searched_filter}"

            products = requests.get(url).json()['data']

            pos_counter = 0

            for product in products:
                
                    
                        try:
                            name = product['attributes']['title']
                        except Exception as e:
                            print(f"{seller_name} Product Endpoint Error (NAME): {e}")

                        try:
                            offer = product['attributes']['offer']
                        except Exception as e:
                            offer = ""
                            print(f"{seller_name} Product Endpoint Error (OFFER): {e}")
                        
                        try:
                            if offer:
                                price_in_cash = float(product['attributes']['offer']['price_with_discount'])
                            else:
                                price_in_cash = float(product['attributes']['price_with_discount'])
                        except Exception as e:
                            print(f"{seller_name} Product Endpoint Error (P_IN_CASH): {e}")
                        
                        try:
                            installments_info = product['attributes']['max_installment']

                        except Exception as e:
                            installments_info = ""
                            print(f"{seller_name} Product Endpoint Error (INSTALLMENTS_INFO): {e}")

                        if installments_info:

                            try:
                                installment_value = float(
                                    installments_info
                                    .split("R$ ")[-1]
                                    .replace(",",".")
                                    .strip()
                                )
                            except Exception as e:
                                print(f"{seller_name} Product Endpoint Error (INSTALLMENT_VALUE): {e}")
                                print(f"Product Tag: \n{product}")
                                print(f"Installments info len: {installments_info}")
                            
                            try:
                                installments_num = int(
                                    installments_info
                                    .split("x")[0]
                                    .strip()
                                )
                            except Exception as e:
                                print(f"{seller_name} Product Endpoint Error (INSTALLMENTS_NUM): {e}")
                                print(f"Product Tag: \n{product}")
                                print(f"Installments info len: {installments_info}")
                        else:
                            installment_value = -999.0
                            installments_num = 0
                        
                        try:
                            url = (
                                f"https://www.kabum.com.br/produto/{product['id']}/"
                            )
                        except Exception as e:
                            print(f"{seller_name} Product Endpoint Error (URL): {e}")

                        try:
                            img = product['attributes']['images'][0]
                        except Exception as e:
                            print(f"{seller_name} Product Endpoint Error (IMG): {e}")

                        try:
                            rating_val = float(product['attributes']['score_of_ratings'])
                            rating_users = int(product['attributes']['number_of_ratings'])
                        except Exception as e:
                            print(f"{seller_name} Product Endpoint Error (RATING): {e}")
                            rating_val = -999.0
                            rating_users = 0
                        
                        pos_counter += 1

                        try:
                            product_instances.append(
                                Product(
                                    id=str(product_id),
                                    name=name,
                                    price_in_cash=price_in_cash,
                                    installments_num=installments_num,
                                    installment_value=installment_value,
                                    price_in_installments= (floor(price_in_cash / installments_num * 100) / 100) if installment_value != -999.0 else None,
                                    url=url,
                                    img=img,
                                    category=category,
                                    seller=seller_name,
                                    rating=rating_val if rating_val != -999.0 else None,
                                    rating_users=rating_users,
                                    position=pos_counter
                                )
                            )
                            product_id += 1
                        except Exception as e:
                            print(f"{seller_name} Product Endpoint Error (APPEND_TO_CLASS): {e}")
                        

    
    return product_id, product_instances