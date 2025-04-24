from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import re
import time
from model.products_model import *

seller_name = "Magalu"

def get_html_categories(browser, url):

    categories = []
    

    print(f"Obtaining categories for {seller_name}...")
    browser.get(url)
    time.sleep(1)

    # MAINTENANCE - Categories Area
    elem = browser.find_element(By.XPATH, "//nav[@data-testid='links-menu']")
    inner_html = elem.get_attribute('innerHTML')
    soup = BeautifulSoup(inner_html, "html.parser")
    filtered_html = soup.find_all('a', {'href': re.compile(r'\/.*\/l\/.*\/')})


    for tag in filtered_html:
        try:
            category = {}
            category[tag.get_text()] = f'https://www.magazineluiza.com.br{tag.attrs.get("href")}?page=1&sortOrientation=desc&sortType=soldQuantity' # Most sold products filter
            categories.append(category)
        except Exception as e:
            print(f"{seller_name} Appending Categories Tag Error: {e}")
    
    return categories

def get_product_prices(browser, category_list, product_id, limit:int = None):

    print(f"Obtaining products for {seller_name}...")
    product_instances = []

    for category_dict in category_list:

        for category, cat_url in category_dict.items():
            browser.get(cat_url)
            time.sleep(1)

            # MAINTENANCE - Product Cards Area
            elem = browser.find_element(By.XPATH, "//div[@data-testid='product-list']")
            inner_html = elem.get_attribute('innerHTML')

            soup = BeautifulSoup(inner_html, "html.parser")

            filtered_html = soup.find_all("a", {"data-testid": "product-card-container"})
            filtered_html = filtered_html[:limit] if limit else filtered_html

            
            pos_counter = 0

            for product in filtered_html:
                    
                        
                        try:
                            name = product.find("h2", {"data-testid": "product-title"}).text.strip()
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (NAME): {e}")

                        try:
                            price_in_cash = float(
                                product.find("p", {"data-testid": "price-value"}).text.strip()
                                .replace("ou ","")
                                .replace("R$\xa0","")
                                .replace(".","")
                                .replace(",",".")
                                .strip()
                            )
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (P_IN_CASH): {e}")
                        
                        try:
                            installments_info = (
                                product.find("p", {"data-testid": "installment"})
                            )
                        except Exception as e:
                            installments_info = ""
                            print(f"{seller_name} Product Cards Tag Error (INSTALLMENTS_INFO): {e}")

                        if installments_info:
                            
                            try:
                                price_in_installments = float(
                                    installments_info
                                    .text.strip()
                                    .split(" em ")[0]
                                    .replace("R$\xa0","")
                                    .replace(".","")
                                    .replace(",",".")
                                    .strip()
                                )
                            except Exception as e:
                                print(f"{seller_name} Product Cards Tag Error (PRICE_IN_INSTALLMENT): {e}")
                                print(f"Product Tag: \n{product}")
                                print(f"Installments info len: {installments_info}")
                            
                            try:
                                installment_value = float(
                                    installments_info
                                    .text.strip()
                                    .split(" em ")[1]
                                    .split("x de")[1]
                                    .replace("sem juros","")
                                    .replace("R$\xa0","")
                                    .replace(".","")
                                    .replace(",",".")
                                    .replace("no Cartão Magalu","")
                                    .strip()
                                )
                            except Exception as e:
                                print(f"{seller_name} Product Cards Tag Error (INSTALLMENT_VALUE): {e}")
                                print(f"Product Tag: \n{product}")
                                print(f"Installments info len: {installments_info}")
                            
                            try:
                                installments_num = int(
                                    installments_info
                                    .text.strip()
                                    .split(" em ")[1]
                                    .split("x")[0]
                                    .strip()
                                )
                            except Exception as e:
                                print(f"{seller_name} Product Cards Tag Error (INSTALLMENTS_NUM): {e}")
                                print(f"Product Tag: \n{product}")
                                print(f"Installments info len: {installments_info}")
                        else:
                            installment_value = -999.0
                            installments_num = 0
                            price_in_installments = -999.0
                        
                        try:
                            url = f'https://www.magazineluiza.com.br{product["href"]}'
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (URL): {e}")

                        try:
                            img_tag = product.find("img", {"data-testid": "image"})
                            img = img_tag["src"] if img_tag else None
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (IMG): {e}")

                        try:
                            rating_tag = product.find("div", {"data-testid": "review"})
                        
                            if rating_tag:

                                rating = rating_tag.find("span", {"format": "score-count"})

                                rating_val = float(rating.text.strip().split(" ")[0])
                                rating_users = int(
                                    rating.text.strip().split(" ")[1]
                                    .replace("(","")
                                    .replace(")","")
                                    .strip()
                                )
                            else:
                                rating_val = -999.0
                                rating_users = 0

                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (RATING): {e}")
                        
                        pos_counter += 1

                        try:
                            product_instances.append(
                                Product(
                                    id=str(product_id),
                                    name=name,
                                    price_in_cash=price_in_cash,
                                    installments_num=installments_num,
                                    installment_value=installment_value,
                                    price_in_installments=price_in_installments if price_in_installments != -999.0 else None,
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
                            print(f"{seller_name} Product Cards Tag Error (APPEND_TO_CLASS): {e}")
                        

    
    return product_id, product_instances