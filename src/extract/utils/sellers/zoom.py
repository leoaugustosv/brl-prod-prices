from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import re
import time
from model.products_model import *

seller_name = "Zoom"

def get_html_categories(browser, url):

    categories = []
    

    print(f"Obtaining categories for {seller_name}...")
    browser.get(url)
    time.sleep(1)
    elem = browser.find_element(By.XPATH, "//div[@data-testid='header::menu-region']")
    inner_html = elem.get_attribute('innerHTML')
    soup = BeautifulSoup(inner_html, "html.parser")
    filtered_html = soup.find_all('a', {'class': re.compile(r'^MenuRegion_zmenu__link--more__.*')})


    for tag in filtered_html:
        try:
            category = {}
            category[tag.get('title')] = f'https://www.zoom.com.br{tag.attrs.get("href")}'
            categories.append(category)
        except Exception as e:
            print(f"{seller_name} Appending Categories Tag Error: {e}")
    
    return categories

def get_product_prices(browser, category_list, product_id, limit:int = None):

    print(f"\nObtaining products for {seller_name}...\n")
    product_instances = []

    for category_dict in category_list:
        for category, cat_url in category_dict.items():
            browser.get(cat_url)
            time.sleep(1)

            inner_html = browser.page_source
            soup = BeautifulSoup(inner_html, "html.parser")

            filtered_html = soup.find_all("a", {"data-testid": "product-card::card"})
            filtered_html = filtered_html[:limit] if limit else filtered_html

            pos_counter = 0

            for product in filtered_html:
                    
                        
                        try:
                            name = product.find("h2", {"data-testid": "product-card::name"}).text.strip()
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (NAME): {e}")

                        try:
                            price_in_cash = float(
                                product.find("p", {"data-testid": "product-card::price"}).text.strip()
                                .replace("R$ ","")
                                .replace(".","")
                                .replace(",",".")
                                .strip()
                            )
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (P_IN_CASH): {e}")
                        
                        try:
                            installments_info = (
                                str(product.find("span", {"data-testid": "product-card::installment"}).text.strip())
                            )
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (INSTALLMENTS_INFO): {e}")
                            print(f"Product Tag: \n{product}")

                        if installments_info:
                            try:
                                installment_value = float(
                                    installments_info
                                    .split("R$ ")[1]
                                    .replace(".","")
                                    .replace(",",".")
                                    .strip()
                                )
                            except Exception as e:
                                print(f"{seller_name} Product Cards Tag Error (INSTALLMENT_VALUE): {e}")
                                print(f"Product Tag: \n{product}")
                                print(f"Installments info len: {installments_info}")
                            
                            try:
                                installments_num = int(
                                    installments_info
                                    .split("x")[0][-2:]
                                    .replace(" ","")
                                    .strip()
                                )
                            except Exception as e:
                                print(f"{seller_name} Product Cards Tag Error (INSTALLMENTS_NUM): {e}")
                                print(f"Product Tag: \n{product}")
                                print(f"Installments info len: {installments_info}")
                        else:
                            installment_value = -999.0
                            installments_num = 0
                        
                        try:
                            if product["href"][:1] == "/":
                                product["href"] = "https://www.zoom.com.br"+product["href"]
                            url = product["href"]
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (URL): {e}")

                        try:
                            img_tag = product.find("img")
                            img = img_tag["src"] if img_tag else None
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (IMG): {e}")

                        try:
                            rating = product.find("div", {"data-testid": "product-card::rating"})

                            if rating:
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
                                    price_in_installments=round(installment_value*installments_num,2) if installment_value != -999.0 else None,
                                    url=url,
                                    img=img,
                                    category=category,
                                    seller=seller_name,
                                    rating=rating_val,
                                    rating_users=rating_users,
                                    position=pos_counter
                                )
                            )
                            product_id += 1
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (APPEND_TO_CLASS): {e}")
                        

    
    return product_id, product_instances