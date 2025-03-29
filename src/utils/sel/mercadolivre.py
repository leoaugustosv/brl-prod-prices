from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import re
import time
from model.products_model import *

seller_name = "MercadoLivre"

def get_html_categories(browser, url):

    categories = []
    

    print(f"Obtaining categories for {seller_name}...")
    browser.get(url)
    time.sleep(1)

    elem = browser.find_element(By.XPATH, "//div[@class='CategoryList']")
    inner_html = elem.get_attribute('innerHTML')
    soup = BeautifulSoup(inner_html, "html.parser")

    cat_name_tags = soup.find_all('div', {'class': re.compile(r'category-list-item__permalink-custom-fw-regular category-list-item__permalink-custom')})
    cat_link_tags = soup.find_all('a', {'class': re.compile(r'splinter-link')})


    for cat in list(zip(cat_name_tags, cat_link_tags)):
        try:
            category = {}
            category[cat[0].get_text()] = f'{cat[1].attrs.get("href")}'
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

            # MAINTENANCE - Product Cards Area
            elem = browser.find_element(By.XPATH, "//section[@class='ui-best-seller-content-main-components']")
            inner_html = elem.get_attribute('innerHTML')

            soup = BeautifulSoup(inner_html, "html.parser")

            filtered_html = soup.find_all('div', {'class': re.compile(r'^andes-card*')})
            filtered_html = filtered_html[:limit] if limit else filtered_html

            pos_counter = 0

            for product in filtered_html:
                    
            
                        try:
                            name_href_component = product.find("a", {"class": "poly-component__title"})
                            name = name_href_component.text.strip()
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (NAME): {e}")

                        try:
                            price_main_fraction = int(
                                product.find("span", {"class": "andes-money-amount__fraction"}).text.strip()
                                .replace(".","")
                                .strip()
                            )
                        
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (PRICE_MAIN_FRACTION): {e}")

                        try:
                            price_cents = int(
                                product.find("span", {"class": "andes-money-amount__cents andes-money-amount__cents--superscript-24"})
                                .text.strip()
                            )
                        
                        except Exception as e:
                            price_cents = None
                            # print(f"{seller_name} Product Cards Tag Error (PRICE_CENTS): {e}")

                        try:
                            
                            if price_cents:
                                price_in_cash = float(
                                str(price_main_fraction)+"."+str(price_cents)
                            )

                            else:
                                price_in_cash = float(price_main_fraction)

                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (P_IN_CASH_MELI): {e}")
                        
                        try:
                            installments_info = (
                                product.find("span", {"class": "poly-price__installments"})
                            )
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (INSTALLMENTS_INFO): {e}")
                            




                        


                        if installments_info:
                            try:

                                installment_fraction = int(
                                    installments_info
                                    .find("span", {"class": "andes-money-amount__fraction"})
                                    .text
                                    .replace(".","")
                                    .strip()
                                )
                            
                            except Exception as e:
                                print(f"{seller_name} Product Cards Tag Error (INSTALLMENTS_FRACTION): {e}")

                            try:

                                installment_cents = int(
                                    installments_info
                                    .find("span", {"class": "andes-money-amount__cents"})
                                    .text
                                    .strip()
                                )
                            
                            except Exception as e:
                                installment_cents = None
                                # print(f"{seller_name} Product Cards Tag Error (INSTALLMENTS_CENTS): {e}")

                            try:
                                if installment_cents:
                                    installment_value = float(
                                        str(installment_fraction)+"."+str(installment_cents)
                                    )
                                else:
                                    installment_value = float(installment_fraction)

                            except Exception as e:
                                print(f"{seller_name} Product Cards Tag Error (INSTALLMENT_VALUE): {e}")
                                
                                print(f"Installments info len: {installments_info}")
                            
                            try:
                                installments_num = int(
                                    installments_info
                                    .text
                                    .split('x')[0][-2:]
                                    .replace(' ', '')
                                    
                                )
                            except Exception as e:
                                # print(f"{seller_name} Product Cards Tag Error (INSTALLMENTS_NUM): {e}")
                                installments_num = 0

                        else:
                            installment_value = -999.0
                            installments_num = 0
                        
                        try:
                            url = name_href_component["href"]
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (URL): {e}")

                        try:
                            img_tag = product.find("img", {"class": "poly-component__picture"})
                            img = img_tag["src"] if img_tag else None
                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (IMG): {e}")

                        try:
                            rating = product.find("div", {"class": "poly-component__reviews"})

                            if rating:
                                rating_val = float(
                                    rating
                                    .find("span", {"class": "poly-reviews__rating"})
                                    .text.strip()
                                )
                                rating_users = int(
                                    rating
                                    .find("span", {"class": "poly-reviews__total"})
                                    .text
                                    .replace("(","")
                                    .replace(")","")
                                    .strip()
                                )
                            else:
                                rating_val = -999.0
                                rating_users = 0

                        except Exception as e:
                            print(f"{seller_name} Product Cards Tag Error (RATING): {e}")


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