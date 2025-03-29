from libs.spark import *

class Product:

    def __init__(self, id, name, url, category, seller, position:int = None,
                 price_in_cash:float = 0.0, price_in_installments:float = None,
                 installments_num:int = None, installment_value:float = None,
                 img:str = None, rating:float = None, rating_users:int = 0
                 ):
        self.id = id
        self.name = name
        self.url = url
        self.category = category
        self.price_in_cash = price_in_cash
        self.price_in_installments = price_in_installments
        self.installments_num = installments_num
        self.installment_value = installment_value
        self.img = img
        self.seller = seller
        self.position = position
        self.rating = rating
        self.rating_users = rating_users

    def show_info(self):
        for attr, value in vars(self).items():
            print(f"{attr}: {value}")

    