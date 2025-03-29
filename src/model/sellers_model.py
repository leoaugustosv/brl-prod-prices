from libs.spark import *

class Seller:

    def __init__(self, id, name, url, active, categories=[], products=[]):
        self.id = id
        self.name = name
        self.url = url
        self.categories = categories
        self.active = active
        self.products = products
    
    def show_info(self):
        for attr, value in vars(self).items():
            if type(value) == "dict":
                vals = "{ "
                for key, val in value.items():
                    vals =+ f"{key}: {val}\n"
                vals =+ " }"
                print(vals)
            elif type(value) == "list":
                vals = "["
                for val in value:
                    vals =+ f"{val}, "
                vals =+ "]"
                print(vals)
            else:
                print(f"{attr}: {value} {type(value)}")
