sellers_params = [

    ## URL value:
    # 0 for homepage
    # 1 for categories page
    # 2 for specific products page

    {
        "id": 1,
        "name" : "Zoom",
        "url" : [
            ["https://www.zoom.com.br/", 0],
            ["https://www.zoom.com.br/todas-categorias", 1],
            ["https://www.zoom.com.br/cata-pechincha", 2]
        ],
        "categories" : [{
            "Cata-Pechincha":"https://www.zoom.com.br/cata-pechincha"
        }],
        "active": False,
        
    },

    {
        "id": 2,
        "name" : "Magalu",
        "url" : [
            ["https://www.magazineluiza.com.br/", 0]
        ],
        "categories" : [],
        "active": False,
    },

    {
        "id": 3,
        "name" : "MercadoLivre",
        "url" : [
            ["https://www.magazineluiza.com.br/", 0],
            ["https://www.mercadolivre.com.br/mais-vendidos", 1]
        ],
        "categories" : [],
        "active": True,
    },

]

