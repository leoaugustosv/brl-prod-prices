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
        "active": True,
        "endpoints":{}
    },

    {
        "id": 2,
        "name" : "Magalu",
        "url" : [
            ["https://www.magazineluiza.com.br/", 0]
        ],
        "categories" : [],
        "active": True,
        "endpoints":{}
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
        "endpoints":{}
    },

    {
        "id": 4,
        "name" : "Kabum",
        "url" : [
            ["https://www.kabum.com.br/", 0],
        ],
        "categories" : [],
        "active": True,
        "endpoints": {
            "public":{
                "predicate":"https://servicespub.prod.api.aws.grupokabum.com.br/catalog/v2",
                "all_products": "/products",
                "most_sold_promo": "/promotion/maisvendidos",
                "category": "/products-by-category/PLACEHOLDER_STR",
                "filters":{
                    "predicate":"?page_number=1&page_size=100",
                    "most_sold":"&sort=most_searched",
                    "price_asc":"&sort=price",
                    "price_desc":"&sort=-price",
                    "most_rated":"&sort=-number_ratings",
                    "most_recent":"&sort=-date_product_arrived",
                },
            },
            "campaign":{
                "predicate": "https://b2lq2jmc06.execute-api.us-east-1.amazonaws.com/PROD/ofertas?campanha=PLACEHOLDER_STR",
                "filters":{
                    "predicate":"&pagina=1&limite=100&marcas=&valor_min=&valor_max=&estrelas=&desconto_minimo=&desconto_maximo=&dep=&sec=&vendedor_codigo=&string=&app=1&separar_campos=1&tipo=&selo=&entrega_flash=0&frete_gratis=0",
                    "price_asc":"&ordem=3",
                    "price_desc":"&ordem=4",
                    "discount_asc":"&ordem=5",
                    "discount_desc":"&ordem=6",
                }
            }
        }
    }

]

