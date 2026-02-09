import httpx
import filter

sup-db = {} # db of suppliers_id:supplier


def parse_item(url):
    r = httpx.get(url)
    if r.status_code != httpx.codes.OK:
        return False
    card = r.json()
    return {
        "id":card["imt_name"], 
        "sup_id":card["selling"]["supplier_id"]
        }

def parse_sup_name(url):
    r = httpx.get(url)
    if r.status_code != httpx.codes.OK:
        return False
    card = r.json()
    return card["trademark"]
    

def get_sup(sup_id):
    name = sup-db.get(id,False)
    if not name:
        url = f"https://static-basket-01.wbbasket.ru/vol0/data/supplier-by-id/{sup_id}.json" #to_constants
        name = parse_sup_name(url)
        if not name:
            return False
        sup-db[sup_id] = name
    return name


def parser(vol,part,pos): # return id of item
    # rewrite parser with yield
    url = f"https://basket-{CUR_BASKET_ID}.wbbasket.ru/vol{vol}/part{vol}{part}/{vol}{part}{pos}/info/ru/card.json"
    parser_result = parse_item(url)
    if not parser_result:
        CUR_BASKET_ID += 1
        parser_result = parse_item(url)
        if not parser_result:
            CUR_BASKET_ID -= 1
            return False
    if parser_result and filter.filter_result(parser_result):
        return f"{vol}{part}{pos}"
