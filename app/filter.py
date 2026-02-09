import re

SCORE_FILTER = 0.9

ANTI_WORDS_HARD = {
    "чехол","case","кейс","бампер","накладка","обложка","флип","flip",
    "книжка","кобура","футляр","сумка","карман","папка","мешок",

    "стекло","стеклышко","стеклянное","пленка","плёнка","гидрогелевая",
    "гидрогель","антибликовая","антишпион","privacy","protective","защитная",

    "кабель","провод","шнур","зарядка","зарядное","адаптер","блок",
    "блокпитания","powerbank","power","аккумулятор","акб","батарея","battery",

    "запчасть","запчасти","деталь","детали","ремонт","ремкомплект",
    "шлейф","разъем","разъём","коннектор","плата","микросхема","чип",
    "корпус","крышка","стеклоэкрана","дисплей","экран","тачскрин",

    "игра","game","диск","cd","dvd","blu-ray","ключ","код","активация",
    "подписка","subscription","psn","steam","xbox","license","лицензия",

    "бу","б/у","used","refurb","refurbished","восстановленный",
    "восстановлен","уценка","уцененный","витринный","демо","demo",

    "копия","реплика","аналог","fake","неоригинал","совместимый",
    "replacement","oem",

    "набор","сет","bundle","kit","комплект","без","безкоробки",
    "безупаковки","no",

    "держатель","подставка","крепление","штатив","стойка","рамка",

    "доставка","гарантия","страховка","услуга","настройка","чистка"
}

ANTI_WORDS_SOFT = {
    "для","под","совместим","подходит","аксессуар","ориг","original",
    "brand","new","новинка","хит","топ","sale","скидка",

    "ростест","рст","eu","us","cn","china","hk","global","версии","версия",

    "без коробки","без упаковки","упаковка","пломба","запечатан",

    "идеал","отличный","новый","как новый","полный комплект",

    "официальный","сертифицирован","гарантийный","магазин","продавец"
}


SAMSUNG_KW = {}
IPHONE_KW = {}
MACBOOK_KW = {}
PS5_KW = {}
OTHER_KW = {}

allowed_suppliers_db = {}

def upload_keywords():
    # from file to dict
    # for the future from db to dict
    # with normalization of lines, line split to keywords
    # with 5 categories: samsung, iphone, macbook, ps5, other
    ...

def upload_suppliers():
    # from file to dict
    ...
    # for the future from db to dict

def upload_antiwords():
    ...
    # todo

def normalize_name(name: str):
    name = name.lower
    name = name.replace('+'," plus ")
    name = re.sub(r"[^a-z0-9а-я\s]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.split()



def choose_kw_part(name_tokens):
    if "samsung" in name_tokens:
        return SAMSUNG_KW
    if "iphone" in name_tokens:
        return IPHONE_KW
    if "playstation" in name_tokens or "ps5" in name_tokens:
        return PS5_KW
    if "macbook" in name_tokens:
        return MACBOOK_KW
    return OTHER_KW

def has_anti_words(tokens):
    return any(t in ANTI_WORDS_HARD for t in tokens)

def name_score(name_tokens, pattern_tokens):
    matched_cnt = sum(1 for t in pattern_tokens if t in name_tokens)
    penalty_cnt = sum(1 for t in ANTI_WORDS_SOFT if t in name_tokens)
    return (matched_cnt-penalty_cnt) / len(pattern_tokens)

def check_name(name):
    normal_name = normalize_name(name)
    if has_anti_words(normal_name):
        return False
    keywords = choose_kw_part(normal_name)
    for item_name,words in keywords.items():
        score = name_score(normal_name,words)
        if score > SCORE_FILTER:
            return item_name
    return False

def filter_result(parser_res):
    sup_name = allowed_suppliers_db.get(parser_res["sup_name"],False)
    item_name = check_name(parser_res["name"])
    if item_name and sup_name:
        return {
            "sup_name":sup_name,
            "item_name":item_name
        }
    return False