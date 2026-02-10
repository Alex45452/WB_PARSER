import re

SCORE_FILTER = 0.9
KW_FILENAME = "keywords.txt"
SUP_FILENAME = "suppliers.txt"

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

    "игра","game","диск","cd","dvd","ключ","код","активация",
    "подписка","subscription","psn","xbox","license","лицензия",

    "бу","б/у","used","refurb","refurbished","восстановленный",
    "восстановлен","уценка","уцененный","витринный","демо","demo",

    "копия","реплика","аналог","fake","неоригинал","совместимый",
    "replacement","oem",

    "набор","сет","bundle","kit","комплект","без","безкоробки",
    "безупаковки","no",

    "держатель","подставка","крепление","штатив","стойка","рамка",

    "доставка","гарантия","страховка","услуга","настройка","чистка","аксессуар",
    "без коробки","без упаковки","как новый"
}

ANTI_WORDS_SOFT = {
    "для","под","совместим","подходит","ориг","original",
    "brand","new","новинка","хит","топ","sale","скидка",

    "cn","china","hk","global","версии","версия",

    "упаковка","пломба","запечатан",

    "идеал","отличный","новый","полный комплект",

    "официальный","сертифицирован","гарантийный","магазин","продавец"
}

SAMSUNG_KW_AW = {}
IPHONE_KW_AW = {}
MACBOOK_KW_AW = {}
PS5_KW_AW = {}
YANDEX_KW_AW = {}
OTHER_KW_AW = {}
CATEGORIES = [SAMSUNG_KW_AW,IPHONE_KW_AW,MACBOOK_KW_AW,PS5_KW_AW,YANDEX_KW_AW,OTHER_KW_AW]

ALLOWED_SUPS = set()
checked_suppliers_db = {}

def upload_keywords():
    SAMSUNG_KW_AW = {}
    IPHONE_KW_AW = {}
    MACBOOK_KW_AW = {}
    PS5_KW_AW = {}
    YANDEX_KW_AW = {}
    OTHER_KW_AW = {}
    f = open(KW_FILENAME,"r",encoding="UTF-8")
    for line in f:
        item = line.strip('\n ')
        normal_item = normalize_name(line)
        choose_kw_part(normal_item)[item] = [normal_item]
    f.close()
    ...
    # for the future from db

def upload_suppliers():
    f = open(ALLOWED_SUPS,'r',encoding="UTF-8")
    for line in f:
        item = line.strip('\n ')
        ALLOWED_SUPS.add(item)
    ...
    # for the future from db

def create_anitiwods(category):
    all_kw = set()
    for kw in category.values():
        all_kw.add(kw)
    for name,kw in category.items():
        aw = list(all_kw.difference(kw))
        category[name].append(aw)

def upload_antiwords():
    for category in CATEGORIES:
        ...
    ...
    # todo db antiwords uploading

def filter_uploader():
    upload_keywords()
    upload_antiwords()

def normalize_name(name: str):
    name = name.lower
    name = name.replace('+'," plus ")
    name = re.sub(r"[^a-z0-9а-я\s]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.split()

def choose_kw_part(name_tokens):
    if "samsung" in name_tokens:
        return SAMSUNG_KW_AW
    if "iphone" in name_tokens:
        return IPHONE_KW_AW
    if "playstation" in name_tokens or "ps5" in name_tokens:
        return PS5_KW_AW
    if "macbook" in name_tokens:
        return MACBOOK_KW_AW
    if "яндекс" in name_tokens:
        return YANDEX_KW_AW
    return OTHER_KW_AW

def has_anti_words(tokens):
    return any(t in ANTI_WORDS_HARD for t in tokens)

def name_score(name_tokens, pattern_tokens):
    matched_cnt = sum(1 for t in pattern_tokens if t in name_tokens)
    if "samsung" in pattern_tokens: 
        matched_cnt += 0.95
    penalty_score = sum(0.1 for t in ANTI_WORDS_SOFT if t in name_tokens)
    return matched_cnt / len(pattern_tokens) - penalty_score

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
    sup_name = checked_suppliers_db.get(parser_res["sup_name"],False)
    item_name = check_name(parser_res["name"])
    if item_name and sup_name:
        return {
            "sup_name":sup_name,
            "item_name":item_name
        }
    return False

filter_uploader()