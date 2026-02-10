from settings import ANTI_WORDS_HARD,ANTI_WORDS_SOFT,KW_FILENAME,SUP_FILENAME,SCORE_FILTER
import re


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
        all_kw.update(kw[0])
    for name,kw in category.items():
        aw = list(all_kw.difference(kw[0]))
        category[name].append(aw)

def upload_antiwords():
    for category in CATEGORIES:
        create_anitiwods(category)
    # todo db antiwords uploading

def filter_uploader():
    upload_keywords()
    upload_antiwords()

def normalize_name(name: str):
    name = name.lower()
    name = name.replace('+'," plus ")
    name = re.sub(r"[^a-z0-9а-я\s]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return set(name.split())

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
    matched_cnt = sum(1 for t in pattern_tokens[0] if t in name_tokens)
    if "samsung" in pattern_tokens[0]: 
        matched_cnt += 0.95
    penalty_score = sum(0.1 for t in ANTI_WORDS_SOFT if t in name_tokens)
    penalty_score += sum(0.2 for t in pattern_tokens[1] if t in name_tokens)
    return matched_cnt / len(pattern_tokens[0]) - penalty_score

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
    item_name = check_name(parser_res["item_name"])
    if item_name and sup_name:
        return {
            "sup_name":sup_name,
            "item_name":item_name
        }
    return False

filter_uploader()