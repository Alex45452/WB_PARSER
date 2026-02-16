def card_url(nm_id: int, basket: int) -> str:
    vol = nm_id // 100000
    part = nm_id // 1000
    return f"https://basket-{basket:02d}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/info/ru/card.json"
