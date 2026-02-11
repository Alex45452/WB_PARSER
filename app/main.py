from parser import parser
from settings import CUR_VOL
from openpyxl import Workbook

# add logging

def main():    
    wb = Workbook()
    ws = wb.active
    ws.title = "products"

    ws.append(["product_id", "supplier_id", "link"])
    wb.save("wb_products.xlsx")

    for vol in range(CUR_VOL,8000):
        for part in range(100):
            for pos in range(1000):
                try:
                    vol,part,pos = 3610,77,249
                    res = parser(vol,part,pos)                    
                    if res:
                        id,sup,name = res
                        link = f"https://www.wildberries.ru/catalog/{id}/detail.aspx"
                        ws.append([sup, name, link])
                        wb.save("wb_products.xlsx")
                except:
                    print(vol,part,pos)
                    
if __name__ == "__main__":
    main()