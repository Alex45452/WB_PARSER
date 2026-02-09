from parser import parser
from settings import CUR_VOL

def main():    
    for vol in range(CUR_VOL,8000):
        for part in range(100):
            for pos in range(1000):
                id = parser(vol,part,pos)
                if id:
                    ...
                    # write id to file in url style