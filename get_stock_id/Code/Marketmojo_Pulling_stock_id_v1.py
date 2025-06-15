# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 17:02:15 2021

@author: SAJI
"""
from datetime import datetime, timedelta
import pandas as pd
import urllib.request as request
import json
from itertools import chain   
import concurrent.futures

start_time = datetime.now()
df = pd.read_csv("D:\StockMarket\Program\Marketmojo_Pulling_stock_id\Input\EQUITY_L.csv")
symbol = df.iloc[:,6]
#symbol =['INE492A01029','INE175A01038','INE636A01039','INE454M01024','INE294G01026','INE429F01012','INE236G01019','INE144J01027','INE253B01015','INE748C01020','INE470A01017','INE105C01023']

def fetch_data(isin):
    url = f'https://www.marketsmojo.com/portfolio-plus/frontendsearch?SearchPhrase={isin}'
    try:
        with request.urlopen(url) as response:
            source = response.read()
            data = json.loads(source)
            if len(data) >= 1:
                return ('found', data)
            else:
                return ('not_found', isin)
    except Exception:
        return ('not_found', isin)
id_f = []
id_nf = []
symbols = list(symbol)
batch_size = 10
for batch_start in range(0, len(symbols), batch_size):
    batch = symbols[batch_start:batch_start+batch_size]
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        results = list(executor.map(fetch_data, batch))
    for result_type, value in results:
        if result_type == 'found':
            id_f.append(value)
        else:
            id_nf.append(value)
    t = len(id_f)
    print(t) if t % 10 == 0 else t
found=pd.DataFrame(list(chain.from_iterable(id_f)))
found = found.drop_duplicates(subset=['Id'])
not_found=pd.DataFrame(id_nf)
End_time = datetime.now()
print(start_time)
print(End_time)

            #print(new)
found.to_csv('D:\StockMarket\Program\Marketmojo_Pulling_stock_id\Output\stock_id_found.csv', index=False, header=True)
not_found.to_csv('D:\StockMarket\Program\Marketmojo_Pulling_stock_id\Output\stock_id_not_found.csv', index=False, header=True)
text_file = open("D:\StockMarket\Program\Marketmojo_Pulling_stock_id\Output\Duration.txt", "w")
text_file.write("start_time "+ str(start_time)+'\n'+"End_time "+ str(End_time)+'\n'
                +"ISIN_count :" + str(len(symbol))+'\n'
                +"Stock_found_count :" + str(len(id_f))+'\n'
                +"Stock_not_found_count : "+str(len(id_nf)))
text_file.close()

