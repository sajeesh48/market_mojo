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
import threading

start_time = datetime.now()

df = pd.read_csv("D:\StockMarket\Program\Marketmojo_Pulling_stock_id\Input\equity_bse.csv")

stock_id = df['SCRIPCODE'].tolist()

stock_id = list(set(stock_id))

#symbol =['INE492A01029','INE175A01038','INE636A01039','INE454M01024','INE294G01026','INE429F01012','INE236G01019','INE144J01027','INE253B01015','INE748C01020','INE470A01017','INE105C01023']

id_f=[]
id_nf=[]



def find_symbols(i):
    t=len(id_f)
    print(t) if t%10==0 else t
    #url1 = 'https://www.marketsmojo.com/portfolio-plus/frontendsearch?SearchPhrase='+i
    with request.urlopen('https://www.marketsmojo.com/portfolio-plus/frontendsearch?SearchPhrase='+i) as response:
            source = response.read()
            data = json.loads(source)
            if (len(data)) >= 1 :
                id_f.append(data)
            else:
                id_nf.append(i)
    
thread_list = []
for x in range(len(stock_id)):
    li=str(stock_id[x])
    print(li)
    t = threading.Thread(target = find_symbols, args = (li,))
    thread_list.append(t)
for threads in thread_list:
    threads.start()
for threads in thread_list:
    threads.join()


found=pd.DataFrame(list(chain.from_iterable(id_f)))
found = found.drop_duplicates(subset=['id'])
not_found=pd.DataFrame(id_nf)
End_time = datetime.now()
print(start_time)
print(End_time)

            #print(new)
found.to_csv('D:\StockMarket\Program\Marketmojo_Pulling_stock_id\Output\stock_id_found_bse.csv', index=False, header=True)
not_found.to_csv('D:\StockMarket\Program\Marketmojo_Pulling_stock_id\Output\stock_id_not_found_bse.csv', index=False, header=True)



text_file = open("D:\StockMarket\Program\Marketmojo_Pulling_stock_id\Output\Duration_bse.txt", "w")

text_file.write("start_time "+ str(start_time)+'\n'+"End_time "+ str(End_time)+'\n'
                +"ISIN_count :" + str(len(stock_id))+'\n'
                +"Stock_found_count_bse :" + str(len(id_f))+'\n'
                +"Stock_not_found_count_bse : "+str(len(id_nf)))
text_file.close()

