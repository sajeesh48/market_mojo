# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 22:48:56 2024

@author: sajie
"""

from datetime import datetime
import pandas as pd
import urllib.request as request
import json
import requests
import threading

start_time = datetime.now()



df = pd.read_csv('D:\StockMarket\Program\Marketmojo_Pulling_data_from_stockid\Input\stock_id_found_bse.csv')

data_f=[]
data_nf=[]

stock_id = df['Id'].tolist()

stock_id = list(set(stock_id))


#stock_id =['984165','80980980090898']
def find_data(stock_id):
    t=len(data_f)
    print('stockid_data'+str(t)) if t%10==0 else t
    url = 'https://frapi.marketsmojo.com/stocks_stocksid/header_info?sid='+str(stock_id)
    print(url)
    with requests.get(url) as response:
        data = response.json()
    #with request.urlopen('https://frapi.marketsmojo.com/stocks_stocksid/header_info?sid='+str(i)) as response:
     #   source = response.read()
      #  data = json.loads(source)
        if data['code'] == '200':
            data_f.append(data)
        else:
            data_nf.append(stock_id)


thread_list = []
for x in range(len(stock_id)):
    li=str(stock_id[x])
    print(li)
    t = threading.Thread(target = find_data, args = (li,))
    thread_list.append(t)
for threads in thread_list:
    threads.start()
for threads in thread_list:
    threads.join()
         

df1 = pd.DataFrame(data_nf)
df1.to_csv('D:\StockMarket\Program\Marketmojo_Pulling_data_from_stockid\Output_bse\stock_id_404_error_bse.csv', index=False, header=True)
        

columns=[]
m_lst=[]
m_lst_nf=[]
for j in data_f:
    t=len(m_lst)
    print('stock_Valuations'+str(t)) if t%10==0 else t
    if 'details' in j['data']:
        d = j['data']['details']
        if 'dot_summary' in j['data']:
            e = j['data']['dot_summary']
            lst=[]
            for data in d:
                col=data['field_name']
                if col not in columns:
                    columns.append(col)
                lst.append(data['value'])
            for data in e:
                #print(e[data])
                #print(data,e[data])
                if data.endswith('_txt') | (data=='score') | (data=='tech_score') | (data.endswith('Text')) | (data.endswith('_rank')):
                    lst.append(e[data])
                    if data not in columns:
                        columns.append(data)
            m_lst.append(lst)
        else:
            m_lst_nf.append(j)
    else:
        m_lst_nf.append(j)

my_df = pd.DataFrame(m_lst,columns=columns)
my_df_nf = pd.DataFrame(m_lst_nf)
    
#my_df = pd.DataFrame(m_lst,columns=["BSE_ID", "Company_Name", "ISIN", "Industry", "Market_cap","Quality","Valuation","Financial","Technical","Quality_value"])

my_df.to_csv('D:\StockMarket\Program\Marketmojo_Pulling_data_from_stockid\Output_bse\stock_data_found_bse.csv', index=False, header=True)
my_df_nf.to_csv('D:\StockMarket\Program\Marketmojo_Pulling_data_from_stockid\Output_bse\stock_data_not_found_bse.csv', index=False, header=True)

End_time = datetime.now()

text_file = open("D:\StockMarket\Program\Marketmojo_Pulling_data_from_stockid\Output\Duration_bse.txt", "w")

text_file.write("start_time "+ str(start_time)+'\n'
                +"End_time "+ str(End_time)+'\n'
                + "How Many Stock ID :" + str(len(stock_id))+'\n'
                +"404_error_count :"+str(len(data_nf))+'\n'
                +"stock_data_found_count :" +str(len(m_lst))+'\n'
                +"stock_data_not_found_count :" +str(len(m_lst_nf)))
text_file.close()


End_time = datetime.now()
print(start_time)
print(End_time) 

