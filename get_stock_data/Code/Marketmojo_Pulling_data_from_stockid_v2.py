# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 17:02:15 2021

@author: SAJI
"""
from datetime import datetime
import pandas as pd
import requests
import threading
import yfinance as yf
import logging

api_url = 'https://frapi.marketsmojo.com/stocks_stocksid/header_info?sid='
#output_dir = "D:\\StockMarket\\Program\\Marketmojo_Pulling_data_from_stockid\\Output\\"
#archives = "D:\\StockMarket\\Program\\Marketmojo_Pulling_data_from_stockid\\archives\\"
#stock_id_dir= 'D:\\StockMarket\\Program\\Marketmojo_Pulling_data_from_stockid\\Input\\stock_id_found.csv'
#raw_data = output_dir + "stock_data_found.csv"
#stocks_inprogress = 'D:\\StockMarket\\Program\\Marketmojo_Pulling_data_from_stockid\\Output\\stocks_inprogress.csv'
#pl_booked_stocks = 'D:\\StockMarket\\Program\\Marketmojo_Pulling_data_from_stockid\\Output\\overall_profit_loss.csv'
#log_path = "D:\\StockMarket\\Program\\Marketmojo_Pulling_data_from_stockid\\logs\\"*



output_dir = "/home/ec2-user/market_mojo/tmp/get_stock_data/Output/"
archives = "/home/ec2-user/market_mojo/tmp/get_stock_data/archives/"
stock_id_dir= '/home/ec2-user/market_mojo/tmp/get_stock_data/Input/stock_id_found.csv'
raw_data = output_dir + "stock_data_found.csv"
stocks_inprogress = '/home/ec2-user/market_mojo/tmp/get_stock_data/Output/stocks_inprogress.csv'
pl_booked_stocks = '/home/ec2-user/market_mojo/tmp/get_stock_data/Output/overall_profit_loss.csv'
log_path = '/home/ec2-user/market_mojo/tmp/get_stock_data/logs/'

# Logger setup
def setup_logger(log_path):
    logging.basicConfig(
        filename=log_path,
        filemode="a",
        format="%(asctime)s %(levelname)s:%(message)s",
        level=logging.INFO
    )

def get_nse_stock_price(ticker_symbol):
    """
    Returns the current price of the given NSE stock ticker (e.g., 'RELIANCE.NS').
    Always appends '.NS' to the ticker symbol.
    """
    ticker_symbol = f"{ticker_symbol}.NS"
    stock = yf.Ticker(ticker_symbol)
    stock_info = stock.info
    return stock_info.get("currentPrice")

def find_data(stock_id, data_f, data_nf):
    t = len(data_f)
    print('stock_id_data' + str(t)) if t % 10 == 0 else t
    url = f'{api_url}{stock_id}'
    print(url)
    with requests.get(url) as response:
        data = response.json()
        if data['code'] == '200':
            data_f.append(data)
        else:
            data_nf.append(stock_id)


def process_data(data_f):
    logging.info("Processing data...")
    columns = []
    m_lst = []
    m_lst_nf = []
    for j in data_f:
        t = len(m_lst)
        print('stock_Valuations' + str(t)) if t % 10 == 0 else t
        if 'details' in j['data']:
            d = j['data']['details']
            if 'dot_summary' in j['data']:
                e = j['data']['dot_summary']
                lst = []
                for data in d:
                    col = data['field_name']
                    if col not in columns:
                        columns.append(col)
                    lst.append(data['value'])
                for data in e:
                    if data.endswith('_txt') or (data == 'score') or (data == 'tech_score') or data.endswith(
                            'Text') or data.endswith('_rank'):
                        lst.append(e[data])
                        if data not in columns:
                            columns.append(data)
                m_lst.append(lst)
            else:
                m_lst_nf.append(j)
        else:
            m_lst_nf.append(j)
    logging.info(f"Total processed records: {len(m_lst)} found, {len(m_lst_nf)} not found.")
    return m_lst, m_lst_nf, columns


def filter_strong_buy_stocks(my_df):
    """
    Filter stocks with:
    scoreText == 'Strong Buy' AND
    f_txt in ['Very Positive', 'Positive', 'Outstanding'] AND
    v_txt in ['Attractive', 'Very Attractive'] AND
    q_txt in ['Excellent', 'Good']
    Then remove specified columns from the filtered result.
    After filtering, add columns: today's date and Boughtprice (from get_nse_stock_price).
    """
    logging.info("Filtering stocks with Strong Buy criteria...")
    filtered = my_df[
        (my_df['scoreText'] == 'Strong Buy') &
        (my_df['f_txt'].isin(['Very Positive', 'Positive', 'Outstanding'])) &
        (my_df['v_txt'].isin(['Attractive', 'Very Attractive'])) &
        (my_df['q_txt'].isin(['Excellent', 'Good']))
        ]
    drop_cols = [
        'BSEID', 'ISIN', 'industry', 'q_rank', 'q_txt', 'v_rank', 'v_txt',
        'f_txt', 'tech_score', 'tech_txt', 'score', 'scoreText', 'prevScoreText'
    ]
    filtered_df = filtered.drop(columns=[col for col in drop_cols if col in filtered.columns], errors='ignore')
    filtered_df = filtered_df.reset_index(drop=True)
    # Add today's date column
    today_str = datetime.now().strftime("%d-%m-%Y")
    filtered_df['date'] = today_str
    filtered=check_filtered_in_stock_id_dir(filtered_df, stocks_inprogress)
    check_score_of_stock_id_df(filtered,pl_booked_stocks,raw_data)
    logging.info("Filtering complete. Strong Buy stocks processed.")

def check_filtered_in_stock_id_dir(filtered_df, stocks_inprogress):
    """
    Checks which NSEIDs from filtered_csv_path are present in stock_id_dir_csv_path.
    Prints NSEIDs not found in stock_id_dir.
    """
    logging.info("Checking filtered NSEIDs in stock_id_dir")
    stock_id_df = pd.read_csv(stocks_inprogress)
    for i in range(len(filtered_df)):
        nseid = filtered_df.loc[i, 'NSEID']
        if nseid not in stock_id_df['NSEID'].values:
            logging.info(f"NSEID {nseid} not found in stock_id_dir.")
            new_row = {
                'NSEID': nseid,
                'MARKETCAP': filtered_df.loc[i, 'MARKET CAP'],
                'Start_Date': filtered_df.loc[i, 'date'],
                'Bought_Price': get_nse_stock_price(nseid)
            }
            stock_id_df = pd.concat([stock_id_df, pd.DataFrame([new_row])], ignore_index=True)
    stock_id_df['Current_Price'] = stock_id_df['NSEID'].apply(get_nse_stock_price)
    stock_id_df.to_csv(stocks_inprogress, index=False)
    logging.info(f"Updated stock_id_dir with {len(stock_id_df)} entries.")
    return stock_id_df

    
def check_score_of_stock_id_df(filtered, pl_booked_stocks, raw_data):
    logging.info("Checking Current scores of filtered stocks in stock_id_dir")
    value = []
    raw_data_df = pd.read_csv(raw_data)
    pl_data = pd.read_csv(pl_booked_stocks)
    for i in range(len(filtered)):
        score = filtered.loc[i, 'NSEID']
        logging.info(f"Processing NSEID: {score}")
        if score in raw_data_df['NSEID'].values:            
            # get the scoreText from raw_data
            score_text = raw_data_df.loc[raw_data_df['NSEID'] == score, 'scoreText'].values[0]
            logging.info(f"ScoreText for {score} is {score_text}.")
            end_date =datetime.now().strftime("%d-%m-%Y")
            if score_text != 'Strong Buy' and score_text != 'Buy':
                new_row = {
                    'NSEID': score,
                    'market_cap': filtered.loc[i, 'MARKETCAP'],
                    'Start_Date': filtered.loc[i, 'Start_Date'],
                    'End_Date': end_date,
                    'Bought_Price': filtered.loc[i, 'Bought_Price'],
                    'Sold_price': get_nse_stock_price(score),
                }
                new_row['Returns'] = (new_row['Sold_price'] - new_row['Bought_Price']) / new_row['Bought_Price'] * 100
                value.append(new_row)
                closed_stock = pd.concat([pl_data, pd.DataFrame([new_row])], ignore_index=True)
                #append the closed stock to pl_booked_stocks with existing data
                closed_stock.to_csv(pl_booked_stocks, index=False)
                logging.info(f"Closed stock {score} with scoreText {score_text} added to pl_booked_stocks.")
                return closed_stock
            else:
                logging.info(f"NSEID {score} already exists in stock_id_dir with scoreText {score_text}. No action taken")
                

def save_outputs(data_nf, m_lst, m_lst_nf, columns, date_str, start_time, End_time, stock_id):
    df1 = pd.DataFrame(data_nf)
    df1.to_csv(f'{output_dir}stock_id_404_error_{date_str}.csv', index=False, header=True)
    df1.to_csv(f'{archives}stock_id_404_error.csv', index=False, header=True)
    my_df = pd.DataFrame(m_lst, columns=columns)
    my_df_nf = pd.DataFrame(m_lst_nf)
    my_df.to_csv(f'{output_dir}stock_data_found.csv', index=False, header=True)
    my_df.to_csv(f'{archives}stock_data_found_{date_str}.csv', index=False, header=True)
    my_df_nf.to_csv(f'{output_dir}stock_data_not_found.csv', index=False, header=True)
    my_df_nf.to_csv(f'{archives}stock_data_not_found_{date_str}.csv', index=False, header=True)
    #my_df = pd.read_csv(        'D:\\StockMarket\\Program\\Marketmojo_Pulling_data_from_stockid\\Output\\stock_data_found_080625.csv')
    # Filter and save strong buy stocks
    filter_strong_buy_stocks(my_df)
    # Write summary to Duration.txt
    with open(f"{output_dir}Duration.txt", "w") as text_file:
        text_file.write("start_time " + str(start_time) + '\n'
                        + "End_time " + str(End_time) + '\n'
                        + "How Many Stock ID :" + str(len(stock_id)) + '\n'
                        + "404_error_count :" + str(len(data_nf)) + '\n'
                        + "stock_data_found_count :" + str(len(m_lst)) + '\n'
                        + "stock_data_not_found_count :" + str(len(m_lst_nf)))

    logging.info("Outputs saved successfully.")

def main():
    setup_logger(log_path + f"marketmojo_data_pull_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    start_time = datetime.now()
    df = pd.read_csv(stock_id_dir)
    data_f = []
    data_nf = []
    stock_id = list(set(df['Id'].tolist()))
    date_str = datetime.now().strftime("%d%m%y")
    thread_list = []
    for sid in stock_id:
        li = str(sid)
        print(li)
        t = threading.Thread(target=find_data, args=(li, data_f, data_nf))
        thread_list.append(t)
    for t in thread_list:
        t.start()
    for t in thread_list:
        t.join()

    m_lst, m_lst_nf, columns = process_data(data_f)
    end_time = datetime.now()
    save_outputs(data_nf, m_lst, m_lst_nf, columns, date_str, start_time, end_time, stock_id)
    logging.info("Data processing completed.")
    logging.info(f"Start time: {start_time}, End time: {end_time}")
    print("Data processing completed.")


if __name__ == "__main__":
    main()
