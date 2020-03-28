import sys
sys.path.append("..") # Adds higher directory to python modules path.

import json, requests, logging, urllib3, ftplib, os, re, time, datetime, helpers, pickle, multiprocessing, traceback, threading
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures as futures
import psycopg2
import config
from helpers import *
import bs4 as bs
from yahoo_finance import Share
import yfinance as yf
import pandas as pd
http = urllib3.PoolManager()

# setup the logger
logger = logging.getLogger(__name__)

# define file handler and set formatter
file_handler = logging.FileHandler('logfile.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)




    
class StockListFetcher:
    
    def get_sp500_members(self):
        resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        soup = bs.BeautifulSoup(resp.text, features='html.parser')
        table = soup.find('table',{'class':'wikitable sortable'})
        tickers = []
        conn = postgres_conn()
        # Get current list from Wikipedia
        # logger.setLevel(logging.INFO)
        # logger.info(f'Stock list fetcher: Updating S&P 500 members...')
        print ("Processing the S&P 500 list...")
        with conn.cursor() as cursor:
            for row in table.findAll('tr')[1:]:
                ticker = row.findAll('td')[0].text
                cleaned_ticker = ticker.rstrip()
                tickers.append(cleaned_ticker)

                check_str = 'SELECT symbol FROM  tickerdata_symbol WHERE symbol = %s'
                args = (cleaned_ticker,)
                cursor.execute(check_str, args)
                if cursor.rowcount == 0:
                    # add the ticker
                    insert_str = 'INSERT INTO tickerdata_symbol (symbol,sp500_member,spdr_member,alphavantage_status,yahoo_status) VALUES (%s,%s,%s,%s,%s)'
                    args = (cleaned_ticker,1,0,200,200)
                    print (f'Adding ticker: {cleaned_ticker} as sp500 member')
                    cursor.execute(insert_str,args)
                else:
                    # update the ticker  
                    update_str = 'UPDATE tickerdata_symbol SET sp500_member = true WHERE symbol = %s'
                    args = (cleaned_ticker,)
                    
                    cursor.execute(update_str, args)
                    
                conn.commit()
            
            # update S&P 500 not members anymore
            select_all_str = 'SELECT symbol FROM tickerdata_symbol'
            cursor.execute(select_all_str)
            data = cursor.fetchall()
            
            with conn.cursor() as cursor:
                for row in data:
                    str_symbol = row[0]
                    if str_symbol not in tickers:
                        update_excluded_str = 'UPDATE tickerdata_symbol SET sp500_member = false WHERE symbol = %s'
                        args = (str_symbol,)
                        cursor.execute(update_excluded_str,args)
                conn.commit()

        #print(tickers)
    def get_etfs(self):
        resp = requests.get('https://topforeignstocks.com/etf-lists/the-complete-list-of-spdr-etfs-trading-on-the-us-stock-exchanges/')
        soup = bs.BeautifulSoup(resp.text, features="html.parser")
        table = soup.find('table',{'class':'tablepress-id-2511'})
        tickers = []
        conn = postgres_conn()
        # Get current list from Wikipedia
        # logger.setLevel(logging.INFO)
        # logger.info(f'Stock list fetcher: Updating SPDR ETFs...')
        print ("Processing the SPDR ETFs list...")
        with conn.cursor() as cursor:
            for row in table.findAll('tr')[1:]:
                ticker = row.findAll('td')[2].text
                cleaned_ticker = ticker.rstrip()
                tickers.append(cleaned_ticker)

                check_str = 'SELECT symbol FROM tickerdata_symbol WHERE symbol = %s'
                args = (cleaned_ticker,)
                cursor.execute(check_str, args)
                if cursor.rowcount == 0:
                    # add the ticker
                    insert_str = 'INSERT INTO tickerdata_symbol (symbol,spdr_member,sp500_member,alphavantage_status,yahoo_status) VALUES (%s,%s,%s,%s,%s)'
                    args = (cleaned_ticker,True,False,200,200)
                    print (f'Adding ticker: {cleaned_ticker} as SPDR member')
                    cursor.execute(insert_str,args)
                else:
                    # update the ticker  
                    update_str = 'UPDATE tickerdata_symbol SET spdr_member = 1 WHERE symbol = %s'
                    args = (cleaned_ticker,)
                    
                    cursor.execute(update_str, args)
                    
                conn.commit()
            
            # update S&P 500 not members anymore
            select_all_str = 'SELECT symbol FROM tickerdata_symbol'
            cursor.execute(select_all_str)
            data = cursor.fetchall()
            
            with conn.cursor() as cursor:
                for row in data:
                    str_symbol = row[0]
                    if str_symbol not in tickers:
                        update_excluded_str = 'UPDATE tickerdata_symbol SET spdr_member = false WHERE symbol = %s'
                        args = (str_symbol,)
                        cursor.execute(update_excluded_str,args)
                conn.commit()

class FeedEngine:

    def fetch_all_data_with_alpha(self,pull_frequency = 3,sleep = 1):
        nt = NasdaqTraderParser()
        nt.get_stock_symbols()
        alpha = AlphaVantageParser()
        # open db connection
        conn = postgres_conn()
        # USE ALPHA VANTAGE FIRST
        with conn.cursor() as cursor:
            select_str = 'select symbol from tickerdata_symbol where (now() > last_pull + interval \'%s day\') OR last_pull is null) AND (alphavantage_status = 200) order by symbol desc'
            args = (pull_frequency,)
            cursor.execute(select_str,args)
            data = cursor.fetchall()
            for count, row in enumerate(data):
                while not internet_on():
                    print ('Internet not on. Sleeping...')
                    time.sleep(5)
                    
                print (f"Downloading 1 min data for: {row[0]}")
                alpha.get_historical_data(row[0])
                time.sleep(sleep)
                
            # logger.setLevel(logging.INFO)
            # logger.info(f'(Feed Engine) Processed ({count}) stock data')
            
    def fetch_sp500_data(self,feed = 'Yahoo',pull_frequency = 3,sleep = 0):
        
        nt = NasdaqTraderParser()
        nt.get_stock_symbols()
        fetcher = StockListFetcher()
        fetcher.get_sp500_members()
        #fetcher.get_etfs()
        if feed == 'Yahoo':
            parser = YahooFinanceParser()
            select_str = 'select symbol from tickerdata_symbol where (now() > last_pull + INTERVAL \'%s days\' OR last_pull is null) AND (yahoo_status = 200) AND (sp500_member = true) order by symbol ASC'
        elif feed == 'Alpha':
            parser = AlphaVantageParser()
            select_str = 'select symbol from tickerdata_symbol where (now() > last_pull + INTERVAL \'%s days\') OR last_pull is null) AND (alphavantage_status = 200) AND (sp500_member = true) order by symbol ASC'
        else:
            
            return False

        conn = postgres_conn()
        with conn.cursor() as cursor:
            args = (pull_frequency,)
            cursor.execute(select_str,args)
            symbols = [row[0] for row in cursor.fetchall()]
            all_count = cursor.rowcount #the number of symbols to be processed
        conn.close()
        

        #processes = []
        def _fetch_symbol(symbol):
                #try:
            #actual_count = count + 1
            while not internet_on():
                print ('Internet not on. Sleeping...')
                time.sleep(5)
                
            print (f"Downloading 1 min data for: {symbol}...")
            parser.get_historical_data(symbol)
            if not sleep == 0:
                time.sleep(sleep)
                    
                #except Exception as e:
                            
                #    print (f'Failed to process: {symbol}')
                #    print (e)

        print (symbols)
        
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=50) as executor:
            jobs = executor.map(_fetch_symbol,symbols)

            for future in futures.as_completed(jobs):
                print (future)
        #for count, row in enumerate(data):
        #    actual_count = count + 1
        #    while not internet_on():
        #        print ('Internet not on. Sleeping...')
        #        time.sleep(5)
                
        #    print (f"Downloading 1 min data for: {row[0]}. Position: {actual_count} of {all_count}")
        #    parser.get_historical_data(row[0])
        #    time.sleep(sleep)
            
        #    p = multiprocessing.Thread(target=_multiprocessing_fetch_data, args=(row,count))
        #    processes.append(p)   
        #    p.start()
        
        #for process in processes: # join the processes
        #    process.join()

    
        #fetch_all_symbols(symbols)
        duration = time.time() - start_time
        print (f"Downloaded all S&P in {duration} seconds")
        # logger.setLevel(logging.INFO)
        # logger.info(f'(Feed Engine) Processed stock data')


    def fetch_spdr_data(self,feed = 'Yahoo',pull_frequency = 3,sleep = 1):
        
        nt = NasdaqTraderParser()
        nt.get_stock_symbols()
        fetcher = StockListFetcher()
        fetcher.get_sp500_members()
        fetcher.get_etfs()
        if feed == 'Yahoo':
            parser = YahooFinanceParser()
            second_parser = AlphaVantageParser()
            select_str = 'select symbol from tickerdata_symbol where (now() > last_pull + INTERVAL \'%s days\') OR last_pull is null) AND (yahoo_status = 200) AND (spdr_member = true) order by symbol ASC'
        elif feed == 'Alpha':
            parser = AlphaVantageParser()
            second_parser = YahooFinanceParser()
            select_str = 'select symbol from tickerdata_symbol where (now() > last_pull + INTERVAL \'%s days\') OR last_pull is null) AND (alphavantage_status = 200) AND (spdr_member = true) order by symbol ASC'
        else:
            return False

        
        # open db connection
        conn = postgres_conn()

        
        with conn.cursor() as cursor:
            
            args = (pull_frequency,)
            cursor.execute(select_str,args)
            data = cursor.fetchall()
            all_count = cursor.rowcount
            my_count = 0
            for count, row in enumerate(data):
                actual_count = count + 1
                while not internet_on():
                    print ('Internet not on. Sleeping...')
                    time.sleep(5)
                    
                print (f"Downloading 1 min data for: {row[0]}. Position: {actual_count} of {all_count}")
                print (row[0])
                parser.get_historical_data(row[0])
                time.sleep(sleep)
                my_count = count
                
            # logger.setLevel(logging.INFO)
            # logger.info(f'(Feed Engine) Processed ({my_count}) stock data')
    
    # end fetch spdr data ////////////////////////////////////////////////////////////////////////////////////////


    

#import yahooFeed
#p = yahooFeed.YahooFinanceFeed()
#p.get_historical_data('SPY')
# nt = NasdaqTraderParser()
# nt.get_stock_symbols()

#fe = FeedEngine()                                               
#fe.fetch_spdr_data('Yahoo',3,.25)
#fe.fetch_sp500_data('Yahoo',3,5)

#sd = StockListFetcher()
#sd.get_sp500_members()
#sd.get_etfs()
#sd.get_sp500_members()
#fe.fetch_sp500_data()
#yp = YahooFinanceParser()
#yp.get_historical_data('AAPL')