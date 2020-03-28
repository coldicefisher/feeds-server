import sys
sys.path.append("..") # Adds higher directory to python modules path.

import json, requests, logging, urllib3, ftplib, os, re, time, datetime, pickle, multiprocessing, traceback, threading
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures as futures
import psycopg2
from feedEngine import config

import bs4 as bs
import pandas as pd
import regex as re
from feedEngine import helpers
http = urllib3.PoolManager()

# setup the logger
logger = logging.getLogger(__name__)

# define file handler and set formatter
file_handler = logging.FileHandler('logfile.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)




    
class WikipediaCrawler:
    
    def get_sp500_members(self):
        resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        soup = bs.BeautifulSoup(resp.text, features='html.parser')
        table = soup.find('table',{'class':'wikitable sortable'})
        tickers = []
        with helpers.postgres_conn() as conn:
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

class TopForeignStocksCrawler:
    def get_etfs(self):
        resp = requests.get('https://topforeignstocks.com/etf-lists/the-complete-list-of-spdr-etfs-trading-on-the-us-stock-exchanges/')
        soup = bs.BeautifulSoup(resp.text, features="html.parser")
        table = soup.find('table',{'class':re.compile('^tablepress.')})
        tickers = []
        with helpers.postgres_conn() as conn:
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
                        update_str = 'UPDATE tickerdata_symbol SET spdr_member = %s WHERE symbol = %s'
                        args = (True,cleaned_ticker)
                        
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


        

class NasdaqTraderCrawler():
    
    def get_stock_symbols(self):
        # check internet connection
        if not helpers.internet_on():
            # logger.setLevel(logging.ERROR)
            # logger.error("(Nasdaq Trader fetch) Internet connection failed. Exiting...")
            return None

        # open db connection
        with helpers.postgres_conn() as conn:
            
            #!/usr/bin/env python
            # Connect to ftp.nasdaqtrader.com
            ftp = ftplib.FTP('ftp.nasdaqtrader.com', 'anonymous', 'anonymous@debian.org')
            # Download files nasdaqlisted.txt and otherlisted.txt from ftp.nasdaqtrader.com
            for ficheiro in ["nasdaqlisted.txt", "otherlisted.txt"]:
                ftp.cwd("/SymbolDirectory")
                localfile = open(ficheiro, 'wb')
                ftp.retrbinary('RETR ' + ficheiro, localfile.write)
                localfile.close()
            ftp.quit()
            # Grep for common stock in nasdaqlisted.txt and otherlisted.txt
            for ficheiro in ["nasdaqlisted.txt", "otherlisted.txt"]:
                localfile = open(ficheiro, 'r')
                
                t_file = open("tickers.txt","w+")
                t_file.seek(0)
                with conn.cursor() as cursor:
                    check_str = "SELECT symbol FROM tickerdata_symbol WHERE symbol = %s"
                    insert_str = "INSERT INTO tickerdata_symbol (symbol,alphavantage_status,yahoo_status,sp500_member,spdr_member) VALUES (%s,%s,%s,%s,%s)"
                    tickers_added = 0
                    tickers_processed = 0
                    for line in localfile:
                        # if re.search("Common Stock", line) or re.search("ETF",line):
                        if not re.search("File Creation", line) and not re.search("Symbol", line):
                            ticker = line.split("|")[0]
                            # Append tickers to file tickers.txt
                            t_file.write(ticker + "\n")
                            # write to db
                            args = (ticker,)
                            insert_args = (ticker,200,200,False,False)

                            tickers_processed += 1
                            cursor.execute(check_str,args)
                            if cursor.rowcount == 0:
                                cursor.execute(insert_str,insert_args)
                                tickers_added += 1
                    
                    t_file.truncate()
                    conn.commit()

                    # log results
                    # logger.setLevel(logging.INFO)
                    # logger.info(f'(Nasdaq Trader fetch) symbols processed: {tickers_processed}. symbols added: {tickers_added}')
                    print(f'(Nasdaq Trader fetch) symbols processed: {tickers_processed}. symbols added: {tickers_added}')
            
        # END get_stock_symbols

