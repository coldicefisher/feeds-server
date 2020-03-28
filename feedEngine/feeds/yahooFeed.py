import sys
sys.path.append("..") # Adds higher directory to python modules path.

import json, requests, logging, os, re, time, datetime
from helpers import *
import settings


import bs4 as bs
from yahoo_finance import Share
import yfinance as yf
import pandas as pd

#print (internet_on)
class YahooFinanceFeed:

    def get_historical_data(self,ticker):
        
        print ("test: historical data")
        conn = postgres_conn()
        # check internet connection
        if not internet_on():
            print ('no internet')
            logger.setLevel(logging.ERROR)
            logger.error("(Yahoo Finance feed) Internet connection failed. Exiting...")
            conn.close()
            return None

        
        # use yfinance to get Yahoo finance data
        stock = yf.Ticker(ticker)
        
        print ('got the stock')
        try:
            print ('getting data...')
            data = stock.history(period='7d',interval='1m')
            print ('got data')
        except:
            print ('didnt get it')
            update_str = 'UPDATE tickerdata_symbol SET yahoo_status = 404 WHERE symbol = %s'
            args = (ticker,)
            with conn.cursor() as cursor: 
                cursor.execute(update_str,args)
                conn.commit()

            logger.setLevel(logging.WARNING)
            logger.warning(f'(Yahoo Finance feed) Stock symbol ({ticker}) does not exist')
            conn.close()
            return False                    


        df = pd.DataFrame(data) # set data into a Pandas dataframe
        print ('here')
        if df.empty:
            print ('empty')
            # Update symbol status
            update_str = 'UPDATE tickerdata_symbol SET yahoo_status = 400 WHERE symbol = %s'
            args = (ticker,)
            with conn.cursor() as cursor:
                cursor.execute(update_str,args)
                conn.commit()
            
            logger.setLevel(logging.WARNING)
            logger.warning(f'(Yahoo Finance feed) Stock symbol ({ticker}) has no data')

            conn.close()
            return False

        # iterate through the dataframe and write it to the database

    
        for index, row in df.iterrows():
            print ('iterating')
            date = index
            open = row['Open']
            high = row['High']
            low = row['Low']
            close = row['Close']
            volume = row['Volume']
            dividends = row['Dividends']
            stock_splits = row['Stock Splits']

            check_str = "SELECT symbol FROM tickerdata_tick WHERE symbol = %s AND ticker_datetime = %s"
            check_args = (ticker,date)
            with conn.cursor() as cursor:
                cursor.execute(check_str,check_args)
            #conn.commit()

                if cursor.rowcount == 0:
                    insert_str = "INSERT INTO tickerdata_tick(symbol, open, high, low, close, volume, frequency, ticker_datetime, stock_splits, dividends) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    insert_args = (ticker,open,high,low,close,volume,60,helpers.convert_date_to_mysql(date),stock_splits,dividends)
                    cursor.execute(insert_str,insert_args)

                conn.commit()
            
            
            # Update symbol status
            now = helpers.convert_date_to_mysql(datetime.datetime.now())
            #update_str = 'UPDATE tickerdata_symbol SET alphavantage_status = 200,last_pull = "%s" WHERE symbol = "%s"'
            update_str = f'UPDATE tickerdata_symbol SET alphavantage_status = 200,last_pull = "{now}" WHERE symbol = "{ticker}"'
            args = (now,ticker)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_str)
                    conn.commit()
            except:
                pass
            
        conn.close()
    # END get_historical_data

feed = YahooFinanceFeed()
feed.get_historical_data('SPY')