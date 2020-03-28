import sys
sys.path.append("..") # Adds higher directory to python modules path.

import json, requests, logging, os, re, time, datetime
from helpers import *
import settings


import bs4 as bs
import pandas as pd

class AlphaVantageFeed:

    def get_historical_data(self,ticker):
        # check internet connection
        if not internet_on():
            logger.setLevel(logging.ERROR)
            logger.error("(Alpha Vantage feed) Internet connection failed. Exiting...")
            return None

        # open db connection
        conn = postgres_conn()
        

        # make api request to Alpha Vantage
        r = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' + str(ticker) + '&interval=1min&apikey=FRTGRFPESYJD9DBD&outputsize=full')
        try:
            data = r.json()
        except:
            return False

        error_msg = data.get('Error Message')
        if not error_msg is None:
            # Update symbol status
            #try:
            update_str = 'UPDATE tickerdata_symbol SET alphavantage_status = 404 WHERE symbol = %s'
            args = (ticker,)
            with conn.cursor() as cursor:
                cursor.execute(update_str,args)
                conn.commit()
           
            logger.setLevel(logging.INFO)
            logger.info(f'(Alpha Vantage feed) Stock symbol ({ticker}) does not exist')

            return False

        check_str = "SELECT symbol FROM tickerdata_tick WHERE symbol = %s AND ticker_datetime = %s"
        insert_str = "INSERT INTO tickerdata_tick(symbol, open, high, low, close, volume, frequency, ticker_datetime) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        
        ticks = data.get('Time Series (1min)')
        if ticks is None:
            
            logger.setLevel(logging.INFO)
            logger.info(f'(Alpha Vantage feed) Stock symbol ({ticker}) does not have ticker data')
            update_str = 'UPDATE tickerdata_symbol SET alphavantage_status = 400 WHERE symbol = %s'
            args = (ticker,)
            with conn.cursor() as cursor:
                cursor.execute(update_str,args)
                conn.commit()
    
            return False

        # meta_data = data.get('Meta Data')
        with conn.cursor() as cursor:
            
            for time,value in ticks.items(): # iterate through data and create rows if not exists (on date)
                check_args = (ticker,time)
                cursor.execute(check_str,check_args)

                #print ('adding data: ' + time)
                open = value.get('1. open')
                high = value.get('2. high')
                low = value.get('3. low')
                close = value.get('4. close')
                volume = value.get('5. volume')

                if cursor.rowcount == 0:
                    insert_args = (ticker,open,high,low,close,volume,60,time)
                    cursor.execute(insert_str,insert_args)
                    #print (f"Open: {open}, high: {high}, low: {low}, close: {close}, volume: {volume}")
        
            conn.commit()
            # Update symbol status
            now = convert_date_to_mysql(datetime.now())
            #update_str = 'UPDATE tickerdata_symbol SET alphavantage_status = 200,last_pull = "%s" WHERE symbol = "%s"'
            update_str = f'UPDATE tickerdata_symbol SET alphavantage_status = 200,last_pull = "{now}" WHERE symbol = "{ticker}"'
            args = (now,ticker)
            try:
                cursor.execute(update_str)
                conn.commit()
            except:
                pass
            
        conn.close()
    # END get_historical_data
feed = AlphaVantageFeed()
feed.get_historical_data('SPY')