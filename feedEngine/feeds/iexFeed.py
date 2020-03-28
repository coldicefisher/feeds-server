
import os
# import iexfinance.stocks as iexStocks
import datetime as dt
from feedEngine import stock as st
from feedEngine import helpers
import pandas_market_calendars as mcal 
import pandas as pd
# import numpy as np
# import pyarrow as pa
import requests,urllib3
from feedEngine import config
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

import pytz

#from iexfinance import stocks as iexStocks
#import pyEX


class iexFeed:
    def __init__(self):
        pass
    
    def get_historical_data(self,stock,days=90):
        
        if isinstance(stock,str):my_stock = st.Stock(stock) #initialize the stock
        elif isinstance(stock,st.Stock):my_stock = stock
            
        if not my_stock.exists: return my_stock.symbol,"Stock does not exist"

        http = urllib3.PoolManager()

        
        
        iex_token = os.environ.get('IEX_TOKEN')
        iex_env = os.environ.get('IEX_ENVIRONMENT')
        iex_version = os.environ.get('IEX_VERSION')
        
        def read_date_into_df(my_date):
            pull_date = dt.datetime.strftime(my_date,'%Y%m%d')
            if iex_env == 'live':url = f'https://cloud.iexapis.com/{iex_version}/stock/{my_stock.symbol}/chart/date/{pull_date}?token={iex_token}'
            elif iex_env == 'sandbox':url = f'https://sandbox.iexapis.com/{iex_version}/stock/{my_stock.symbol}/chart/date/{pull_date}?token={iex_token}'

            r = http.request('GET',url) #make the request
            if r.status != 200: return my_stock.symbol,r.status # check to see if return is good
            #read data into dataframe //////////////////////////////////////////////////////
            
            try:
                return pd.read_json(r.data,encoding='utf8')
                
            except Exception as e:
                print (f'Failed to retrieve dataframe with error: \n \n {e}')
                return pd.DataFrame(),'Dataframe failed'


        missing_days = my_stock.get_missing_days_and_minutes(days_to_check=days,get_minutes=False)
        if missing_days == False: return my_stock.symbol,"Missing days is false" #check to see if the stock is in the database
        if len(missing_days) == 0: return my_stock.symbol,"Up to date" #all data is up to date. 
        
        #Remove today if the market is open
        nyse = mcal.get_calendar('NYSE')
        tz = pytz.timezone('UTC')
        today = dt.datetime.now()
        today = tz.normalize(tz.localize(today))
        nyse_schedule = nyse.schedule(start_date=today - dt.timedelta(days=90),end_date=today)
        try:
            if nyse.open_at_time(nyse_schedule,today):
                missing_days.remove(today)
        except:
            pass
        
        df = pd.DataFrame()
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = [executor.submit(read_date_into_df,day) for day in missing_days]
        
        
        for future in as_completed(futures):
            result = future.result()
            if isinstance(result,pd.DataFrame):    
                joined_df = pd.concat([df,result],axis = 0,sort= False,join='outer').drop_duplicates() # Concatenate cache and pull
                joined_df = joined_df.sort_index()
                df = joined_df
            else: return my_stock.symbol,result[1] #failed to return a dataframe that can be added to current df

        
        if df.empty:return my_stock.symbol,False #check to see if there is a df and return false is none
                
        #format dataframe columns of the data pull /////////////////////////////////////
        #set date ////////////////
        df['date'] = pd.to_datetime(df['date'])
        df[['hours','minutes']] = df.minute.str.split(':',expand=True)
        df['seconds'] = '00'
        df['time'] = df['hours'] + ':' + df['minutes'] + ':' + df['seconds']
        df['date_and_time'] = df['date'] + pd.to_timedelta(df['time'])
        df = df.drop('time',1)
        df = df.drop('date',1)
        df = df.drop('minute',1)
        df = df.drop('label',1)
        df = df.drop('hours',1)
        df = df.drop('minutes',1)
        df = df.drop('seconds',1)
        
        #end set date ////////////

        #df['adjusted_close'] = df['adjustedClose']
        df['number_of_trades'] = df['numberOfTrades']
        df = df.drop('numberOfTrades',1)
        try:
            df['change_over_time'] = df['changeOverTime']
            df = df.drop('changeOverTime',1)
        except:
            pass
        try:
            df['market_open'] = df['marketOpen']
            df = df.drop('marketOpen',1)
        except:
            pass
        try:
            df['market_high'] = df['marketHigh']
            df = df.drop('marketHigh',1)
        except:
            pass
        try:
            df['market_low'] = df['marketLow']
            df = df.drop('marketLow',1)
        except:
            pass
        try:
            df['market_close'] = df['marketClose']
            df = df.drop('marketClose',1)
        except:
            pass
        try:
            df['market_volume'] = df['marketVolume']
            df = df.drop('marketVolume',1)
        except:
            pass
        try:
            df['market_average'] = df['marketAverage']
            df = df.drop('marketAverage',1)
        except:
            pass
        try:
            df['market_notional'] = df['marketNotional']
            df = df.drop('marketNotional',1)
        except:
            pass
        try:
            df['market_change_over_time'] = df['marketChangeOverTime']
            df = df.drop('marketChangeOverTime',1)
        except:
            pass
        try:
            df['market_number_of_trades'] = df['marketNumberOfTrades']
            df = df.drop('marketNumberOfTrades',1)
        except:
            pass

        df['source'] = 'iex'
        df['frequency'] = 60

        df['adjusted_close'] = None
        df['stock_splits'] = None
        df['id'] = None
        df['dividends'] = None

        #end formatting columns ////////////////////////////////////
        
        #set pulled df index //////////////////////////////////////
        df.index = pd.DatetimeIndex(df['date_and_time'])
        df = df.tz_localize(tz='UTC')
        df = df.drop('date_and_time',1)

        
        # if iex_env == "sandbox":
        #     print ('SANDBOX ENVIRONMENT!!! \n \n')
        #     print (df)
        #     return my_stock.symbol,"sandbox"

        
        joined_df = pd.concat([my_stock.dataframe,df],axis = 0,sort= False,join='outer').drop_duplicates() # Concatenate cache and pull
        joined_df = joined_df.sort_index()
        
        my_stock._cache_dataframe = joined_df
        return my_stock.symbol,missing_days
        
    # End get historical data ///////////////////////////////////////////////////////////
    
        # TEST CACHE DF
        # context = pa.default_serialization_context()
        # redis.set(f'{stock}:df', context.serialize(data).to_buffer().to_pybytes())


#test code ////////////////////////////////

    
# feed = iexFeed()
# print (feed.get_historical_data('AAPL'))

# my_stock = st.Stock("AAPL")
# my_stock.save_dataframe_to_disk()
# print (my_stock.dataframe)

# my_stock._disk_dataframe = my_stock._cache_dataframe
#for day in my_stock.get_missing_days():
#    print (day)
    #datetime.strptime(day,'%Y-%m-%d')
# my_stock.save_bars_to_disk()

#my_stock = stock.Stock('test')
# redis = helpers.redis_conn()

