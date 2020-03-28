import sys
import logging, psycopg2, requests, datetime as dt, time, concurrent.futures

from time import sleep
import time
import pandas_market_calendars as mcal
import pytz
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import multiprocessing
import pandas as pd
import pyarrow as pa
import sqlalchemy
import urllib3
import os
from feedEngine import config
from feedEngine import helpers
class Bar:
    
    def __init__(self,symbol,date_and_time,open = None, high = None, low = None, close = None, volume = None, 
                    frequency = 60,adjusted_close = None,dividends = None,stock_splits=None,
                    average=None,notional=None,number_of_trades=None,change_over_time=None,
                    market_open=None,market_high=None,market_low=None,market_close=None,
                    market_volume=None,market_average=None,market_notional=None,market_number_of_trades=None,market_change_over_time=None):
        # required fields
        self.symbol = symbol
        self._date_and_time = date_and_time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.frequency = frequency
        #iex fields - for quote
        self.average = average
        self.notional = notional
        self.number_of_trades = number_of_trades
        self.change_over_time = change_over_time
        #iex fields - for market
        self.market_open = market_open
        self.market_high = market_high
        self.market_low = market_low
        self.market_close = market_close
        self.market_volume = market_volume
        self.market_average = market_average
        self.market_notional = market_notional
        self.market_number_of_trades = market_number_of_trades
        self.market_change_over_time = market_change_over_time
        #yahoo fields
        self.adjusted_close = adjusted_close
        self.dividends = dividends
        self.stock_splits = stock_splits
    
    @property
    def date_and_time(self):
        return self._date_and_time

    @date_and_time.setter
    def date_and_time(self,value):
        self._date_and_time = value
    
    @property
    def date(self):
        try:
            converted_datetime = dt.datetime.strftime(self._date_and_time, '%Y-%m-%d')
        except:
            converted_datetime = None
        return converted_datetime

    @property
    def time(self):
        try:
            converted_datetime = dt.datetime.strftime(self._date_and_time, '%H:%M')
        except:
            converted_datetime = None
        return converted_datetime
# End Bars ////////////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////

# Bars ////////////////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////

class Bars:
    def __init__(self,my_bars):
        self._bars = my_bars
        self._index = 0

    def __next__(self):
        
        if self._index < len(self._bars):
            result = self._bars[self._index]
            self._index += 1
            return result
        #end of iteration
        raise StopIteration
    
    def __iter__(self):
        return self

    def __len__(self):
        return len(self._bars)

    def __getitem__(self,key):
        try:
            for bar in self._bars:
                if bar.date_and_time == key: return bar
        except:
            return None
            raise(KeyError('invalid key'))

# End Bars ////////////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////

# Stocks //////////////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////

class Stocks:
    def __init__(self,my_stocks):
        self._stocks = my_stocks
        self._index = 0

    def __next__(self):
        
        if self._index < len(self._stocks):
            result = self._stocks[self._index]
            self._index += 1
            return result
        #end of iteration
        raise StopIteration
    
    def __iter__(self):
        return self

    def __len__(self):
        return len(self._stocks)

    def __getitem__(self,key):
        try:
            for stock in self._stocks:
                if stock.symbol == key: return stock
        except:
            return None
            raise(KeyError('invalid key'))


# End Stocks //////////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////

# Stock ///////////////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////

class Stock:
    def __init__(self,symbol):
        self.symbol = symbol
        self._bars = []
        self.force_disk_pull = False


    @property
    def exists(self):
        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                select_str = 'SELECT symbol FROM tickerdata_tick WHERE symbol = %s'
                select_args = (self.symbol,)
                cursor.execute(select_str,select_args)
                
                while True:
                    
                    if cursor.rowcount > 0: #check the tick database to see if it exists
                        return True
                    select_str = 'SELECT symbol FROM tickerdata_symbol WHERE symbol = %s'
                    cursor.execute (select_str,select_args)
                    
                    if cursor.rowcount > 0: #check the symbol database to see if it exists
                        return True
                    #check for the symbol on iex cloud
                    http = urllib3.PoolManager()
                    iex_token = os.environ.get('IEX_TOKEN')
                    iex_version = os.environ.get('IEX_VERSION')
                    url = f'https://cloud.iexapis.com/{iex_version}/stock/{self.symbol}/quote?token={iex_token}'
                    r = http.request('GET',url)
                    if r.status == 200:

                        #add to the disk
                        insert_sql = 'INSERT INTO tickerdata_symbol (symbol,iex_status) VALUES (%s,%s)'
                        insert_args = (self.symbol,200)
                        cursor.execute(insert_sql,insert_args)
                        conn.commit()

                        return True
                    
                    return False
                    

    @property
    def dataframe(self):
        df_cache = self._cache_dataframe

        #return the cached dataframe if not forced pull and there is a cache
        if not self.force_disk_pull and not self._cache_dataframe.empty: return self._cache_dataframe
        
        df_disk = self._disk_dataframe #pulled from disk AFTER cache is returned to be faster

        #there is no cached data or disk data (NO DISK | NO CACHE)
        if df_cache.empty and df_disk.empty:
            return pd.DataFrame() 

        #there is no cached data (DISK | NO CACHE)
        elif df_cache.empty and not df_disk.empty: 
            self._cache_dataframe = df_disk # set the cache
            return df_disk

        #there is cached data no disk data (CACHE | NO DISK)
        elif not df_cache.empty and df_disk.empty: 
            self._disk_dataframe = df_cache #set the disk cache
            return df_cache

        #there is cached data and disk data (CACHE | DISK)
        elif not df_cache.empty and not df_disk.empty:
            joined_df = pd.concat([df_disk,df_cache],axis = 0,sort= True,join='outer').drop_duplicates() # Concatenate cache and disk
            return joined_df

    def save_dataframe_to_disk(self):
        self._disk_dataframe = self._cache_dataframe

    @property
    def _disk_dataframe(self):
            
        with helpers.postgres_conn() as conn:
            df = pd.read_sql_query(f"SELECT open,high,low,close,volume,frequency,adjusted_close,dividends,stock_splits,average,notional,number_of_trades,change_over_time,market_open,market_high,market_low,market_close,market_volume,market_average,market_notional,market_change_over_time,market_number_of_trades,ticker_datetime,id,source FROM tickerdata_tick WHERE symbol = '{self.symbol}' ORDER BY ticker_datetime ASC",conn)
            if df.empty:
                return df
            df.index = pd.DatetimeIndex(df['ticker_datetime'])
            new_df = df.drop(columns=['ticker_datetime'])
            return new_df
    
    @_disk_dataframe.setter
    def _disk_dataframe(self,value):
        df = value
        with helpers.postgres_conn(True) as conn:
            insert_str = '''INSERT INTO tickerdata_tick (symbol,ticker_datetime,volume,open,high,low,close,
                frequency,adjusted_close,dividends,stock_splits,average,notional,number_of_trades,change_over_time,
                market_open,market_high,market_low,market_close,market_average,market_volume,market_notional,market_change_over_time,
                market_number_of_trades,source) VALUES '''
        
            insert_args = []
            for count,row in enumerate(df.itertuples()): # iterate over all the stocks bars
                
                #yahoo fields
                adjusted_close = getattr(row,'adjusted_close')
                dividends = getattr(row,'dividends')
                stock_splits = getattr(row,'stock_splits')
                average = getattr(row,'average')
                
                #iex fields - stock
                notional = getattr(row,'notional')
                number_of_trades = getattr(row,'number_of_trades')
                change_over_time = getattr(row,'change_over_time')
                
                #iex fields - market
                market_open = getattr(row,'market_open')
                market_high = getattr(row,'market_high')
                market_low = getattr(row,'market_low')
                market_close = getattr(row,'market_close')
                market_average = getattr(row,'market_average')
                market_volume = getattr(row,'market_volume')
                market_notional = getattr(row,'market_notional')
                market_change_over_time = getattr(row,'market_change_over_time')
                market_number_of_trades = getattr(row,'market_number_of_trades')
                
                source = getattr(row,'source')
                
                bar_to_write = [
                    self.symbol,
                    (df.index[count]),
                    getattr(row,"volume"),
                    getattr(row,"open"),
                    getattr(row,"high"),
                    getattr(row,"low"),
                    getattr(row,"close"),
                    getattr(row,"frequency"),
                    adjusted_close,
                    dividends,
                    stock_splits,
                    average,
                    notional,
                    number_of_trades,
                    change_over_time,
                    market_open,
                    market_high,
                    market_low,
                    market_close,
                    market_average,
                    market_volume,
                    market_notional,
                    market_change_over_time,
                    market_number_of_trades,
                    source
                    
                ]

                insert_args.extend(bar_to_write)
                
                insert_str += " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),"
                
                #END IF CHECK FOR BARS THAT NEED TO BE WRITTEN
            
            #INSERT INTO DB ///////////////////////////////////////////////////////////

            if len(insert_args) > 22: #check that there are bars to add
                insert_str2 = insert_str[:-1]
                insert_str2 += " ON CONFLICT DO NOTHING"
                
                with helpers.postgres_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(insert_str2,insert_args)
                    conn.commit()
          
        #end for loop over all bars
    # End set property - disk dataframe ////////////////////////////////////////////////////
    #///////////////////////////////////////////////////////////////////////////////////////

    # Property get - cache dataframe ///////////////////////////////////////////////////////
    #///////////////////////////////////////////////////////////////////////////////////////
    @property        
    def _cache_dataframe(self):
        with helpers.redis_conn() as redis:
            cached_df = redis.get(f'{self.symbol}:dataframe')
            
            #check if there is a cache and return None if it doesn't exist
            if cached_df == None:
                return pd.DataFrame()
            
            context = pa.default_serialization_context() #gets the dataframe
            try:
                df = context.deserialize(cached_df)
            except:
                return pd.DataFrame()
            
            #validate cached data and return empty if not validated ////////////
            required_cols = ['open','high','low','close','volume','frequency','adjusted_close','dividends','stock_splits','average','notional','number_of_trades','change_over_time','market_open','market_high','market_low','market_close','market_volume','market_average','market_notional','market_change_over_time','market_number_of_trades','id','source']
            my_cols = []
            for col in df.columns:
                my_cols.append(col)
            
            if all(elem in my_cols for elem in required_cols): 
                return df
            else: 
                redis.delete(f'{self.symbol}:dataframe')
                return pd.DataFrame()
                
    # End property get - cache dataframe /////////////////////////////////////////////////////////

    # Property set - cache dataframe /////////////////////////////////////////////////////////////
    @_cache_dataframe.setter
    def _cache_dataframe(self,value):
        
        context = pa.default_serialization_context()
        with helpers.redis_conn() as redis:
            redis.delete(f'{self.symbol}:dataframe')
            redis.set(f'{self.symbol}:dataframe', context.serialize(value).to_buffer().to_pybytes())
            #self._cache_dataframe = value
    
    # End property set - cache dataframe /////////////////////////////////////////////////////////

    # Method update cached dataframe /////////////////////////////////////////////////////////////
    def update_cached_dataframe(self):
        my_date = self._cache_dataframe.index[-1]
        
        with helpers.postgres_conn() as conn:
            df = pd.read_sql_query(f"SELECT open,high,low,close,volume,frequency,adjusted_close,dividends,stock_splits,average,notional,number_of_trades,change_over_time,market_open,market_high,market_low,market_close,market_volume,market_average,market_notional,market_change_over_time,market_number_of_trades,ticker_datetime,id,source FROM tickerdata_tick WHERE symbol = '{self.symbol}' AND ticker_datetime > '{my_date}' ORDER BY ticker_datetime ASC",conn)
            if df.empty:
                return self._cache_dataframe
                
            df.index = pd.DatetimeIndex(df['ticker_datetime'])
            new_df = df.drop(columns=['ticker_datetime'])

            joined_df = pd.concat([new_df,self._cache_dataframe],axis = 0,sort= True,join='outer').drop_duplicates() # Concatenate cache and disk
            return joined_df
            
    @property
    def bars(self):
        
        all_bars = []
        df = self.dataframe
        for count,row in enumerate(df.itertuples()):
            #new_bar = Bar(self.symbol,getattr(row,'date_and_time'))
            new_bar = Bar(self.symbol,df.index[count])
            new_bar.open = getattr(row,'open')
            new_bar.high = getattr(row,'high')
            new_bar.low = getattr(row,'low')
            new_bar.close = getattr(row,'close')
            new_bar.adjusted_close = getattr(row,'adjusted_close')
            new_bar.frequency = getattr(row,'frequency')
            new_bar.volume = getattr(row,'volume')
            new_bar.dividends = getattr(row,'dividends')
            new_bar.stock_splits = getattr(row,'stock_splits')
            new_bar.average = getattr(row,'average')
            new_bar.notional = getattr(row,'notional')
            new_bar.number_of_trades = getattr(row,'number_of_trades')
            new_bar.change_over_time = getattr(row,'change_over_time')
            new_bar.market_open = getattr(row,'market_open')
            new_bar.market_high = getattr(row,'market_high')
            new_bar.market_low = getattr(row,'market_low')
            new_bar.market_close = getattr(row,'market_close')
            new_bar.market_volume = getattr(row,'market_volume')
            new_bar.market_average = getattr(row,'market_average')
            new_bar.market_notional = getattr(row,'market_notional')
            new_bar.market_change_over_time = getattr(row,'market_change_over_time')
            new_bar.market_number_of_trades = getattr(row,'market_number_of_trades')
            new_bar.source = getattr(row,'source')
            all_bars.append(new_bar)
        
        bars_obj = Bars(all_bars)
        return bars_obj

    def get_missing_days_and_minutes(self,days_to_check=90,get_minutes=True):
        if self.exists == False: return False
        
        missing_days = []
        
        # get the schedule for the past 90 days
        nyse = mcal.get_calendar('NYSE')

        #localize the now time and check is the market is open. If it is open, do not get historical data as the bars will
        #not contain additional information
        tz = pytz.timezone('UTC')
        today = dt.datetime.now()
        today = tz.normalize(tz.localize(today))
        nyse_schedule = nyse.schedule(start_date=today - dt.timedelta(days=90),end_date=today)
        # if nyse.open_at_time(nyse_schedule,today): nyse_schedule = nyse.schedule(start_date=today - dt.timedelta(days=days_to_check),end_date=today - dt.timedelta(days=1))
        # else: nyse_schedule = nyse.schedule(start_date=today - dt.timedelta(days=days_to_check),end_date=today)

        
        df = self.dataframe
        if df.empty: #return all of the calendar days if the dataframe is empty
            
            for day,schedule in nyse_schedule.iterrows():
                missing_days.append(day)
        else:
            #slice the dataframe for the dates to check
            df_to_check = df[dt.datetime.strftime(today - dt.timedelta(days=days_to_check),'%Y-%m-%d %H:%M'):dt.datetime.strftime(today,'%Y-%m-%d %H:%M')]
        
            #resample the dataframe for days count
            df_resampled = df_to_check['open'].resample('D').count()
        
            for day,schedule in nyse_schedule.iterrows():
                # str_day = dt.datetime.strftime(day,'%Y-%m-%d')
                if day not in df_resampled:
                    missing_days.append(day)
                    continue
                    
                if df_resampled[day] == 0:
                    missing_days.append(day)
                

        if get_minutes == False: return missing_days

        minutes_to_check = []
        for day,schedule in nyse_schedule.iterrows():
            market_open = schedule['market_open'].tz_convert('US/Eastern')
            market_close = schedule['market_close'].tz_convert('US/Eastern')
            time_to_check = market_open

            minute_count = 0
        
            # while time_to_check < market_close + dt.timedelta(minutes=1): #go through each minute
            while time_to_check < market_close:
                minutes_to_check.append(dt.datetime.strftime(time_to_check,'%Y-%m-%d %H:%M'))
                minute_count += 1

                time_to_check = time_to_check + dt.timedelta(minutes=1)

            
        
        for time,row in df_to_check.iterrows():
            try:
                minutes_to_check.remove(dt.datetime.strftime(time,'%Y-%m-%d %H:%M'))
            except ValueError as e:
                pass
            
        return missing_days,minutes_to_check

# End Stock ///////////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////




# TEST CODE        
# start_time = time.time()
# aapl = Stock('AAPL')
# aapl.save_dataframe_to_disk()
# print ('done')
# aapl.force_disk_pull = True
# print (aapl.dataframe)

# start_time = time.time()
# df = aapl.dataframe
#missing_days = aapl.get_missing_days_and_minutes(get_minutes=False)
#print (missing_days)
# elapsed = time.time() - start_time
# print (f'Getting missing minutes took {elapsed} seconds')

# start_time = time.time()
# df = aapl._cache_dataframe
# # print (df)
# elapsed = time.time() - start_time
# print (f'Getting cached dataframe took {elapsed} seconds')
# print (f'There are {aapl.row_count} rows')

# start_time = time.time()
# aapl.force_disk_pull = False
# print (aapl.dataframe)
# elapsed = time.time() - start_time
# print(f'Final dataframe operation took {elapsed} seconds')


# aapl._cache_dataframe = aapl._disk_dataframe
# print (aapl.dataframe)
# for col in aapl.dataframe.columns:
#     print (col)

# start_time = time.time()
# my_bars = aapl.bars
# elapsed = time.time() - start_time
# print (f'Getting bars took {elapsed} seconds')
# df = aapl.dataframe
# for count, row in enumerate(df.itertuples()):
#     print (df.index[count])
#     print (getattr(row,'open'))

# print (aapl.get_missing_days(90))

# get the schedule for the past 90 days
# nyse = mcal.get_calendar('NYSE')


# aapl = Stock('AAPL')
# print (aapl.dataframe)
# print (aapl.update_cached_dataframe())
# print (aapl.get_missing_days_and_minutes())clear
