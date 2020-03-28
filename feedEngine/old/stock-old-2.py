import sys
sys.path.append("..") # Adds higher directory to python modules path.
import helpers, logging, psycopg2, requests, datetime as dt, time, concurrent.futures
from time import sleep
import time
import pandas_market_calendars as mcal

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import multiprocessing
import pandas as pd
#import yfinance as yf
#import pandas as pd
def format_redis_date_for_postgres(redis_key):
    date_and_time = redis_key.decode('UTF-8').split(":")[2] #get date and time from hash name
    
    try:
        # get the cached minute bar
        year, month, day, hour, minute = date_and_time.split("-")
        my_date = year + '-' + month + "-" + day + " " + hour + ":" + minute
        
        return my_date
    except Exception as e:
        return False
    
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
        # try:
        #     converted_datetime = dt.datetime.strptime(str(self._date_and_time), '%Y-%m-%d %H:%M')
        # except:
        #     print (f'Property date_and_time of Bar: Error converting {self._date_and_time} into datetime format.')
        #     converted_datetime = None
        
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

    def __reversed__(self):
        #sort by date
        sorted_bars = []
        unsorted_bars = self._bars
        

        while True:
            sorted_index = 0
            if len(unsorted_bars) == 1: #only one left
                sorted_bars.append(unsorted_bars[0])
                break

            for i, bar in enumerate(unsorted_bars):
                
                if i == 0: # skip the first iteration
                    continue    
                    
                
                if dt.datetime.strptime(unsorted_bars[sorted_index].date_and_time,'%Y-%m-%d %H:%M') > dt.datetime.strptime(unsorted_bars[i].date_and_time,'%Y-%m-%d %H:%M'):
                    #print (datetime.datetime.strptime(unsorted_bars[i].date_and_time,'%Y-%m-%d %H:%M').timetuple())
                    sorted_index = i
                
            sorted_bars.append(unsorted_bars.pop(sorted_index))

        self._bars = sorted_bars
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

    def bars_for_date(self,day):
        bars = []
        for bar in self._bars:
            if day == bar.date:
                bars.append(bar)
        return bars


#END CLASS - BARS ///////////////////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////////////////////////////////////

#PRIVATE STOCK FUNCTIONS ////////////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////////////////////////////////////

# Pull bar from redis ///////////////////////////////////////////////////////////////////////////////
def _pull_from_redis(redis_bar):
    my_redis = helpers.redis_conn()    
    
    # get the date and time from the has name
    try:
        
        date_and_time = redis_bar.decode('UTF-8').split(":")[2] #get date and time from hash name
        symbol = redis_bar.decode('UTF-8').split(":")[0]
        #print (symbol)
        year, month, day, hour, minute = date_and_time.split("-")
    except Exception as e: #delete the bar if the date and time are improperly formatted
        print (f'Getting stock minute bar failed: redis hash: {date_and_time} improperly encoded: {e}. Deleting data...')
        my_redis.delete(redis_bar)
        return
    
    try:
        # create the minute bar object using the stored data
        my_bar = Bar(symbol,dt.datetime(int(year),int(month),int(day),int(hour),int(minute)),
                    float(my_redis.hget(redis_bar,"open").decode('UTF-8')),
                    float(my_redis.hget(redis_bar,"high").decode('UTF-8')),
                    float(my_redis.hget(redis_bar,'low').decode('UTF-8')),
                    float(my_redis.hget(redis_bar,'close').decode('UTF-8')),
                    int(my_redis.hget(redis_bar,'volume').decode('UTF-8')),
                    int(my_redis.hget(redis_bar,'frequency').decode('UTF-8'))
                )
    except Exception as e:
        print (f'Getting stock minute bar failed: redis hash: {date_and_time} did not have all data required: {e}')
        my_redis.delete(redis_bar)
        return
    
    # get the optional data from the redis cache specific to iex datafeed
    #yahoo fields
    if my_redis.hget(redis_bar,"adjusted_close") is not None: my_bar.adjusted_close = float(my_redis.hget(redis_bar,"adjusted_close"))
    if my_redis.hget(redis_bar,"dividends") is not None: my_bar.dividends = float(my_redis.hget(redis_bar,"dividends"))
    if my_redis.hget(redis_bar, "stock_splits") is not None: my_bar.stock_splits = float(my_redis.hget(redis_bar, "stock_splits"))
    #iex fields - for bar
    if my_redis.hget(redis_bar,"average") is not None: my_bar.average = float(my_redis.hget(redis_bar,'average'))
    if my_redis.hget(redis_bar,"notional") is not None: my_bar.notional = float(my_redis.hget(redis_bar,'notional'))
    if my_redis.hget(redis_bar,'number_of_trades') is not None: my_bar.number_of_trades = float(my_redis.hget(redis_bar,'number_of_trades'))
    if my_redis.hget(redis_bar,'change_over_time') is not None: my_bar.change_over_time = float(my_redis.hget(redis_bar,'change_over_time'))
    #iex fields - for market
    if my_redis.hget(redis_bar,'market_open') is not None: my_bar.market_open = float(my_redis.hget(redis_bar,'market_open'))
    if my_redis.hget(redis_bar,'market_high') is not None: my_bar.market_high = float(my_redis.hget(redis_bar,'market_high'))
    if my_redis.hget(redis_bar,'market_low') is not None: my_bar.market_low = float(my_redis.hget(redis_bar,'market_low'))
    if my_redis.hget(redis_bar,'market_close') is not None: my_bar.market_close = float(my_redis.hget(redis_bar,'market_close'))
    if my_redis.hget(redis_bar,"market_average") is not None: my_bar.market_average = float(my_redis.hget(redis_bar,"market_average"))
    if my_redis.hget(redis_bar,'market_volume') is not None: my_bar.market_volume = float(my_redis.hget(redis_bar,'market_volume'))
    if my_redis.hget(redis_bar,"market_notional") is not None: my_bar.market_notional = float(my_redis.hget(redis_bar,"market_notional"))
    if my_redis.hget(redis_bar,"market_number_of_trades") is not None: my_bar.market_number_of_trades = float(my_redis.hget(redis_bar,"market_number_of_trades"))
    if my_redis.hget(redis_bar,"market_change_over_time") is not None: my_bar.market_change_over_time = float(my_redis.hget(redis_bar,"market_change_over_time"))
    return my_bar
#End pull bar from redis ///////////////////////////////////////////////////////////////////////////////////

#Create dataframe //////////////////////////////////////////////////////////////////////////////////////////
def _create_dataframe_and_bars(futures):
    all_dates = []
    all_opens = []
    all_highs = []
    all_lows = []
    all_closes = []
    all_volumes = []
    all_frequencies = []
    #all_sources = []
    all_adjusted_closes = []
    all_dividends = []
    all_stock_splits = []
    all_averages = []
    all_notionals = []
    all_number_of_trades = []
    all_change_over_times = []

    all_market_opens = []
    all_market_highs = []
    all_market_lows = []
    all_market_closes = []
    all_market_volumes = []
    all_market_averages = []
    all_market_notionals = []
    all_market_number_of_trades = []
    all_market_change_over_times = []


    for future in as_completed(futures):
        all_bars = []
        result = future.result()
        #print (f'Appending {result}')
        all_bars.append(result)
        all_dates.append(result.date_and_time)
        all_opens.append(result.open)
        all_highs.append(result.high)
        all_lows.append(result.low)
        all_closes.append(result.close)
        all_volumes.append(result.volume)
        all_frequencies.append(result.frequency)
        #all_sources.append(result.source)
        all_adjusted_closes.append(result.adjusted_close)
        all_dividends.append(result.dividends)
        all_stock_splits.append(result.stock_splits)
        all_averages.append(result.average)
        all_notionals.append(result.notional)
        all_number_of_trades.append(result.number_of_trades)
        all_change_over_times.append(result.change_over_time)

        all_market_opens.append(result.market_open)
        all_market_highs.append(result.market_high)
        all_market_lows.append(result.market_low)
        all_market_closes.append(result.market_close)
        all_market_volumes.append(result.market_volume)
        all_market_averages.append(result.market_average)
        all_market_notionals.append(result.market_notional)
        all_market_number_of_trades.append(result.market_number_of_trades)
        all_market_change_over_times.append(result.market_change_over_time)

#print (f'Multiprocessing took {elapsed} seconds')

    df = pd.DataFrame({'date_and_time' : all_dates, 'open':all_opens,'high':all_highs,'low':all_lows,'close':all_closes,
                        'volume':all_volumes, 'frequency':all_frequencies,'adjusted_close':all_adjusted_closes,
                        'dividends':all_dividends,'stock_splits':all_stock_splits,'average':all_averages,'notional':all_notionals,
                        'number_of_trades':all_number_of_trades,'change_over_time':all_change_over_times,'market_open':all_market_opens,'market_close':all_market_closes,
                        'market_high':all_market_highs,'market_low':all_market_lows,'market_volume':all_market_volumes,'market_average':all_market_averages,
                        'market_notional':all_market_notionals,'market_number_of_trades':all_market_number_of_trades,'market_change_over_time':all_market_change_over_times
                    })
    
    #Create a time series pandas dataframe
    sorted_df = df.sort_values('date_and_time')
    sorted_df.index = pd.DatetimeIndex(sorted_df['date_and_time'])
        
    return df,all_bars

#End create dataframe //////////////////////////////////////////////////////////////////////////////////////


#End Private Stock Functions ///////////////////////////////////////////////////////////////////////////////
#///////////////////////////////////////////////////////////////////////////////////////////////////////////


#Stock Class ///////////////////////////////////////////////////////////////////////////////////////////////
#///////////////////////////////////////////////////////////////////////////////////////////////////////////

class Stock:

    def __init__(self, symbol):
        self._symbol = symbol
        self._bars = []

        self.status_code = None
        self.parser_used = None
        self.from_database = False
        self.realtime = False
        self.force_disk_pull = False
        self.df = None
    
    @property 
    def symbol(self):
        return self._symbol
    
    @property
    def redis_bar(self,date_of):
        my_bar = self._bars[date_of]

    @property
    def last_bar(self):
        date_of = (self.df.iloc[len(self._bars) - 1]['date_and_time'])
        formatted_date = dt.datetime.strftime(date_of,'%Y-%m-%d %H:%M')
        my_bar = self.bars[date_of]
        print (f'last bar date {date_of}')
        #for bar in self._bars:
        return my_bar
    
    @property
    def bars(self):
        
        redis = helpers.redis_conn() # redis connection
        
        start_time = time.time()
        all_bars = []
        
        #Check for existing bars and return last bar to pull additional bar from
        if not len(self._bars) == 0 :
            print ('There are bars when checking for bars')
            #print (self.last_bar.date_and_time)
        else:
            print ('No bars man!')



        # RETRIEVE REDIS CACHE //////////////////////////////////////////////////////////////////////////////////////
        
        #see if the stock has read from the hard disk yet
        try:
            if redis.get(f'{self.symbol}:pulled_from_disk').decode('UTF-8') =='true': was_pulled = True
            else: was_pulled = False
        except:
            was_pulled = False
        
        if was_pulled and not self.force_disk_pull: # Pull bars from redis
            start_time = time.time()
            nprocs = multiprocessing.cpu_count()
            #print (f'Starting with {nprocs}...')

            with ThreadPoolExecutor(max_workers=nprocs) as executor:
            #with ThreadPoolExecutor(max_workers=40) as executor:
                futures = [executor.submit(_pull_from_redis,redis_bar) for redis_bar in redis.scan_iter(f"{self.symbol}:bar:*")]

                self.df,all_bars = _create_dataframe_and_bars(futures)

            #for redis_bar in redis.scan_iter(f"{self.symbol}:bar:*"):

        # END RETRIEVE REDIS CACHE ////////////////////////////////////////////////////////////////////////////////////
            
        # PULL FROM DISK IF STOCK NOT INITILIAZED in redis /////////////////////////////////////////////////////////////
        else:
            all_dates = []
            all_opens = []
            all_highs = []
            all_lows = []
            all_closes = []
            all_volumes = []
            all_frequencies = []
            #all_sources = []
            all_adjusted_closes = []
            all_dividends = []
            all_stock_splits = []
            all_averages = []
            all_notionals = []
            all_number_of_trades = []
            all_change_over_times = []

            all_market_opens = []
            all_market_highs = []
            all_market_lows = []
            all_market_closes = []
            all_market_volumes = []
            all_market_averages = []
            all_market_notionals = []
            all_market_number_of_trades = []
            all_market_change_over_times = []


            # retrieve the bars from the disk
            with helpers.postgres_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""SELECT id, symbol, ticker_datetime, volume, open, high, low, close,adjusted_close, 
                                    frequency, dividends, stock_splits, average, notional,
                                    number_of_trades, change_over_time, market_open, market_high, market_low, market_close,
                                    market_average, market_volume, market_average, market_notional, market_change_over_time, 
                                    market_number_of_trades
                                     FROM tickerdata_tick WHERE symbol = \'{self.symbol}\'""")
            
                    # add the bars from disk
                    for row in cursor.fetchall():
                        id,symbol,date_and_time,volume,open,high,low,close,adjusted_close,frequency,dividends,stock_splits, \
                            average, notional, number_of_trades,change_over_time, market_open, market_high, market_low, \
                                market_close,market_average,market_volume,market_average,market_notional,market_change_over_time, \
                                    market_number_of_trades = row
                        
                        my_bar = Bar(self.symbol,date_and_time,open,high,low,close,volume,frequency,adjusted_close,dividends, \
                            stock_splits,average,notional,number_of_trades,change_over_time,market_open,market_high,market_low,market_close, \
                                market_volume,market_average,market_notional,market_number_of_trades,market_change_over_time)
                        
                        # add the bars
                        all_bars.append(my_bar)

                        # add to redis cache /////////////////////////////////////////////////////////////
                        str_date_time = date_and_time.strftime('%Y-%m-%d-%H-%M')
                        
                        hash_name = f'{self.symbol}:bar:{str_date_time}'
                        #required fields
                        redis.hset(hash_name,"open",open)
                        redis.hset(hash_name,"high",high)
                        redis.hset(hash_name,"low",low)
                        redis.hset(hash_name,"close",close)
                        redis.hset(hash_name,"volume",volume)
                        redis.hset(hash_name,"frequency",frequency)
                        redis.hset(hash_name,"source","disk")
                        #yahoo fields
                        if adjusted_close is not None: redis.hset(hash_name,"adjusted_close",adjusted_close) 
                        if dividends is not None: redis.hset(hash_name,"dividends",dividends)
                        if stock_splits is not None: redis.hset(hash_name,"stock_splits",stock_splits)
                        #iex fields - stock
                        if average is not None:redis.hset(hash_name,'average',average)
                        if change_over_time is not None: redis.hset(hash_name,"change_over_time",change_over_time)
                        if notional is not None: redis.hset(hash_name,"notional",notional)
                        if number_of_trades is not None: redis.hset(hash_name,"number_of_trades",number_of_trades)
                        #iex fields - market
                        if market_open is not None: redis.hset(hash_name,'market_open',market_open)
                        if market_high is not None: redis.hset(hash_name,'market_high',market_high)
                        if market_low is not None: redis.hset(hash_name,'market_low',market_low)
                        if market_close is not None: redis.hset(hash_name,'market_close',market_close)
                        if market_average is not None: redis.hset(hash_name,"market_average",market_average)
                        if market_volume is not None: redis.hset(hash_name,'market_volume',market_volume)
                        if market_notional is not None: redis.hset(hash_name,"market_notional",market_notional)
                        if market_change_over_time is not None: redis.hset(hash_name,"market_change_over_time",market_change_over_time)
                        if market_number_of_trades is not None: redis.hset(hash_name,'market_number_of_trades',market_number_of_trades)
                        
                        # Add to dataframe
                        all_dates.append(date_and_time)
                        all_opens.append(open)
                        all_highs.append(high)
                        all_lows.append(low)
                        all_closes.append(close)
                        all_volumes.append(volume)
                        all_frequencies.append(frequency)
                        #all_sources.append(result.source)
                        all_adjusted_closes.append(adjusted_close)
                        all_dividends.append(dividends)
                        all_stock_splits.append(stock_splits)
                        all_averages.append(average)
                        all_notionals.append(notional)
                        all_number_of_trades.append(number_of_trades)
                        all_change_over_times.append(change_over_time)

                        all_market_opens.append(market_open)
                        all_market_highs.append(market_high)
                        all_market_lows.append(market_low)
                        all_market_closes.append(market_close)
                        all_market_volumes.append(market_volume)
                        all_market_averages.append(market_average)
                        all_market_notionals.append(market_notional)
                        all_market_number_of_trades.append(market_number_of_trades)
                        all_market_change_over_times.append(market_change_over_time)

            # add the redis initialized
            redis.set(f"{self.symbol}:pulled_from_disk","true")
            
            # Postgres pull - create dataframe
            df = pd.DataFrame({'date_and_time' : all_dates, 'open':all_opens,'high':all_highs,'low':all_lows,'close':all_closes,
                        'volume':all_volumes,'frequency':all_frequencies,'adjusted_close':all_adjusted_closes,
                        'dividends':all_dividends,'stock_splits':all_stock_splits,'average':all_averages,
                        'notional':all_notionals,'number_of_trades':all_number_of_trades,'change_over_time':all_change_over_times,
                        'market_open':all_market_opens,'market_high':all_market_highs,'market_low':all_market_lows,'market_close':all_market_closes,
                        'market_volume':all_market_volumes,'market_average':all_market_averages,'market_notional':all_market_notionals,
                        'market_number_of_trades':all_market_number_of_trades,'market_change_over_time':all_market_change_over_times
                    })
            self.df = df
        
        # Bars - finished with Postgres and Redis. Df is created and bars are created

        sorted_df = self.df.sort_values('date_and_time')
        sorted_df.index = pd.DatetimeIndex(sorted_df['date_and_time'])
        self.df = sorted_df
        
        #sort the bars
        sorted_bars = []
        start_time = time.time()
        for row in self.df.itertuples():
            new_bar = Bar(self.symbol,getattr(row,'date_and_time'))
            
            
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
            sorted_bars.append(new_bar)
            #sorted_bars.append(row)
        
        elapsed = time.time() - start_time
        print (f'Sorted bars in {elapsed} seconds')
        self._bars = sorted_bars
        return self._bars
    
    # END BARS PROPERTY - STOCK //////////////////////////////////////////////////////////////////////////////////////
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    # SAVE BARS TO DISK METHOD - STOCK ///////////////////////////////////////////////////////////////////////////////
    def save_bars_to_disk(self):
        
        redis = helpers.redis_conn()
        
        insert_str = '''INSERT INTO tickerdata_tick (symbol,ticker_datetime,volume,open,high,low,close,
            frequency,adjusted_close,dividends,stock_splits,average,notional,number_of_trades,change_over_time,
            market_open,market_high,market_low,market_close,market_average,market_volume,market_notional,market_change_over_time,
            market_number_of_trades,source) VALUES '''
        
        insert_args = []
        for redis_bar in redis.scan_iter(f"{self.symbol}:bar:*"): # iterate over all the stocks bars
           #check the stock for required elements 
            if not redis.hexists(redis_bar,'source') or not redis.hexists(redis_bar,'volume') or not redis.hexists(redis_bar,'open') or \
                not redis.hexists(redis_bar,'high') or not redis.hexists(redis_bar,'low') or not redis.hexists(redis_bar,'close') or \
                    not redis.hexists(redis_bar,'frequency'):
                    print ('Redis hash did not have all required keys...')
                    redis.delete(redis_bar) #delete the redis bar because it does not ocntain the required data
                    continue
        
            
            #yahoo fields
            if redis.hexists(redis_bar,'adjusted_close'): adjusted_close = redis.hget(redis_bar,"adjusted_close").decode('UTF-8') 
            else: adjusted_close = None
            if redis.hexists(redis_bar,'dividends'): dividends = redis.hget(redis_bar,'dividends').decode('UTF-8')
            else: dividends = None
            if redis.hexists(redis_bar,'stock_splits'): stock_splits = redis.hget(redis_bar,'stock_splits').decode('UTF-8')
            else: stock_splits = None
            if redis.hexists(redis_bar,'average'): average = redis.hget(redis_bar,'average').decode('UTF-8')
            else: average = None
            #iex fields - stock
            if redis.hexists(redis_bar,'notional'): notional = redis.hget(redis_bar,'notional').decode('UTF-8')
            else: notional = None
            if redis.hexists(redis_bar,'number_of_trades'): number_of_trades = redis.hget(redis_bar,'number_of_trades').decode('UTF-8')
            else: number_of_trades = None
            if redis.hexists(redis_bar,'change_over_time'): change_over_time = redis.hget(redis_bar,'change_over_time').decode('UTF-8')
            else: change_over_time = None
            #iex fields - market
            
            if redis.hexists(redis_bar,'market_open'): market_open = redis.hget(redis_bar,'market_open').decode('UTF-8')
            else: market_open = None
            if redis.hexists(redis_bar,'market_high'): market_high = redis.hget(redis_bar,'market_high').decode('UTF-8')
            else: market_high = None
            if redis.hexists(redis_bar,'market_low'): market_low = redis.hget(redis_bar,'market_low').decode('UTF-8')
            else: market_low = None
            if redis.hexists(redis_bar,'market_close'): market_close = redis.hget(redis_bar,'market_close').decode('UTF-8')
            else: market_close = None
            if redis.hexists(redis_bar,'market_average'): market_average = redis.hget(redis_bar,'market_average').decode('UTF-8')
            else: market_average = None
            if redis.hexists(redis_bar,'market_volume'): market_volume = redis.hget(redis_bar,'market_volume').decode('UTF-8')
            else: market_volume = None
            if redis.hexists(redis_bar,'market_notional'): market_notional = redis.hget(redis_bar,'market_notional').decode('UTF-8')
            else: market_notional = None
            if redis.hexists(redis_bar,'market_change_over_time'): market_change_over_time = redis.hget(redis_bar,'market_change_over_time').decode('UTF-8')
            else: market_change_over_time = None
            if redis.hexists(redis_bar,'market_number_of_trades'): market_number_of_trades = redis.hget(redis_bar,'market_number_of_trades').decode('UTF-8')
            else: market_number_of_trades = None
            
            bar_to_write = [
                self.symbol,
                format_redis_date_for_postgres(redis_bar),
                redis.hget(redis_bar,"volume").decode('UTF-8'),
                redis.hget(redis_bar,"open").decode('UTF-8'),
                redis.hget(redis_bar,"high").decode('UTF-8'),
                redis.hget(redis_bar,"low").decode('UTF-8'),
                redis.hget(redis_bar,"close").decode('UTF-8'),
                redis.hget(redis_bar,"frequency").decode('UTF-8'),
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
                redis.hget(redis_bar,"source").decode('UTF-8')
            ]

            insert_args.extend(bar_to_write)
            
            insert_str += " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),"
            
            #END IF CHECK FOR BARS THAT NEED TO BE WRITTEN
        
        #INSERT INTO DB ///////////////////////////////////////////////////////////

        if len(insert_args) > 23: #check that there are bars to add
            insert_str2 = insert_str[:-1]
            insert_str2 += " ON CONFLICT DO NOTHING"
            with helpers.postgres_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(insert_str2,insert_args)
                conn.commit()
        else:
            #print (f'No bars for ({self.symbol}) to add...')   
            pass     

    #end for loop over all bars

    
    def get_missing_days(self,days_to_check=90):
        # see if there are any bars to ebe had

        # get the schedule for the past 90 days
        nyse = mcal.get_calendar('NYSE')
        today = dt.datetime.now()
        nyse_schedule = nyse.schedule(start_date=today - dt.timedelta(days=days_to_check),end_date=today)
        missing_days = []
        for day,schedule in nyse_schedule.iterrows():
            market_open = schedule['market_open'].tz_convert('US/Eastern')
            market_close = schedule['market_close'].tz_convert('US/Eastern')
            
            time_to_check = market_open
            
            while time_to_check < market_close + dt.timedelta(minutes=1): #go through each minute
                try:
                    check_bar = self.bars[dt.datetime.strftime(time_to_check, '%Y-%m-%d %H:%M')]
                except:
                    missing_days.append(day)
                    break
                if check_bar is None:
                    missing_days.append(day)
                    break
                time_to_check = time_to_check + dt.timedelta(minutes=1)
            
        return missing_days
        # End validate days

#End Stock //////////////////////////////////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////////////////////////////////////////////

# TEST CODE
start_time = time.time()
aapl = Stock("AAPL")
elapsed = time.time() - start_time
print (f'Initializing took {elapsed} seconds')

aapl.force_disk_pull = False

start_time = time.time()
bars = (aapl.bars)
print (f'First time last bar {aapl.last_bar}')
elapsed = time.time() - start_time
print (f'Getting bars took {elapsed} seconds')

start_time = time.time()
aapl.save_bars_to_disk()
elapsed = time.time() - start_time
print (f'Saving bars took {elapsed} seconds')
#for day in aapl.get_missing_days(90):
#    print (day)
print ('Getting bars again')
bars = aapl.bars
print (f'Second time last bars {aapl.last_bar}')


#aapl.missing_days_past_90









# aapl.force_disk_pull = False


#bars_for_date = r.bars_for_date('2020-01-06')
#for bar in bars_for_date:
#    print (bar.open)
#for bar in r:
    
#    print (bar.date_and_time)
# my_bar = aapl.bars['202-5']
# if my_bar is not None:
#     print (my_bar.high)
# else:
#     print ('its none')
# print (aapl.bars['2020-01-06 09:30'].time)


#aapl.save_bars_to_disk()
