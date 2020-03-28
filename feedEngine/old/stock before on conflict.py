import sys
sys.path.append("..") # Adds higher directory to python modules path.
import helpers, logging, psycopg2, requests, datetime as dt, time, concurrent.futures
from time import sleep

#import yfinance as yf
#import pandas as pd
def format_redis_date_for_postgres(redis_key):
    date_and_time = redis_key.decode('UTF-8').split(":")[2] #get date and time from hash name
    #print (redis_bar)
    try:
        # get the cached minute bar
        year, month, day, hour, minute = date_and_time.split("-")
        my_date = year + '-' + month + "-" + day + " " + hour + ":" + minute
        
        return my_date
    except Exception as e:
        return False
    
class Bars:
    
    def __init__(self,symbol,date_and_time,open = None, high = None, low = None, close = None, volume = None, 
                    period = 60,adjusted_close = None,dividends = None,stock_splits=None,
                    average=None,notional=None,number_of_trades=None,change_over_time=None,
                    market_open=None,market_high=None,market_low=None,market_close=None,
                    market_volume=None,market_notional=None,market_number_of_trades=None,market_change_over_time=None):
        # required fields
        self.symbol = symbol
        self.date_and_time = date_and_time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.period = period
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
        self.market_notional = market_notional
        self.market_number_of_trades = market_number_of_trades
        self.market_change_over_time = market_change_over_time
        #yahoo fields
        self.adjusted_close = adjusted_close
        self.dividends = dividends
        self.stock_splits = stock_splits
        


class Stock:

    def __init__(self, symbol):
        self._symbol = symbol
        self.status_code = None
        self.parser_used = None
        self._bars = []
        self.from_database = False
        self.realtime = False
    
    @property 
    def symbol(self):
        return self._symbol
    
    #@symbol.setter
    #def symbol(self,value):
    #    self._symbol = value
    
    @property
    def bars(self):
        all_bars = []
        
        # RETRIEVE REDIS CACHE //////////////////////////////////////////////////////////////////////////////////////
        redis = helpers.redis_conn()
        print (self.symbol)
        if redis.get(f'{self.symbol}:initialized') is not None:
            for redis_bar in redis.scan_iter(f"{self.symbol}:bar:*"):
                date_and_time = redis_bar.decode('UTF-8').split(":")[2] #get date and time from hash name
                #print (redis_bar)
                try:
                    # get the cached minute bar
                    year, month, day, hour, minute = date_and_time.split("-")
                except Exception as e:
                    print (f'Getting stock minute bar failed: redis hash: {date_and_time} improperly encoded: {e}. Deleting data...')
                    redis.delete(redis_bar)
                    continue
                
                try:
                    # create the minute bar object using the stored data
                    my_bar = Bars(self.symbol,dt.datetime(int(year),int(month),int(day),int(hour),int(minute)),
                                float(redis.hget(redis_bar,"open").decode('UTF-8')),
                                float(redis.hget(redis_bar,"high").decode('UTF-8')),
                                float(redis.hget(redis_bar,'low').decode('UTF-8')),
                                float(redis.hget(redis_bar,'close').decode('UTF-8')),
                                int(redis.hget(redis_bar,'volume').decode('UTF-8')),
                                int(redis.hget(redis_bar,'period').decode('UTF-8'))
                            )
                except Exception as e:
                    print (f'Getting stock minute bar failed: redis hash: {date_and_time} did not have all data required: {e}')
                    #redis.delete(redis_bar)
                    continue
                
                # get the optional data from the redis cache specific to iex datafeed
                #yahoo fields
                if redis.hget(redis_bar,"adjusted_close") is not None: my_bar.adjusted_close = float(redis.hget(redis_bar,"adjusted_close"))
                if redis.hget(redis_bar,"dividends") is not None: my_bar.dividends = float(redis.hget(redis_bar,"dividends"))
                if redis.hget(redis_bar, "stock_splits") is not None: my_bar.stock_splits = float(redis.hget(redis_bar, "stock_splits"))
                #iex fields - for bar
                if redis.hget(redis_bar,"average") is not None: my_bar.average = float(redis.hget(redis_bar,'average'))
                if redis.hget(redis_bar,"notional") is not None: my_bar.notional = float(redis.hget(redis_bar,'notional'))
                if redis.hget(redis_bar,'number_of_trades') is not None: my_bar.number_of_trades = float(redis.hget(redis_bar,'number_of_trades'))
                if redis.hget(redis_bar,'change_over_time') is not None: my_bar.change_over_time = float(redis.hget(redis_bar,'change_over_time'))
                #iex fields - for market
                if redis.hget(redis_bar,'market_open') is not None: my_bar.market_open = float(redis.hget(redis_bar,'market_open'))
                if redis.hget(redis_bar,'market_high') is not None: my_bar.market_high = float(redis.hget(redis_bar,'market_high'))
                if redis.hget(redis_bar,'market_low') is not None: my_bar.market_low = float(redis.hget(redis_bar,'market_low'))
                if redis.hget(redis_bar,'market_close') is not None: my_bar.market_close = float(redis.hget(redis_bar,'market_close'))
                if redis.hget(redis_bar,"market_average") is not None: my_bar.market_average = float(redis.hget(redis_bar,"market_average"))
                if redis.hget(redis_bar,'market_volume') is not None: my_bar.market_volume = float(redis.hget(redis_bar,'market_volume'))
                if redis.hget(redis_bar,"market_notional") is not None: my_bar.market_notional = float(redis.hget(redis_bar,"market_notional"))
                if redis.hget(redis_bar,"market_number_of_trades") is not None: my_bar.market_number_of_trades = float(redis.hget(redis_bar,"market_number_of_trades"))
                if redis.hget(redis_bar,"market_change_over_time") is not None: my_bar.market_change_over_time = float(redis.hget(redis_bar,"market_change_over_time"))
            
                all_bars.append(my_bar)
            #print (my_bar.open)
        # END RETRIEVE REDIS CACHE ////////////////////////////////////////////////////////////////////////////////////

        # PULL FROM DISK IF STOCK NOT INITILIAZED in redis /////////////////////////////////////////////////////////////
        else:
            # purge any bars from redis cache
            all_bars = []
            keys_to_delete = [redis_bar.decode('UTF-8') for redis_bar in redis.scan_iter(f"{self.symbol}:bar:*")]
            for key in keys_to_delete: 
                redis.delete(key)
            
            # retrieve the bars from the disk
            with helpers.postgres_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""SELECT id, symbol, ticker_datetime, volume, open, high, low, close,adjusted_close, 
                                    frequency, dividends, stock_splits, average, notional,
                                    number_of_trades, change_over_time, market_open, market_high, market_low, market_close,
                                    market_average, market_volume, market_notional, market_change_over_time, 
                                    market_number_of_trades
                                     FROM tickerdata_tick WHERE symbol = \'{self.symbol}\'""")
            
                    # add the bars from disk
                    for row in cursor.fetchall():
                        id,symbol,date_and_time,volume,open,high,low,close,adjusted_close,period,dividends,stock_splits, \
                            average, notional, number_of_trades,change_over_time, market_open, market_high, market_low, \
                                market_close,market_average,market_volume,market_notional,market_change_over_time, \
                                    market_number_of_trades = row
                        
                        my_bar = Bars(self.symbol,date_and_time,open,high,low,close,volume,period,adjusted_close,dividends, \
                            stock_splits,average,notional,number_of_trades,change_over_time,market_open,market_high,market_low,market_close, \
                                market_volume,market_notional,market_number_of_trades,market_change_over_time)

                        all_bars.append(my_bar)                        
                        
                        # add to redis cache
                        str_date_time = date_and_time.strftime('%Y-%m-%d-%H-%M')
                        print (str_date_time)
                        hash_name = f'{self.symbol}:bar:{str_date_time}'
                        #required fields
                        redis.hset(hash_name,"open",open)
                        redis.hset(hash_name,"high",high)
                        redis.hset(hash_name,"low",low)
                        redis.hset(hash_name,"close",close)
                        redis.hset(hash_name,"volume",volume)
                        redis.hset(hash_name,"period",period)
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
                        
            # add the redis initialized
            redis.set(f"{self.symbol}:initialized","true")
        return all_bars
        

    def save_bars_to_disk(self):
        
        redis = helpers.redis_conn()
        #INSERT COMMAND BEFORE UNIQUE CONSTRAINT ///////////////////////////////////////////////////
        #///////////////////////////////////////////////////////////////////////////////////////////
        
    #     #return if stock isnt initialized in redis
    #     if not redis.get(f'{self.symbol}:initialized').decode('UTF-8') == "true": return None

        
    #     # start the insert string command
        
    #     insert_str = '''INSERT INTO tickerdata_tick (symbol,ticker_datetime,volume,open,high,low,close,
    #         frequency,adjusted_close,dividends,stock_splits,average,notional,number_of_trades,change_over_time,
    #         market_open,market_high,market_low,market_close,market_average,market_volume,market_notional,market_change_over_time,
    #         market_number_of_trades) VALUES '''
        
    #     insert_args = []
    #     for redis_bar in redis.scan_iter(f"{self.symbol}:bar:*"):
    #         #check the stock for required elements
    #         if not redis.hexists(redis_bar,'source') or not redis.hexists(redis_bar,'volume') or not redis.hexists(redis_bar,'open') or \
    #             not redis.hexists(redis_bar,'high') or not redis.hexists(redis_bar,'low') or not redis.hexists(redis_bar,'close') or \
    #                 not redis.hexists(redis_bar,'period'):
    #                 print ('Redis hash did not have all required keys...')
    #                 continue
        
    #         #get only the bars that arent from disk
    #         if not redis.hget(redis_bar,'source').decode('UTF-8') == 'disk':
                
    #             #yahoo fields
    #             if redis.hexists(redis_bar,'adjusted_close'): adjusted_close = redis.hget(redis_bar,"adjusted_close").decode('UTF-8') 
    #             else: adjusted_close = None
    #             if redis.hexists(redis_bar,'dividends'): dividends = redis.hget(redis_bar,'dividends').decode('UTF-8')
    #             else: dividends = None
    #             if redis.hexists(redis_bar,'stock_splits'): stock_splits = redis.hget(redis_bar,'stock_splits').decode('UTF-8')
    #             else: stock_splits = None
    #             if redis.hexists(redis_bar,'average'): average = redis.hget(redis_bar,'average').decode('UTF-8')
    #             else: average = None
    #             #iex fields - stock
    #             if redis.hexists(redis_bar,'notional'): notional = redis.hget(redis_bar,'notional').decode('UTF-8')
    #             else: notional = None
    #             if redis.hexists(redis_bar,'number_of_trades'): number_of_trades = redis.hget(redis_bar,'number_of_trades').decode('UTF-8')
    #             else: number_of_trades = None
    #             if redis.hexists(redis_bar,'change_over_time'): change_over_time = redis.hget(redis_bar,'change_over_time').decode('UTF-8')
    #             else: change_over_time = None
    #             #iex fields - market
                
    #             if redis.hexists(redis_bar,'market_open'): market_open = redis.hget(redis_bar,'market_open').decode('UTF-8')
    #             else: market_open = None
    #             if redis.hexists(redis_bar,'market_high'): market_high = redis.hget(redis_bar,'market_high').decode('UTF-8')
    #             else: market_high = None
    #             if redis.hexists(redis_bar,'market_low'): market_low = redis.hget(redis_bar,'market_low').decode('UTF-8')
    #             else: market_low = None
    #             if redis.hexists(redis_bar,'market_close'): market_close = redis.hget(redis_bar,'market_close').decode('UTF-8')
    #             else: market_close = None
    #             if redis.hexists(redis_bar,'market_average'): market_average = redis.hget(redis_bar,'market_average').decode('UTF-8')
    #             else: market_average = None
    #             if redis.hexists(redis_bar,'market_volume'): market_volume = redis.hget(redis_bar,'market_volume').decode('UTF-8')
    #             else: market_volume = None
    #             if redis.hexists(redis_bar,'market_notional'): market_notional = redis.hget(redis_bar,'market_notional').decode('UTF-8')
    #             else: market_notional = None
    #             if redis.hexists(redis_bar,'market_change_over_time'): market_change_over_time = redis.hget(redis_bar,'market_change_over_time').decode('UTF-8')
    #             else: market_change_over_time = None
    #             if redis.hexists(redis_bar,'market_number_of_trades'): market_number_of_trades = redis.hget(redis_bar,'market_number_of_trades').decode('UTF-8')
    #             else: market_number_of_trades = None
                
    #             bar_to_write = [
    #                 self.symbol,
    #                 format_redis_date_for_postgres(redis_bar),
    #                 redis.hget(redis_bar,"volume").decode('UTF-8'),
    #                 redis.hget(redis_bar,"open").decode('UTF-8'),
    #                 redis.hget(redis_bar,"high").decode('UTF-8'),
    #                 redis.hget(redis_bar,"low").decode('UTF-8'),
    #                 redis.hget(redis_bar,"close").decode('UTF-8'),
    #                 redis.hget(redis_bar,"period").decode('UTF-8'),
    #                 adjusted_close,
    #                 dividends,
    #                 stock_splits,
    #                 average,
    #                 notional,
    #                 number_of_trades,
    #                 change_over_time,
    #                 market_open,
    #                 market_high,
    #                 market_low,
    #                 market_close,
    #                 market_average,
    #                 market_volume,
    #                 market_notional,
    #                 market_change_over_time,
    #                 market_number_of_trades
    #             ]

    #             insert_args.extend(bar_to_write)
                
    #             insert_str += " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),"
                
    #         #END IF CHECK FOR BARS THAT NEED TO BE WRITTEN
        
    #     #INSERT INTO DB ///////////////////////////////////////////////////////////

    #     #check that there are values to add
    #     if len(insert_args) > 7:
    #         insert_str2 = insert_str[:-1]
            
    #         with helpers.postgres_conn() as conn:
    #             with conn.cursor() as cursor:
    #                 cursor.execute(insert_str2,insert_args)
    #             conn.commit()
    #     else:
    #         print ('no args')        

    # #end for loop over all bars

        
        
        #END INSERT COMMAND BEFORE UNIQUE CONSTRAINT ///////////////////////////////////////////////////
        #///////////////////////////////////////////////////////////////////////////////////////////
        
        #return if stock isnt initialized in redis
        #if not redis.get(f'{self.symbol}:initialized').decode('UTF-8') == "true": return None

        
        # start the insert string command
        
        insert_str = '''INSERT INTO tickerdata_tick (symbol,ticker_datetime,volume,open,high,low,close,
            frequency,adjusted_close,dividends,stock_splits,average,notional,number_of_trades,change_over_time,
            market_open,market_high,market_low,market_close,market_average,market_volume,market_notional,market_change_over_time,
            market_number_of_trades) VALUES '''
        
        insert_args = []
        for redis_bar in redis.scan_iter(f"{self.symbol}:bar:*"): # iterate over all the stocks bars
           #check the stock for required elements 
            if not redis.hexists(redis_bar,'source') or not redis.hexists(redis_bar,'volume') or not redis.hexists(redis_bar,'open') or \
                not redis.hexists(redis_bar,'high') or not redis.hexists(redis_bar,'low') or not redis.hexists(redis_bar,'close') or \
                    not redis.hexists(redis_bar,'period'):
                    print ('Redis hash did not have all required keys...')
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
                redis.hget(redis_bar,"period").decode('UTF-8'),
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
                market_number_of_trades
            ]

            insert_args.extend(bar_to_write)
            
            insert_str += " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),"
            
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
            print (f'No bars for stock: {self.symbol} to add...')        

    #end for loop over all bars

        
class testFeed:

    def __init__(self):
        pass

    

# TEST CODE
aapl = Stock("test")
for bar in aapl.bars:
    print (bar.volume)
    print (bar.date_and_time)
aapl.save_bars_to_disk()
