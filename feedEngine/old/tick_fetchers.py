import helpers, logging, MySQLdb, requests, datetime as dt, time, concurrent.futures

import yfinance as yf
import pandas as pd

class WebTick:
    test = None
    def __init__(self,date_of,open = None, high = None, low = None, close = None, volume = None):
        # required instance variables
        self.date_of = date_of
        
        # optional variables
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.volume = None
        self.adjusted_close = None
        self.dividends = None
        self.stock_splits = None

class WebStock:

    def __init__(self, symbol):
        self._symbol = symbol
        self.status_code = None
        self.parser_used = None
        self._ticks = []
    
    @property 
    def symbol(self):
        return self._symbol
    
    #@symbol.setter
    #def symbol(self,value):
    #    self._symbol = value
    
    @property
    def ticks(self):
        return self._ticks
    
    @ticks.setter
    def ticks(self,value):
        self._ticks = value


class YahooFetcher:
    def __init__(self):
        pass
    
    def get_historical_data(self,ticker):
        my_stock = WebStock(str(ticker)) #stock object to be built and returned
        my_stock.parser_used = 'Yahoo'

        # check internet connection
        if not helpers.internet_on():
            helpers.logger.setLevel(logging.ERROR)
            helpers.logger.error("(Yahoo feed) Internet connection failed. Exiting...")
            return False # return false as stock

        # use yfinance to get Yahoo finance data
        stock = yf.Ticker(ticker)
        
        # make api request to Alpha Vantage
        #r = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={str_ticker}&interval=1min&apikey={str_api_key}&outputsize=full')
        try:
            data = stock.history(period='7d',interval='1m')
        except:

            my_stock.status_code = 404
            #logger.setLevel(logging.WARNING)
            #logger.warning(f'(Yahoo Finance feed) Stock symbol ({ticker}) does not exist')
            return False                    
        if data.empty:
            return False

        df = pd.DataFrame(data) # set data into a Pandas dataframe
        print (f'Data: {data}')
        print (f'df: {df}')
        if df.empty:
            
            # Update symbol status
            #logger.setLevel(logging.WARNING)
            #logger.warning(f'(Yahoo Finance feed) Stock symbol ({ticker}) has no data')
            return False
        

        # dropping null value columns to avoid errors 
        df.dropna(inplace = True)

        # filling na to avoid errors and
        df.fillna(0)

        # iterate through the dataframe and write it to the database

        for index, row in df.iterrows():
            tick = WebTick(helpers.convert_date_to_mysql(index))
            tick.open = row['Open']
            tick.high = row['High']
            tick.low = row['Low']
            tick.close = row['Close']
            tick.volume = row['Volume']
            tick.dividends = row['Dividends']
            tick.stock_splits = row['Stock Splits']
            my_stock.ticks.append(tick)
        my_stock.status_code = 200 #return stock with data and successful status code
        return my_stock    
          
    # END get_historical_data


class AlphaVantageFetcher:
    def __init__(self,api_key='FRTGRFPESYJD9DBD'):
        self._api_key = api_key
    
    def get_historical_data(self,ticker):
        my_stock = WebStock(str(ticker)) #stock object to be built and returned
        my_stock.parser_used = 'AlphaVantage'

        #print (f'Processing {ticker}')
        # check internet connection
        if not helpers.internet_on():
            helpers.logger.setLevel(logging.ERROR)
            helpers.logger.error("(Alpha Vantage feed) Internet connection failed. Exiting...")
            print (f'Returned false for {ticker}. No internet connection')
            return False # return false as stock

        str_ticker = str(ticker)
        str_api_key = str(self._api_key)
        
        # make api request to Alpha Vantage
        r = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={str_ticker}&interval=1min&apikey={str_api_key}&outputsize=full')
        try:
            data = r.json()
        except Exception as e:

            print (f'Failed getting request from {ticker}. Exception: {e}')
            my_stock.status_code = 404
            return my_stock #stock doesn't exist return 404 error

        error_msg = data.get('Error Message') #stock exists but error in data. return 404 error
        if not error_msg is None:
            helpers.logger.setLevel(logging.INFO)
            helpers.logger.info(f'(Alpha Vantage feed) Stock symbol ({ticker}) does not exist')
            print (f'Stock symbol {ticker} does not exist')
            my_stock.status_code = 404
            return my_stock

        ticks = data.get('Time Series (1min)') #retrieve data from JSON parser

        # stock is good, no data
        if ticks is None:
            helpers.logger.setLevel(logging.INFO)
            helpers.logger.info(f'(Alpha Vantage feed) Stock symbol ({ticker}) does not have ticker data')
            my_stock.status_code = 400
            print (f'Alpha Vantage stock: {ticker} returned no data.')
            return my_stock

        #now there is data. iterate it and return
        for time,value in ticks.items(): # iterate through data and create rows if not exists (on date)
            tick = WebTick(time)
            tick.open = value.get('1. open')
            tick.high = value.get('2. high')
            tick.low = value.get('3. low')
            tick.close = value.get('4. close')
            tick.volume = value.get('5. volume')
            my_stock.ticks.append(tick)

        my_stock.status_code = 200 #return stock with data and successful status code
        print (f'{ticker} returned data and is good!')
        return my_stock
        #end AlphaVantageFetcher //////////////////////////////////////////////////////////////

class FetcherBrain:

    def __init__(self):
        pass

    def _multiprocessing_fetch_data(self,stock,parser):
        alpha = parser()
        #alpha = AlphaVantageFetcher()
        #time.sleep(3)
        my_stock = alpha.get_historical_data(stock)
        
        try: #sleep on bad connection
            while not helpers.internet_on():
                print ('Internet not on. Sleeping...')
                time.sleep(5)
                
            #print (f"Downloading 1 min data for: {my_stock.symbol}.")
            return my_stock       
        except Exception as e:
            print (f'Failed to process: {stock}')
            print (e)

    """ def process_historical_minute_data(self,stocks):
        count = 1
        how_many = 0
        while not count == 0:
            
            how_many += 1
     """
    def process_historical_minute_data(self,stocks,parser,thread_count = 50, sleep = 5):
        
        start = time.perf_counter() #performance start
        
        processed_stocks = []
        unprocessed_stocks = stocks
        stocks_col = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            #my_stocks = executor.map(_multiprocessing_fetch_data, stock) for stock in stocks]
            
            
            #my_stocks = executor.map(self._multiprocessing_fetch_data,args)
            future_results = []
            
            while len(stocks) > len(processed_stocks):
                    #except IndexError:
                    #    print (f'Stock {p_stock.symbol} was not in the list. WIERD!')
                    #except Exception as e:
                    #    print (e)

                for stock in unprocessed_stocks:
                    
                    future_results.append(executor.submit(self._multiprocessing_fetch_data,stock, parser))
                    print (f'Job for {stock} submitted')
                    #time.sleep()
                for result in concurrent.futures.as_completed(future_results):
                    
                    fetched_stock = result.result()
                    if fetched_stock:
                        print (f'Returned stock: {fetched_stock.symbol} returned status code: {fetched_stock.status_code}')
                        if fetched_stock.status_code == 200:
                            if fetched_stock.symbol not in processed_stocks:
                                
                                processed_stocks.append(fetched_stock.symbol)
                                stocks_col.append(result.result())
                            else:
                                print (f'Stock {fetched_stock.symbol} was fetched but already belonged to processed stocks')
                            
                            try:
                                res_list = [i for i in range(len(unprocessed_stocks)) if unprocessed_stocks[i] == fetched_stock.symbol] 
                                print (res_list)
                                print (f'removing stock {fetched_stock.symbol} as res_list: {unprocessed_stocks[res_list[0]]}')
                                
                                del unprocessed_stocks[res_list[0]]
                            except Exception as e:
                                print (e)    
                        else:
                            print (f'Fetched stock {fetched_stock.symbol} was an error')

                    print (f'Stocks remaining: {len(stocks)}. Stocks processed {len(processed_stocks)}. Stocks remaining {len(unprocessed_stocks)}')
                    if sleep > 0:
                        time.sleep(sleep)

        #done getting all the data
        finish = time.perf_counter()
        total_secs = finish - start
        print (f'Processed all stocks in {total_secs} seconds')

        #commit data to database
        conn = MySQLdb.connect(host="localhost",user='root',passwd='Iwas#36@Divorced',database='django_trader')
        with conn.cursor() as cursor:
            for stock in stocks_col:
                # Update symbol status
                now = helpers.convert_date_to_mysql(dt.datetime.now())
                update_str = 'UPDATE tickerdata_symbol SET alphavantage_status = %s,last_pull = "%s" WHERE symbol = "%s"'
                args = (stock.status_code,now,stock.symbol)
                cursor.execute(update_str,args)
                conn.commit()

                #save ticks /////////////////////////////////////////////////////
                if stock.status_code == 200:
                    start = time.perf_counter()
                    print (f'Saving data for {stock.symbol}')
                    values_list = []
                
                    # Get existing ticks
                    select_str = 'SELECT symbol,ticker_datetime FROM tickerdata_tick WHERE symbol = %s'
                    select_args = (stock.symbol,)
                    cursor.execute(select_str,select_args)
                    data = cursor.fetchall()
                    check_ticks = []
                    for row in data:
                        check_ticks.append(helpers.convert_mysql_to_date(row[1]))

                    ticker_count = 0
                    for tick in stock.ticks:
                        
                        if tick.date_of not in check_ticks: #tick doesn't exist
                            ticker_count += 1
                            values_list.append(f'("{stock.symbol}",{tick.open},{tick.high},{tick.low},{tick.close},{tick.volume},60,"{tick.date_of}")')
                        
                        else: #tick exists. check data
                            pass
                        
                    if len(values_list) > 0:
                        insert_str = "INSERT INTO tickerdata_tick(symbol, open, high, low, close, volume, frequency, ticker_datetime) VALUES " + ','.join(values_list)
                        cursor.execute(insert_str)
                        conn.commit()
                    
                    finish = time.perf_counter()
                    total = finish - start
                    print (f'finished saving {stock.symbol} in {total} seconds. Added {ticker_count} ticks')
                        
        conn.close()    

        #end process_historical_minute_data ///////////////////////////////////////////////////////////////////////


conn = MySQLdb.connect(host="localhost",user='root',passwd='Iwas#36@Divorced',database='django_trader')
with conn.cursor() as cursor:
    select_str = 'SELECT symbol FROM tickerdata_symbol WHERE sp500_member = 1'
    cursor.execute(select_str)
    data = cursor.fetchall()
    stocks = []
    for row in data:
        stocks.append(row[0])

    brain = FetcherBrain()
my_stocks = ['SPY','EFX']
brain.process_historical_minute_data(stocks,YahooFetcher,100,0)
#yh = YahooFetcher()
#yh.get_historical_data('EFX')