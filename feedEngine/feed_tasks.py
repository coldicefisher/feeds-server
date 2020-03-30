# sys.path.append("..") # Adds higher directory to python modules path.
# sys.path.append('feeds')
import logging, psycopg2, requests, datetime as dt, time,multiprocessing
from time import sleep
import time
from feedEngine import stockList
from feedEngine import stock as st
from feedEngine import helpers
from feedEngine.feeds import crawlers
from feedEngine.feeds.iexFeed import iexFeed
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from feedEngine.celery_app import app
from celery import group
from celery.result import GroupResult
import os
import socket
import urllib3
import subprocess
        
        
        

#import logging



# BIG TODO PROGRAMMING NOTES!!!!
# the feed engine accepts a list of stocks and a configuration of feeds. The feeds will either be online or offline. The 
#feed itself returns a value of whether it is online. In case that feed goes down (scrapers), then it is logged and another
#feed will be used. The last - default - feed is IEX. But, that costs money. It is reliable and should be used for live data
#feeds.
#The feed engine should be where multiprocessing/multithreading should take place. All code outside of this should be written
#in the feeds itself. The Feeds should accept a stock Object and return a stock object with no database connections.
# - Redis connections need to be tested

#@app.task
def get_historical_data():
    
    all_lists = stockList.get_all_lists()
    
    list_of_stocks = []
    
    for l in all_lists:
        
        if l.pull_historical:
            
            for member in l.members:
            
                if member not in list_of_stocks:
                    list_of_stocks.append(member)
    
    tasks = [get_single_stock_historical_data.s((stock)) for stock in list_of_stocks]
    job = group(tasks)
    result = job()
    result.get()
    return result.as_tuple()
    
@app.task
def get_single_stock_historical_data(stock):
    
    # pull the data from IEX
    feed = iexFeed()
    stock_name,status = feed.get_historical_data(stock)
    print (f'Pull result for {stock_name} returned {status}')

    # save the dataframe to disk
    my_stock = st.Stock(stock)
    my_stock.save_dataframe_to_disk()
    
    # save the pull record to disk
    with helpers.postgres_conn() as conn:
        with conn.cursor() as cursor:
            insert_str = 'INSERT INTO logs_historical_fetches (symbol,feed,status) VALUES (%s,%s,%s)'
            insert_args = (stock_name,'iex',status)
            cursor.execute(insert_str,insert_args)
            conn.commit()
    # helpers.logger.setLevel(logging.ERROR)
    # helpers.logger.error("Pulled all historical data")

    # f = open("demofile2.txt", "w")
    # f.write("Now the file has more content!")
    # f.close()

    
@app.task
def get_live_data():
    pass

@app.task
def get_options_data():
    pass

@app.task
def get_stock_members():
    wiki = crawlers.WikipediaCrawler()
    tfs = crawlers.TopForeignStocksCrawler()
    nt = crawlers.NasdaqTraderCrawler()
    try:
        nt.get_stock_symbols()
    except Exception as e:
        print (f'Nasdaq Trader crawler failed: \n \n {e}')
    try:
        tfs.get_etfs()
    except Exception as e:
        print (f'Top Foreign Stocks crawler failed: \n \n {e}')
    try:
        wiki.get_sp500_members()
    except Exception as e:
        print (f'Wikipedia crawler failed: \n \n {e}')
    
    
def num_of_workers():
    count = app.control.inspect().ping()
    if count == None:
        return 0
    else:
        return len(count)

def start_worker():
    start_flower()
    num = num_of_workers() + 1
    # argv = [
    #     'worker',
    #     '--loglevel=DEBUG',
    #     f'-n worker{num}@%h'
    # ]
    # app.worker_main(argv)
    # return num
    subprocess.Popen(f'nohup celery -A celery_app worker -n worker{num} --loglevel=INFO &', shell=True,cwd='/var/www/myproject/feedEngine')
    return (num)

    


def start_flower():
    def isOpen():
        try:
            http = urllib3.PoolManager()
            r = http.request('GET', 'http://localhost:5555', timeout=10)
            
            return True if r.status == 200 else False
        except:
            return False

    
    if not isOpen():
        subprocess.Popen('nohup flower -A celery_app --port=5555 &', shell=True,cwd='/var/www/myproject/feedEngine')
        return ('started flower')

    return 'flower already running'


def backup_db():
    today = dt.datetime.today().date()
    subprocess.Popen(f'nohup pg_dump -U postgres -Ft django_trader > {today}.tar',shell=True,cwd='/var/www/myproject/db_backups')
    return f'backup successful to file: {today}'
#TEST CODE

# start_time = time.time()
# fe.get_stock_members()

# elapsed = time.time() - start_time
# print (f'Fetched all historical data in {elapsed} seconds')
# print (f'Crawled all data in {elapsed} seconds')
#start_worker()
# aapl = st.Stock('AAPL')
# print (aapl.dataframe)
#print (get_historical_data())

#print (start_flower())
# print (get_single_stock_historical_data('AAPL'))

#print (get_historical_data())
# for result in get_historical_data():
#     print (result)
