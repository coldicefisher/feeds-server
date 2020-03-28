import sys
sys.path.append("..") # Adds higher directory to python modules path.
from datetime import datetime
import time
from datetime import timedelta
import pandas_market_calendars as mcal
import helpers
#class nyseMarket:
#    def __init__(self):
#        # Check the redis database for calendar data
#        self._redis_conn = helpers.redis_conn()
        
        # Get 90 days ago
#        self._iex_token = os.environ['IEX_TOKEN'] = 'Tpk_d024741ac58c457eaa3f87c9e426de3b'
#        today = datetime.now()
#        three_months_ago = today - timedelta(days=90)
#        dates = [today,three_months_ago]
        
#        nyse = mcal.schedule()
    
#    @property
#    def nyse_schedule(self,start_date = datetime(2010,1,1),end_date = datetime(2019,1,1)):
#        pass

#nyse = nyseMarket()
def save_nyse_schedule_to_disk(start_date = datetime(2010,1,1),end_date = datetime(2019,1,1)):
    nyse = mcal.get_calendar('NYSE')
    #get 1 years 
    pd_schedule = nyse.schedule(start_date='2019-01-01',end_date='2021-12-31')
    pd_schedule.to_sql
    print (schedule)

save_nyse_schedule_to_disk()