from datetime import datetime
import logging, urllib3
import psycopg2, os
import redis
def convert_date_to_mysql(date):
    formatted_date = date.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_date

def convert_mysql_to_date(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')

# setup the logger
logger = logging.getLogger(__name__)

# define file handler and set formatter
file_handler = logging.FileHandler('logfile.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)


# Basic function for creating connection
def postgres_conn(auto_commit = False):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        os.environ['DB_HOST'] = 'localhost'
        # connect to the PostgreSQL server
        #print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST'),
            database=os.environ.get('DB_NAME'), 
            user=os.environ.get('DB_USER'), 
            password=os.environ.get('DB_PASS'),
            port=os.environ.get('DB_PORT')
        )
        
        #conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # create a cursor
        #cur = conn.cursor()
        
        # execute a statement
        #print('PostgreSQL database version:')
        #cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        #db_version = cur.fetchone()
        #print(db_version)
       
       # close the communication with the PostgreSQL
        #cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    #finally:
    #    if conn is not None:
            #conn.close()
            #print('Database connection closed.')
    return conn

#Check to see if internet is connected
def internet_on():
    http = urllib3.PoolManager()
    r = http.request('GET', 'https://1.1.1.1', timeout=10)
    return True if r.status == 200 else False

#    try:
#        r = http.request('GET', 'https://1.1.1.1', timeout=60)
#        return True if r.status == 200 else False
#    except:
#        return False

def redis_conn():
    try:
        os.environ['REDIS_HOST'] = 'redis'
        conn = redis.Redis(host=os.environ['REDIS_HOST'],port=os.environ['REDIS_PORT'],password=os.environ['REDIS_PASS'])
        return conn
    except:
        print ('failed to connect to Redis...')
    
    
