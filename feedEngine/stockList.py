# import sys
# sys.path.append("..") # Adds higher directory to python modules path.
# sys.path.append('../stockLists')

import logging, psycopg2, requests, datetime as dt, time, concurrent.futures
from time import sleep
import time
from feedEngine import helpers
from feedEngine import stock as st

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

def get_all_lists(only=''):
        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                if only == 'live': select_str = 'SELECT list_name FROM "stockLists_list" WHERE pull_live = True'
                elif only =='historical': select_str = 'SELECT list_name FROM "stockLists_list" WHERE pull_historical = True'
                elif only == 'options': select_str = 'SELECT list_name FROM "stockLists_list" WHERE pull_options = True'
                else: select_str = 'SELECT list_name FROM "stockLists_list"'
                cursor.execute(select_str)
                data = cursor.fetchall()
                list_names = [StockList(row[0]) for row in data]
            
            return StockLists(list_names)
    
# Stocklists //////////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////

class StockLists:
    def __init__(self,my_lists):
        self._stock_lists = my_lists
        self._index = 0

    def __next__(self):
        
        if self._index < len(self._stock_lists):
            result = self._stock_lists[self._index]
            self._index += 1
            return result
        #end of iteration
        raise StopIteration
    
    def __iter__(self):
        return self

    def __len__(self):
        return len(self._stock_lists)

    def __getitem__(self,key):
        try:
            for stock_list in self._stock_lists:
                if stock_list.list_name == key: return stock_list
        except:
            return None
            raise(KeyError('invalid key'))

            
# End Stocklists //////////////////////////////////////////////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////


class StockList:
    def __init__(self,list_name):
        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                select_str = 'SELECT list_name,members,pull_historical,pull_live,pull_options FROM "stockLists_list" WHERE list_name = %s'
                select_args = (list_name,)
                cursor.execute(select_str,select_args)
                if cursor.rowcount == 1:self._list_name = list_name
                elif cursor.rowcount == 0:
                    self.create_list(list_name)
                    self._list_name = list_name
                else:
                    print ('Getting stock list created a fatal error! There are 2 lists with the same name')
                
        
                if cursor.rowcount == 0:
                    self._members = []
                    self._pull_historical = False
                    self._pull_live = False
                    self._pull_options = False
                    return
                data = cursor.fetchone()
                self._members = data[1].split(',')
                self._pull_historical = data[2]
                self._pull_live = data[3]
                self._pull_options = data[4]
    #End init stock lists ////////////////////////////////////////////////////////////////
    
    #List name ///////////////////////////////////////////////////////////////////////////
    
    @property
    def list_name(self):
        return self._list_name

    @list_name.setter
    def list_name(self,value):
        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                insert_str = 'UPDATE "stockLists_list" SET list_name = %s WHERE list_name = %s'
                insert_args = (value,self._list_name)
                try:
                    cursor.execute(insert_str,insert_args)
                    self._list_name = value

                except Exception as e:
                    print (f'Error in setting name: \n \n {e}')

    #End list name ///////////////////////////////////////////////////////////////////////
    
    #Pull historical /////////////////////////////////////////////////////////////////////
    @property
    def pull_historical(self):
        return self._pull_historical

    @pull_historical.setter
    def pull_historical(self,value):
        if not isinstance(value,bool):return

        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                insert_str = 'UPDATE "stockLists_list" SET pull_historical = %s WHERE list_name = %s'
                insert_args = (value,self._list_name)
                try:
                    cursor.execute(insert_str,insert_args)
                    self._pull_historical = value

                except Exception as e:
                    print (f'Error in setting pull_historical: \n \n {e}')
        
    #End pull historical /////////////////////////////////////////////////////////////////

    #Pull live ///////////////////////////////////////////////////////////////////////////
    @property
    def pull_live(self):
        return self._pull_live

    @pull_live.setter
    def pull_live(self,value):
        if not isinstance(value,bool):return

        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                insert_str = 'UPDATE "stockLists_list" SET pull_live = %s WHERE list_name = %s'
                insert_args = (value,self._list_name)
                try:
                    cursor.execute(insert_str,insert_args)
                    self._pull_live = value

                except Exception as e:
                    print (f'Error in setting pull_live: \n \n {e}')
    
    #End pull live ///////////////////////////////////////////////////////////////////////
    
    #Pull options ////////////////////////////////////////////////////////////////////////
    @property
    def pull_options(self):
        return self._pull_live

    @pull_options.setter
    def pull_options(self,value):
        if not isinstance(value,bool):return

        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                insert_str = 'UPDATE "stockLists_list" SET pull_options = %s WHERE list_name = %s'
                insert_args = (value,self._list_name)
                try:
                    cursor.execute(insert_str,insert_args)
                    self._pull_options = value

                except Exception as e:
                    print (f'Error in setting pull options \n \n {e}')
    
    #End pull options ////////////////////////////////////////////////////////////////////
    
    #Create a new list ///////////////////////////////////////////////////////////////////
    def create_list(self,list_name,members=[],pull_historical=False,pull_live=False,pull_options=False):
        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                insert_str = 'INSERT INTO "stockLists_list" (list_name,members,pull_historical,pull_live,pull_options) VALUES (%s,%s,%s,%s,%s)'
                insert_args = (list_name,members,pull_historical,pull_live,pull_options)
                cursor.execute(insert_str,insert_args)
                conn.commit()

    #End create a new list //////////////////////////////////////////////////////////////
                
    # add members ///////////////////////////////////////////////////////////////////////
    #////////////////////////////////////////////////////////////////////////////////////

    def add_members(self,members,check_exists=False):
        current_members = self._members
        

        # Process new list
        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                
                if isinstance(members,list):
                    for member in members:
                        if isinstance(member,st.Stock):
                            if check_exists:
                                if member.exists and not member.symbol in current_members:
                                    current_members.append(member.symbol) #Stock instance
                            else:
                                if not member.symbol in current_members:
                                    current_members.append(member.symbol) #Stock instance

                        elif isinstance(member,str):
                            if check_exists:
                                if st.Stock(member).exists and not member in current_members:
                                    current_members.append(member) #string instance
                            else:
                                if not member in current_members:
                                    current_members.append(member)
                    
                elif isinstance(members,str):
                    current_members = self._members
                #end process new list
                
                
                insert_str = 'UPDATE "stockLists_list" SET members = %s WHERE list_name = %s'
                insert_args = (','.join(current_members),self.list_name)
                cursor.execute(insert_str,insert_args)
                conn.commit()
                self._members = current_members
                if check_exists: self.scrub_members()

    # End add_members ///////////////////////////////////////////////////////////////////////
    #////////////////////////////////////////////////////////////////////////////////////////


                
    @property
    def members(self):
        return self._members

    @members.setter
    def members(self,value):
        with helpers.postgres_conn() as conn:
            with conn.cursor() as cursor:
                final_stocks = []
                
                if type(value) is str:
                    initial_stocks = value.split(',')
                    for stock in initial_stocks:
                        if stock not in final_stocks:
                            final_stocks.append(stock)  
                            


                if type(value) is list:
                    for stock in value:
                        stock = stock.upper()
                        if isinstance(stock,st.Stock):
                            if stock.symbol not in final_stocks:final_stocks.append(stock.symbol)
                                
                        elif isinstance(stock,str):
                            if stock not in final_stocks: final_stocks.append(stock)
                
                    

                insert_str = 'UPDATE "stockLists_list" SET members = %s WHERE list_name = %s'
                insert_args = (','.join(final_stocks),self._list_name)
                cursor.execute(insert_str,insert_args)
                conn.commit()
                self._members = final_stocks
                self.scrub_members()
    
    def scrub_members(self):
        pass
        # scrubbed_members = [member for member in self._members if st.Stock(member).exists]
        
        # with helpers.postgres_conn() as conn:
        #     with conn.cursor() as cursor:
        #         update_str = 'UPDATE "stockLists_list" SET members = %s WHERE list_name = %s'
        #         update_args = (','.join(scrubbed_members),self._list_name)
        #         cursor.execute(update_str,update_args)
        #         conn.commit()
        #         self._members = scrubbed_members


#TEST CODE
# for l in get_all_lists():
#     print (l.list_name)
# myList = get_all_lists()['test2']
# for member in myList.members:
#     print (member)
# myList.members = 'ejklwn,f,t'
# myList.scrub_members()
# print (myList.members)

# start_time = time.time()
# myList.members = ["P","R"]
# elapsed = time.time() - start_time
# print (f'Setting took {elapsed} seconds')

# start_time = time.time()
# for member in myList.members:
#     print (member)
# elapsed = time.time() - start_time
# print (f'Iterationg took {elapsed} seconds')
# add_list = ['F','T','AEM','AEM','T','FYAO','QQQ','FZRION']
# start_time = time.time()
# myList.add_members(add_list,check_exists=True)
# elapsed = time.time() - start_time
# print (f'Add members took {elapsed} seconds')
#myList.add_members(add_list)

# myList.pull_historical = True
