# -*- coding: utf-8 -*-
"""
Created on Sat Aug 26 23:22:18 2017

@author: Praca
"""

import _thread
import time
import ddconfig
from sqlalchemy import create_engine
from random import randint

rds_host  = ddconfig.rds_host
db_username = ddconfig.db_username
db_password = ddconfig.db_password
db_name = ddconfig.db_name

table = 'test'

def randomQuery(process, n):
    query = "INSERT IGNORE INTO "+db_name+"."+table + " VALUES "
    for i in range(n):
        query = query + '(' + str(process) + ',' + str(randint(0, 1000000)) + ',' + str(randint(0, 1000000)) + '),'
    query = query[0:-1]
    return query
    
def executeSQLQuery(query):
    engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(db_username,db_password,rds_host,db_name),encoding='utf-8')   
    engine.execute(query)
def uploadData(process):
    
    start_time = time.time()
    query = randomQuery(process, 50000)
    executeSQLQuery(query)
    print(len(query))
    print("--- Process %s: %s seconds ---" % (process, (time.time() - start_time)) )

# Create two threads as follows
try:
    for i in range(100):
        _thread.start_new_thread(uploadData, (i,))
except:
   print("Error: unable to start thread")

while 1:
   pass


