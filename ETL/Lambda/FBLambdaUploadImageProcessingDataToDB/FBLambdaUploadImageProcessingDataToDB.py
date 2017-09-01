# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 12:47:13 2017

@author: Wojtek
"""
from __future__ import print_function

import boto3
import ddconfig
from sqlalchemy import create_engine
import time

print('Loading function')

# Łączy z s3
s3 = boto3.client('s3')

rds_host  = ddconfig.rds_host
db_username = ddconfig.db_username
db_password = ddconfig.db_password
db_name = ddconfig.db_name

bucket = 'fbdeepdocdata'
folder = 'facebook/'

# Tworzy połączenie z bazą danych oraz wykonuje zapytanie sql
def executeSQLQuery(query):
    engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(db_username,db_password,rds_host,db_name),encoding='utf-8')   
    engine.execute(query)
    
# --------------- Main handler ------------------
def lambda_handler(event, context):
    # 'zlepia' zapytania sql z plików w jedno wielkie zapytanie
    files = event.get('files')
    print('Get ' + str(len(files)) + ' files and concatenate query.' )
    start_time = time.time()
    query = ""
    for file in files:
        response = s3.get_object(Bucket=bucket, Key=file)
        content = response['Body'].read().decode('utf-8')
        query = query + content
    # jesli dlugosc zapytania jest równa 0, kończy działanie
    if len(query) == 0:
        return
    print('Query length: ' + str(len(query)))
    print("--- %s seconds ---" % (time.time() - start_time))
    
    # wykonuje zapytanie
    print('Execute sql query.')
    start_time = time.time()
    try:
        executeSQLQuery(query)
    except:
        print("executeSQLQuery error")
        return query
    print('SQL query executed')
    print("--- %s seconds ---" % (time.time() - start_time))
    return
