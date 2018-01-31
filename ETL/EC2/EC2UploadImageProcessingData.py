# -*- coding: utf-8 -*-
"""
Created on Sun Aug 27 13:02:58 2017

@author: Wojtek
"""

from __future__ import print_function

import boto3
from botocore.client import Config
import time
import ddconfig
from sqlalchemy import create_engine
import multiprocessing as mp
from multiprocessing import Lock

# Pobiera nazwy plików z bucketu, przyjmuje argument okreslający maskymalną ilosć pobranych nazw plików
def getFileNamesFromBucket(bucket, prefix, extension, limit=1000000000):
    client = boto3.client('s3',config=Config(signature_version='s3v4'))
    paginator = client.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket,
                            'Prefix': prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    result = []
    for page in page_iterator:
        size = len(page['Contents'])
        files = [page['Contents'][i]['Key'] for i in range(size)]
        for file in files:
            if file.endswith(extension):
                result.append(file)
            if len(result) == limit:
                break
        if len(result) == limit:
            break
    return result
    
def extractQueries(query):
    query = query.replace('\n', '')
    HSVQuery = ""
    AWSLabelsQuery = ""
    AWSFacesQuery = ""
    if query.__contains__('INSERT IGNORE INTO deepdoc3.photosHSV VALUES '):
        HSVQuery = query.split('VALUES ')[1].split(';')[0] + ','
    if query.__contains__('INSERT IGNORE INTO deepdoc3.photosAWSLabels VALUES '):
        AWSLabelsQuery = query.split('INSERT IGNORE INTO deepdoc3.photosAWSLabels VALUES ')[1].split(';')[0]  + ','
    if query.__contains__('INSERT IGNORE INTO deepdoc3.photosAWSFaces VALUES '):
        AWSFacesQuery = query.split('INSERT IGNORE INTO deepdoc3.photosAWSFaces VALUES ')[1].split(';')[0] + ','
    return HSVQuery, AWSLabelsQuery, AWSFacesQuery
    

def worker(worker_id, engine, engine_lock, bucket, files):
    # Moze tak byc
    if len(files) == 0:
        return
    # Tworzy połączenie z s3
    s3 = boto3.client('s3')
#    print('Get ' + str(len(files)) + ' files and concatenate query.' )
    # Łączy zapytania SQL z plików wejsciowych w jedno
    start_time = time.time()
    HSVQuery = "INSERT IGNORE INTO deepdoc3.photosHSV VALUES "
    AWSLabelsQuery = "INSERT IGNORE INTO deepdoc3.photosAWSLabels VALUES "
    AWSFacesQuery = "INSERT IGNORE INTO deepdoc3.photosAWSFaces VALUES "
    for file in files:
        response = s3.get_object(Bucket=bucket, Key=file)
        content = response['Body'].read().decode('utf-8')
        HSVQueryTemp, AWSLabelsQueryTemp, AWSFacesQueryTemp = extractQueries(content)
        HSVQuery = HSVQuery + HSVQueryTemp
        AWSLabelsQuery = AWSLabelsQuery + AWSLabelsQueryTemp
        AWSFacesQuery = AWSFacesQuery + AWSFacesQueryTemp
    
    HSVQuery = HSVQuery[:-1]
    AWSLabelsQuery = AWSLabelsQuery[:-1]
    AWSFacesQuery = AWSFacesQuery[:-1]
    print("%d Get %d files and concatenate queries(%d) OK --- %s seconds ---" % (worker_id, len(files), len(HSVQuery) + len(AWSLabelsQuery) + len(AWSFacesQuery),time.time() - start_time))

    # Czeka aż inny proces zwolni silnik
    engine_lock.acquire()
    start_time = time.time()
    # wykonuje zapytanie SQL
    if len(HSVQuery) != 44:
        try:
            engine.execute(HSVQuery)
        except:
            print('%d Execute SQL HSVQuery error' % (worker_id))
    
    if len(AWSLabelsQuery) != 50:
        try:
            engine.execute(AWSLabelsQuery)
        except:
            print('%d Execute SQL AWSLabelsQuery error' % (worker_id))
            
    if len(AWSFacesQuery) != 49:
        try:
            engine.execute(AWSFacesQuery)
        except:
            print('%d Execute SQL AWSFacesQuery error' % (worker_id))
    
    print("%d Execute SQL query OK --- %s seconds ---" % (worker_id, time.time() - start_time))
    # Zwalnia silnik
    engine_lock.release()
    return HSVQuery, AWSLabelsQuery, AWSFacesQuery

def uploadImageProcessingDataToDB(limit, workers_count):
    # Pobiera dane do logowania z pliku ddconfig oraz tworzy silnik do połączenia z bazą danych
    rds_host  = ddconfig.rds_host
    db_username = ddconfig.db_username
    db_password = ddconfig.db_password
    db_name = ddconfig.db_name
    engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8mb4')
    
    # Tworzy zmienną umożliwiająca synchronizację między procesami 
    engine_lock = Lock()
    
    # Ściąga pliki Output.txt z s3
    bucket = 'fbdeepdocdata'
    folder = 'facebook/'
    getFiles_time = time.time()
    files = getFileNamesFromBucket(bucket, folder, 'Output.txt', limit)
    print("getFileNamesFromBucket(%d) --- %s seconds ---" % (len(files),time.time() - getFiles_time))
    
    # Tworzy zadaną liczbę procesów. Każdy z nich otrzymuje częsć plików do przetworzenia.
    # Zmienna files_per_process jest przycinana do calosci i oznacza liczbę plików do przetworzenia dla jednego procesu
    files_per_process = int(len(files) / workers_count)
    processes = []    
    for i in range(workers_count):
        p = mp.Process(target=worker, args=(i, engine, engine_lock, bucket, files[i*files_per_process:(i+1)*files_per_process]))
        processes.append(p)
        p.start()
    
    # Pozstałą częsc plików przetwarza proces główny oznaczony jako worker_id = -1
    worker(-1, engine, engine_lock, bucket, files[workers_count*files_per_process:])
    worker 
    # Czeka aż wszystkie procesy wykonają swoją pracę
    for p in processes:
        p.join()
    return


#rds_host  = ddconfig.rds_host
#db_username = ddconfig.db_username
#db_password = ddconfig.db_password
#db_name = ddconfig.db_name
#engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8mb4')
#files = getFileNamesFromBucket('fbdeepdocdata', 'facebook/', 'Output.txt', 20)
#a, b, c = worker(0, engine, 0, 'fbdeepdocdata', files)
#
#print('')
#print(a)
#print('')
#print(b)
#print('')
#print(c)
# =============================================================================
# EXECUTION 
# =============================================================================
print('EC2UploadImageProcessingData.py')
print('')
start_time = time.time()
uploadImageProcessingDataToDB(1000000000, 35)
print('')
print("EC2UploadImageProcessingData.py OK --- %s seconds ---" % (time.time() - start_time))