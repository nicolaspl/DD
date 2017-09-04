# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 08:59:45 2017

@author: Mikołaj
"""

import boto3
import pandas as pd
import json
import time
import csv
from botocore.client import Config
from io import StringIO
import ddconfig

###############################################################################
############################## Funkcje do gadania z S3#########################
###############################################################################

def saveCSV2Bucket(DataFrame,filename,bucket):    
    ''' zapisuje dowolne DF pod wskazaną nazwą w wybranym buckecie '''
    
    csv_buffer=StringIO()
    DataFrame.to_csv(csv_buffer,index=False)
    
    s3=boto3.client('s3',config=Config(signature_version='s3v4'))
    s3.put_object(Body=csv_buffer.getvalue(),Bucket=bucket,Key=filename)

def ReadCSVFromBucket(filename,bucket):
    ''' wczytuje csv jako dataframe'''
    s3=boto3.client('s3',config=Config(signature_version='s3v4'))
    s3.download_file(bucket, filename, ddconfig.tempcsvpath)
    result=pd.read_csv(ddconfig.tempcsvpath,encoding='utf-8')
    return result

def DeleteFileFromBucket(bucket, filename):
    s3=boto3.client('s3',config=Config(signature_version='s3v4'))
    s3.delete_object(Bucket=bucket, Key=filename)
    
###############################################################################
############################## Funkcje pod ETL1 ###############################
###############################################################################

def getFileNamesFromBucket(bucket,extension):
    ''' zwraca df z nazwami plikow o danym rozszerzeniu, ktore wystepuja w konkretnym buckecie i jego podfolderach'''
    
    s3=boto3.client('s3',config=Config(signature_version='s3v4'))
      
    #bucket=s3.list_buckets()['Buckets'][0]['Name']

    objects=s3.list_objects(Bucket=bucket)
    objects.keys()
    
    obs=len(objects['Contents'])
    pliki=[objects['Contents'][i]['Key'] for i in range(obs)]
    JSONs=[plik for plik in pliki if plik.endswith(extension)]
    result=pd.DataFrame({'File':JSONs})
    
    return result

def getPhotoLisFromJSON(bucket='deepdocpics',filename='1960614799/1.json'):
    ''' wczytuje danego jsona i tworzy listę zdjęć '''
    try:
        s3=boto3.client('s3',config=Config(signature_version='s3v4'))
        json_object=s3.get_object(Bucket=bucket,Key=filename)['Body']
        plik=json.load(json_object)
        return plik['data']
    except:
        return []
    
def getOnePhotoData(photo):
    ''' z jsona dla jednego zdjęcia tworzy wektor zmiennych '''
    photo_created_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(photo['created_time'])))
    user_id=photo['user']['id']
    
    photo_caption=photo['caption']
    if photo_caption==None:
        photo_opis=None
    else:
        photo_opis=photo_caption['text'].replace('\n',' ').replace('\r',' ')
    photo_comments_cnt=photo['comments']['count']
    photo_filter=photo['filter']
    photo_id=photo['id']
    photo_url=photo['images']['thumbnail']['url']
    photo_filename=photo_url[photo_url.rfind('/')+1:]
    photo_likes=photo['likes']['count']
    #photo_tags=photo['tags'].replace('\n','')
    photo_tags=[tag.replace('\n','') for tag in photo['tags']] 
    photo_users_in_foto=len(photo['users_in_photo'])
    photo_data=[user_id,photo_id,photo_url,photo_filename,photo_created_time,photo_opis,photo_comments_cnt,photo_filter,photo_likes,photo_tags,photo_users_in_foto]
    return photo_data

def getAllPhotoData(photo_list):
    '''z listy zdjęć, ktora jest generowana przez getPhotoListFromJSON zwraca listę wektorow z danymi zdjec '''
    summary=[]
    for i in range(len(photo_list)):
        photo_data=getOnePhotoData(photo_list[i])
        summary.append(photo_data)
    return summary

def scrapBucket4JSONs(bucket):

    JSONs=getFileNamesFromBucket(bucket,'.json')['File'].tolist()
    JSONs_data=[]

    for i in range(len(JSONs)):
        photo_list=getPhotoLisFromJSON(bucket,JSONs[i])
        photos_data=getAllPhotoData(photo_list)
        JSONs_data+=photos_data

    header=['user_id','photo_id','photo_url','photo_filename','photo_created_time','photo_opis','photo_comments_cnt','photo_filter','photo_likes','photo_tags','photo_users_in_foto']
    df_summary=pd.DataFrame(JSONs_data,columns=header)
    return df_summary

###############################################################################
############################## Funkcje pod ETL1 ###############################
###############################################################################
