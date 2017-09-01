# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 12:47:13 2017

@author: Wojtek
"""
from __future__ import print_function

import boto3
import ddconfig
from PIL import Image
from io import StringIO
from botocore.client import Config
from HSV import analyzeOnePic

print('Loading function')

# połączenie z s3
s3 = boto3.client('s3')

# adres bazy danych
db_name = ddconfig.db_name

# nazwy tabel
dd_photos_table = ddconfig.photos_table
dd_hsv_table = ddconfig.hsv_table


# Poniżej przykładowa nazwa pliku zwracana z triggera s3
# facebook/56942561789524652/1013553_237760999763307_7126724387170253782_n.jpg-oh=bd6e50eb30d908ecbd3ea72e553b8c6b&oe=5A2FB9C2.jpg
def getPhotoIDFromFilename(filename):
    return filename.split('_')[1]

def getUserIDFromFilename(filename):
    return filename.split('/')[1]

def getFolderNameFromFilename(filename):
    return filename.split('/')[0]

# Konwersja tabelek do formatu sql query insert values
def HSVDataToSqlValues(hsv, photo_id, user_id):
    values = ("(now(),'%s','%s'," % (user_id, photo_id)) + str(hsv)[1:-1] + ")"
    return values
# Tworzenie sql query
def AWSHSVDataToSQLQuery(hsv, photo_id, user_id):
    query = "INSERT IGNORE INTO "+db_name+"."+dd_hsv_table + " VALUES "
    sql_values = HSVDataToSqlValues(hsv, photo_id, user_id)
    if(len(sql_values) == 0):
        return ""
    query = query + sql_values
    query = query + '; \n'
    query = query + "UPDATE " +  dd_photos_table + " SET av_HSV = 1 WHERE (photo_id='" + photo_id + "' AND user_id <> '0'); \n"
    return query

# Zapisuje sql query do pliku tekstowego w folderze zdjęcia
def writeSQLQueryToFile(bucket, folder, user_id, photo_id, query, extension):
    filename = folder + '/' + user_id + '/' + photo_id + extension
    file=StringIO()
    file.write(query)
    s3=boto3.client('s3',config=Config(signature_version='s3v4'))
    s3.put_object(Body=file.getvalue(),Bucket=bucket,Key=filename)

# --------------- Main handler ------------------
def lambda_handler(event, context):
    # Pobiera nazwe bucketu oraz nazwe pliku triggerującego
    bucket = event.get('bucket')
    filename = event.get('filename')
    try:
        user_id = getUserIDFromFilename(filename)
        photo_id = getPhotoIDFromFilename(filename)
        folder = getFolderNameFromFilename(filename)
    except:
        return
    print("Bucket: " + bucket)
    print("Filename: " + filename)
    print("user_id: " + user_id)
    print("photo_id: " + photo_id)
    try:  
        # Pobiera plik s3
        print("Open image")
        file = s3.get_object(Bucket=bucket, Key=filename)
        # Otwiera dane pliku jako zdjęcie
        image=Image.open(file['Body'])
        print("Open image OK")
        print("Do HSV")
        HSV=analyzeOnePic(image)
        query = AWSHSVDataToSQLQuery(HSV, photo_id, user_id)
        print("Do HSV OK")
        print("Write SQL query to file")
        writeSQLQueryToFile(bucket, folder, user_id, photo_id, query, 'HSVOutput.txt')
        print("Write SQL query to file OK")
        return
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(filename, bucket))
        raise e
