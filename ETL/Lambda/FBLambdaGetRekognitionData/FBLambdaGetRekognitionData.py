# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 12:47:13 2017

@author: Wojciech Drężek
"""
from __future__ import print_function

import boto3
import ddconfig
from io import StringIO
from botocore.client import Config
from Rekognition import OneFaceAnalysis

print('Loading function')

# połączenie z rekognition
rekognition = boto3.client('rekognition')
# połączenie z s3
s3 = boto3.client('s3')

db_name = ddconfig.db_name

db_labels_table = ddconfig.labels_table
db_faces_table = ddconfig.faces_table
dd_photos_table = ddconfig.photos_table

# poniżej przykładowa nazwa pliku zwracana z triggera s3
# facebook/56942561789524652/1013553_237760999763307_7126724387170253782_n.jpg-oh=bd6e50eb30d908ecbd3ea72e553b8c6b&oe=5A2FB9C2.jpg
def getPhotoIDFromFilename(filename):
    return filename.split('_')[1]

def getUserIDFromFilename(filename):
    return filename.split('/')[1]

def getFolderNameFromFilename(filename):
    return filename.split('/')[0]

# call AWS rekognition
def detect_labels(bucket, filename):
    AWSLabels = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": filename}})
    return AWSLabels['Labels']

def detect_faces(bucket, filename):
    AWSFaces = rekognition.detect_faces(Image={'S3Object': {'Bucket': bucket,'Name': filename}},Attributes=['ALL'])
    return AWSFaces['FaceDetails']

# convert labels dictionaries into sql values format
def AWSLabelsToSqlValues(labels, photo_id, user_id):
    values = ""
    for dic in labels:
        values = values + ("(now(),'%s','%s','%s','%f')," % (user_id, photo_id, dic['Name'], dic['Confidence']))
    return values[:-1]

# Konwersja tabelek do formatu sql query insert values
def AWSFacesToSqlValues(faces, photo_id, user_id):
    values = ""
    for face in faces:
        values = values + ("(now(),'%s','%s'," % (user_id, photo_id)) + str(face)[1:-1] + "),"
    return values[:-1]
# Tworzenie sql query
def AWSRekLabelsDataToSQLQuery(labels, photo_id, user_id):
    query = "INSERT IGNORE INTO "+db_name+"."+db_labels_table + " VALUES "
    sql_values = AWSLabelsToSqlValues(labels, photo_id, user_id)
    if(len(sql_values) == 0):
        return ""
    query = query + sql_values
    query = query + "; \n"
    query = query + "UPDATE " +  dd_photos_table + " SET av_Labels = 1 WHERE (photo_id='" + photo_id + "' AND user_id <> '0'); \n"
    return query

def AWSRekFacesDataToSQLQuery(faces, photo_id, user_id):    
    query = "INSERT IGNORE INTO "+db_name+"."+db_faces_table + " VALUES "
    sql_values = AWSFacesToSqlValues(faces, photo_id, user_id)
    if(len(sql_values) == 0):
        return ""
    query = query + sql_values
    query = query + '; \n'
    query = query + "UPDATE " +  dd_photos_table + " SET av_Faces = 1 WHERE (photo_id='" + photo_id + "' AND user_id <> '0'); \n"
    return query
    
# write sql query to file
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
        # Call rekognition DetectLabels API to detect labels in S3 object
        print("Detect labels")
        labels = detect_labels(bucket, filename)
        print("Detect labels OK")
        # Call rekognition DetectFaces API to detect labels in S3 object
        print("Detect faces")
        AWSFaces = rekognition.detect_faces(Image={'S3Object': {'Bucket': bucket,'Name': filename}},Attributes=['ALL'])
        print("Detect faces OK")
        print("Make SQL query")
        faces = [OneFaceAnalysis(face) for face in AWSFaces['FaceDetails']]
        query = AWSRekLabelsDataToSQLQuery(labels, photo_id, user_id)
        query = query + AWSRekFacesDataToSQLQuery(faces, photo_id, user_id)
        print("Make SQL query OK")
        print("Write SQL query to file")
        writeSQLQueryToFile(bucket, folder, user_id, photo_id, query, 'RekognitionOutput.txt')
        print("Write SQL query to file OK")
        return
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(filename, bucket))
        raise e