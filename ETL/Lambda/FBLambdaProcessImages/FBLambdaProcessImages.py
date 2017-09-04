# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 12:47:13 2017

@author: Wojtek
"""
from __future__ import print_function

import boto3
import json
import urllib

print('Loading function')

# Połączenie z lambda
lambda_client = boto3.client('lambda')

# Poniżej przykładowa nazwa pliku zwracana z triggera s3
# facebook/56942561789524652/1013553_237760999763307_7126724387170253782_n.jpg-oh=bd6e50eb30d908ecbd3ea72e553b8c6b&oe=5A2FB9C2.jpg
def getPhotoIDFromFilename(filename):
    return filename.split('_')[1]

def getUserIDFromFilename(filename):
    return filename.split('/')[1]

def getFolderNameFromFilename(filename):
    return filename.split('/')[0]

# Sprawdza czy zdjęcie jest oznaczone jako 'small' czyli do obróbki HSV
def isSmall(filename):
    return filename.split('/')[2].startswith('small')

# Sprawdza czy nazwa zdjęcia w s3 ma standardową nazwę t.j. zawiera w sobie nazwe 2 folderów oraz nazwe zdjęcia z fb rozdzielonymi slashem /
# oraz czy zawiera 3 liczby rodzielone podkreslnikiem _
def isNotStandard(filename):
    try:
        splitted = filename.split('/')[2].split('_')
    except:
        return True
    if len(splitted) < 4:
        return True
    if not(splitted[1].isdigit()):
        return True
    if not(splitted[2].isdigit()):
        return True
    return False

# --------------- Main handler ------------------
def lambda_handler(event, context):
    # Pobiera nazwe bucketu oraz nazwe pliku triggerującego
    bucket = event['Records'][0]['s3']['bucket']['name']
    filename = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    print('filename: ' + filename)
    # Sprawdza czy nazwa pliku jest standardowa. Jesli nie, przerywa dzialanie
    if isNotStandard(filename):
        print('Error: Photo does not have standard filename format')
        return
    # Tworzy argumenty do przekazania dalej
    args = {}
    args['filename'] = filename
    args['bucket'] = bucket
    file = json.dumps(args)
    if isSmall(filename):
        print("Invoke FBLambdaGetHSVData")
        lambda_client.invoke(FunctionName="FBLambdaGetHSVData", InvocationType='Event', Payload=file)
        print("Invoke FBLambdaGetHSVData OK")
    else:
        print("Invoke FBLambdaGetRekognitionData")
        lambda_client.invoke(FunctionName="FBLambdaGetRekognitionData", InvocationType='Event', Payload=file)
        print("Invoke FBLambdaGetRekognitionData OK")