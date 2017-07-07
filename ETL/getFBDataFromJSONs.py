import boto3
import pandas as pd
import json
from botocore.client import Config

################################################################################################
############################ FUNKCJE POMOCNICZE ################################################

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

def getJSONFromBucket(bucket,filename):
    ''' wczytuje danego jsona i zwraca go'''
    try:
        s3=boto3.client('s3',config=Config(signature_version='s3v4'))
        json_object=s3.get_object(Bucket=bucket,Key=filename)['Body']
        plik=json.load(json_object)
        return plik

################################################################################################
############################ FUNKCJE WŁAŚCIWE ##################################################
    
def getFBUserDataFromJSONs():  
  ## przeszukaj S3 pod kątem JSONów z danymi usera i utwórz listę z adresmi do plików
  FBUserJSONsList=getFileNamesFromBucket('deepdocfbdata','user.json')
  ## wczytaj każdego JSONa funkcją  getJSONFromBucket (pętla po licie jsonow)
  ## pobierz z niego dane funkcją getFBUserFromJSON() ( appendowanie danych wynikowych)
  ## połącz rezultat przetwarzania wszystkich JSONów w jedną tabelę (lub więcej, jeśli funkcja zwraca tabelki pomocniczne)
  ## zwróć tabele wynikowe

def getFBPicsDataFromJSONs():  
  
def getFBLikesDataFromJSONs():  
  
def getFBPostsDataFromJSONs():  
  
def matchFBDataTables(wyniki wyniki przetwarzania wszystkich JSONów):
  # weź tabele, będące wynikiem przetwarzania wszystkich plików (zakres tabel dubluje się pomiędzy wynikami zapytań)
  # zwróć zestaw tabel w takiej postaci, w jakiej będą zasilone do bazy
  
################################################################################################
############################ WYWOŁANIE ################################################

def getFBDataFromJSONs():

  getFBUserDataFromJSONs()
  getFBPicsFromJSONs()
  getFBLikesFromJSONs() 
  getFBPostsFromJSONs()
  
  #ponieważ poszczególne 4 funkcje mogą zasilać te same tabelki docelowe, użyj funkcji matchFBDataTables(), żeby połączyć te df (np. likes z User z Likes)
  matchFBDataTables(wyniki wyniki przetwarzania wszystkich JSONów)
  #zwróć zestaw dataframeów, zawierający wszystkie dane ze wszystkich plików JSON, w takiej formie, żeby można było je załadować do bazy
  #ta funkcja powinna zawierać <10 linijek kodu
  
  
