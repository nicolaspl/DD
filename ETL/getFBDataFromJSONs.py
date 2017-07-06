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

def scrapBucket4JSONs(bucket,extension):

    JSONs=getFileNamesFromBucket(bucket,extension)['File'].tolist()
    JSONs_data=[]

    for i in range(len(JSONs)):
        photo_list=getPhotoLisFromJSON(bucket,JSONs[i])
        photos_data=getAllPhotoData(photo_list)
        JSONs_data+=photos_data

    header=['user_id','photo_id','photo_url','photo_filename','photo_created_time','photo_opis','photo_comments_cnt','photo_filter','photo_likes','photo_tags','photo_users_in_foto']
    df_summary=pd.DataFrame(JSONs_data,columns=header)
    return df_summary

def ReadCSVFromBucket(filename,bucket):
    ''' wczytuje csv jako dataframe'''
    s3=boto3.client('s3',config=Config(signature_version='s3v4'))
    s3.download_file(bucket, filename, ddconfig.tempcsvpath)
    result=pd.read_csv(ddconfig.tempcsvpath,encoding='utf-8')
    return result

def getFBUserDataFromJSONs():  
  ## przeszukaj S3 pod kątem JSONów z danymi usera i utwórz listę z adresmi do plików
  FBUserJSONsList=scrapBucket4JSONs(deepdocfbdata,'user.json')
  ## z każdego pliku wczytaj JSONa i pobierz z niego dane funkcją getFBUserFromJSON() (pętla po liście, appendowanie danych wynikowych)
  ## połącz rezultat przetwarzania wszystkich JSONów w jedną tabelę (lub więcej, jeśli funkcja zwraca tabelki pomocniczne
  ## zwróć tabele wynikowe

def getFBPicsDataFromJSONs():  
  
def getFBLikesDataFromJSONs():  
  
def getFBPostsDataFromJSONs():  
  
def matchFBDataTables(wyniki wyniki przetwarzania wszystkich JSONów):
  # weź tabele, będące wynikiem przetwarzania wszystkich plików (zakres tabel dubluje się pomiędzy wynikami zapytań)
  # zwróć zestaw tabel w takiej postaci, w jakiej będą zasilone do bazy
  
# wywołanie procesu
def getFBDataFromJSONs():

  getFBUserDataFromJSONs()
  getFBPicsFromJSONs()
  getFBLikesFromJSONs() 
  getFBPostsFromJSONs()
  
  #ponieważ poszczególne 4 funkcje mogą zasilać te same tabelki docelowe, użyj funkcji matchFBDataTables(), żeby połączyć te df (np. likes z User z Likes)
  matchFBDataTables(wyniki wyniki przetwarzania wszystkich JSONów)
  #zwróć zestaw dataframeów, zawierający wszystkie dane ze wszystkich plików JSON, w takiej formie, żeby można było je załadować do bazy
  #ta funkcja powinna zawierać <10 linijek kodu
  
  
