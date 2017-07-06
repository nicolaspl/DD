def ReadCSVFromBucket(filename,bucket):
    ''' wczytuje csv jako dataframe'''
    s3=boto3.client('s3',config=Config(signature_version='s3v4'))
    s3.download_file(bucket, filename, ddconfig.tempcsvpath)
    result=pd.read_csv(ddconfig.tempcsvpath,encoding='utf-8')
    return result

def getFBUserDataFromJSONs():  
  ## przeszukaj S3 pod kątem JSONów z danymi usera i utwórz listę z adresmi do plików
  FBUserJSONsList=...
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
  
  
