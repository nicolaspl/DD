def getFBDataFromJSONs():

  ## przeszukaj S3 pod kątem JSONów z danymi usera i utwórz listę z adresmi do plików
  FBUserJSONsList=...
  ## z każdego pliku wczytaj JSONa i pobierz z niego dane funkcją getFBUserFromJSON() (pętla po liście, appendowanie danych wynikowych)
  ## połącz rezultat przetwarzania wszystkich JSONów w jedną tabelę (lub więcej, jeśli funkcja zwraca tabelki pomocniczne
  
  #wykonaj analogiczne czynności dla pzostałych rodzajów JSONów, wykorzystując:
  FBPicsData=getFBPicsFromJSON()
  FBLikesData=getFBLikesFromJSON() 
  getFBPostsFromJSON()
  
  #ponieważ poszczególne 4 funkcje mogą zasilać te same tabelki docelowe, użyj funkcji matchFBDataTables(), żeby połączyć te df (np. likes z User z Likes)
  #zwróć zestaw dataframeów, zawierający wszystkie dane ze wszystkich plików JSON
  #ta funkcja powinna zawierać max 15 linijek kodu- przeszukiwanie S3 pod kątem JSONów danego typu i ich parsowanie należy zapakować w oddzielne funkcję i tutaj tlko je wywołać
  
  
