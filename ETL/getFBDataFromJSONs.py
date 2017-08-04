import boto3
import pandas as pd
import json
from botocore.client import Config
from getFBUserFromJSON import getFBUserFromJSON
from getFBPhotosFromJSON import getFBPhotosFromJSON
from getFBLikesFromJSON import getFBLikesFromJSON
from getFBPostsFromJSON import getFBPostsFromJSON


################################################################################################
############################ FUNKCJE POMOCNICZE ################################################

def getFileNamesFromBucket(bucket,extension):
    #zwraca df z nazwami plikow o danym rozszerzeniu, ktore wystepuja w konkretnym buckecie i jego podfolderach
    
    s3=boto3.client('s3',config=Config(signature_version='s3v4'))
    prefix = 'facebook'
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    file_names = []

    for response in response_iterator:
        for object_data in response['Contents']:
            key = object_data['Key']
            if key.endswith(extension):
                file_names.append(key)
                result=pd.DataFrame({'File':file_names})
    
    return result


def getJSONFromBucket(bucket,filename):
    ''' wczytuje danego jsona i zwraca go'''
    try:
        s3=boto3.client('s3',config=Config(signature_version='s3v4'))
        json_object=s3.get_object(Bucket=bucket,Key=filename)['Body']
        plik=json.load(json_object)
        return plik
    except:
        pass

 
################################################################################################
############################ FUNKCJE WŁAŚCIWE ##################################################
    
def getFBUserDataFromJSONs():  
    # 1. przeszukaj S3 pod kątem JSONów z danymi usera i utwórz listę z adresmi do plików
    FBUserJSONFileNameDF = getFileNamesFromBucket('fbdeepdocdata','user.json')
    
    # 2. wczytaj każdego JSONa funkcją  getJSONFromBucket (pętla po licie jsonow)
    FBUserJSONList = []
    for filename in FBUserJSONFileNameDF['File']:
        FBUserJSONList.append(getJSONFromBucket('fbdeepdocdata',filename))
    
    # 3. pobierz z niego dane funkcją getFBUserFromJSON() ( appendowanie danych wynikowych)
    user_df, photos_df, location_df, education_df, languages_df, likes_df, work_df = (pd.DataFrame(),)*7
    
    for FBUserJSON in FBUserJSONList:
        (current_user_df, current_photos_df, current_location_df, current_education_df, 
        current_languages_df, current_likes_df, current_work_df) = getFBUserFromJSON(FBUserJSON)       
        user_df = user_df.append(current_user_df)
        photos_df = photos_df.append(current_photos_df)
        location_df = location_df.append(current_location_df)
        education_df = education_df.append(current_education_df)
        languages_df = languages_df.append(current_languages_df)
        likes_df = likes_df.append(current_likes_df)
        work_df = work_df.append(current_work_df)
        
    # 4. zwróć tabele wynikowe
    return user_df, photos_df, location_df, education_df, languages_df, likes_df, work_df

   
def getFBPhotosDataFromJSONs():
    # 1.
    FBPhotosJSONFileNameDF = getFileNamesFromBucket('fbdeepdocdata','photos.json')
    
    # 2.
    FBPhotosJSONList = []
    for filename in FBPhotosJSONFileNameDF['File']:
        FBPhotosJSONList.append(getJSONFromBucket('fbdeepdocdata',filename))
    
    # 3.  
    photos_df, photos_reaction_df = (pd.DataFrame(),)*2
    for FBPhotosJSON in FBPhotosJSONList:
        current_photos_df, current_photos_reaction_df = getFBPhotosFromJSON(FBPhotosJSON)
        photos_df = photos_df.append(current_photos_df)
        photos_reaction_df = photos_reaction_df.append(current_photos_reaction_df)
    # 4.
    return photos_df, photos_reaction_df

def getFBLikesDataFromJSONs(): 
    # 1.
    FBLikesJSONFileNameDF = getFileNamesFromBucket('fbdeepdocdata','likes.json')
    
    # 2.
    FBLikesJSONList = []
    for filename in FBLikesJSONFileNameDF['File']:
        FBLikesJSONList.append(getJSONFromBucket('fbdeepdocdata',filename))
    
    # 3.
    likes_df, likes_category_df = (pd.DataFrame(),)*2
    for FBLikesJSON in FBLikesJSONList:
        current_likes_df, current_likes_category_df = getFBLikesFromJSON(FBLikesJSON)
        likes_df = likes_df.append(current_likes_df)
        likes_category_df = likes_category_df.append(current_likes_category_df)
    
    # 4.
    return likes_df, likes_category_df

def getFBPostsDataFromJSONs():  
    # 1.
    FBPostsJSONFileNameDF = getFileNamesFromBucket('fbdeepdocdata','posts.json')
    
    # 2.
    FBPostsJSONList = []
    for filename in FBPostsJSONFileNameDF['File']:
        FBPostsJSONList.append(getJSONFromBucket('fbdeepdocdata',filename))
    
    # 3.
    posts_df, location_df, posts_reactions_df = (pd.DataFrame(),)*3
    for FBPostsJSON in FBPostsJSONList:
       current_posts_df, current_location_df, current_reactions_df = getFBPostsFromJSON(FBPostsJSON)
       posts_df = posts_df.append(current_posts_df)
       location_df = location_df.append(current_location_df)
       posts_reactions_df=posts_reactions_df.append(current_reactions_df) 
    # 4.
    return posts_df, location_df, posts_reactions_df

# weź tabele, będące wynikiem przetwarzania wszystkich plików (zakres tabel dubluje się pomiędzy wynikami zapytań)
# zwróć zestaw tabel w takiej postaci, w jakiej będą zasilone do bazy
def matchFBDataTables(user_photos_df, photos_photos_df, user_location_df, posts_location_df,
                      user_likes_df, likes_likes_df, posts_reactions_df, photos_reaction_df):
    
    photos_photos_df['type'] = 'other'
    photos_df = photos_photos_df.append(user_photos_df)
    
    location_df = posts_location_df.append(user_location_df)
    
    reactions_df = posts_reactions_df.append(photos_reaction_df)
    
    likes_likes_df['favorite'] = 0
    for index, like in user_likes_df.iterrows():
        likes_likes_df.loc[likes_likes_df['like_id'] == like['like_id'], ['favorite']] = 1
    
    return photos_df, location_df, likes_likes_df, reactions_df

################################################################################################
############################ WYWOŁANIE ################################################
#ponieważ poszczególne 4 funkcje mogą zasilać te same tabelki docelowe, użyj funkcji matchFBDataTables(), żeby połączyć te df (np. likes z User z Likes)
 #zwróć zestaw dataframeów, zawierający wszystkie dane ze wszystkich plików JSON, w takiej formie, żeby można było je załadować do bazy
 #ta funkcja powinna zawierać <10 linijek kodu
def getFBDataFromJSONs():
    # read user 
    (user_df, user_photos_df, user_location_df, education_df, 
     languages_df, user_likes_df, work_df) = getFBUserDataFromJSONs()

    # read photos
    photos_photos_df, photos_reaction_df  = getFBPhotosDataFromJSONs()
    
    # read likes
    likes_likes_df, likes_category_df = getFBLikesDataFromJSONs()
    
    # read posts
    posts_df, posts_location_df, posts_reactions_df = getFBPostsDataFromJSONs()
    
    # match tables
    photos_df, location_df, likes_df, reactions_df = matchFBDataTables(user_photos_df, 
                      photos_photos_df, user_location_df, posts_location_df,
                      user_likes_df, likes_likes_df, posts_reactions_df, photos_reaction_df)
    
    return user_df, photos_df, location_df, likes_df, education_df, languages_df, work_df, posts_df, likes_category_df, reactions_df

#==============================================================================
# execution test
#==============================================================================

user_df, photos_df, location_df, likes_df, education_df, languages_df, work_df,posts_df,likes_category_df,reactions_df = getFBDataFromJSONs()











