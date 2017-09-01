# -*- coding: utf-8 -*-
#!/usr/bin/python
from sqlalchemy import create_engine
from getFBDataFromJSONs import getFBDataFromJSONs
from sqlalchemy import text
import boto3
from botocore.client import Config
import time
import datetime
import pandas as pd
##dane do połączenia##
import ddconfig
import multiprocessing as mp
from multiprocessing import Lock

#########################################################################################
######################## FUNKCJE ZAWIERAJĄCE ZAPYTANIA SQL - inserty ####################


def getFileNamesFromBucket(bucket, extension, limit=10000000):
    #zwraca df z nazwami plikow o danym rozszerzeniu, ktore wystepuja w konkretnym buckecie i jego podfolderach
    client = boto3.client('s3',config=Config(signature_version='s3v4'))
    paginator = client.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket}
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
    result_df = pd.DataFrame({'File':result}) 
    return result_df
    
def getSpecificFileNamesFromList(files, extension):
    specific = []
    for index, row in files.iterrows():
        if row['File'].endswith(extension):
            specific.append(row['File'])
    return pd.DataFrame({'File':specific})

def uploadFBLikesFromJSONs(engine, likes_df):
    if(len(likes_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    likes_df.to_sql(con=engine, name='likes_df_tmp', if_exists='replace',index=False)
#    print('likes_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.likes \
    (generationdate ,user_id  ,like_id ,category ,like_name ,like_about ,favorite) \
    SELECT \
    now() ,user_id  ,like_id ,category ,like_name ,like_about ,favorite \
    FROM '+db_name+'.likes_df_tmp;')
    engine.execute(sql) 
    

    
def uploadFBPhotosFromJSONs(engine,photos_df):
    if(len(photos_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    photos_df.to_sql(con=engine, name='photos_df_tmp', if_exists='replace',index=False)
#    print('photos_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.photos \
    (generationdate ,user_id ,photo_id ,backdated_time ,created_time, image_big_height ,image_big_source ,image_big_width ,av_Faces ,av_HSV ,av_Labels ,image_type ,from_id ,from_name ,likes_cnt ,comments_cnt ,tags_cnt ,picture ,image_name ,image_small_source ,image_small_width ,image_small_height) \
    SELECT \
    now() ,user_id ,photo_id ,backdated_time ,created_time, image_big_height ,image_big_source ,image_big_width ,0 ,0 ,0 ,type ,from_id ,from_name ,likes_cnt ,comments_cnt ,tags_cnt ,picture ,name ,image_small_source ,image_small_width ,image_small_height \
    FROM '+db_name+'.photos_df_tmp;')
    engine.execute(sql) 
    
def uploadFBUserFromJSONs(engine,user_df):
    if(len(user_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    user_df.to_sql(con=engine, name='user_df_tmp', if_exists='replace',index=False)
#    print('user_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.user \
    (generationdate ,user_id ,age_range_min ,age_range_max ,birthday ,currency ,devices ,email ,gender ,interested_in ,quotes ,political ,relationship_status ,significant_other_id ,significant_other_name ,religion ,is_verified ,user_name ,user_name_format ,secure_browsing ,test_group ,third_party_id ,timezone ,updated_time ,user_verified) \
    SELECT \
    now() ,user_id ,age_range_min ,age_range_max ,birthday ,currency ,devices ,email ,gender ,interested_in ,quotes ,political ,relationship_status ,significant_other_id ,significant_other_name ,religion ,is_verified ,name ,name_format ,secure_browsing ,test_group ,third_party_id ,time_zone ,updated_time ,verified \
    FROM '+db_name+'.user_df_tmp;')
    engine.execute(sql) 
    
def uploadFBLocationFromJSONs(engine,location_df):
    if(len(location_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    location_df.to_sql(con=engine, name='location_df_tmp', if_exists='replace',index=False)
#    print('location_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.locations \
    (generationdate ,post_id ,user_id ,category, city ,country ,latitude ,created_time ,longitude) \
    SELECT \
    now() ,post_id ,user_id ,category ,city ,country ,latitude ,created_time ,longitude \
    FROM '+db_name+'.location_df_tmp;')
    engine.execute(sql) 

def uploadFBEducaionFromJSONs(engine,education_df):
    if(len(education_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    education_df.to_sql(con=engine, name='education_df_tmp', if_exists='replace',index=False)
#    print('education_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.education \
    (`generationdate`, `user_id`, `school_name`, `school_type`, `degree_name`, `degree_type`, `concentration_name`) \
    SELECT \
    now() ,`degree`, `user_id`, `school_name`, `school_type`, `type`, `concentration` \
    FROM '+db_name+'.education_df_tmp;')
    engine.execute(sql) 
    
def uploadFBLanguagesFromJSONs(engine,languages_df):
    if(len(languages_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    languages_df.to_sql(con=engine, name='languages_df_tmp', if_exists='replace',index=False)
#    print('languages_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.languages \
    (`generationdate`, `user_id`, `language_name`) \
    SELECT \
    now(), `user_id`, `language` \
    FROM '+db_name+'.languages_df_tmp;')
    engine.execute(sql) 
    
def uploadFBWorkFromJSONs(engine, work_df):
    if(len(work_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    work_df.to_sql(con=engine, name='work_df_tmp', if_exists='replace',index=False)
#    print('work_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.users_work \
    (`generationdate`, `user_id`, `description`, `employer_name`, `position`, `location_id`) \
    SELECT \
    now() ,`user_id`, `description`, `employer_name`, `position`, `location_id` \
    FROM '+db_name+'. work_df_tmp;')
    engine.execute(sql) 
    
def uploadFBPostsFromJSONs(engine, posts_df):
    if(len(posts_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    posts_df.to_sql(con=engine, name='posts_df_tmp', if_exists='replace',index=False)
#    print('posts_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.posts \
    (`generationdate`, `user_id`, `post_id`, `created_time`, `full_picture_source`, `message`, `picture_source`, `status_type`, `story`, `description`, `privacy_value`, `post_source`, `from_id`, `comments_cnt`, `likes_cnt`, `with_tags_cnt`, `reactions_cnt`) \
    SELECT \
    now() ,`user_id`, `post_id`, `created_time`, `full_picture`, `message`, `picture`,`status_type`, `story`,`description`,`privacy_value`, `source`, `from_id`, `comments_cnt`, `likes_cnt`,  `with_tags_cnt`,`reactions_cnt` \
    FROM '+db_name+'.posts_df_tmp;')
    engine.execute(sql) 
    
def uploadFBLikes_categoryFromJSONs(engine,likes_category_df):
    if(len(likes_category_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    likes_category_df.to_sql(con=engine, name='likes_category_df_tmp', if_exists='replace',index=False)
#    print('likes_category_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.likes_category \
    (`generationdate`, `user_id`, `like_id`, `category_name`, `category_id`) \
    SELECT \
    now(), `user_id`, `like_id`, `name`, `category_id` \
    FROM '+db_name+'.likes_category_df_tmp;')
    engine.execute(sql) 
    
def uploadFBReactionsFromJSONs(engine,reactions_df):
    if(len(reactions_df) == 0):
        return
    db_name = ddconfig.db_name
    # łaowanie danych na serwer json.temp
    reactions_df.to_sql(con=engine, name='reactions_df_tmp', if_exists='replace',index=False)
#    print('reactions_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.reactions \
    (`generationdate`, `user_id`, `post_id`, `photo_id`, `reaction_category`, `reaction_id`,`from_name`, `reaction_type`, `created_time`) \
    SELECT \
    now(), `user_id`, `post_id`, `photo_id`, `category`, `reaction_id`,`from_name`, `type`, `created_time` \
    FROM '+db_name+'.reactions_df_tmp;')
    engine.execute(sql) 

#########################################################################################
######################################### WYWOŁANIE #####################################


def worker(worker_id, engine, engine_lock, FBUserJSONFileNameDF, FBPhotosJSONFileNameDF, FBLikesJSONFileNameDF, FBPostsJSONFileNameDF):

    getFBData_time = time.time()
    user_df, photos_df, location_df, likes_df, education_df, languages_df, work_df, posts_df, likes_category_df, reactions_df = getFBDataFromJSONs(FBUserJSONFileNameDF, FBPhotosJSONFileNameDF, FBLikesJSONFileNameDF, FBPostsJSONFileNameDF)
    print(str(worker_id) + ' ' + 'Parsowanie jsonów OK --- %s seconds ---' % (time.time() - getFBData_time))
    ##load data ##
    
    engine_lock.acquire()
    upload_time = time.time()
    try:
#        print(str(worker_id) + ' ' + 'loading likes')
        uploadFBLikesFromJSONs(engine,likes_df)
    except:
        print(str(worker_id) + ' ' + 'likes upload failed')
    try:
#        print(str(worker_id) + ' ' + 'loading photos')
        uploadFBPhotosFromJSONs(engine,photos_df)
    except:
        print(str(worker_id) + ' ' + 'photos upload failed')
    try:
#        print(str(worker_id) + ' ' + 'loading user')
        uploadFBUserFromJSONs(engine,user_df)
    except:
        print(str(worker_id) + ' ' + 'users upload failed')
    try:
#        print(str(worker_id) + ' ' + 'loading location')
        uploadFBLocationFromJSONs(engine,location_df) 
    except:
        print(str(worker_id) + ' ' + 'location upload failed')    
    try:
#        print('loading education')
        uploadFBEducaionFromJSONs(engine,education_df)
    except:
        print('education upload failed')
    try:
#        print(str(worker_id) + ' ' + 'loading languages')
        uploadFBLanguagesFromJSONs(engine,languages_df)
    except:
        print(str(worker_id) + ' ' + 'languages upload failed')   
    try:
#        print('loading work')
        uploadFBWorkFromJSONs(engine, work_df)
    except:
        print('work upload failed')   
    try:
#        print(str(worker_id) + ' ' + 'loading posts')
        uploadFBPostsFromJSONs(engine, posts_df)
    except:
        print(str(worker_id) + ' ' + 'posts upload failed')   
    try:
#        print(str(worker_id) + ' ' + 'loading likes_cat')
        uploadFBLikes_categoryFromJSONs(engine,likes_category_df)
    except:
        print(str(worker_id) + ' ' + 'likes_category upload failed')
    try:
#        print(str(worker_id) + ' ' + 'loading reactions')
        uploadFBReactionsFromJSONs(engine,reactions_df)
    except:
        print(str(worker_id) + ' ' + 'reactions upload failed')
    print(str(worker_id) + ' ' + 'Upload OK --- %s seconds ---' % (time.time() - upload_time))
    engine_lock.release()
    return
    
def uploadFBDataFromJSONs(limit, workers_count):
    # Pobiera dane do logowania z pliku ddconfig oraz tworzy silnik do połączenia z bazą danych
    rds_host  = ddconfig.rds_host
    db_username = ddconfig.db_username
    db_password = ddconfig.db_password
    db_name = ddconfig.db_name
    engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8mb4')
    
    # Tworzy zmienną umożliwiająca synchronizację między procesami
    engine_lock = Lock()
    
    getFiles_time = time.time()
    # Ściąga pliki .json z s3
    jsons = getFileNamesFromBucket('fbdeepdocdata','.json', limit*4)
    # Wybiera poszczególne rodzaje 
    FBUserJSONFileNameDF = getSpecificFileNamesFromList(jsons, 'user.json')
    FBPhotosJSONFileNameDF = getSpecificFileNamesFromList(jsons, 'photos.json')
    FBLikesJSONFileNameDF = getSpecificFileNamesFromList(jsons, 'likes.json')
    FBPostsJSONFileNameDF = getSpecificFileNamesFromList(jsons, 'posts.json')
    print("getFileNamesFromBucket(%d) --- %s seconds ---" % (len(FBUserJSONFileNameDF),time.time() - getFiles_time))
    
    # Tworzy zadaną liczbę procesów. Każdy z nich otrzymuje częsć plików do przetworzenia.
    # Zmienne ...Split są przycinane do calosci i oznaczają liczbę plików do przetworzenia dla jednego procesu
    userSplit = int(len(FBUserJSONFileNameDF) / workers_count)
    photosSplit = int(len(FBPhotosJSONFileNameDF) / workers_count)
    likesSplit = int(len(FBLikesJSONFileNameDF) / workers_count)
    postsSplit = int(len(FBPostsJSONFileNameDF) / workers_count)
    
    processes = []
    for i in range(workers_count):
        p = mp.Process(target=worker, args=(i, engine, engine_lock, FBUserJSONFileNameDF[i*userSplit:(i+1)*userSplit], FBPhotosJSONFileNameDF[i*photosSplit:(i+1)*photosSplit], FBLikesJSONFileNameDF[i*likesSplit:(i+1)*likesSplit], FBPostsJSONFileNameDF[i*postsSplit:(i+1)*postsSplit],))
        processes.append(p)
        p.start()
        
    # Pozstałą częsc plików przetwarza proces główny oznaczony jako worker_id = -1
    worker(-1, engine, engine_lock, FBUserJSONFileNameDF[workers_count*userSplit:], FBPhotosJSONFileNameDF[workers_count*photosSplit:], FBLikesJSONFileNameDF[workers_count*likesSplit:], FBPostsJSONFileNameDF[workers_count*postsSplit:])
 
    # Czeka aż wszystkie procesy wykonają swoją pracę
    for p in processes:
        p.join()

####usuwanie tabel tymczasowych####    
def dropFBtemptablesJSONs():
    # Pobiera dane do logowania z pliku ddconfig oraz tworzy silnik do połączenia z bazą danych
    rds_host  = ddconfig.rds_host
    db_username = ddconfig.db_username
    db_password = ddconfig.db_password
    db_name = ddconfig.db_name
    engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8mb4')
    sql=text('DROP TABLE IF EXISTS likes_category_df_tmp ,likes_df_tmp ,photos_df_tmp ,user_df_tmp ,location_df_tmp,languages_df_tmp,posts_df_tmp ,reactions_df_tmp;')
    engine.execute(sql) 



# =============================================================================
# EXECUTION 
# =============================================================================
print("Upload data from json files")
print('')
start_time = time.time()
uploadFBDataFromJSONs(10000000, 35) 
dropFBtemptablesJSONs()
print('')
print("Upload data drom json files OK --- %s seconds ---" % (time.time() - start_time))

date = datetime.datetime.today()
today = str(date.date()) + '_' + str(date.time())
print("")
print("EC2UploadFBDataFromJSONs.py OK")
print(today)