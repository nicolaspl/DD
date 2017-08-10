import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
from getFBDataFromJSONs import getFBDataFromJSONs
from sqlalchemy import text
import time

##dane do połączenia##
import ddconfig
## logowanie do serwera##
rds_host  = ddconfig.rds_host
db_username = ddconfig.db_username
db_password = ddconfig.db_password
db_name = ddconfig.db_name

##Wgrywanie dataframow-load data##
print('wgrywanie dataframow')
user_df, photos_df, location_df, likes_df, education_df, languages_df, work_df, posts_df, likes_category_df, reactions_df = getFBDataFromJSONs(100000)
print('dataframy wgrane')
##load data ##

# Set up of the engine to connect to the database
engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8mb4')
#engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(db_username,db_password,rds_host,db_name),encoding='utf-8')  
conn = engine.connect()

#########################################################################################
######################## FUNKCJE ZAWIERAJĄCE ZAPYTANIA SQL - inserty ####################

 
def uploadFBLikesFromJSONs(engine,likes_df):
    
    # łaowanie danych na serwer json.temp
    likes_df.to_sql(con=engine, name='likes_df_tmp', if_exists='replace',index=False)
    print('likes_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.likes \
    (generationdate ,user_id  ,like_id ,category ,like_name ,like_about ,favorite) \
    SELECT \
    now() ,user_id  ,like_id ,category ,like_name ,like_about ,favorite \
    FROM '+db_name+'.likes_df_tmp;')
    engine.execute(sql) 
    

    
def uploadFBPhotosFromJSONs(engine,photos_df):
    
    # łaowanie danych na serwer json.temp
    photos_df.to_sql(con=engine, name='photos_df_tmp', if_exists='replace',index=False)
    print('photos_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.photos \
    (generationdate ,user_id ,photo_id ,backdated_time ,created_time, image_big_height ,image_big_source ,image_big_width ,av_Faces ,av_HSV ,av_Labels ,image_type ,from_id ,from_name ,likes_cnt ,comments_cnt ,tags_cnt ,picture ,image_name ,image_small_source ,image_small_width ,image_small_height) \
    SELECT \
    now() ,user_id ,photo_id ,backdated_time ,created_time, image_big_height ,image_big_source ,image_big_width ,0 ,0 ,0 ,type ,from_id ,from_name ,likes_cnt ,comments_cnt ,tags_cnt ,picture ,name ,image_small_source ,image_small_width ,image_small_height \
    FROM '+db_name+'.photos_df_tmp;')
    engine.execute(sql) 
    
def uploadFBUserFromJSONs(engine,user_df):
    
    # łaowanie danych na serwer json.temp
    user_df.to_sql(con=engine, name='user_df_tmp', if_exists='replace',index=False)
    print('user_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.user \
    (generationdate ,user_id ,age_range_min ,age_range_max ,birthday ,currency ,devices ,email ,gender ,interested_in ,quotes ,political ,relationship_status ,significant_other_id ,significant_other_name ,religion ,is_verified ,user_name ,user_name_format ,secure_browsing ,test_group ,third_party_id ,timezone ,updated_time ,user_verified) \
    SELECT \
    now() ,user_id ,age_range_min ,age_range_max ,birthday ,currency ,devices ,email ,gender ,interested_in ,quotes ,political ,relationship_status ,significant_other_id ,significant_other_name ,religion ,is_verified ,name ,name_format ,secure_browsing ,test_group ,third_party_id ,time_zone ,updated_time ,verified \
    FROM '+db_name+'.user_df_tmp;')
    engine.execute(sql) 
    
def uploadFBLocationFromJSONs(engine,location_df):
    
    # łaowanie danych na serwer json.temp
    location_df.to_sql(con=engine, name='location_df_tmp', if_exists='replace',index=False)
    print('location_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.locations \
    (generationdate ,post_id ,user_id ,category, city ,country ,latitude ,created_time ,longitude) \
    SELECT \
    now() ,post_id ,user_id ,category ,city ,country ,latitude ,created_time ,longitude \
    FROM '+db_name+'.location_df_tmp;')
    engine.execute(sql) 

def uploadFBEducaionFromJSONs(engine,education_df):
    
    # łaowanie danych na serwer json.temp
    education_df.to_sql(con=engine, name='education_df_tmp', if_exists='replace',index=False)
    print('education_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.education \
    (`generationdate`, `user_id`, `school_name`, `school_type`, `degree_name`, `degree_type`, `concentration_name`) \
    SELECT \
    now() ,`degree`, `user_id`, `school_name`, `school_type`, `type`, `concentration` \
    FROM '+db_name+'.education_df_tmp;')
    engine.execute(sql) 
    
def uploadFBLanguagesFromJSONs(engine,languages_df):
    
    # łaowanie danych na serwer json.temp
    languages_df.to_sql(con=engine, name='languages_df_tmp', if_exists='replace',index=False)
    print('languages_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.languages \
    (`generationdate`, `user_id`, `language_name`) \
    SELECT \
    now(), `user_id`, `language` \
    FROM '+db_name+'.languages_df_tmp;')
    engine.execute(sql) 
    
def uploadFBWorkFromJSONs(engine, work_df):
    
    # łaowanie danych na serwer json.temp
    work_df.to_sql(con=engine, name='work_df_tmp', if_exists='replace',index=False)
    print('work_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.users_work \
    (`generationdate`, `user_id`, `description`, `employer_name`, `position`, `location_id`) \
    SELECT \
    now() ,`user_id`, `description`, `employer_name`, `position`, `location_id` \
    FROM '+db_name+'. work_df_tmp;')
    engine.execute(sql) 
    
def uploadFBPostsFromJSONs(engine, posts_df):
    
    # łaowanie danych na serwer json.temp
    posts_df.to_sql(con=engine, name='posts_df_tmp', if_exists='replace',index=False)
    print('posts_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.posts \
    (`generationdate`, `user_id`, `post_id`, `created_time`, `full_picture_source`, `message`, `picture_source`, `status_type`, `story`, `description`, `privacy_value`, `post_source`, `from_id`, `comments_cnt`, `likes_cnt`, `with_tags_cnt`, `reactions_cnt`) \
    SELECT \
    now() ,`user_id`, `post_id`, `created_time`, `full_picture`, `message`, `picture`,`status_type`, `story`,`description`,`privacy_value`, `source`, `from_id`, `comments_cnt`, `likes_cnt`,  `with_tags_cnt`,`reactions_cnt` \
    FROM '+db_name+'.posts_df_tmp;')
    engine.execute(sql) 
    
def uploadFBLikes_categoryFromJSONs(engine,likes_category_df):
    
    # łaowanie danych na serwer json.temp
    likes_category_df.to_sql(con=engine, name='likes_category_df_tmp', if_exists='replace',index=False)
    print('likes_category_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.likes_category \
    (`generationdate`, `user_id`, `like_id`, `category_name`, `category_id`) \
    SELECT \
    now(), `user_id`, `like_id`, `name`, `category_id` \
    FROM '+db_name+'.likes_category_df_tmp;')
    engine.execute(sql) 
    
def uploadFBReactionsFromJSONs(engine,reactions_df):
    
    # łaowanie danych na serwer json.temp
    reactions_df.to_sql(con=engine, name='reactions_df_tmp', if_exists='replace',index=False)
    print('reactions_df_tmp zaladowany')
    # insert nowych do tabeli docelowej
    sql=text('INSERT IGNORE INTO '+db_name+'.reactions \
    (`generationdate`, `user_id`, `post_id`, `photo_id`, `reaction_category`, `reaction_id`,`from_name`, `reaction_type`, `created_time`) \
    SELECT \
    now(), `user_id`, `post_id`, `photo_id`, `category`, `reaction_id`,`from_name`, `type`, `created_time` \
    FROM '+db_name+'.reactions_df_tmp;')
    engine.execute(sql) 

#########################################################################################
######################################### WYWOŁANIE #####################################

def uploadFBDataFromJSONs():
    try:
        print('loading likes')
        uploadFBLikesFromJSONs(engine,likes_df)
    except:
        print('likes upload failed')
    try:
        print('loading photos')
        uploadFBPhotosFromJSONs(engine,photos_df)
    except:
        print('photos upload failed')
    try:
        print('loading user')
        uploadFBUserFromJSONs(engine,user_df)
    except:
        print('users upload failed')
    try:
        print('loading location')
        uploadFBLocationFromJSONs(engine,location_df) 
    except:
        print('location upload failed')    
    try:
        print('loading education')
        uploadFBEducaionFromJSONs(engine,education_df)
    except:
        print('education upload failed')
    try:
        print('loading languages')
        uploadFBLanguagesFromJSONs(engine,languages_df)
    except:
        print('languages upload failed')   
    try:
        print('loading work')
        uploadFBWorkFromJSONs(engine, work_df)
    except:
        print('work upload failed')   
    try:
        print('loading posts')
        uploadFBPostsFromJSONs(engine, posts_df)
    except:
        print('posts upload failed')   
    try:
        print('loading likes_cat')
        uploadFBLikes_categoryFromJSONs(engine,likes_category_df)
    except:
        print('likes_category upload failed')
    try:
        print('loading reactions')
        uploadFBReactionsFromJSONs(engine,reactions_df)
    except:
        print('reactions upload failed')   
    
####usuwanie tabel tymczasowych####    
def dropFBtemptablesJSONs():
    

    
    sql=text('DROP TABLE IF EXISTS likes_category_df_tmp ,likes_df_tmp ,photos_df_tmp ,user_df_tmp ,location_df_tmp ,education_df_tmp ,languages_df_tmp ,work_df_tmp ,posts_df_tmp ,reactions_df_tmp;')
    engine.execute(sql) 
################################### 

##testy



start = time.time()
uploadFBDataFromJSONs() 
dropFBtemptablesJSONs()  
total = time.time()-start
print(total)


#testy

