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


# Set up of the engine to connect to the database
engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8mb4')
conn = engine.connect()

 # łaowanie danych na serwer json.temp

# insert nowych do tabeli docelowej
sql=text('INSERT IGNORE INTO '+db_name+'.likes \
(generationdate ,user_id  ,like_id ,category ,like_name ,like_about ,favorite) \
SELECT \
now() ,user_id  ,like_id ,category ,like_name ,like_about ,favorite \
FROM '+db_name+'.likes_df_tmp;')
engine.execute(sql) 