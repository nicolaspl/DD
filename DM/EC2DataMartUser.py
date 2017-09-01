# -*- coding: utf-8 -*-
"""
Created on Sat Aug 19 16:39:58 2017

@author: Pawel
"""
import os
import pymysql
from sqlalchemy import create_engine
from sqlalchemy import text
import time
import datetime

#os.chdir('D:\DeepDoc\Kody\produkcja\ETL')

#zparsuj funkcję przeszukującą bucket i subfoldery oraz generującą tabelkę z danymi jsonowymi 
#import ddlib
#pobierz hasła z pliku konfiguracyjnego
import ddconfig

rds_host  = ddconfig.rds_host
db_username = ddconfig.db_username
db_password = ddconfig.db_password
db_name = ddconfig.db_name

engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8mb4')

sql=text('USE deepdoc3; \
 \
 \
DROP TABLE IF EXISTS U14, U13, U12, U11, U10, U9, U6,U5, U4, U3, U2, U1, U100, U111  \
,RU1, RU2, RU3, RU4, RU5, RU6, RU7, RU8, RU9, RUcombo7 \
,RP1, RP2, RP3, RP4, RP5, RP6, RP7, RP8, RP9, RPcombo8  \
,RU1L, RU2L, RU3L, RU4L, RU5L, RU6L, RU7L, RU8L, RU9L, RUcombo7L \
,RP1L, RP2L, RP3L, RP4L, RP5L, RP6L, RP7L, RP8L, RP9L, RPcombo8L \
,DatamartUser_tmp; \
 \
 \
CREATE TEMPORARY TABLE U1 \
(INDEX userkey (user_id)) \
SELECT user_id \
,SUM(likes_cnt) as sum_likescnt_photos \
,SUM(comments_cnt) as sum_commentscnt_photos \
,COUNT(image_name) as cnt_image_name_photos \
,COUNT(photo_id) as users_total_photos \
,DATEDIFF(NOW(),(MIN(created_time))) as oldest_photos_age_days_photos \
,DATEDIFF(NOW(),(MAX(created_time))) as latest_photos_age_days_photos \
FROM deepdoc3.photos \
GROUP BY user_id; \
 \
CREATE TEMPORARY TABLE U2 \
(INDEX userkey (user_id)) \
SELECT user_id  \
,SUM(likes_cnt) as sum_likescnt_lastmonth_photos \
,SUM(comments_cnt) as sum_commentscnt_lastmonth_photos \
,COUNT(image_name) as cnt_image_name_lastmonth_photos \
,COUNT(photo_id) as users_lastmonth_photos \
FROM deepdoc3.photos  \
WHERE DATEDIFF(NOW(),created_time) < 31 \
GROUP BY user_id; \
 \
CREATE TEMPORARY TABLE U3 \
(INDEX userkey (user_id)) \
SELECT user_id \
,case when user.age_range_min = 13  then 1 else 0 end as age_group_13_17_user \
,case when user.age_range_min = 18  AND user.age_range_max < 21 then 1 else 0 end as age_group_18_20_user \
,case when user.age_range_min = 21  then 1 else 0 end as age_group_more_then_21_user \
,devices as users_devices_user \
,currency as users_currency_user \
,case when user.gender = "male" then 1 else 0 end as gender_male_1_user \
,test_group as test_group_user \
,timezone as timezone_user  \
,DATEDIFF(NOW(),updated_time) as users_account_update_age_days_user \
FROM deepdoc3.user \
GROUP BY user_id; \
 \
CREATE TEMPORARY TABLE U4 \
(INDEX userkey (user_id)) \
SELECT  \
user_id, \
count(user_id) as users_languages_cnt_languages \
FROM deepdoc3.languages  \
GROUP BY user_id; \
 \
 \
 \
CREATE TEMPORARY TABLE U5 \
(INDEX userkey (user_id)) \
SELECT  \
user_id \
,COUNT(post_id) as users_total_posts \
,SUM(comments_cnt) as total_comments_cnt_posts \
,SUM(likes_cnt) as total_likes_cnt_posts \
,SUM(with_tags_cnt) as total_with_tags_cnt_posts \
,SUM(reactions_cnt) as total_reactions_cnt_posts \
,DATEDIFF(NOW(),(MIN(created_time))) as oldest_posts_age_days_posts \
,DATEDIFF(NOW(),(MAX(created_time))) as latest_posts_age_days_posts \
FROM deepdoc3.posts  \
GROUP BY user_id; \
 \
CREATE TEMPORARY TABLE U6 \
(INDEX userkey (user_id)) \
SELECT user_id  \
,COUNT(post_id) as users_posts_cnt_lastmonth_posts \
,SUM(comments_cnt) as comments_cnt_lastmonth_posts \
,SUM(likes_cnt) as likes_cnt_lastmonth_posts \
,SUM(with_tags_cnt) as with_tags_cnt_lastmonth_posts \
,SUM(reactions_cnt) as reactions_cnt_lastmonth_posts  \
FROM deepdoc3.posts  \
WHERE DATEDIFF(NOW(),created_time) < 31 \
GROUP BY user_id; \
 \
 \
CREATE TEMPORARY TABLE RU1 \
(INDEX userkey (user_id)) \
SELECT user_id,  COUNT(user_id) as users_posts_reactioncnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL  \
group by user_id; \
 \
create TEMPORARY TABLE RU2 \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_likecnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "LIKE" \
group by user_id; \
 \
create TEMPORARY TABLE RU3 \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_lovecnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "LOVE" \
group by user_id; \
 \
create TEMPORARY TABLE RU4 \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_angrycnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "ANGRY" \
group by user_id; \
 \
create TEMPORARY TABLE RU5 \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_sadcnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "SAD" \
group by user_id; \
 \
create TEMPORARY TABLE RU6 \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_wowcnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "WOW" \
group by user_id; \
 \
create TEMPORARY TABLE RU7 \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_hahacnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type LIKE "HAHA" \
group by user_id; \
 \
create TEMPORARY TABLE RU8 \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_pridecnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type LIKE "PRIDE" \
group by user_id; \
 \
create TEMPORARY TABLE RU9 \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_thankfullcnt_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type LIKE "THANKFULL" \
group by user_id; \
 \
create TEMPORARY TABLE RUcombo7 \
(INDEX userkey (user_id)) \
SELECT RU1.*,RU2.users_posts_likecnt_reactions ,RU3.users_posts_lovecnt_reactions , RU4.users_posts_angrycnt_reactions ,RU5.users_posts_sadcnt_reactions  \
,RU6.users_posts_wowcnt_reactions ,RU7.users_posts_hahacnt_reactions ,RU8.users_posts_pridecnt_reactions ,RU9.users_posts_thankfullcnt_reactions \
FROM RU1 \
LEFT JOIN RU2  \
	ON RU1.user_id= RU2.user_id \
LEFT JOIN RU3  \
	ON RU1.user_id= RU3.user_id \
LEFT JOIN RU4  \
	ON RU1.user_id= RU4.user_id \
LEFT JOIN RU5  \
	ON RU1.user_id= RU5.user_id \
LEFT JOIN RU6  \
	ON RU1.user_id= RU6.user_id \
LEFT JOIN RU7  \
	ON RU1.user_id= RU7.user_id \
LEFT JOIN RU8  \
	ON RU1.user_id= RU8.user_id \
LEFT JOIN RU9  \
	ON RU1.user_id= RU9.user_id; \
  \
UPDATE \
    RUcombo7 \
SET \
     users_posts_likecnt_reactions = COALESCE(users_posts_likecnt_reactions, 0) \
     ,users_posts_lovecnt_reactions = COALESCE(users_posts_lovecnt_reactions, 0) \
     ,users_posts_angrycnt_reactions = COALESCE(users_posts_angrycnt_reactions, 0) \
     ,users_posts_sadcnt_reactions = COALESCE(users_posts_sadcnt_reactions, 0) \
     ,users_posts_wowcnt_reactions = COALESCE(users_posts_wowcnt_reactions, 0) \
     ,users_posts_hahacnt_reactions = COALESCE(users_posts_hahacnt_reactions, 0) \
     ,users_posts_pridecnt_reactions = COALESCE(users_posts_pridecnt_reactions, 0) \
     ,users_posts_thankfullcnt_reactions  = COALESCE(users_posts_thankfullcnt_reactions , 0); \
 \
DROP TABLE IF EXISTS RU1, RU2, RU3, RU4, RU5, RU6, RU7, RU8, RU9; \
 \
CREATE TEMPORARY TABLE RP1 \
(INDEX userkey (user_id)) \
SELECT user_id, photo_id, COUNT(photo_id) as users_photos_reactioncnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL  \
group by user_id; \
 \
create TEMPORARY TABLE RP2 \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_likecnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "LIKE" \
group by user_id; \
 \
create TEMPORARY TABLE RP3 \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_lovecnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "LOVE" \
group by user_id; \
 \
create TEMPORARY TABLE RP4 \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_angrycnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "ANGRY" \
group by user_id; \
 \
create TEMPORARY TABLE RP5 \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_sadcnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "SAD" \
group by user_id; \
 \
create TEMPORARY TABLE RP6 \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_wowcnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "WOW" \
group by user_id; \
 \
create TEMPORARY TABLE RP7 \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_hahacnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "HAHA" \
group by user_id; \
 \
create TEMPORARY TABLE RP8 \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_pridecnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "PRIDE" \
group by user_id; \
 \
create TEMPORARY TABLE RP9 \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_thankfullcnt_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "THANKFULL" \
group by user_id; \
 \
create TEMPORARY TABLE RPcombo8 \
(INDEX userkey (user_id)) \
SELECT RP1.*, RP2.users_photos_likecnt_reactions ,RP3.users_photos_lovecnt_reactions , RP4.users_photos_angrycnt_reactions ,RP5.users_photos_sadcnt_reactions  \
,RP6.users_photos_wowcnt_reactions ,RP7.users_photos_hahacnt_reactions ,RP8.users_photos_pridecnt_reactions ,RP9.users_photos_thankfullcnt_reactions \
FROM RP1 \
LEFT JOIN RP2  \
	ON RP1.user_id= RP2.user_id \
LEFT JOIN RP3  \
	ON RP1.user_id= RP3.user_id \
LEFT JOIN RP4  \
	ON RP1.user_id= RP4.user_id \
LEFT JOIN RP5  \
	ON RP1.user_id= RP5.user_id \
LEFT JOIN RP6  \
	ON RP1.user_id= RP6.user_id \
LEFT JOIN RP7  \
	ON RP1.user_id= RP7.user_id \
LEFT JOIN RP8  \
	ON RP1.user_id= RP8.user_id \
LEFT JOIN RP9  \
	ON RP1.user_id= RP9.user_id; \
  \
UPDATE \
    RPcombo8 \
SET \
     users_photos_likecnt_reactions = COALESCE(users_photos_likecnt_reactions, 0) \
     ,users_photos_lovecnt_reactions = COALESCE(users_photos_lovecnt_reactions, 0) \
     ,users_photos_angrycnt_reactions = COALESCE(users_photos_angrycnt_reactions, 0) \
     ,users_photos_sadcnt_reactions = COALESCE(users_photos_sadcnt_reactions, 0) \
     ,users_photos_wowcnt_reactions = COALESCE(users_photos_wowcnt_reactions, 0) \
     ,users_photos_hahacnt_reactions = COALESCE(users_photos_hahacnt_reactions, 0) \
     ,users_photos_pridecnt_reactions = COALESCE(users_photos_pridecnt_reactions, 0) \
     ,users_photos_thankfullcnt_reactions  = COALESCE(users_photos_thankfullcnt_reactions , 0); \
    \
DROP TABLE IF EXISTS RP1, RP2, RP3, RP4, RP5, RP6, RP7, RP8, RP9;   \
 \
 \
 \
CREATE TEMPORARY TABLE RU1L \
(INDEX userkey (user_id)) \
SELECT user_id,  COUNT(user_id) as users_posts_reactioncnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RU2L \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_likecnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "LIKE" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RU3L \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_lovecnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "LOVE" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RU4L \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_angrycnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "ANGRY" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RU5L \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_sadcnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "SAD" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RU6L \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_wowcnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type = "WOW" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RU7L \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_hahacnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type LIKE "HAHA" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RU8L \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_pridecnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type LIKE "PRIDE" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RU9L \
(INDEX userkey (user_id)) \
SELECT reaction_type, user_id, COUNT(reaction_type) as users_posts_thankfullcnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE post_id IS NOT NULL AND reaction_type LIKE "THANKFULL" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RUcombo7L \
(INDEX userkey (user_id)) \
SELECT RU1L.*,RU2L.users_posts_likecnt_lastmonth_reactions ,RU3L.users_posts_lovecnt_lastmonth_reactions , RU4L.users_posts_angrycnt_lastmonth_reactions ,RU5L.users_posts_sadcnt_lastmonth_reactions  \
,RU6L.users_posts_wowcnt_lastmonth_reactions ,RU7L.users_posts_hahacnt_lastmonth_reactions ,RU8L.users_posts_pridecnt_lastmonth_reactions ,RU9L.users_posts_thankfullcnt_lastmonth_reactions \
FROM RU1L \
LEFT JOIN RU2L  \
	ON RU1L.user_id= RU2L.user_id \
LEFT JOIN RU3L  \
	ON RU1L.user_id= RU3L.user_id \
LEFT JOIN RU4L  \
	ON RU1L.user_id= RU4L.user_id \
LEFT JOIN RU5L  \
	ON RU1L.user_id= RU5L.user_id \
LEFT JOIN RU6L  \
	ON RU1L.user_id= RU6L.user_id \
LEFT JOIN RU7L  \
	ON RU1L.user_id= RU7L.user_id \
LEFT JOIN RU8L  \
	ON RU1L.user_id= RU8L.user_id \
LEFT JOIN RU9L  \
	ON RU1L.user_id= RU9L.user_id; \
  \
UPDATE \
    RUcombo7L \
SET \
     users_posts_likecnt_lastmonth_reactions = COALESCE(users_posts_likecnt_lastmonth_reactions, 0) \
     ,users_posts_lovecnt_lastmonth_reactions = COALESCE(users_posts_lovecnt_lastmonth_reactions, 0) \
     ,users_posts_angrycnt_lastmonth_reactions = COALESCE(users_posts_angrycnt_lastmonth_reactions, 0) \
     ,users_posts_sadcnt_lastmonth_reactions = COALESCE(users_posts_sadcnt_lastmonth_reactions, 0) \
     ,users_posts_wowcnt_lastmonth_reactions = COALESCE(users_posts_wowcnt_lastmonth_reactions, 0) \
     ,users_posts_hahacnt_lastmonth_reactions = COALESCE(users_posts_hahacnt_lastmonth_reactions, 0) \
     ,users_posts_pridecnt_lastmonth_reactions = COALESCE(users_posts_pridecnt_lastmonth_reactions, 0) \
     ,users_posts_thankfullcnt_lastmonth_reactions  = COALESCE(users_posts_thankfullcnt_lastmonth_reactions , 0); \
 \
DROP TABLE IF EXISTS RU1L, RU2L, RU3L, RU4L, RU5L, RU6L, RU7L, RU8L, RU9L; \
 \
CREATE TEMPORARY TABLE RP1L \
(INDEX userkey (user_id)) \
SELECT user_id, photo_id, COUNT(photo_id) as users_photos_reactioncnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RP2L \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_likecnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "LIKE" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RP3L \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_lovecnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "LOVE" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RP4L \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_angrycnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "ANGRY" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RP5L \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_sadcnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "SAD" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RP6L \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_wowcnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "WOW" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RP7L \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_hahacnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "HAHA" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RP8L \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_pridecnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "PRIDE" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RP9L \
(INDEX userkey (user_id)) \
SELECT user_id, reaction_type, photo_id, COUNT(reaction_type) as users_photos_thankfullcnt_lastmonth_reactions FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "THANKFULL" AND DATEDIFF(NOW(),created_time) < 31 \
group by user_id; \
 \
create TEMPORARY TABLE RPcombo8L \
(INDEX userkey (user_id)) \
SELECT RP1L.*, RP2L.users_photos_likecnt_lastmonth_reactions ,RP3L.users_photos_lovecnt_lastmonth_reactions , RP4L.users_photos_angrycnt_lastmonth_reactions ,RP5L.users_photos_sadcnt_lastmonth_reactions  \
,RP6L.users_photos_wowcnt_lastmonth_reactions ,RP7L.users_photos_hahacnt_lastmonth_reactions ,RP8L.users_photos_pridecnt_lastmonth_reactions ,RP9L.users_photos_thankfullcnt_lastmonth_reactions \
FROM RP1L \
LEFT JOIN RP2L  \
	ON RP1L.user_id= RP2L.user_id \
LEFT JOIN RP3L  \
	ON RP1L.user_id= RP3L.user_id \
LEFT JOIN RP4L  \
	ON RP1L.user_id= RP4L.user_id \
LEFT JOIN RP5L \
	ON RP1L.user_id= RP5L.user_id \
LEFT JOIN RP6L  \
	ON RP1L.user_id= RP6L.user_id \
LEFT JOIN RP7L  \
	ON RP1L.user_id= RP7L.user_id \
LEFT JOIN RP8L  \
	ON RP1L.user_id= RP8L.user_id \
LEFT JOIN RP9L  \
	ON RP1L.user_id= RP9L.user_id; \
  \
UPDATE \
    RPcombo8L \
SET \
     users_photos_likecnt_lastmonth_reactions = COALESCE(users_photos_likecnt_lastmonth_reactions, 0) \
     ,users_photos_lovecnt_lastmonth_reactions = COALESCE(users_photos_lovecnt_lastmonth_reactions, 0) \
     ,users_photos_angrycnt_lastmonth_reactions = COALESCE(users_photos_angrycnt_lastmonth_reactions, 0) \
     ,users_photos_sadcnt_lastmonth_reactions = COALESCE(users_photos_sadcnt_lastmonth_reactions, 0) \
     ,users_photos_wowcnt_lastmonth_reactions = COALESCE(users_photos_wowcnt_lastmonth_reactions, 0) \
     ,users_photos_hahacnt_lastmonth_reactions = COALESCE(users_photos_hahacnt_lastmonth_reactions, 0) \
     ,users_photos_pridecnt_lastmonth_reactions = COALESCE(users_photos_pridecnt_lastmonth_reactions, 0) \
     ,users_photos_thankfullcnt_lastmonth_reactions  = COALESCE(users_photos_thankfullcnt_lastmonth_reactions , 0); \
    \
DROP TABLE IF EXISTS RP1L, RP2L, RP3L, RP4L, RP5L, RP6L, RP7L, RP8L, RP9L;  \
 \
 \
CREATE TEMPORARY TABLE U100 \
SELECT category_name,COUNT(category_name) as category_frequency FROM deepdoc3.likes_category \
GROUP BY category_name; \
 \
CREATE TEMPORARY TABLE U10 \
SELECT likes_category.* \
,U100.category_frequency \
FROM likes_category \
LEFT JOIN U100 \
ON likes_category.category_name = U100.category_name;  \
 \
ALTER TABLE U10 ADD COLUMN  likes_in_popular_categories SMALLINT; \
SELECT AVG(category_frequency) as avg_category_frequency into @avgcf0 FROM U10; \
UPDATE U10 SET likes_in_popular_categories = (case when category_frequency >= @avgcf0 then 1 else 0 end); \
 \
CREATE TEMPORARY TABLE U111 \
SELECT category,COUNT(category) as category_frequency FROM deepdoc3.likes \
GROUP BY category; \
 \
CREATE TEMPORARY TABLE U11 \
SELECT likes.* \
,U111.category_frequency  \
FROM likes \
LEFT JOIN U111 \
ON likes.category = U111.category;  \
 \
ALTER TABLE U11 ADD COLUMN  likes_in_popular_categories SMALLINT; \
SELECT AVG(category_frequency) as avg_category_frequency into @avgcf FROM U11; \
UPDATE U11 SET likes_in_popular_categories = (case when category_frequency >= @avgcf then 1 else 0 end); \
 \
CREATE TEMPORARY TABLE U12 \
(INDEX userkey (user_id)) \
SELECT  \
user_id \
,COUNT(like_id) as total_liked_by_user_likes \
,SUM(likes_in_popular_categories) as number_of_likes_in_popular_category_likes \
FROM U11 \
GROUP BY user_id; \
 \
CREATE TEMPORARY TABLE U13 \
(INDEX userkey (user_id)) \
SELECT  \
user_id \
,COUNT(category_id) as total_categories_liked_by_user_likes_category \
,SUM(likes_in_popular_categories) as likes_in_popular_category_likes_category \
FROM U10 \
GROUP BY user_id; \
 \
CREATE TEMPORARY TABLE U14 \
(INDEX userkey (user_id)) \
SELECT  \
user_id \
,COUNT(user_id) as user_all_tagged_location_cnt_locations \
,COUNT(distinct city) as user_distinct_location_cnt_city_locations \
,COUNT(distinct country) as user_distinct_location_cnt_country_locations \
FROM deepdoc3.locations \
GROUP BY user_id; \
 \
 \
CREATE TEMPORARY TABLE DatamartUser_tmp \
(INDEX userkey (user_id)) \
SELECT   \
U1.sum_likescnt_photos, U1.sum_commentscnt_photos, U1.cnt_image_name_photos ,U1.users_total_photos, U1.oldest_photos_age_days_photos, U1.latest_photos_age_days_photos \
,U2.sum_likescnt_lastmonth_photos, U2.sum_commentscnt_lastmonth_photos ,U2.cnt_image_name_lastmonth_photos ,U2.users_lastmonth_photos \
,U3.* \
,U4.users_languages_cnt_languages \
,U5.users_total_posts, U5.total_comments_cnt_posts ,U5.total_likes_cnt_posts ,U5.total_with_tags_cnt_posts ,U5.total_reactions_cnt_posts ,U5.oldest_posts_age_days_posts ,U5.latest_posts_age_days_posts \
,U6.users_posts_cnt_lastmonth_posts ,U6.comments_cnt_lastmonth_posts ,U6.likes_cnt_lastmonth_posts ,U6.with_tags_cnt_lastmonth_posts ,U6.reactions_cnt_lastmonth_posts  \
,RUcombo7.users_posts_reactioncnt_reactions ,RUcombo7.users_posts_likecnt_reactions ,RUcombo7.users_posts_lovecnt_reactions ,RUcombo7.users_posts_angrycnt_reactions ,RUcombo7.users_posts_sadcnt_reactions ,RUcombo7.users_posts_wowcnt_reactions ,RUcombo7.users_posts_hahacnt_reactions ,RUcombo7.users_posts_pridecnt_reactions ,RUcombo7.users_posts_thankfullcnt_reactions \
,RPcombo8.users_photos_reactioncnt_reactions ,RPcombo8.users_photos_likecnt_reactions ,RPcombo8.users_photos_lovecnt_reactions ,RPcombo8.users_photos_angrycnt_reactions ,RPcombo8.users_photos_sadcnt_reactions ,RPcombo8.users_photos_wowcnt_reactions ,RPcombo8.users_photos_hahacnt_reactions ,RPcombo8.users_photos_pridecnt_reactions ,RPcombo8.users_photos_thankfullcnt_reactions \
,RUcombo7L.users_posts_reactioncnt_lastmonth_reactions ,RUcombo7L.users_posts_likecnt_lastmonth_reactions ,RUcombo7L.users_posts_lovecnt_lastmonth_reactions ,RUcombo7L.users_posts_angrycnt_lastmonth_reactions ,RUcombo7L.users_posts_sadcnt_lastmonth_reactions ,RUcombo7L.users_posts_wowcnt_lastmonth_reactions ,RUcombo7L.users_posts_hahacnt_lastmonth_reactions ,RUcombo7L.users_posts_pridecnt_lastmonth_reactions ,RUcombo7L.users_posts_thankfullcnt_lastmonth_reactions \
,RPcombo8L.users_photos_reactioncnt_lastmonth_reactions ,RPcombo8L.users_photos_likecnt_lastmonth_reactions ,RPcombo8L.users_photos_lovecnt_lastmonth_reactions ,RPcombo8L.users_photos_angrycnt_lastmonth_reactions ,RPcombo8L.users_photos_sadcnt_lastmonth_reactions ,RPcombo8L.users_photos_wowcnt_lastmonth_reactions ,RPcombo8L.users_photos_hahacnt_lastmonth_reactions ,RPcombo8L.users_photos_pridecnt_lastmonth_reactions ,RPcombo8L.users_photos_thankfullcnt_lastmonth_reactions \
,U12.total_liked_by_user_likes ,U12.number_of_likes_in_popular_category_likes \
,U13.total_categories_liked_by_user_likes_category ,U13.likes_in_popular_category_likes_category \
,U14.user_all_tagged_location_cnt_locations ,U14.user_distinct_location_cnt_city_locations ,U14.user_distinct_location_cnt_country_locations \
 \
FROM U3 \
LEFT JOIN U1 \
ON U3.user_id=U1.user_id \
LEFT JOIN U2 \
ON U3.user_id=U2.user_id \
LEFT JOIN U4 \
ON U3.user_id=U4.user_id \
LEFT JOIN U5 \
ON U3.user_id=U5.user_id \
LEFT JOIN U6 \
ON U3.user_id=U6.user_id \
LEFT JOIN RUcombo7 \
ON U3.user_id=RUcombo7.user_id \
LEFT JOIN RPcombo8 \
ON U3.user_id=RPcombo8.user_id \
LEFT JOIN RUcombo7L \
ON U3.user_id=RUcombo7L.user_id \
LEFT JOIN RPcombo8L \
ON U3.user_id=RPcombo8L.user_id \
LEFT JOIN U12 \
ON U3.user_id=U12.user_id \
LEFT JOIN U13 \
ON U3.user_id=U13.user_id \
LEFT JOIN U14 \
ON U3.user_id=U14.user_id; ')

sql1 = text('INSERT INTO deepdoc3.DatamartUser \
(`generationdate`, \
`user_id`, \
`sum_likescnt_photos`, \
`sum_commentscnt_photos`, \
`cnt_image_name_photos`, \
`users_total_photos`, \
`oldest_photos_age_days_photos`, \
`latest_photos_age_days_photos`, \
`sum_likescnt_lastmonth_photos`, \
`sum_commentscnt_lastmonth_photos`, \
`cnt_image_name_lastmonth_photos`, \
`users_lastmonth_photos`, \
`age_group_13_17_user`, \
`age_group_18_20_user`, \
`age_group_more_then_21_user`, \
`users_devices_user`, \
`users_currency_user`, \
`gender_male_1_user`, \
`test_group_user`, \
`timezone_user`, \
`users_account_update_age_days_user`, \
`users_languages_cnt_languages`, \
`users_total_posts`, \
`total_comments_cnt_posts`, \
`total_likes_cnt_posts`, \
`total_with_tags_cnt_posts`, \
`total_reactions_cnt_posts`, \
`oldest_posts_age_days_posts`, \
`latest_posts_age_days_posts`, \
`users_posts_cnt_lastmonth_posts`, \
`comments_cnt_lastmonth_posts`, \
`likes_cnt_lastmonth_posts`, \
`with_tags_cnt_lastmonth_posts`, \
`reactions_cnt_lastmonth_posts`, \
`users_posts_reactioncnt_reactions`, \
`users_posts_likecnt_reactions`, \
`users_posts_lovecnt_reactions`, \
`users_posts_angrycnt_reactions`, \
`users_posts_sadcnt_reactions`, \
`users_posts_wowcnt_reactions`, \
`users_posts_hahacnt_reactions`, \
`users_posts_pridecnt_reactions`, \
`users_posts_thankfullcnt_reactions`, \
`users_photos_reactioncnt_reactions`, \
`users_photos_likecnt_reactions`, \
`users_photos_lovecnt_reactions`, \
`users_photos_angrycnt_reactions`, \
`users_photos_sadcnt_reactions`, \
`users_photos_wowcnt_reactions`, \
`users_photos_hahacnt_reactions`, \
`users_photos_pridecnt_reactions`, \
`users_photos_thankfullcnt_reactions`, \
`users_posts_reactioncnt_lastmonth_reactions`, \
`users_posts_likecnt_lastmonth_reactions`, \
`users_posts_lovecnt_lastmonth_reactions`, \
`users_posts_angrycnt_lastmonth_reactions`, \
`users_posts_sadcnt_lastmonth_reactions`, \
`users_posts_wowcnt_lastmonth_reactions`, \
`users_posts_hahacnt_lastmonth_reactions`, \
`users_posts_pridecnt_lastmonth_reactions`, \
`users_posts_thankfullcnt_lastmonth_reactions`, \
`users_photos_reactioncnt_lastmonth_reactions`, \
`users_photos_likecnt_lastmonth_reactions`, \
`users_photos_lovecnt_lastmonth_reactions`, \
`users_photos_angrycnt_lastmonth_reactions`, \
`users_photos_sadcnt_lastmonth_reactions`, \
`users_photos_wowcnt_lastmonth_reactions`, \
`users_photos_hahacnt_lastmonth_reactions`, \
`users_photos_pridecnt_lastmonth_reactions`, \
`users_photos_thankfullcnt_lastmonth_reactions`, \
`total_liked_by_user_likes`, \
`number_of_likes_in_popular_category_likes`, \
`total_categories_liked_by_user_likes_category`, \
`likes_in_popular_category_likes_category`, \
`user_all_tagged_location_cnt_locations`, \
`user_distinct_location_cnt_city_locations`, \
`user_distinct_location_cnt_country_locations`) \
SELECT  \
now(), \
`user_id`, \
`sum_likescnt_photos`, \
`sum_commentscnt_photos`, \
`cnt_image_name_photos`, \
`users_total_photos`, \
`oldest_photos_age_days_photos`, \
`latest_photos_age_days_photos`, \
`sum_likescnt_lastmonth_photos`, \
`sum_commentscnt_lastmonth_photos`, \
`cnt_image_name_lastmonth_photos`, \
`users_lastmonth_photos`, \
`age_group_13_17_user`, \
`age_group_18_20_user`, \
`age_group_more_then_21_user`, \
`users_devices_user`, \
`users_currency_user`, \
`gender_male_1_user`, \
`test_group_user`, \
`timezone_user`, \
`users_account_update_age_days_user`, \
`users_languages_cnt_languages`, \
`users_total_posts`, \
`total_comments_cnt_posts`, \
`total_likes_cnt_posts`, \
`total_with_tags_cnt_posts`, \
`total_reactions_cnt_posts`, \
`oldest_posts_age_days_posts`, \
`latest_posts_age_days_posts`, \
`users_posts_cnt_lastmonth_posts`, \
`comments_cnt_lastmonth_posts`, \
`likes_cnt_lastmonth_posts`, \
`with_tags_cnt_lastmonth_posts`, \
`reactions_cnt_lastmonth_posts`, \
`users_posts_reactioncnt_reactions`, \
`users_posts_likecnt_reactions`, \
`users_posts_lovecnt_reactions`, \
`users_posts_angrycnt_reactions`, \
`users_posts_sadcnt_reactions`, \
`users_posts_wowcnt_reactions`, \
`users_posts_hahacnt_reactions`, \
`users_posts_pridecnt_reactions`, \
`users_posts_thankfullcnt_reactions`, \
`users_photos_reactioncnt_reactions`, \
`users_photos_likecnt_reactions`, \
`users_photos_lovecnt_reactions`, \
`users_photos_angrycnt_reactions`, \
`users_photos_sadcnt_reactions`, \
`users_photos_wowcnt_reactions`, \
`users_photos_hahacnt_reactions`, \
`users_photos_pridecnt_reactions`, \
`users_photos_thankfullcnt_reactions`, \
`users_posts_reactioncnt_lastmonth_reactions`, \
`users_posts_likecnt_lastmonth_reactions`, \
`users_posts_lovecnt_lastmonth_reactions`, \
`users_posts_angrycnt_lastmonth_reactions`, \
`users_posts_sadcnt_lastmonth_reactions`, \
`users_posts_wowcnt_lastmonth_reactions`, \
`users_posts_hahacnt_lastmonth_reactions`, \
`users_posts_pridecnt_lastmonth_reactions`, \
`users_posts_thankfullcnt_lastmonth_reactions`, \
`users_photos_reactioncnt_lastmonth_reactions`, \
`users_photos_likecnt_lastmonth_reactions`, \
`users_photos_lovecnt_lastmonth_reactions`, \
`users_photos_angrycnt_lastmonth_reactions`, \
`users_photos_sadcnt_lastmonth_reactions`, \
`users_photos_wowcnt_lastmonth_reactions`, \
`users_photos_hahacnt_lastmonth_reactions`, \
`users_photos_pridecnt_lastmonth_reactions`, \
`users_photos_thankfullcnt_lastmonth_reactions`, \
`total_liked_by_user_likes`, \
`number_of_likes_in_popular_category_likes`, \
`total_categories_liked_by_user_likes_category`, \
`likes_in_popular_category_likes_category`, \
`user_all_tagged_location_cnt_locations`, \
`user_distinct_location_cnt_city_locations`, \
`user_distinct_location_cnt_country_locations` \
FROM  \
deepdoc3.DatamartUser_tmp; ')

print("Execute SQL query")
start_time = time.time()
engine.execute(sql)
engine.execute(sql1)
print("Execute SQL query OK --- %s seconds ---" % (time.time() - start_time))

print("")
print("EC2DataMartUser.py OK")