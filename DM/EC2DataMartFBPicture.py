# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 12:20:59 2017

@author: Mikołaj
"""
import os
import pymysql
from sqlalchemy import create_engine
from sqlalchemy import text
import time
import datetime

##os.chdir('D:\DeepDoc\Kody\produkcja\ETL')
#
##zparsuj funkcję przeszukującą bucket i subfoldery oraz generującą tabelkę z danymi jsonowymi 
##import ddlib
##pobierz hasła z pliku konfiguracyjnego
import ddconfig

rds_host  = ddconfig.rds_host
db_username = ddconfig.db_username
db_password = ddconfig.db_password
db_name = ddconfig.db_name
engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8mb4')
   
sql=text('USE deepdoc3; SET SQL_SAFE_UPDATES=0; \
DROP TABLE IF EXISTS R1, R2, R3, R4, R5, R6, R7, R8, R9, Rcombo, F1, F2, F3, Fcombo, P1, P2, DataMartFBPicture_tmp; \
\
\
CREATE TABLE R1 \
SELECT user_id, photo_id, COUNT(photo_id) as reactioncnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL \
group by photo_id, user_id; \
\
create TABLE R2 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as likecnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "LIKE" \
group by photo_id; \
\
create TABLE R3 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as lovecnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "LOVE" \
group by photo_id; \
\
create TABLE R4 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as angrycnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "ANGRY" \
group by photo_id; \
\
create TABLE R5 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as sadcnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "SAD" \
group by photo_id; \
\
create TABLE R6 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as wowcnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "WOW" \
group by photo_id; \
\
create TABLE R7 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as hahacnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "HAHA" \
group by photo_id; \
\
create TABLE R8 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as pridecnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "PRIDE" \
group by photo_id; \
\
create TABLE R9 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as thankfullcnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "THANKFULL" \
group by photo_id; \
\
create TABLE Rcombo \
SELECT R1.*,R2.likecnt ,R3.lovecnt , R4.angrycnt ,R5.sadcnt \
,R6.wowcnt ,R7.hahacnt ,R8.pridecnt ,R9.thankfullcnt \
FROM R1 \
LEFT JOIN R2 \
    ON R1.photo_id= R2.photo_id \
LEFT JOIN R3 \
	ON R1.photo_id= R3.photo_id \
LEFT JOIN R4 \
	ON R1.photo_id= R4.photo_id \
LEFT JOIN R5 \
	ON R1.photo_id= R5.photo_id \
LEFT JOIN R6 \
	ON R1.photo_id= R6.photo_id \
LEFT JOIN R7 \
	ON R1.photo_id= R7.photo_id \
LEFT JOIN R8 \
	ON R1.photo_id= R8.photo_id \
LEFT JOIN R9 \
	ON R1.photo_id= R9.photo_id; \
\
UPDATE \
    Rcombo \
SET \
     likecnt = COALESCE(likecnt, 0) \
     ,lovecnt = COALESCE(lovecnt, 0) \
     ,angrycnt = COALESCE(angrycnt, 0) \
     ,sadcnt = COALESCE(sadcnt, 0) \
     ,wowcnt = COALESCE(wowcnt, 0) \
     ,hahacnt = COALESCE(hahacnt, 0) \
     ,pridecnt = COALESCE(pridecnt, 0) \
     ,thankfullcnt = COALESCE(thankfullcnt, 0); \
\
ALTER TABLE Rcombo MODIFY Rcombo.photo_id VARCHAR(100), \
ADD INDEX photo_idkey (photo_id) ; \
\
CREATE TABLE F1 \
(INDEX photokey (photo_id)) \
SELECT \
photo_id,COUNT(*) as facescnt \
,sum(SAD) as sum_SAD \
,sum(HAPPY) as sum_HAPPY \
,sum(ANGRY) as sum_ANGRY \
,sum(CONFUSED) as sum_confused \
,sum(DISGUSTED) as sum_DISGUSTED \
,sum(SURPRISED) as sum_SURPRISED \
,sum(CALM) as sum_CALM \
,sum(`UNKNOWN`) as sum_UNKNOWN \
,sum(Sunglasses) as sum_Sunglasses \
,SUM(case when SmileTrue > 50 then 1 else 0 end ) as smiletrue \
,SUM(case when SmileFalse > 50 then 1 else 0 end ) as smilefalse \
,min(AgeRange_Low) as min_AgeRange_Low \
,max(AgeRange_High) as max_AgeRange_High \
,avg(AgeRange_High)-avg(AgeRange_Low) as agegap \
,(avg(AgeRange_High)+avg(AgeRange_Low))/2 as avgage \
FROM deepdoc3.photosAWSFaces \
GROUP BY photo_id; \
\
CREATE TABLE F2 \
(INDEX photokey (photo_id)) \
SELECT user_id \
,photo_id,COUNT(*) as femalecnt \
FROM deepdoc3.photosAWSFaces \
WHERE Gender = "Female" \
GROUP BY photo_id; \
\
CREATE TABLE F3 \
(INDEX photokey (photo_id)) \
SELECT user_id \
,photo_id,COUNT(*) as malecnt \
FROM deepdoc3.photosAWSFaces \
WHERE Gender = "Male" \
GROUP BY photo_id; \
\
create TABLE Fcombo \
(INDEX photokey (photo_id)) \
SELECT F1.*, F2.femalecnt, F3.malecnt \
FROM F1 \
LEFT JOIN F2 \
	ON F1.photo_id= F2.photo_id \
LEFT JOIN F3 \
	ON F1.photo_id= F3.photo_id; \
\
create TABLE P1 \
(INDEX photokey (photo_id)) \
SELECT user_id, photo_id, from_id, generationdate, created_time, image_name, \
 comments_cnt, likes_cnt, image_type, av_Faces, av_HSV, av_Labels FROM deepdoc3.photos; \
\
create TABLE P2 \
(INDEX photokey (photo_id)) \
SELECT \
P.user_id, P.photo_id, \
case when P.user_id=P.from_id then 1 else 0 end as sam_sobie \
FROM deepdoc3.photos P; \
\
CREATE TABLE DataMartFBPicture_tmp \
(INDEX photokey (photo_id)) \
SELECT P1.* ,P2.sam_sobie, R.reactioncnt, R.likecnt, R.lovecnt, R.angrycnt \
,R.sadcnt, R.wowcnt, R.hahacnt, R.pridecnt, R.thankfullcnt,F.facescnt, F.sum_SAD, F.sum_HAPPY \
, F.sum_ANGRY, F.sum_confused, F.sum_DISGUSTED, F.sum_SURPRISED, F.sum_CALM, F.sum_UNKNOWN \
, F.sum_Sunglasses, F.smiletrue, F.smilefalse, F.min_AgeRange_Low, F.max_AgeRange_High \
, F.agegap, F.avgage, F.femalecnt, F.malecnt \
, HSV.hue, HSV.saturation, HSV.value, HSV.red, HSV.green, HSV.blue  \
\
FROM P1 \
LEFT JOIN P2 \
	ON P1.photo_id = P2.photo_id \
LEFT JOIN Rcombo R \
	ON P1.photo_id = R.photo_id  \
LEFT JOIN Fcombo F \
	ON P1.photo_id = F.photo_id  \
LEFT JOIN photosHSV HSV \
    ON P1.photo_id=HSV.photo_id \
GROUP BY P1.photo_id; \
\
INSERT INTO `DataMartFBPicture` \
(generationdate, \
`user_id`, \
`photo_id`, \
`from_id`, \
`created_time`, \
`image_name`, \
`comments_cnt`, \
`likes_cnt`, \
`image_type`, \
`av_Faces`, \
`av_HSV`, \
`av_Labels`, \
`sam_sobie`, \
`reactioncnt`, \
`likecnt`, \
`lovecnt`, \
`angrycnt`, \
`sadcnt`, \
`wowcnt`, \
`hahacnt`, \
`pridecnt`, \
`thankfullcnt`, \
`facescnt`, \
`sum_SAD`, \
`sum_HAPPY`, \
`sum_ANGRY`, \
`sum_confused`, \
`sum_DISGUSTED`, \
`sum_SURPRISED`, \
`sum_CALM`, \
`sum_UNKNOWN`, \
`sum_Sunglasses`, \
`smiletrue`, \
`smilefalse`, \
`min_AgeRange_Low`, \
`max_AgeRange_High`, \
`agegap`, \
`avgage`, \
`femalecnt`, \
`malecnt`, \
`hue`, \
`saturation`, \
`value`, \
`red`, \
`green`, \
`blue`) \
SELECT \
now(), \
user_id, \
photo_id, \
from_id, \
created_time, \
image_name, \
comments_cnt, \
likes_cnt, \
image_type, \
av_Faces, \
av_HSV, \
av_Labels, \
sam_sobie, \
reactioncnt, \
likecnt, \
lovecnt, \
angrycnt, \
sadcnt, \
wowcnt, \
hahacnt, \
pridecnt, \
thankfullcnt, \
facescnt, \
sum_SAD, \
sum_HAPPY, \
sum_ANGRY, \
sum_confused, \
sum_DISGUSTED, \
sum_SURPRISED, \
sum_CALM, \
sum_UNKNOWN, \
sum_Sunglasses, \
smiletrue, \
smilefalse, \
min_AgeRange_Low, \
max_AgeRange_High, \
agegap, \
avgage, \
femalecnt, \
malecnt, \
hue, \
saturation, \
value, \
red, \
green, \
blue \
FROM DataMartFBPicture_tmp; \
DROP TABLE IF EXISTS R1, R2, R3, R4, R5, R6, R7, R8, R9, Rcombo, F1, F2, F3, Fcombo, P1, P2, DataMartFBPicture_tmp;')
#FROM DataMartFBPicture_tmp z2 \
#LEFT JOIN DataMartFBPicture z1 \
#ON z1.photo_id=z2.photo_id \
#WHERE z1.photo_id is NULL OR (z1.reactioncnt<>z2.reactioncnt OR z1.facescnt<>z2.facescnt OR (z1.hue is NULL AND z2.hue is not NULL)); ')

print("Execute SQL query")
start_time = time.time()
engine.execute(sql)
print("Execute SQL query OK --- %s seconds ---" % (time.time() - start_time))


print("")
print("EC2DataMartFBPicture.py OK")