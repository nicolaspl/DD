# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 12:20:59 2017

@author: Mikołaj
"""
import os
import pymysql
from sqlalchemy import create_engine
from sqlalchemy import text

os.chdir('D:\DeepDoc\Kody\produkcja\ETL')

#zparsuj funkcję przeszukującą bucket i subfoldery oraz generującą tabelkę z danymi jsonowymi 
import ddlib
#pobierz hasła z pliku konfiguracyjnego
import ddconfig

rds_host  = ddconfig.rds_host
db_username = ddconfig.db_username
db_password = ddconfig.db_password
db_name = ddconfig.db_name
engine = create_engine('mysql+pymysql://'+db_username+':'+db_password+'@'+rds_host+'/'+db_name+'?charset=utf8',encoding='utf-8')
   
sql=text('USE deepdoc3; \
\
CREATE TEMPORARY TABLE R1 \
SELECT user_id, photo_id, COUNT(photo_id) as reactioncnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL \
group by photo_id, user_id; \
\
create TEMPORARY TABLE R2 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as likecnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "LIKE" \
group by photo_id; \
\
create TEMPORARY TABLE R3 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as lovecnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "LOVE" \
group by photo_id; \
\
create TEMPORARY TABLE R4 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as angrycnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "ANGRY" \
group by photo_id; \
\
create TEMPORARY TABLE R5 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as sadcnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "SAD" \
group by photo_id; \
\
create TEMPORARY TABLE R6 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as wowcnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type = "WOW" \
group by photo_id; \
\
create TEMPORARY TABLE R7 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as hahacnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "HAHA" \
group by photo_id; \
\
create TEMPORARY TABLE R8 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as pridecnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "PRIDE" \
group by photo_id; \
\
create TEMPORARY TABLE R9 \
SELECT reaction_type, photo_id, COUNT(reaction_type) as thankfullcnt FROM deepdoc3.reactions \
WHERE photo_id IS NOT NULL AND reaction_type LIKE "THANKFULL" \
group by photo_id; \
\
create TEMPORARY TABLE Rcombo \
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
CREATE TEMPORARY TABLE F1 \
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
CREATE TEMPORARY TABLE F2 \
(INDEX photokey (photo_id)) \
SELECT user_id \
,photo_id,COUNT(*) as femalecnt \
FROM deepdoc3.photosAWSFaces \
WHERE Gender = "Female" \
GROUP BY photo_id; \
\
CREATE TEMPORARY TABLE F3 \
(INDEX photokey (photo_id)) \
SELECT user_id \
,photo_id,COUNT(*) as malecnt \
FROM deepdoc3.photosAWSFaces \
WHERE Gender = "Male" \
GROUP BY photo_id; \
\
create TEMPORARY TABLE Fcombo \
(INDEX photokey (photo_id)) \
SELECT F1.*, F2.femalecnt, F3.malecnt \
FROM F1 \
LEFT JOIN F2 \
	ON F1.photo_id= F2.photo_id \
LEFT JOIN F3 \
	ON F1.photo_id= F3.photo_id; \
\
create TEMPORARY TABLE P1 \
(INDEX photokey (photo_id)) \
SELECT user_id, photo_id, from_id, generationdate, created_time, image_name, \
 comments_cnt, likes_cnt, image_type, av_Faces, av_HSV, av_Labels FROM deepdoc3.photos; \
\
create TEMPORARY TABLE P2 \
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
z2.user_id, \
z2.photo_id, \
z2.from_id, \
z2.created_time, \
z2.image_name, \
z2.comments_cnt, \
z2.likes_cnt, \
z2.image_type, \
z2.av_Faces, \
z2.av_HSV, \
z2.av_Labels, \
z2.sam_sobie, \
z2.reactioncnt, \
z2.likecnt, \
z2.lovecnt, \
z2.angrycnt, \
z2.sadcnt, \
z2.wowcnt, \
z2.hahacnt, \
z2.pridecnt, \
z2.thankfullcnt, \
z2.facescnt, \
z2.sum_SAD, \
z2.sum_HAPPY, \
z2.sum_ANGRY, \
z2.sum_confused, \
z2.sum_DISGUSTED, \
z2.sum_SURPRISED, \
z2.sum_CALM, \
z2.sum_UNKNOWN, \
z2.sum_Sunglasses, \
z2.smiletrue, \
z2.smilefalse, \
z2.min_AgeRange_Low, \
z2.max_AgeRange_High, \
z2.agegap, \
z2.avgage, \
z2.femalecnt, \
z2.malecnt, \
z2.hue, \
z2.saturation, \
z2.value, \
z2.red, \
z2.green, \
z2.blue \
FROM DataMartFBPicture_tmp z2 \
LEFT JOIN DataMartFBPicture z1 \
ON z1.photo_id=z2.photo_id \
WHERE z1.photo_id is NULL OR (z1.reactioncnt<>z2.reactioncnt OR z1.facescnt<>z2.facescnt OR (z1.hue is NULL AND z2.hue is not NULL)); ')
engine.execute(sql)
