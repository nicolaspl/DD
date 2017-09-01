# -*- coding: utf-8 -*-
"""
Created on Tue Aug 22 23:14:07 2017

@author: Praca
"""


# ''' przekształca dane dot. jednej twarzy do postaci tabelarycznej '''
def OneFaceAnalysis(FaceDetails):
    
    #demograficzne
    AgeRange_Low=FaceDetails['AgeRange']['Low']
    AgeRange_High=FaceDetails['AgeRange']['High']
    Gender=FaceDetails['Gender']['Value']
    
    #emocje
    emotions={'HAPPY':0,'SAD':0,'ANGRY':0,'CONFUSED':0,'DISGUSTED':0,'SURPRISED':0,'CALM':0,'UNKNOWN':0}
    for i in range(len(FaceDetails['Emotions'])):
        
        emotions[FaceDetails['Emotions'][i]['Type']]=FaceDetails['Emotions'][i]['Confidence']
        #emo_labels.append(FaceDetails['Emotions'][i]['Type'])
        #exec(FaceDetails['Emotions'][i]['Type']+'='+str(FaceDetails['Emotions'][i]['Confidence']))
        
    if FaceDetails['Smile']['Value']==False:
        SmileFalse=FaceDetails['Smile']['Confidence']
    else:
        SmileFalse=0
    
    if FaceDetails['Smile']['Value']==True:
        SmileTrue=FaceDetails['Smile']['Confidence']
    else:
        SmileTrue=0
    
    #ogolne
    Sunglasses=FaceDetails['Sunglasses']['Value']
    Pose_Roll=FaceDetails['Pose']['Roll']
    Pose_Yaw=FaceDetails['Pose']['Yaw']
    Pose_Pitch=FaceDetails['Pose']['Pitch']
    Quality_Brightness=FaceDetails['Quality']['Brightness']
    Quality_Sharpness=FaceDetails['Quality']['Sharpness']
    
    OneFaceData=[AgeRange_Low,AgeRange_High,Gender
               ,emotions['HAPPY']
               ,emotions['SAD']
               ,emotions['ANGRY']
               ,emotions['CONFUSED']
               ,emotions['DISGUSTED']
               ,emotions['SURPRISED']
               ,emotions['CALM']
               ,emotions['UNKNOWN']
               ,SmileFalse
               ,SmileTrue
               ,Sunglasses
               ,Pose_Roll
               ,Pose_Yaw
               ,Pose_Pitch
               ,Quality_Brightness
               ,Quality_Sharpness]
    return OneFaceData