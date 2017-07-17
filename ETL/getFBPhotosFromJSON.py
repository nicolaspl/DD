# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 16:32:05 2017

@author: Mikołaj
"""
import json
import pandas as pd

def openFile(path):
    f=open(path,'r')
    file=json.load(f)
    return file

def getFBPhotosFromJSON(photos):

    user_id=photos['id']

    photos_data=photos['photos']['data']
    result=pd.DataFrame()
    
    for photo in photos_data:
    
        photo_dict={}
        
        try:
            photo_dict['user_id']=user_id
        except:
            photo_dict['user_id']=None              
        
        try:
            photo_dict['photo_id']=photo['id']   
        except:
            photo_dict['photo_id']=None
        
        try:
            photo_dict['created_time']=photo['created_time']   
        except:
            photo_dict['created_time']=None
                      
        try:
            photo_dict['backdated_time']=photo['backdated_time']   
        except:    
            photo_dict['backdated_time']=None
    
        try:
            photo_dict['image_big_height']=photo['images'][0]['height']
            photo_dict['image_big_width']=photo['images'][0]['width']
            photo_dict['image_big_source']=photo['images'][0]['source']
            
            n=len(photo['images'])-1
            
            photo_dict['image_small_height']=photo['images'][n]['height']
            photo_dict['image_small_width']=photo['images'][n]['width']
            photo_dict['image_small_source']=photo['images'][n]['source']
        except:
            print('no')
            
        try:            
            photo_dict['name']=photo['name']
        except:
            photo_dict['name']=None
                      
        try:              
            photo_dict['picture']=photo['picture']
        except:
            photo_dict['picture']=None
                      
        try:              
            photo_dict['tags_cnt']=len(photo['tags']['data'])
        except:
            photo_dict['tags_cnt']=None
                      
        try:              
            photo_dict['comments_cnt']=len(photo['comments']['data'])
        except:
            photo_dict['comments_cnt']=0
                      
        try:              
            photo_dict['likes_cnt']=len(photo['likes']['data'])
        except:    
            photo_dict['likes_cnt']=0
                                       
        try:            
            photo_dict['from_name']=photo['from']['name']
            photo_dict['from_id']=photo['from']['id']
        except:
            photo_dict['from']=None
                         
        result=result.append(pd.DataFrame([photo_dict],columns=photo_dict.keys()))
    return result


################ wywołanie ################
#path='C:\\Users\\Mikołaj\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\wojtek\\photos.json'
#photos =openFile(path)
#
#zdjecia=getFBPhotosFromJSON(photos)

