# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 16:32:05 2017

@author: Mikołaj
"""
import json
import pandas as pd

'''def openFile(path):
    f=open(path,'r')
    file=json.load(f)
    return file

path = 'C:\\Users\\P\\Desktop\\DDSandbox\\przykładowe dane\\photos(4).json'
photos =openFile(path)'''

def getFBPhotosFromJSON(photos):
    photos_table=pd.DataFrame()
    photo_dict={}
    photo_reaction_df=pd.DataFrame()
    photo_reaction_table=pd.DataFrame()
    try:
        user_id=photos['id']
    except:
        pass
    try:
        try:
            photos_data=photos['photos']['data']
        except:
            photos_data=photos['photos']
    except:
        pass

    try:
        for photo in photos_data:
           
            try:
                photo_dict['user_id']=user_id
            except:
                photo_dict['user_id']=None              
            
            try:
                photo_dict['photo_id']=photo['id']   
            except:
                photo_dict['photo_id']=None
            
            try:
                photo_dict['created_time']=pd.to_datetime(photo['created_time'])     
            except:
                try:
                    photo_dict['created_time']=pd.to_datetime(photo['created_time']['date'])
                except:
                    photo_dict['created_time']=pd.NaT
                          
            try:
                photo_dict['backdated_time']=pd.to_datetime(photo['backdated_time'])   
            except:
                try:
                   photo_dict['backdated_time']=pd.to_datetime(photo['backdated_time']['date'])
                except:
                    photo_dict['backdated_time']=pd.NaT
        
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
                try:
                   photo_dict['tags_cnt']=len(photo['tags'])
                except:
                      photo_dict['tags_cnt']=None                          
            try:              
                photo_dict['comments_cnt']=len(photo['comments']['data'])
            except:
                try:
                   photo_dict['comments_cnt']=len(photo['comments'])            
                except:
                    photo_dict['comments_cnt']=0
                          
            try:              
                photo_dict['likes_cnt']=len(photo['likes']['data'])
            except: 
                try:
                   photo_dict['likes_cnt']=len(photo['likes'])
                except:
                    photo_dict['likes_cnt']=0
                                           
            try:            
                photo_dict['from_name']=photo['from']['name']
                photo_dict['from_id']=photo['from']['id']
            except:
                photo_dict['from']=None                      
            photos_table=photos_table.append(pd.DataFrame([photo_dict],columns=photo_dict.keys()))
#photo reactions             
            try:
                for reaction in photo['reactions']:	
                    photo_reaction_df['user_id'] = [user_id]
                    photo_reaction_df['photo_id']=photo['id']
                    photo_reaction_df['category'] = ['photo_reaction']
                    photo_reaction_df['post_id'] = [None]
                    try:
                        photo_reaction_df['reaction_id'] = [reaction['id']]
                    except:
                        photo_reaction_df['reaction_id'] = [None]
                    try:
                        photo_reaction_df['from_name'] = [reaction['name']]
                    except:
                         photo_reaction_df['from_name'] = [None]
                    try:
                        photo_reaction_df['type'] = [reaction['type']]
                    except:
                         photo_reaction_df['type'] = [None]                    
                    try:
                        photo_reaction_df['created_time']=pd.to_datetime(photo['created_time'])
                    except:
                        try:
                            photo_reaction_df['created_time']=pd.to_datetime(photo['created_time']['date'])
                        except:
                            photo_reaction_df['created_time']=pd.NaT
                    photo_reaction_table = photo_reaction_table.append(photo_reaction_df)
            except:
                pass            
            
            
    except:
        pass
                         

    return photos_table, photo_reaction_table


################ wywołanie ################
#path='C:\\Users\\Mikołaj\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\wojtek\\photos.json'
#
#
#zdjecia=getFBPhotosFromJSON(photos)
#photos_table, photo_reaction_table =getFBPhotosFromJSON(photos)
