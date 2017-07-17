import pandas as pd
import json

#path = 'C:\\Users\\P\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\paweł\\likes.json'
#path = 'C:\\Users\\P\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\mikołaj\\likes.json'
#path = 'C:\\Users\\P\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\wojtek\\likes.json'
#def openFile(path):
#    f = open(path,'r')
#    file = json.load(f)
#    return file 
#
#likes = openFile (path)

def getFBLikesFromJSON (likes):

    user_id = likes['id']
    like_list = likes['likes']['data']
    like_table=pd.DataFrame()
    like_category_table=pd.DataFrame()
        
    for like in like_list:
        like_dict={}  
        like_dict['user_id']=user_id
        like_dict['like_id']=like['id']
        try:
            like_dict['category']=like['category']
        except:
            like_dict['category']=None
        try:
            like_dict['like_name']=like['name']
        except:
            like_dict['like_name']=None
        try:
            like_dict['like_about']=like['about']
        except:
            like_dict['like_about']=None   
        like_table=like_table.append(pd.DataFrame([like_dict],columns=like_dict.keys())) 
    #druga pętla dla category_list#
        try:
            for category in like['category_list']:
                like_category_dict = {}
                like_category_dict['user_id']=user_id
                like_category_dict['like_id']=like['id']
                like_category_dict['name']= category['name']
                like_category_dict['category_id']= category['id']
                like_category_table=like_category_table.append(pd.DataFrame([like_category_dict],columns=like_category_dict.keys()))
        except:
           pass
    return like_table, like_category_table

#like_table, like_category_table = getFBLikesFromJSON (likes)