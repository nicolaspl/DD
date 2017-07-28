import pandas as pd
import json

path = 'C:\\Users\\P\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\paweł\\posts.json'
#path = 'C:\\Users\\P\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\mikołaj\\posts.json'
path = 'C:\\Users\\P\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\wojtek\\posts.json'
path = 'C:\\Users\\P\\Desktop\\DDSandbox\\przykładowe dane\\posts_2.json'
def openFile(path):
    f = open(path,'r')
    file = json.load(f)
    return file 
#
posts = openFile (path)

def getFBPostsFromJSON (posts):
    post_table=pd.DataFrame()
    location_table = pd.DataFrame()
    reaction_table = pd.DataFrame()
    post_dict={} 
    post_location_df = pd.DataFrame()
    reaction_df = pd.DataFrame()
#post 
    try:
        user_id = posts['id']
    except:
        user_id = None
    try:
        try:
            post_list = posts['posts']['data']
        except:
            post_list = posts['posts']
    except:
        pass
    try:      
        for post in post_list:
             
            post_dict['user_id']=user_id
            try:
                post_dict['post_id']=post['id']
            except:
                post_dict['post_id']=None
            
            try:
                post_dict['created_time']=pd.to_datetime(post['created_time'])
            except:
                try:
                    post_dict['created_time']=pd.to_datetime(post['created_time']['date'])
                except:
                    pass
            try:
                post_dict['full_picture']=post['full_picture']
            except:
                post_dict['full_picture']=None
            try:
                post_dict['message']=post['message']
            except:
                post_dict['message']=None   
            try:
                post_dict['picture']=post['picture']
            except:
                post_dict['picture']=None  
            try:
                post_dict['status_type']=post['status_type']
            except:
                post_dict['status_type']=None              
            try:
                post_dict['story']=post['story']
            except:
                post_dict['story']=None 
            try:
                post_dict['description']=post['description']
            except:
                post_dict['description']=None 
            try:
                post_dict['source']=post['source']
            except:
                post_dict['source']=None              
            try:
                post_dict['privacy_value']=post['privacy']['value']
            except:
                post_dict['privacy_value']=None     
            try:
                post_dict['from_id']=post['from']['id']
            except:
                post_dict['from_id']=None  
            try:              
                post_dict['comments_cnt']=len(post['comments']['data'])
            except:    
                try:
                    post_dict['comments_cnt']=len(post['comments'])
                except:
                    post_dict['comments_cnt']=0            
            try:              
                post_dict['likes_cnt']=len(post['likes']['data'])
            except:    
                try:
                    post_dict['likes_cnt']=len(post['likes'])
                except:
                    post_dict['likes_cnt']=0   
            try:              
                post_dict['with_tags_cnt']=len(post['with_tags']['data'])
            except:    
                try:
                   post_dict['with_tags_cnt']=len(post['with_tags']) 
                except:
                    post_dict['with_tags_cnt']=0
            try:              
                post_dict['reactions_cnt']=len(post['reactions']['data'])
            except:    
                try:
                   post_dict['reactions_cnt']=len(post['reactions']) 
                except:
                    post_dict['reactions_cnt']=0                  
            post_table=post_table.append(pd.DataFrame([post_dict],columns=post_dict.keys())) 
#post location             
            try:
                for location in [post['place']]:	
                    post_location_df['user_id'] = [user_id]
                    post_location_df['category'] = ['post_location']
                    post_location_df['post_id'] = post['id']
                    try:
                        post_location_df['city'] = [location['location']['city']]
                    except:
                        post_location_df['city'] = [None]
                    try:
                        post_location_df['country'] = [location['location']['country']]
                    except:
                        post_location_df['country'] = [None]
                    try:
                        post_location_df['latitude'] = [location['location']['latitude']]
                    except:
                        post_location_df['latitude'] = [None]
                    try:
                        post_location_df['longitude'] = [location['location']['longitude']]
                    except:
                        post_location_df['longitude'] = [None]
                    try:
                        post_location_df['created_time']=pd.to_datetime(post['created_time'])
                    except:
                        try:
                            post_location_df['created_time']=pd.to_datetime(post['created_time']['date'])
                        except:
                            pass
                    location_table = location_table.append(post_location_df)
            except:
                pass
#post reactions             
            try:
                for reaction in post['reactions']:	
                    reaction_df['user_id'] = [user_id]
                    reaction_df['post_id']=post['id']
                    reaction_df['category'] = ['post_reaction']
                    reaction_df['photo_id'] = [None]
                    try:
                        reaction_df['reaction_id'] = [reaction['id']]
                    except:
                        reaction_df['reaction_id'] = [None]
                    try:
                        reaction_df['from_name'] = [reaction['name']]
                    except:
                         reaction_df['from_name'] = [None]
                    try:
                        reaction_df['type'] = [reaction['type']]
                    except:
                         reaction_df['type'] = [None]                    
                    try:
                        reaction_df['created_time']=pd.to_datetime(post['created_time'])
                    except:
                        try:
                            reaction_df['created_time']=pd.to_datetime(post['created_time']['date'])
                        except:
                            pass
                    reaction_table = reaction_table.append(reaction_df)
            except:
                pass
    except:
        pass 
			
    
    return post_table, location_table, reaction_table

post_table,location_table,reaction_table = getFBPostsFromJSON (posts)

