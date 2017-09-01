# -*- coding: utf-8 -*-
import pandas as pd
import json

'''path = 'C:\\Users\\P\\Desktop\\DDSandbox\\przykładowe dane\\user(4).json'
path = 'C:\\Users\\P\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\wojtek\\user.json'
def openFile(path):
   f = open(path,'r')
    file = json.load(f)
    return file 

user_json = openFile (path)'''


#==============================================================================
# Function takes user json file's data and returns 7 tables:
# user_df, photos_df, location_df, education_df, languages_df, likes_df, work_df
#==============================================================================
def getFBUserFromJSON (user_json):
    try:
        user_id = user_json['id']
    except:
        return None, None,
    
    # create tables
    user_df = pd.DataFrame()
    photos_df = pd.DataFrame()
    location_df = pd.DataFrame()
    education_df = pd.DataFrame()
    languages_df = pd.DataFrame()
    likes_df = pd.DataFrame()
    work_df = pd.DataFrame()
    #0 read user id
    try:
        user_df['user_id'] = [user_json['id']]
    except:
        user_df['user_id'] = [None]

   
    # 1. read age_range
    try:
        user_df['age_range_min'] = [user_json['age_range']['min']]
    except:
        user_df['age_range_min'] = [None]
    try:
        user_df['age_range_max'] = [user_json['age_range']['max']]
    except:
        user_df['age_range_max'] = [None]
    
    # 2. read birthday
    try:
        user_df['birthday'] = [pd.to_datetime(user_json['birthday'])] 
    except:
        user_df['birthday'] = [pd.NaT] 
    
    # 3. read cover photo
    try:
        for coverphoto in user_json['cover']:
            photos_df['user_id'] = [user_id]
            photos_df['image_big_height'] = [None]
            photos_df['image_big_width'] = [None]
            photos_df['image_small_height'] = [None]
            photos_df['image_small_width'] = [None]
            photos_df['image_small_source'] = [None]        
            photos_df['name'] = [None]         
            photos_df['picture'] = [None]      
            photos_df['tags_cnt'] = [None]           
            photos_df['comments_cnt'] = [None]      
            photos_df['likes_cnt'] = [None]    
            photos_df['from_name'] = [None]
            photos_df['from_id'] =  [None]
            photos_df['type'] = ['cover']
        try:
            photos_df['photo_id'] = [user_json['cover']['id']]
        except:
            photos_df['photo_id'] = [None]
        try:
            photos_df['image_big_source'] = [user_json['cover']['source']]
        except:
            photos_df['image_big_source'] = [None]
        photos_df = photos_df.append(photos_df)
    except:
        pass
    
    # 4. read currency
    try:
        user_df['currency'] = [user_json['currency']['user_currency']]
    except:
        user_df['currency'] = [None]
    
    # 5. read devices
    try:
        user_df['devices'] = [str(user_json['devices'])]
    except:
        user_df['devices'] = [None]
        
    # 6. read email
    try:
        user_df['email'] = [user_json['email']]
    except:
        user_df['email'] = [None]
    
    # 7. read gender
    try:
        user_df['gender'] = [user_json['gender']]
    except:
        user_df['gender'] = [None]
    
    # 8. read hometown information
    try:
        for hometown in [user_json['hometown']['location']['city']]:
            hometown_df = pd.DataFrame()
            hometown_df['user_id'] = [user_id]
            hometown_df['category'] = ['hometown']
            hometown_df['post_id'] = [None]
            try:
                hometown_df['city'] = [user_json['hometown']['location']['city']]
            except:
                hometown_df['city'] = [None]
            try:
                hometown_df['country'] = [user_json['hometown']['location']['country']]
            except:
                hometown_df['country'] = [None]
            try:
                hometown_df['latitude'] = [user_json['hometown']['location']['latitude']]
            except:
                hometown_df['latitude'] = [None]
            try:
                hometown_df['longitude'] = user_json['hometown']['location']['longitude']
            except:
                hometown_df['longitude'] = [None]
            location_df = location_df.append(hometown_df)
    except:
        pass
            
    
    # 9. read current location information
    try:
        for current_loc in [user_json['location']['location']['city']]:
            current_location_df = pd.DataFrame()
            current_location_df['user_id'] = [user_id]
            current_location_df['category'] = ['current']
            try:
                current_location_df['city'] = [user_json['location']['location']['city']]
            except:
                current_location_df['city'] = [None]
            try:
                current_location_df['country'] = [user_json['location']['location']['country']]
            except:
                current_location_df['country'] = [None]
            try:
                current_location_df['latitude'] = [user_json['location']['location']['latitude']]
            except:
                current_location_df['latitude'] = [None]
            try:
                current_location_df['longitude'] = [user_json['location']['location']['longitude']]
            except:
                current_location_df['longitude'] = [None]
            location_df = location_df.append(current_location_df)
    except:
        pass

    # 10. read interested in
    try:
        user_df['interested_in'] = [user_json['interested_in']]
    except:
        user_df['interested_in'] = [None]
    
    # 11. read political
    try:
        user_df['political'] = [user_json['political']]
    except:
        user_df['political'] = [None]
    
    # 12. read quotes
    try:
        user_df['quotes'] = [user_json['quotes']]
    except:
        user_df['quotes'] = [None]
    
    # 13. read relationship status
    try:
        user_df['relationship_status'] = [user_json['relationship_status']]
    except:
        user_df['relationship_status'] = [None]
    
    # 14. read significant other id
    try:
        user_df['significant_other_id'] = [user_json['significant_other']['id']]
    except:
        user_df['significant_other_id'] = [None]
    
    # 15. read significant other name
    try:
        user_df['significant_other_name'] = [user_json['significant_other']['name']]
    except:
        user_df['significant_other_name'] = [None]
    
    # 16. read religion
    try:
        user_df['religion'] = [user_json['religion']]
    except:
        user_df['religion'] = [None]

    # 17. read is_verified
    try:
        user_df['is_verified'] = [user_json['is_verified']]
    except:
        user_df['is_verified'] = [None]
    
    # 18. read name
    try:
        user_df['name'] = [user_json['name']]
    except:
        user_df['name'] = [None]
    
    # 19. read name format
    try:
        user_df['name_format'] = [user_json['name_format']]
    except:
        user_df['name_format'] = [None]
    
    # 20. read security settings
    try:
        user_df['secure_browsing'] = [user_json['secure_browsing']['enabled']]
    except:
        try:
            user_df['secure_browsing'] = [user_json['security_settings']['secure_browsing']['enabled']]
        except:
            user_df['secure_browsing'] = [None]
    
    # 21. read test group
    try:
        user_df['test_group'] = [user_json['test_group']]
    except:
        user_df['test_group'] = [None]
        
    # 22. read third party id
    try:
        user_df['third_party_id'] = [user_json['third_party_id']]
    except:
        user_df['third_party_id'] = [None]
        
    # 23. read time zone
    try:
        user_df['time_zone'] = [user_json['time_zone']]
    except:
        try:
           user_df['time_zone'] = [user_json['timezone']]
        except:
            user_df['time_zone'] = [None]
    
    # 24. read updated time
    try:
        user_df['updated_time'] = [pd.to_datetime(user_json['updated_time'])]
    except:
        user_df['updated_time'] = [pd.NaT]
    
    # 25. read verified
    try:
        user_df['verified'] = [user_json['verified']]
    except:
        user_df['verified'] = [None]
    
    
    # 26. read education
    try:
        for education in user_json['education']:
            education_row = pd.DataFrame()
            try:
                education_row['user_id'] = [user_id]
            except:
                education_row['user_id'] = [None]
            try:
                education_row['school_name'] = [education['school']['name']]
            except:
                education_row['school_name'] = [None]
            try:
                education_row['school_type'] = [education['type']]
            except:
                education_row['school_type'] = [None]
            try:
                education_row['degree'] = [education['degree']['name']]
            except:
                education_row['degree'] = [None]
            try:
                education_row['type'] = [education['type']]
            except:
                education_row['type'] = [None]
            try:
                for concentration in education['concentration']:
                    education_row['concentration'] = concentration['name']
            except:
                education_row['concentration'] = [None]
            education_df = education_df.append(education_row)
    except:
        pass
    
    # 27. read languages into language table
    try:
        for language in user_json['languages']:
            language_row = pd.DataFrame()
            try:
                language_row['user_id'] = [user_id]
            except:
                language_row['user_id'] = [None]
            try:
                language_row['language'] = [language['name']]
            except:
                language_row['language'] = [None]
            languages_df = languages_df.append(language_row)
    except:
        pass
    
    # 28. read tagged places into tagged places table
    try:
        for place in user_json['tagged_places']['data']:
            place_row = pd.DataFrame()
            place_row['user_id'] = [user_id]
            place_row['category'] = ['tagged']
            place_row['post_id'] = [None]
            try:
                place_row['city'] = [place['place']['location']['city']]
            except:
                place_row['city'] = [None]
            try:
                place_row['country'] = [place['place']['location']['country']]
            except:
                place_row['country'] = [None]
            try:
                place_row['latitude'] = [place['place']['location']['latitude']]
            except:
                place_row['latitude'] = [None]
            try:
                place_row['longitude'] = [place['place']['location']['longitude']]
            except:
                place_row['longitude'] = [None]
            try:
                place_row['created_time'] = [pd.to_datetime(place['created_time'])]
            except:
                place_row['created_time'] = [pd.NaT]
            location_df = location_df.append(place_row)
    except:
        pass
    
    # 29. read work into work table
    try:
        for work in user_json['work']:
            work_row = pd.DataFrame()
            try:
                work_row['user_id'] = [user_id]
            except:
                work_row['user_id'] = [None]
            try:
                work_row['description'] = work['description']
            except:
                work_row['description'] = [None]
            try:
                work_row['employer_name'] = work['employer']['name']
            except:
                work_row['employer_name'] = [None]
            try:
                work_row['position'] = work['position']['name']
            except:
                work_row['position'] = [None]
            try:
                work_row['location_id'] = work['location']['id']
            except:
                work_row['location_id'] = [None]
            work_df = work_df.append(work_row)
    except:
        pass

    # 30. read favorite athletes into likes table
    try:
        for athlete in user_json['favorite_athletes']:
            athlete_row = pd.DataFrame()
            athlete_row['user_id'] = [user_id]
            try:
                athlete_row['like_id'] = [athlete['id']]
            except:
                pass
            athlete_row['category'] = ['Athlete']
            try:
                athlete_row['name'] = [athlete['name']]
            except:
                athlete_row['name'] = [None]
            athlete_row['about'] = [None]
            athlete_row['favorite'] = [1]
            likes_df = likes_df.append(athlete_row)
    except:
        pass
    
    # 31. read favorite sports teams into likes table
    try:
        for team in user_json['favorite_teams']:
            team_row = pd.DataFrame()
            team_row['user_id'] = [user_id]
            try:
                team_row['like_id'] = [team['id']]
            except:
                pass
            team_row['category'] = ['Sports Team']
            try:
                team_row['name'] = [team['name']]
            except:
                team_row['name'] = [None]
            team_row['about'] = [None]
            team_row['favorite'] = [1]
            likes_df = likes_df.append(team_row)
    except:
        pass
            
    # 32. read inspirational poeple into likes table
    try:
        for person in user_json['inspirational_people']:
            person_row = pd.DataFrame()
            person_row['user_id'] = [user_id]
            try:
                person_row['like_id'] = [person['id']]
            except:
                pass
            person_row['category'] = ['Public Figure']
            try:
                person_row['name'] = [person['name']]
            except:
                person_row['name'] = [None]
            person_row['about'] = [None]
            person_row['favorite'] = [1]
            likes_df = likes_df.append(person_row)
    except:
        pass
    

    
    return user_df, photos_df, location_df, education_df, languages_df, likes_df, work_df


#==============================================================================
# Execution example
#==============================================================================
#def openFile(path):
#    f = open(path, 'r')
#    file = json.load(f)
#    return file
#
#path = 'C:\\Users\\Var\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\wojtek\\user.json'
#path2 = 'C:\\Users\\Var\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\mikołaj\\user.json'
##path3 = 'C:\\Users\\Var\\Dropbox\\DeepDoc\\Materiały\\Przykładowe dane\\paweł\\user.json'
#
#wojtek = openFile(path)
#mikolaj = openFile(path2)
#pawel = openFile(path3)
#
#user_df, photos_df, location_df, education_df, languages_df, likes_df, work_df = getFBUserFromJSON (wojtek)
#user_df2, photos_df2, location_df2, education_df2, languages_df2, likes_df2, work_df2 = getFBUserFromJSON (mikolaj)
#user_df3, photos_df3, location_df3, education_df3, languages_df3, likes_df3, work_df3 = getFBUserFromJSON (pawel)
#user_df, photos_df, location_df, education_df, languages_df, likes_df, work_df = getFBUserFromJSON(user_json)

#################
