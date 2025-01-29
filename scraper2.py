import tweepy
import pandas as pd
import tweepy as tw
from datetime import datetime
from datetime import timedelta
import re
import pytz
import logging
#from IPython.display import Image, display

consumer_key="yHnNAB0FmNJ4phHtrSF036xsQ"
consumer_secret="Wzk2AV8sk2YFAglBjS3N5DkTruMlvZODKGZdJik4kmUo9hGgIH"
access_token = "1778022552209694720-VrhQhka5heICC6tki5UcGw8sCOdH39"
access_secret= "uJcqECB9HHbOLFjkbdv4ChxKDSdWfqo01hoAxyfUvaazJ"
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAJUCtQEAAAAACvkOa6sd2vIzB0Iwvt%2B%2FjvVhvEw%3DOh0HfWONK3LDTgG5367dIIIJdyT1S0mlkgwexCHOrYnKE1tNEm'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tw.API(auth, wait_on_rate_limit=True)
client = tweepy.Client(bearer_token=BEARER_TOKEN)


tweet_data_df = pd.DataFrame()

def get_id(username):
    user = client.get_user(username=username)  
    user_id = user.data.id  
    return user_id

def extract_hashtags(text):
    hashtags = re.findall(r'#\w+', text)
    return hashtags if hashtags else [] 

def extract_image_urls(media_keys, media_data):
    image_urls = []
    
    if media_keys and media_data:
        for key in media_keys:
            for media in media_data:
                if media.get("media_key") == key and media.get("type") == "photo":
                    image_urls.append(media.get("url", ""))
    
    return image_urls  

def pick_latest_tweet(user_id):
    username = 'downdetector'
    tweet_data_df = pd.DataFrame() 
    response = client.get_users_tweets(
        id=user_id,
        max_results=5,  
        tweet_fields=["id", "created_at", "text", "attachments"],
        expansions=["attachments.media_keys"],
        media_fields=["media_key", "type", "url"],
        user_fields=["username"]
    )
    
    if response and response.data:
        latest_tweet = None
        media_data = response.includes.get("media", []) if hasattr(response, 'includes') else []
        current_time = datetime.now(pytz.UTC)

        for tweet in response.data:
            time_difference = current_time - tweet.created_at
            if time_difference > timedelta(minutes=1200):
                logging.info("Tweet is older than 8 minutes; skipping.")
                continue

            hashtags = extract_hashtags(tweet.text)
            media_keys = tweet.attachments.get("media_keys") if 'attachments' in tweet else None
            image_urls = extract_image_urls(media_keys, media_data)
            
            utc_created_at = tweet.created_at.astimezone(pytz.UTC)
            
            latest_tweet = {
                "id": tweet.id,
                "username" : username,
                "created_at": utc_created_at.strftime('%Y-%m-%dT%H:%M:%S'),
                "text": tweet.text,
                "hashtags": ', '.join(hashtags) if hashtags else "", 
                "image_urls": ', '.join(image_urls) if image_urls else "",
                "image_content": ', '.join(image_urls) if image_urls else "No Image"
            }

            latest_tweet_df = pd.DataFrame([latest_tweet])
            tweet_data_df = tweet_data_df.apply(lambda col: col.fillna(''))
            tweet_data_df = pd.concat([tweet_data_df, latest_tweet_df], ignore_index=True)

    return tweet_data_df if not tweet_data_df.empty else pd.DataFrame()





# import tweepy
# import pandas as pd
# import tweepy as tw
# from datetime import datetime
# from datetime import timedelta
# import re
# import pytz
# import logging
# import warnings
# #from IPython.display import Image, display
# warnings.simplefilter(action='ignore', category=FutureWarning)

# consumer_key="yHnNAB0FmNJ4phHtrSF036xsQ"
# consumer_secret="Wzk2AV8sk2YFAglBjS3N5DkTruMlvZODKGZdJik4kmUo9hGgIH"
# access_token = "1778022552209694720-VrhQhka5heICC6tki5UcGw8sCOdH39"
# access_secret= "uJcqECB9HHbOLFjkbdv4ChxKDSdWfqo01hoAxyfUvaazJ"
# BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAJUCtQEAAAAACvkOa6sd2vIzB0Iwvt%2B%2FjvVhvEw%3DOh0HfWONK3LDTgG5367dIIIJdyT1S0mlkgwexCHOrYnKE1tNEm'

# auth = tw.OAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(access_token, access_secret)
# api = tw.API(auth, wait_on_rate_limit=True)
# client = tweepy.Client(bearer_token=BEARER_TOKEN)


# tweet_data_df = pd.DataFrame()

# def get_id(username):
#     user = client.get_user(username=username)  
#     user_id = user.data.id  
#     return user_id

# def extract_hashtags(text):
#     hashtags = re.findall(r'#\w+', text)
#     return hashtags if hashtags else [] 

# def extract_image_urls(media_keys, media_data):
#     image_urls = []
    
#     if media_keys and media_data:
#         for key in media_keys:
#             for media in media_data:
#                 if media.get("media_key") == key and media.get("type") == "photo":
#                     image_urls.append(media.get("url", ""))
    
#     return image_urls  
# def pick_latest_tweet(user_id):
#     tweet_data_df = pd.DataFrame() 
#     response = client.get_users_tweets(
#         id=user_id,
#         max_results=5,
#         tweet_fields=["id", "created_at", "text", "attachments"],
#         expansions=["attachments.media_keys"],
#         media_fields=["media_key", "type", "url"],
#         user_fields=["username"]
#     )
    
#     if response and response.data:
#         media_data = response.includes.get("media", []) if hasattr(response, 'includes') else []
#         current_time = datetime.now(pytz.UTC)
        
#         # Get the latest tweet (first in the response)
#         tweet = response.data[0]
        
#         hashtags = extract_hashtags(tweet.text)
#         media_keys = tweet.attachments.get("media_keys") if 'attachments' in tweet else None
#         image_urls = extract_image_urls(media_keys, media_data)
        
#         latest_tweet = {
#             "id": tweet.id,
#             "created_at": tweet.created_at,
#             "text": tweet.text,
#             "hashtags": ', '.join(hashtags) if hashtags else "", 
#             "image_urls": ', '.join(image_urls) if image_urls else "",
#             "image_content": ', '.join(image_urls) if image_urls else "No Image"
#         }

#         latest_tweet_df = pd.DataFrame([latest_tweet])
#         tweet_data_df = pd.concat([tweet_data_df, latest_tweet_df], ignore_index=True)

#         tweet_data_df = tweet_data_df.apply(lambda col: col.fillna(''))
    
#     return tweet_data_df if not tweet_data_df.empty else pd.DataFrame()

# username = 'downdetector'  
# user_id = get_id(username)

# latest_tweet_df = pick_latest_tweet(user_id)

# print(latest_tweet_df)