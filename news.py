import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import news_constant

SERVER = news_constant.SERVER
DATABASE = news_constant.DATABASE
DRIVER = news_constant.DRIVER
USERNAME = news_constant.USERNAME
PASSWORD = news_constant.PASSWORD
DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'

engine = create_engine(DATABASE_CONNECTION)
connection = engine.connect()

url = 'https://newsapi.org/v2/top-headlines?country=us&category=business&apiKey=a668e44615054002b9a07251b60aa8de'

r = requests.get(url)

data = json.loads(r.text)

art_list = []

for i in data['articles']:
    
    name = i['source']['name']
    author = i['author']
    title = i['title']
    description = i['description']
    url = i['url']
    publishedAt = i['publishedAt']
    
    list = {
        'name' : name,
        'author' : author,
        'title' : title,
        'description' : description,
        'url' : url,
        'publishedAt' : publishedAt
    }

    art_list.append(list)

new = pd.DataFrame.from_dict(art_list)

#print(new['publishedAt'].dtypes)

load_df = pd.DataFrame(new)
load_df.to_sql("news", connection, if_exists='append',index = False, chunksize = 200)



