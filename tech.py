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

url = 'https://newsapi.org/v2/top-headlines?sources=techcrunch&apiKey=a668e44615054002b9a07251b60aa8de'

r = requests.get(url)

data = json.loads(r.text)
#This is a test Git
#print(json.dumps(data, indent= 2, sort_keys= True))

Tech_list = []

for i in data['articles']:

    author = i['author']
    title = i['title']
    description = i['description']
    content = i['content']
    url = i['url']
    publishedAt = i['publishedAt']

    tech = {
       'author' : author,
       'title' : title,
       'description' : description,
       'content' : content,
       'url' : url,
       'publishedAt' : publishedAt
     }

    Tech_list.append(tech)

#print(json.dumps(Tech_list, indent= 2))

new = pd.DataFrame.from_dict(Tech_list)

new.to_sql("tech",connection,if_exists='append',index=False)
