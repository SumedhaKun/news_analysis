import os
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv

import time
import kafka
load_dotenv()
GUARDIAN_URL="https://content.guardianapis.com/search?show-fields=body&section="
GUARDIAN_API=os.getenv("GUARDIAN_API")

sections=["football","culture","politics","technology","books"]
dict_list=[]
for section in sections:
    res=requests.get(GUARDIAN_URL+section+"&api-key="+GUARDIAN_API)
    try:
        res=res.json()
        res=res["response"]["results"]
        for i in range(len(res)):
            article=res[i]["webTitle"]
            content=res[i]["fields"]["body"]
            soup = BeautifulSoup(content, "html.parser")
            text = soup.get_text()
            obj={"source":"The Guardian", "category":section, "article":article, "content":text}
            kafka.send_data(obj,"guardian")
            dict_list.append(obj)
    except:
        pass

categories=["politics","sports", "technology", "culture", "books"]
sites={"npr.org":"npr", "nytimes.com":"nyt", "washingtonpost.com":"wpost","bbc.com":"bbc","cnn.com":"cnn", "nbcnews.com":"nbc"}
for category in categories:
    for key,value in sites.items():
        NEWS_URL="https://newsapi.org/v2/everything?q="+category+"&domains="+key+"&apiKey="
        NEWS_API=os.getenv("NEWS_API")
        res=requests.get(NEWS_URL+NEWS_API)
        res=res.json()
        res=res["articles"]
        for i in range(len(res)):
            article=res[i]["title"]
            content=res[i]["content"]
            obj={"source":value, "category":category, "article":article, "content":content}
            kafka.send_data(obj,value) 


