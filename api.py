import os
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
            obj={"source":"The Guardian", "category":section, "article":article, "content":content}
            kafka.send_data(obj,"guardian")
            dict_list.append(obj)
    except:
        pass

# NYT_URL="https://api.nytimes.com/svc/topstories/v2/"
# NYT_API=os.getenv("NYT_API")
# sections=["politics","technology","opinion","insider","books/review"]
# params={
#     'api-key':NYT_API,
#     'sort':'newest'
# }
# for section in sections:
#     res=requests.get(NYT_URL+section+".json",params=params)
#     try:
#         res=res.json()
#         res=res["response"]["docs"]
#         for i in range(len(res)):
#             article=res[i]["headline"]["main"]
#             content=res[i]["abstract"]+res[i]["lead_paragraph"]
#             obj={"source":"NYT", "category":section, "article":article, "content":content}
#             kafka.send_data(obj,"nyt")
#             dict_list.append(obj)
#     except:
#         pass
#     time.sleep(12)

NEWS_URL="https://newsapi.org/v2/everything?q=politics&domains=npr.org&apiKey="
NEWS_API=os.getenv("NEWS_API")

res=requests.get(NEWS_URL+NEWS_API)
res=res.json()
res=res["articles"]
for i in range(len(res)):
    article=res[i]["title"]
    content=res[i]["content"]
    obj={"source":"npr", "category":"politics", "article":article, "content":content}
    kafka.send_data(obj,"npr")


