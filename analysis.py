import snowflake.connector
from dotenv import load_dotenv
import os
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time


load_dotenv()
while True:
    conn = snowflake.connector.connect(
        user='SNOW_USER',
        password=os.getenv("SNOW_PSW"),
        account=os.getenv("SNOW_ACCOUNT"),
        warehouse=os.getenv("SNOW_WH"),
        database="NEWSDB",
        schema="PUBLIC",
        network_timeout=60,
        role='user_role'
    )

    cur = conn.cursor()

    sources=["npr","wpost","cnn","nbc","bbc"]
    sents=["positive","neutral","negative"]
    query2="""
        SELECT COUNT(*) 
        FROM NEWSDB.PUBLIC.NEWS
        WHERE source = %s
        AND sentiment = %s;
    """
    hmap={source:[] for source in sources}
    for source in sources:
        for sent in sents:
            cur.execute(query2, (source, sent))
            result = cur.fetchone()[0]
            hmap[source].append(result)


    
    fig, axs = plt.subplots(2, 3, figsize=(10, 8))
    i=0
    for r in range(2):
        for c in range(2):
            sizes = hmap[sources[i]]
            axs[r,c].pie(sizes, labels=sents,
            autopct='%1.1f%%', shadow=True, startangle=140)
            axs[r,c].set_title(sources[i])
            i+=1
    sizes = hmap[sources[i]]
    axs[r,c+1].pie(sizes, labels=sents,
    autopct='%1.1f%%', shadow=True, startangle=140)
    axs[r,c+1].set_title(sources[i])
        
    plt.tight_layout()
    plt.show()

    positive=[]
    negative=[]
    neutral=[]
    print(hmap)
    i=0
    for k,v in hmap.items():
        positive.append(v[0])
        negative.append(v[2])
        neutral.append(v[1])

    mp={"positive": positive, "neutral":neutral, "negative":negative}
    fig, axs = plt.subplots(1,3, figsize=(10, 8))
    for i in range(3):
        axs[i].pie(mp[sents[i]], labels=hmap.keys(),autopct='%1.1f%%', startangle=140)
        axs[i].set_title(sents[i])
    plt.show()

    categories=["politics","books","culture","technology"]

    query2="""
        SELECT COUNT(*)
        FROM NEWSDB.PUBLIC.NEWS
        WHERE CATEGORY=%s AND SENTIMENT=%s
    """
    hmap={category:[] for category in categories}
    for category in categories:
        for sent in sents:
            cur.execute(query2,(category,sent))
            hmap[category].append(cur.fetchone()[0])
    
    fig, axs = plt.subplots(2, 2, figsize=(10, 8))
    i=0
    for r in range(2):
        for c in range(2):
            sizes = hmap[categories[i]]
            axs[r,c].pie(sizes, labels=sents,
            autopct='%1.1f%%', shadow=True, startangle=140)
            axs[r,c].set_title(categories[i])
            i+=1
    plt.show()

    cur.close()
    conn.close()

    time.sleep(86400)