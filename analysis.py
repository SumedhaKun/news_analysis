import snowflake.connector
from dotenv import load_dotenv
import os
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


load_dotenv()

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


cur.close()
conn.close()
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