from dotenv import load_dotenv
import os
load_dotenv()
import snowflake.connector
conn = snowflake.connector.connect(
        user='SNOW_USER',
        password=os.getenv("SNOW_PSW"),
        account='bz36625.west-us-2.azure',
        warehouse='SNOW_WH',
        database='NEWSDB',
        schema='PUBLIC',
        role='user_role'
    )

def deduplicate():
    cur = conn.cursor()
    query="""
        DELETE FROM NEWSDB.PUBLIC.NEWS
        WHERE ID NOT IN (
            SELECT MAX(ID) AS MaxID
            FROM NEWSDB.PUBLIC.NEWS
            GROUP BY article
        );
    """
    cur.execute(query)
    

    query2="""
        insert overwrite into NEWSDB.PUBLIC.NEWS
        select distinct * from NEWSDB.PUBLIC.NEWS
     """
    cur.execute(query2)
    print("deduplicated.")
    