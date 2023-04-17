from pymongo import MongoClient
import pandas as pd

client = MongoClient()

db = client.test_database
testing = db.testing

def insert_df(df): 
    df_dict = df.to_dict('records')
    testing.insert_many(df_dict)

def delete_df():
    testing.delete_many({})

def extract_df():
    cursor = db['testing'].find({})
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    del df['_id']
    return df

