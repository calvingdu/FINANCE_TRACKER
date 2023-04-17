import pandas as pd 
from backend.src.helper.iterate_datasets import move_file
from backend.src.helper.iterate_datasets import change_transaction_file_name
from backend.src.mongoDB.mongodb_hook import insert_df
from backend.src.mongoDB.mongodb_hook import delete_df

def load_dataset(file_name, file_path, account_name, df):
        file_name = change_transaction_file_name(file_path, file_name, account_name, df)
        now_date = pd.Timestamp.now() 
        df['processed_date'] = now_date 
        
        insert_df(df)
        target_directory = 'processed/'
        move_file(file_name, file_path, target_directory)

def common_transform(df, account_name):
        df.columns = df.columns.map(lambda x: x.lower())
        df.insert(0,"account",account_name)
        
        return df

