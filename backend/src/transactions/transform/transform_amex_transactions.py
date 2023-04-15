import pandas as pd 
from backend.src.helper.schema.accounts_schema import amex_schema as schema
from backend.src.helper.etl_data import load_dataset
from backend.src.helper.etl_data import common_transform
from backend.src.helper.iterate_datasets import move_file

def transform_dataset(file_name, file_path):
    try: 
        column_names = schema['data_fields'].keys()
        account_name = schema['account_name']

        df = pd.read_csv(file_path+file_name, names=column_names, dtype=schema['data_fields'])
        
        # Removes duplicates and drops NA 
        df.dropna(axis = 1, inplace=True)
        df.drop_duplicates(inplace=True)

        df = transform_data(df, account_name)

        load_dataset(file_name, file_path, account_name, df)
        
        target_directory = 'processed/'+account_name+'/'
        #move_file(file_name, file_path, target_directory)
    except Exception as e: 
        print(e)
        move_file(file_name, file_path, 'unprocessed/')


def transform_data(df, account_name):
    common_transform(df, account_name)
    df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
    df.drop('reference', axis = 1, inplace=True)
    df['amount'] = df['amount'].astype(float)

    return df
