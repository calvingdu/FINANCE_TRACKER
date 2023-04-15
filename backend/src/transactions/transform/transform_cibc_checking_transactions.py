import pandas as pd 
from backend.src.helper.iterate_datasets import move_file
from backend.src.helper.iterate_datasets import change_transaction_file_name
from backend.src.helper.schema.accounts_schema import cibc_checking_schema as schema
from backend.src.helper.etl_data import load_dataset
from backend.src.helper.etl_data import common_transform

def transform_dataset(file_name, file_path):
    try: 
        column_names = schema['data_fields'].keys()
        account_name = schema['account_name']

        df = pd.read_csv(file_path+file_name, names=column_names, dtype=schema['data_fields'])
        
        # Removes duplicates
        df.drop_duplicates(inplace=True)

        df = transform_data(df, account_name)\

        load_dataset(file_name, file_path, account_name, df)
        
    except Exception as e: 
        print(e)
        move_file(file_name, file_path, 'unprocessed/')


def transform_data(df, account_name):
    df = common_transform(df, account_name)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['amount'] = df['debit_amount'].fillna(0) + df['credit_amount'].fillna(0)*-1
    df.drop(['debit_amount', 'credit_amount'], axis = 1)
    cols = ['account', 'date', 'amount', 'description']
    df = df[cols]

    return df
