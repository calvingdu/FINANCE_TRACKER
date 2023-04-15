import os 
from pathlib import Path

# Looks through dataset to transform data 
def iterate_dataset(file_path, file_name, transform_dataset):
    if os.path.exists(file_path + 'unprocessed/'): 
        unprocessed_directory = file_path + 'unprocessed/'
        for file in os.listdir(unprocessed_directory): 
            if file.startswith(file_name):
                transform_dataset(file, unprocessed_directory)
    #iterates on the entire folder 
    for file in os.listdir(file_path): 
        if file.startswith(file_name):
            transform_dataset(file, file_path)

# Looks through dataset to transform data 
def iterate_dataset_csv(file_path, file_name, transform_dataset):
    if os.path.exists(file_path + 'unprocessed/'): 
        unprocessed_directory = file_path + 'unprocessed/'
        for file in os.listdir(unprocessed_directory): 
            if file.startswith(file_name) and file.endswith('.csv'):
                transform_dataset(file, unprocessed_directory)
    #iterates on the entire folder 
    for file in os.listdir(file_path): 
        if file.startswith(file_name) and file.endswith('.csv'):
            transform_dataset(file, file_path)

def move_file(file_name, from_path, directory_suffix):
    unprocessed_suffix = 'unprocessed/'
    if(from_path.endswith(unprocessed_suffix)): 
        directory = from_path.replace(unprocessed_suffix,'')
        Path(directory + directory_suffix).mkdir(parents=True,exist_ok=True)
        os.rename(from_path + file_name, directory + directory_suffix + file_name)
    else:
        Path(from_path + directory_suffix).mkdir(parents=True,exist_ok=True)
        os.rename(from_path + file_name, from_path + directory_suffix + file_name)

# changes name 
def change_transaction_file_name(directory, old_name, bank_acount, df):
    earliest_date = df['date'].min().strftime("%Y%m%d")
    latest_date = df['date'].max().strftime("%Y%m%d")
    file_type = '.'+old_name.split('.')[-1]
    bank_acount = bank_acount.upper()
    new_name = bank_acount + '_' + earliest_date + '_' + latest_date + file_type
    os.rename(directory + old_name, directory+new_name)
    return new_name 
    
    
