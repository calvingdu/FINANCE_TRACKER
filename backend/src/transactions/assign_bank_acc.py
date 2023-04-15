from backend.config.config import config
from backend.src.helper.iterate_datasets import iterate_dataset_csv
from backend.src.transactions.transform.transform_amex_transactions import transform_dataset as transform_amex_dataset
from backend.src.transactions.transform.transform_bmo_cc_transactions import transform_dataset as transform_bmo_cc_dataset
from backend.src.transactions.transform.transform_bmo_checking_transactions import transform_dataset as transform_bmo_checking_dataset
from backend.src.transactions.transform.transform_rbc_checking_transactions import transform_dataset as transform_rbc_checking_dataset
from backend.src.transactions.transform.transform_cibc_checking_transactions import transform_dataset as transform_cibc_checking_dataset
from backend.src.transactions.transform.transform_cibc_cc_transactions import transform_dataset as transform_cibc_cc_dataset
from backend.src.transactions.identification import identify_bmo_cc
from backend.src.transactions.identification import identify_bmo_checking
from backend.src.transactions.identification import identify_rbc_checking
from backend.src.transactions.identification import identify_amex
from backend.src.transactions.identification import identify_cibc_checking
from backend.src.transactions.identification import identify_cibc_cc

directory = config.get('bank_directory')
              
def assign_bank_account(file, directory):
        if identify_bmo_checking(file, directory):
            transform_bmo_checking_dataset(file, directory)
        elif identify_bmo_cc(file, directory):
            transform_bmo_cc_dataset(file, directory)
        elif identify_rbc_checking(file, directory):
            transform_rbc_checking_dataset(file, directory)
        elif identify_amex(file, directory):
            transform_amex_dataset(file, directory)
        elif identify_cibc_checking(file, directory):
            transform_cibc_checking_dataset(file, directory)
        elif identify_cibc_cc(file, directory):
            transform_cibc_cc_dataset(file, directory)

iterate_dataset_csv(directory, '', assign_bank_account)
