amex_schema = {
        'account_name':'AMEX', 
        'data_fields':{
            'date':'string', 
            'reference':'string', 
            'amount':'float64', 
            'description':'string', 
            'na1':'string', 
            'na2':'float64' 
            }
    }

bmo_cc_schema = {
        'account_name':'BMOcc', 
        'data_fields': {
            'index':'int64',
            'bank_card': 'float64',
            'date': 'string',
            'posting_date': 'string',
            'amount':'float64',
            'description':'string'
        }
    }

bmo_checking_schema = {
        'account_name':'BMOchecking', 
        'data_fields':{
            'bank_card': 'string',
            'transaction_type': 'string',
            'date': 'string',
            'amount': 'float64',
            'description':'string'
        }
    }

rbc_checking_schema = {
        'account_name':'RBCchecking', 
        'data_fields':{
            'account_type': 'string',
            'account_number': 'float64',
            'date': 'string',
            'cheque_number':'float64',
            'description': 'string',
            'description2':'string',
            'amount':'float64',
            'CAD$':'float64',
            'USD$':'float64',
        }
}

cibc_checking_schema = {
        'account_name':'CIBCchecking', 
        'data_fields':{
            'date':'string',
            'description':'string',
            'debit_amount':'float64',
            'credit_amount':'float64',
        }
}

cibc_cc_schema = {
        'account_name':'CIBCcc', 
        'data_fields':{
            'date':'string',
            'description':'string',
            'debit_amount':'float64',
            'credit_amount':'float64',
            'card_number':'string'
        }
}
