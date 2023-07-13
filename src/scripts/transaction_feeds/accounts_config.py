from __future__ import annotations

accounts_config = {
    "amex_config": {
        "account_name": "AMEX",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "date": "string",
            "reference": "string",
            "amount": "float64",
            "description": "string",
            "na1": "string",
            "na2": "float64",
        },
    },
    "bmo_cc_config": {
        "account_name": "BMOcc",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "index": "int64",
            "bank_card": "float64",
            "date": "string",
            "posting_date": "string",
            "amount": "float64",
            "description": "string",
        },
    },
    "bmo_checking_config": {
        "account_name": "BMOchecking",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "bank_card": "string",
            "transaction_type": "string",
            "date": "string",
            "amount": "float64",
            "description": "string",
        },
    },
    "rbc_checking_config": {
        "account_name": "RBCchecking",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "account_type": "string",
            "account_number": "float64",
            "date": "string",
            "cheque_number": "float64",
            "description": "string",
            "description2": "string",
            "amount": "float64",
            "CAD$": "float64",
            "USD$": "float64",
        },
    },
    " cibc_checking_config": {
        "account_name": "CIBCchecking",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "date": "string",
            "description": "string",
            "debit_amount": "float64",
            "credit_amount": "float64",
        },
    },
    "cibc_cc_config": {
        "account_name": "CIBCcc",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "date": "string",
            "description": "string",
            "debit_amount": "float64",
            "credit_amount": "float64",
            "card_number": "string",
        },
    },
}
