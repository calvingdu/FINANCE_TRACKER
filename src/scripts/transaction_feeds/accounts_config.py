from __future__ import annotations

accounts_config = {
    "amex": {
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
    "bmo_cc": {
        "account_name": "BMO_MASTERCARD",
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
    "bmo_checking": {
        "account_name": "BMO_CHECKING",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "bank_card": "string",
            "transaction_type": "string",
            "date": "string",
            "amount": "float64",
            "description": "string",
        },
    },
    "rbc": {
        "account_name": "RBC",
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
    "rbc_direct_investing": {
        "account_name": "RBC_DIRECT_INVESTING",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "Date": "string",
            "Activity": "string",
            "Symbol": "string",
            "Symbol Description": "string",
            "Quantity": "float64",
            "Price": "float64",
            "Settlement Date": "string",
            "Account": "string",
            "Value": "float64",
            "Currency": "string",
            "Description": "string",
        },
    },
    "cibc_checking": {
        "account_name": "CIBC_CHECKING",
        "expectation_suite_name": "transaction_suite",
        "data_fields": {
            "date": "string",
            "description": "string",
            "debit_amount": "float64",
            "credit_amount": "float64",
        },
    },
    "cibc_cc": {
        "account_name": "CIBC_CC",
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
