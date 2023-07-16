from __future__ import annotations

import csv

import pandas as pd

from src.scripts.transaction_feeds.accounts_config import accounts_config
from src.scripts.transaction_feeds.transform.transform_amex_transactions import (
    transform_dataset as transform_amex_dataset,
)
from src.scripts.transaction_feeds.transform.transform_bmo_cc_transactions import (
    transform_dataset as transform_bmo_cc_dataset,
)
from src.scripts.transaction_feeds.transform.transform_bmo_checking_transactions import (
    transform_dataset as transform_bmo_checking_dataset,
)
from src.scripts.transaction_feeds.transform.transform_cibc_cc_transactions import (
    transform_dataset as transform_cibc_cc_dataset,
)
from src.scripts.transaction_feeds.transform.transform_cibc_checking_transactions import (
    transform_dataset as transform_cibc_checking_dataset,
)
from src.scripts.transaction_feeds.transform.transform_rbc_direct_investing_transactions import (
    transform_dataset as transform_rbc_direct_investing_dataset,
)
from src.scripts.transaction_feeds.transform.transform_rbc_transactions import (
    transform_dataset as transform_rbc_dataset,
)


def identify_accounts(directory, file):
    if identify_bmo_checking(directory, file):
        config = accounts_config["bmo_checking"]
        transform_func = transform_bmo_checking_dataset
    elif identify_bmo_cc(directory, file):
        config = accounts_config["bmo_cc"]
        transform_func = transform_bmo_cc_dataset
    elif identify_rbc(directory, file):
        config = accounts_config["rbc"]
        transform_func = transform_rbc_dataset
    elif identify_rbc_direct_investing(directory, file):
        config = accounts_config["rbc_direct_investing"]
        transform_func = transform_rbc_direct_investing_dataset
    elif identify_amex(directory, file):
        config = accounts_config["amex"]
        transform_func = transform_amex_dataset
    elif identify_cibc_checking(directory, file):
        config = accounts_config["cibc_checking"]
        transform_func = transform_cibc_checking_dataset
    elif identify_cibc_cc(directory, file):
        config = accounts_config["cibc_cc"]
        transform_func = transform_cibc_cc_dataset

    return config, transform_func


def identify_bmo_cc(directory, file):
    with open(directory + file) as csv_file:
        csv_reader = csv.reader(csv_file)
        if any("Following data is valid" in "".join(row) for row in csv_reader) and any(
            "Item #Card #Transaction DatePosting DateTransaction AmountDescription"
            in "".join(row)
            for row in csv_reader
        ):
            return True


def identify_bmo_checking(directory, file):
    with open(directory + file) as csv_file:
        csv_reader = csv.reader(csv_file)
        if any("Following data is valid" in "".join(row) for row in csv_reader) and any(
            "First Bank CardTransaction TypeDate Posted Transaction AmountDescription"
            in "".join(row)
            for row in csv_reader
        ):
            return True


def identify_rbc(directory, file):
    with open(directory + file) as csv_file:
        csv_reader = csv.reader(csv_file)
        if any(
            "Account TypeAccount NumberTransaction DateCheque NumberDescription 1Description 2CAD$USD$"
            in "".join(row)
            for row in csv_reader
        ):
            return True


def identify_rbc_direct_investing(directory, file):
    with open(directory + file) as csv_file:
        csv_reader = csv.reader(csv_file)
        if any("Activity Export as of " in "".join(row) for row in csv_reader):
            return True


def identify_amex(directory, file):
    df = pd.read_csv(directory + file)
    if len(df.columns) == 6 and df.iloc[:, 2].dtype == "float64":
        return True


def identify_cibc_checking(directory, file):
    df = pd.read_csv(directory + file)
    if (
        len(df.columns) == 4
        and df.iloc[:, 2].dtype == "float64"
        and df.iloc[:, 3].dtype == "float64"
        and df.iloc[:, 1].dtype == "object"
    ):
        return True


def identify_cibc_cc(directory, file):
    df = pd.read_csv(directory + file)
    if (
        len(df.columns) == 5
        and df.iloc[:, 2].dtype == "float64"
        and df.iloc[:, 3].dtype == "float64"
        and df.iloc[:, 1].dtype == "object"
    ):
        return True
