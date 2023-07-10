from __future__ import annotations

import csv

import pandas as pd

from src.scripts.helper.etl_data import common_transform


def transform_dataset(file_name, file_path, schema):
    column_names = schema["data_fields"].keys()
    account_name = schema["account_name"]

    with open(file_path + file_name) as csv_file:
        csv_reader = csv.reader(csv_file)

        rows = [
            row
            for row in csv_reader
            if any(field.strip() for field in row)
            and not (
                "Account TypeAccount NumberTransaction DateCheque NumberDescription 1Description 2CAD$USD$"
                in "".join(row)
            )
        ]

    df = pd.DataFrame(rows, columns=column_names)

    df = transform_data(df, account_name)

    return df


def transform_data(df, account_name):
    df = common_transform(df, account_name)
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y")
    df["amount"] = df["amount"].astype(float)
    df.drop(
        [
            "account_type",
            "account_number",
            "cheque_number",
            "description2",
            "cad$",
            "usd$",
        ],
        axis=1,
        inplace=True,
    )
    cols = ["account", "date", "amount", "description"]
    df = df[cols]

    return df
