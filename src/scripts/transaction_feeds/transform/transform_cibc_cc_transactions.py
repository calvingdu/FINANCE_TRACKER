from __future__ import annotations

import pandas as pd

from src.scripts.helper.etl_data import common_transform


def transform_dataset(file_name, file_path, schema):
    column_names = schema["data_fields"].keys()
    account_name = schema["account_name"]

    df = pd.read_csv(
        file_path + file_name,
        names=column_names,
        dtype=schema["data_fields"],
    )

    # Removes duplicates
    df.drop_duplicates(inplace=True)

    df = transform_data(df, account_name)

    return df


def transform_data(df, account_name):
    df = common_transform(df, account_name)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df.drop("card_number", axis=1, inplace=True)
    df["amount"] = df["debit_amount"].fillna(0) + df["credit_amount"].fillna(0) * -1
    df.drop(["debit_amount", "credit_amount"], axis=1, inplace=True)
    cols = ["account", "date", "amount", "description"]
    df = df[cols]

    return df
