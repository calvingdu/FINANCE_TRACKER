from __future__ import annotations

import csv

import pandas as pd

from src.scripts.helper.etl_data import common_transform


def transform_dataset(file: str, schema: dict) -> pd.DataFrame:
    column_names = schema["data_fields"].keys()
    account_name = schema["account_name"]

    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file)

        rows = [
            row
            for row in csv_reader
            if any(field.strip() for field in row)
            and not any("Following data is valid" in field for field in row)
            and not (
                "First Bank CardTransaction TypeDate Posted Transaction AmountDescription"
                in "".join(row)
            )
        ]

    df = pd.DataFrame(rows, columns=column_names)

    df = transform_data(df, account_name)

    return df


def transform_data(df: pd.DataFrame, account_name: str) -> pd.DataFrame:
    df = common_transform(df, account_name)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    df["amount"] = df["amount"].astype(float)
    df["amount"] = df["amount"] * -1
    df["description"] = df["description"].map(lambda x: x.strip()[4:])
    df.drop(["bank_card", "transaction_type"], axis=1, inplace=True)

    return df
