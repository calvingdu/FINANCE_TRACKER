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
            and not (
                "Account TypeAccount NumberTransaction DateCheque NumberDescription 1Description 2CAD$USD$"
                in "".join(row)
            )
        ]

    df = pd.DataFrame(rows, columns=column_names)

    df = transform_data(df, account_name)

    return df


def transform_data(df: pd.DataFrame, account_name: str) -> pd.DataFrame:
    df = common_transform(df, account_name)
    df["account"] = df["account"] + "_" + df["account_type"]
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y")
    df["amount"] = df["amount"].astype(float)
    df["amount"] = df["amount"] * -1
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
