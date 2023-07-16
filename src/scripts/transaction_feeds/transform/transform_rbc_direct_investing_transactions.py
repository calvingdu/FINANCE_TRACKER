from __future__ import annotations

import csv

import pandas as pd

from src.scripts.helper.etl_data import common_transform


def transform_dataset(file_name: str, file_path: str, schema: dict) -> pd.DataFrame:
    column_names = schema["data_fields"].keys()
    account_name = schema["account_name"]

    with open(file_path + file_name) as csv_file:
        csv_reader = csv.reader(csv_file)

        rows: list = [row for row in csv_reader if any(field.strip() for field in row)]

    # Filter rows based on start and end messages
    start_row = [
        "Date",
        "Activity",
        "Symbol",
        "Symbol Description",
        "Quantity",
        "Price",
        "Settlement Date",
        "Account",
        "Value",
        "Currency",
        "Description",
    ]
    end_row = ["Disclaimers"]

    selected_rows = []
    select_rows = False
    for row in rows:
        if end_row == row:
            select_rows = False

        if select_rows:
            selected_rows.append(row)

        if start_row == row:
            select_rows = True

    df = pd.DataFrame(selected_rows, columns=column_names)

    df = transform_data(df, account_name)

    return df


def transform_data(df: pd.DataFrame, account_name: str) -> pd.DataFrame:
    df.drop(["Account", "Description"], axis=1, inplace=True)
    pd.options.display.max_columns = None
    df = common_transform(df, account_name)
    df["date"] = pd.to_datetime(df["date"], format="%B %d, %Y")

    df["amount"] = df["value"].astype(float)
    df["description"] = (
        df["activity"]
        + " "
        + df["symbol"]
        + " "
        + df["symbol description"]
        + " in "
        + df["currency"]
    )

    cols = ["account", "date", "amount", "description"]
    df = df[cols]

    return df
