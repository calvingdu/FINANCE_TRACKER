from __future__ import annotations

import pandas as pd

from src.scripts.helper.etl_data import common_transform


def transform_dataset(file: str, schema: dict) -> pd.DataFrame:
    column_names = schema["data_fields"].keys()
    account_name = schema["account_name"]

    df = pd.read_csv(
        file,
        names=column_names,
        dtype=schema["data_fields"],
    )

    # Removes duplicates and drops NA
    df.dropna(axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

    df = transform_data(df, account_name)

    return df


def transform_data(df: pd.DataFrame, account_name: str) -> pd.DataFrame:
    common_transform(df, account_name)
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y")
    df.drop("reference", axis=1, inplace=True)
    df["amount"] = df["amount"].astype(float)

    return df
