from __future__ import annotations

import re

import pandas as pd


def common_transform(df, account_name):
    df.columns = df.columns.map(lambda x: x.lower())
    df.insert(0, "account", account_name)
    return df


def common_preload_transform(df):
    now_date = pd.Timestamp.now()
    df["processed_date"] = now_date

    # Only processes transactions before the current time to avoid intra day disparity
    # (without this transactions that happen after a processing date would be deleted by insert_df function)
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    df = df[df["date"] < today]

    df["description"] = df["description"].apply(lambda x: re.sub(r"\s+", " ", x))

    return df
