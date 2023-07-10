from __future__ import annotations

import pandas as pd

from src.scripts.helper.iterate_datasets import change_transaction_file_name


def common_transform(df, account_name):
    df.columns = df.columns.map(lambda x: x.lower())
    df.insert(0, "account", account_name)

    return df


def common_preload_transform(directory, file_name, df, account_name):
    file_name = change_transaction_file_name(
        directory=directory,
        old_name=file_name,
        df=df,
        bank_acount=account_name,
    )
    now_date = pd.Timestamp.now()
    df["processed_date"] = now_date
    return df
