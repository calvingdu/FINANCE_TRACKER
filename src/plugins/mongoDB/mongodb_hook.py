from __future__ import annotations

import pandas as pd
import pymongo
from pymongo import MongoClient


class MongoDBHook:
    def __init__(self):
        self.client = MongoClient(
            host="localhost",
            port=27017,
            username="admin",
            password="admin",
        )
        self.db = self.client.test
        self.collection = self.db.testing

    def insert_df(self, df: pd.DataFrame, account: str):
        # Only inserts data that have a date later than the latest transaction date for that account
        latest_date_account = self.collection.find_one(
            {"account": account},
            sort=[("date", pymongo.DESCENDING)],
        )
        if latest_date_account is not None:
            latest_date = latest_date_account["date"]
        else:
            latest_date = pd.to_datetime(0000 - 00 - 00)

        df = df[df["date"] > latest_date]
        if df.empty:
            return 0

        df_dict = df.to_dict("records")

        result = self.collection.insert_many(df_dict, ordered=False)
        new_rows_count = len(result.inserted_ids)
        return new_rows_count

    def delete_all(self):
        self.collection.delete_many({})

    def extract_df(self, account=None):
        if account is None:
            cursor = self.collection.find({})
        else:
            cursor = self.collection.find({"account": account})
        df = pd.DataFrame(list(cursor))

        del df["_id"]
        return df
