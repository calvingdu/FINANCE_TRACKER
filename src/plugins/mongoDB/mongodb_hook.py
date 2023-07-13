from __future__ import annotations

import pandas as pd
import pymongo
from pymongo import MongoClient


class MongoDBHook:
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client.test_database
        self.database = self.db.testing

    def insert_df(self, df, account_name):
        # Only inserts data that have a date later than the latest transaction date for that account
        latest_date_account = self.database.find_one(
            {"account": account_name},
            sort=[("date", pymongo.DESCENDING)],
        )
        if latest_date_account is not None:
            latest_date = latest_date_account["date"]
        else:
            latest_date = pd.to_datetime(0000 - 00 - 00)

        df = df[df["date"] > latest_date]
        df_dict = df.to_dict("records")

        self.database.insert_many(df_dict)

    def delete_all(self):
        self.database.delete_many({})

    def extract_df(self):
        cursor = self.database.find({})
        df = pd.DataFrame(list(cursor))

        # Delete the _id
        del df["_id"]
        return df
