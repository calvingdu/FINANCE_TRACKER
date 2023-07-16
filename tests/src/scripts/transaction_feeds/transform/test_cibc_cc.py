from __future__ import annotations

import pandas as pd
from pandas import Timestamp

from config.config import config
from src.scripts.transaction_feeds.accounts_config import accounts_config
from src.scripts.transaction_feeds.transform.transform_cibc_cc_transactions import (
    transform_dataset,
)

transform_data_directory = config.get("transform_data_directory")

expected_dataframe = pd.DataFrame(
    {
        "account": pd.Series(["CIBCcc", "CIBCcc", "CIBCcc"], dtype="object"),
        "date": pd.Series(
            [
                Timestamp("2023-02-16 00:00:00"),
                Timestamp("2023-02-15 00:00:00"),
                Timestamp("2023-02-15 00:00:00"),
            ],
            dtype="datetime64[ns]",
        ),
        "amount": pd.Series([14.46, 92.3, 228.92], dtype="float64"),
        "description": pd.Series(
            [
                "THE GRAND NOODLE EMPORIUM VANCOUVER, BC",
                "KOODO MOBILE PAC 111-1111111, AB",
                "FLAIR DIRECT0000000000123 KELOWNA",
            ],
            dtype="string",
        ),
    },
)


def test_transform_cibc_cc():
    schema = accounts_config["cibc_cc"]
    df = transform_dataset(
        file_path=transform_data_directory,
        file_name="CIBCCC_20201015_20230216.csv",
        schema=schema,
    )

    assert df.equals(expected_dataframe)
