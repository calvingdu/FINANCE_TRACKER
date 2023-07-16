from __future__ import annotations

import pandas as pd
from pandas import Timestamp

from src.scripts.transaction_feeds.accounts_config import accounts_config
from src.scripts.transaction_feeds.transform.transform_amex_transactions import (
    transform_dataset,
)

transform_data_directory = "tests/src/scripts/transaction_feeds/transform/data/"

expected_dataframe = pd.DataFrame(
    {
        "account": pd.Series(["AMEX", "AMEX", "AMEX", "AMEX"], dtype="object"),
        "date": pd.Series(
            [
                Timestamp("2023-04-04 00:00:00"),
                Timestamp("2023-04-05 00:00:00"),
                Timestamp("2023-04-05 00:00:00"),
                Timestamp("2023-04-06 00:00:00"),
            ],
            dtype="datetime64[ns]",
        ),
        "amount": pd.Series([6.88, 69.69, 10.15, 9.48], dtype="float64"),
        "description": pd.Series(
            [
                "MCDONALD'S 40346 TORONTO",
                "METRO 759 TORONTO",
                "POPEYES #2516 TORONTO",
                "IKEA 40346 TORONTO",
            ],
            dtype="string",
        ),
    },
)


def test_transform_amex():
    schema = accounts_config["amex"]
    df = transform_dataset(
        file_path=transform_data_directory,
        file_name="AMEX_20221216_20230414.csv",
        schema=schema,
    )

    assert df.equals(expected_dataframe)
