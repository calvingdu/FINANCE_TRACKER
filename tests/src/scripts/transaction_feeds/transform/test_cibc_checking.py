from __future__ import annotations

import pandas as pd
from pandas import Timestamp

from configuration.config_setup import config
from src.scripts.transaction_feeds.accounts_config import accounts_config
from src.scripts.transaction_feeds.transform.transform_cibc_checking_transactions import (
    transform_dataset,
)

transform_data_directory = config.get("transform_data_directory")

expected_dataframe = pd.DataFrame(
    {
        "account": pd.Series(
            ["CIBCchecking", "CIBCchecking", "CIBCchecking"],
            dtype="object",
        ),
        "date": pd.Series(
            [
                Timestamp("2023-02-16 00:00:00"),
                Timestamp("2023-02-16 00:00:00"),
                Timestamp("2023-02-15 00:00:00"),
            ],
            dtype="datetime64[ns]",
        ),
        "amount": pd.Series([30.44, 1023.86, -98.62], dtype="float64"),
        "description": pd.Series(
            ["EXPENSE 1", "EXPENSE 2", "EXPENSE 3"],
            dtype="string",
        ),
    },
)


def test_transform_cibc_checking():
    schema = accounts_config["cibc_checking"]
    df = transform_dataset(
        file_path=transform_data_directory,
        file_name="CIBCCHECKING_20200430_20230216.csv",
        schema=schema,
    )

    assert df.equals(expected_dataframe)
