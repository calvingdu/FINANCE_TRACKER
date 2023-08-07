from __future__ import annotations

import pandas as pd
from pandas import Timestamp

from configuration.config_setup import config
from src.scripts.transaction_feeds.accounts_config import accounts_config
from src.scripts.transaction_feeds.transform.transform_bmo_checking_transactions import (
    transform_dataset,
)

transform_data_directory = config.get("transform_data_directory")

expected_dataframe = pd.DataFrame(
    {
        "account": pd.Series(
            ["BMO_CHECKING", "BMO_CHECKING", "BMO_CHECKING"],
            dtype="object",
        ),
        "date": pd.Series(
            [
                Timestamp("2022-12-23 00:00:00"),
                Timestamp("2022-12-23 00:00:00"),
                Timestamp("2022-12-28 00:00:00"),
            ],
            dtype="datetime64[ns]",
        ),
        "amount": pd.Series([-114.54, -112.89, -6000.0], dtype="float64"),
        "description": pd.Series(["NSE 1", "NSE 2", "NSE 3"], dtype="object"),
    },
)


def test_transform_bmo_checking():
    schema = accounts_config["bmo_checking"]
    df = transform_dataset(
        file_path=transform_data_directory,
        file_name="BMOCHECKING_20221128_20230209.csv",
        schema=schema,
    )

    assert df.equals(expected_dataframe)
