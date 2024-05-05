from __future__ import annotations

import pandas as pd
from pandas import Timestamp

from configuration.config_setup import config
from src.scripts.transaction_feeds.accounts_config import accounts_config
from src.scripts.transaction_feeds.transform.transform_rbc_direct_investing_transactions import (
    transform_dataset,
)

transform_data_directory = config.get("transform_data_directory")

expected_dataframe = pd.DataFrame(
    {
        "account": pd.Series(
            ["RBC_DIRECT_INVESTING", "RBC_DIRECT_INVESTING", "RBC_DIRECT_INVESTING"],
            dtype="object",
        ),
        "date": pd.Series(
            [
                Timestamp("2023-07-05 00:00:00"),
                Timestamp("2023-05-09 00:00:00"),
                Timestamp("2023-05-01 00:00:00"),
            ],
            dtype="datetime64[ns]",
        ),
        "amount": pd.Series([30.0, -8000.0, 8000.0], dtype="float64"),
        "description": pd.Series(
            [
                "Distribution ABC INVESTMENT in CAD",
                "Buy ABC INVESTMENT in CAD",
                "Deposits & Contributions   in CAD",
            ],
            dtype="object",
        ),
    },
)


def test_transform_rbc_direct_investing():
    schema = accounts_config["rbc_direct_investing"]
    df = transform_dataset(
        file=transform_data_directory + "RBC_DIRECT_INVESTING_20230501_20230705.csv",
        schema=schema,
    )

    assert df.equals(expected_dataframe)
