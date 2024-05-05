from __future__ import annotations

import pandas as pd
from pandas import Timestamp

from configuration.config_setup import config
from src.scripts.transaction_feeds.accounts_config import accounts_config
from src.scripts.transaction_feeds.transform.transform_rbc_transactions import (
    transform_dataset,
)

transform_data_directory = config.get("transform_data_directory")

expected_dataframe = pd.DataFrame(
    {
        "account": pd.Series(
            ["RBC_Chequing", "RBC_Chequing", "RBC_Chequing"],
            dtype="object",
        ),
        "date": pd.Series(
            [
                Timestamp("2023-01-26 00:00:00"),
                Timestamp("2023-01-26 00:00:00"),
                Timestamp("2023-02-09 00:00:00"),
            ],
            dtype="datetime64[ns]",
        ),
        "amount": pd.Series([-123.45, 52.92, -1000.12], dtype="float64"),
        "description": pd.Series(
            ["STAFF - PAYROLL", "Email Trfs", "STAFF - PAYROLL"],
            dtype="object",
        ),
    },
)


def test_transform_rbc():
    schema = accounts_config["rbc"]
    df = transform_dataset(
        file=transform_data_directory + "RBC_20230126_20230410.csv",
        schema=schema,
    )

    assert df.equals(expected_dataframe)
