from __future__ import annotations

import pandas as pd
from pandas import Timestamp

from configuration.config_setup import config
from src.scripts.transaction_feeds.accounts_config import accounts_config
from src.scripts.transaction_feeds.transform.transform_bmo_cc_transactions import (
    transform_dataset,
)

transform_data_directory = config.get("transform_data_directory")

expected_dataframe = pd.DataFrame(
    {
        "account": pd.Series(
            ["BMO_MASTERCARD", "BMO_MASTERCARD", "BMO_MASTERCARD"],
            dtype="object",
        ),
        "date": pd.Series(
            [
                Timestamp("2023-02-27 00:00:00"),
                Timestamp("2023-03-01 00:00:00"),
                Timestamp("2023-03-04 00:00:00"),
            ],
            dtype="datetime64[ns]",
        ),
        "amount": pd.Series([-90.18, 13.54, 101.66], dtype="float64"),
        "description": pd.Series(
            [
                "TRSF FROM/DE ACCT/CPT 2503-XXXX-940",
                "HARVEY'S #2729 TORONTO ON",
                "DOMINOS PIZZA #39032 604-733-2118 BC",
            ],
            dtype="object",
        ),
    },
)


def test_transform_bmo_cc():
    schema = accounts_config["bmo_cc"]
    df = transform_dataset(
        file_path=transform_data_directory,
        file_name="BMOCC_20221221_20230320.csv",
        schema=schema,
    )

    assert df.equals(expected_dataframe)
