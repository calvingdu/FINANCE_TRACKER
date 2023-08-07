from __future__ import annotations

import pandas as pd
from pandas import Timestamp

from src.plugins.categorization_model.dsci_src.feature_engineering.transaction_matching_features import (
    transaction_matching,
)


def test_transaction_matching_feature_simple():
    df = pd.DataFrame(
        {
            "account": pd.Series(
                ["CHECKING", "CHECKING", "VISA", "OTHER_BANK"],
                dtype="object",
            ),
            "date": pd.Series(
                [
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-04-28 00:00:00"),
                ],
                dtype="datetime64[ns]",
            ),
            "description": pd.Series(
                ["paid", "etransfer out", "etransfer in", "purchase"],
                dtype="object",
            ),
            "amount": pd.Series([15, -15, 15, 16.95], dtype="float64"),
        },
    )

    expected_df = pd.DataFrame(
        {
            "account": pd.Series(
                ["CHECKING", "CHECKING", "VISA", "OTHER_BANK"],
                dtype="object",
            ),
            "date": pd.Series(
                [
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-04-28 00:00:00"),
                ],
                dtype="datetime64[ns]",
            ),
            "description": pd.Series(
                ["paid", "etransfer out", "etransfer in", "purchase"],
                dtype="object",
            ),
            "amount": pd.Series([15.0, -15.0, 15.0, 16.95], dtype="float64"),
            "matched_transaction": pd.Series([[1], [0, 2], [1], []], dtype="object"),
        },
    )

    result_df = transaction_matching(
        df=df,
        transaction_column="amount",
        date_column="date",
        time_window=3,
        new_matched_column="matched_transaction",
    )

    assert result_df.equals(expected_df)


def test_transaction_matching_feature_complex():
    df = pd.DataFrame(
        {
            "account": pd.Series(
                [
                    "CHECKING",
                    "CHECKING",
                    "CHECKING",
                    "VISA",
                    "VISA",
                    "VISA",
                    "OTHER_BANK",
                ],
                dtype="object",
            ),
            "date": pd.Series(
                [
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-04-28 00:00:00"),
                ],
                dtype="datetime64[ns]",
            ),
            "description": pd.Series(
                [
                    "paid",
                    "paid",
                    "paid" "etransfer in",
                    "etransfer in",
                    "etransfer in",
                    "purchase",
                ],
                dtype="object",
            ),
            "amount": pd.Series(
                [15.0, 15.0, 15.0, -15.0, -15.0, -15.0, 16.95],
                dtype="float64",
            ),
        },
    )

    expected_df = pd.DataFrame(
        {
            "account": pd.Series(
                [
                    "CHECKING",
                    "CHECKING",
                    "CHECKING",
                    "VISA",
                    "VISA",
                    "VISA",
                    "OTHER_BANK",
                ],
                dtype="object",
            ),
            "date": pd.Series(
                [
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-07-05 00:00:00"),
                    Timestamp("2023-04-28 00:00:00"),
                ],
                dtype="datetime64[ns]",
            ),
            "description": pd.Series(
                [
                    "paid",
                    "paid",
                    "paid" "etransfer in",
                    "etransfer in",
                    "etransfer in",
                    "purchase",
                ],
                dtype="object",
            ),
            "amount": pd.Series(
                [15.0, 15.0, 15.0, -15.0, -15.0, -15.0, 16.95],
                dtype="float64",
            ),
            "matched_transaction": pd.Series(
                [[3, 4, 5], [3, 4, 5], [3, 4, 5], [0, 1, 2], [0, 1, 2], [0, 1, 2], []],
                dtype="object",
            ),
        },
    )

    result_df = transaction_matching(
        df=df,
        transaction_column="amount",
        date_column="date",
        time_window=3,
        new_matched_column="matched_transaction",
    )
    print(expected_df)
    print(result_df)

    assert result_df.equals(expected_df)
