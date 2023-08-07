from __future__ import annotations

import pandas as pd


def add_numerical_features(
    df: pd.DataFrame,
    amount_column: str = "amount",
) -> pd.DataFrame:
    # Transaction Amount Sign
    df["amount_sign"] = df["amount"].apply(
        lambda x: "positive" if x > 0 else "negative",
    )

    # Transaction Amount Category
    df[f"{amount_column}_category"] = pd.cut(
        df["amount"],
        bins=[-float("inf"), -1500, -500, -50, 0, 50, 200, 1500, float("inf")],
        labels=[
            "extremely_negative",
            "very_negative",
            "negative",
            "small_negative",
            "small_positive",
            "positive",
            "very_positive",
            "extremely_positive",
        ],
    )

    return df
