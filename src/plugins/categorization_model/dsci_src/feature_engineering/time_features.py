from __future__ import annotations

import pandas as pd


def add_time_features(df: pd.DataFrame, date_column: str = "date") -> pd.DataFrame:
    # Transaction Day of Week
    df["day_of_week"] = pd.to_datetime(df[date_column]).dt.dayofweek

    # Transaction Month
    df["month_of_year"] = pd.to_datetime(df[date_column]).dt.month

    return df
