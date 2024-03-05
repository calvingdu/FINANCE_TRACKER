from __future__ import annotations

import re

import pandas as pd


def text_cleanup(column: pd.Series) -> pd.Series:
    # Convert to lowercase
    column = column.str.lower()

    # Remove punctuation and special characters
    column = column.apply(lambda x: re.sub(r"[^\w\s]", " ", x))

    # Replace multiple spaces with a single space
    column = column.apply(lambda x: re.sub(r"\s+", " ", x))

    return column


def add_text_features(
    df: pd.DataFrame,
    desc_column: str = "cleaned_description",
) -> pd.DataFrame:
    df["description_length"] = df[desc_column].apply(len)
    df["num_tokens"] = df[desc_column].apply(lambda x: len(re.findall(r"\w+", x)))

    add_contains_keyword_column()

    return df


def add_contains_keyword_column(
    df: pd.DataFrame,
    desc_column: str = "cleaned_description",
) -> pd.DataFrame:
    print("hello")
