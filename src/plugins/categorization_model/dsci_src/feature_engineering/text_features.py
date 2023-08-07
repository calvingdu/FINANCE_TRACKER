from __future__ import annotations

import re

import pandas as pd
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer


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

    # add_count_vectorizer(df=df, desc_column=desc_column)

    return df


def add_contains_keyword_column(
    df: pd.DataFrame,
    desc_column: str = "cleaned_description",
) -> pd.DataFrame:
    print("hello")


def add_count_vectorizer(
    df: pd.DataFrame,
    desc_column: str = "cleaned_description",
) -> pd.DataFrame:
    # Word Frequency Vectorizer
    min_df_threshold = 0.05
    max_features = 15
    custom_stopwords = set(stopwords.words("english")) | {
        "a",
    }

    word_freq_vectorizer = CountVectorizer(
        stop_words=custom_stopwords,
        max_features=max_features,
        min_df=min_df_threshold,
    )
    word_freq_features = word_freq_vectorizer.fit_transform(df[desc_column]).toarray()
    for i, feature_name in enumerate(word_freq_vectorizer.get_feature_names_out()):
        df[feature_name] = word_freq_features[:, i]

    return df
