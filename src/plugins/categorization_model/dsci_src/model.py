from __future__ import annotations

import joblib
import pandas as pd
from sklearn.compose import make_column_transformer
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import StandardScaler

from src.plugins.categorization_model.dsci_src.feature_engineering.numerical_features import (
    add_numerical_features,
)
from src.plugins.categorization_model.dsci_src.feature_engineering.text_features import (
    add_text_features,
)
from src.plugins.categorization_model.dsci_src.feature_engineering.text_features import (
    text_cleanup,
)
from src.plugins.categorization_model.dsci_src.feature_engineering.time_features import (
    add_time_features,
)
from src.plugins.categorization_model.dsci_src.feature_engineering.transaction_matching_features import (
    count_transactions_matching_feature,
)
from src.plugins.categorization_model.dsci_src.feature_engineering.transaction_matching_features import (
    transaction_matching,
)

# from sklearn.preprocessing import OneHotEncoder


def create_preprocessor(df: pd.DataFrame):
    # Transaction Matching Features
    df = transaction_matching(
        df=df,
        transaction_column="amount",
        date_column="date",
        time_window=3,
        new_matched_column="matched_transactions",
    )
    df = count_transactions_matching_feature(df=df)

    # Time Features
    df = add_time_features(df=df, date_column="date")

    # Numerical Features
    df = add_numerical_features(df=df, amount_column="amount")

    # Text Features
    df["cleaned_description"] = text_cleanup(df["description"])
    df = add_text_features(df=df, desc_column="cleaned_description")

    # categorical_cols = ["account", "amount_category", "amount_sign"]
    numerical_cols = [
        "amount",
        "matched_transactions_count",
        "day_of_week",
        "month_of_year",
        "description_length",
        "num_tokens",
    ]
    # removed_cols = [
    #     "date",
    #     "matched_transactions",
    #     "description",
    #     "cleaned_description",
    #     "category",
    #     "pred_category",
    #     "correct_category",
    # ]

    preprocessor = make_column_transformer(
        (StandardScaler(), numerical_cols),
        # (OneHotEncoder(handle_unknown="ignore", max_categories=10), categorical_cols),
        remainder="drop",
    )

    return df, preprocessor


def create_model(df: pd.DataFrame) -> None:
    df = df[["account", "date", "description", "amount", "category"]]
    df["category"] = df["category"].fillna(0)
    df = df[df["category"] != 0]
    X = df.drop("category", axis=1)
    y = df["category"]

    df, preprocessor = create_preprocessor(X)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
    )
    X_train_preprocessed = preprocessor.fit_transform(X_train)
    X_test_preprocessed = preprocessor.transform(X_test)

    k = 7
    knn_classifier = KNeighborsClassifier(n_neighbors=k)

    # Train the model
    knn_classifier.fit(X_train_preprocessed, y_train)

    # Make predictions on the test set
    y_pred = knn_classifier.predict(X_test_preprocessed)

    # Calculate the accuracy
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy}")

    joblib.dump(knn_classifier, "knn_model.joblib")


def use_model(df: pd.DataFrame) -> pd.DataFrame:
    df, preprocessor = create_preprocessor(df)
    X_preprocessed = preprocessor.fit_transform(df)

    loaded_knn_model = joblib.load(
        "src/plugins/categorization_model/dsci_src/knn_model.joblib",
    )

    predicted_category = loaded_knn_model.predict(X_preprocessed)

    df["category"] = predicted_category
    df = df[["account", "date", "description", "amount", "category"]]

    return df
