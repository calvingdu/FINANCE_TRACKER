from __future__ import annotations

import joblib
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import OneHotEncoder
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


def create_model(df):
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

    X = df.drop("category", axis=1)
    y = df["category"]

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
    )

    # Define columns to be one-hot encoded and scaled
    categorical_cols = ["account", "amount_category", "amount_sign"]
    numerical_cols = [
        "amount",
        "matched_transactions_count",
        "day_of_week",
        "month_of_year",
        "description_length",
        "num_tokens",
    ]
    removed_columns = [
        "date",
        "matched_transactions",
        "description",
        "cleaned_description",
        "category",
        "pred_category",
        "correct_category",
    ]

    categorized_columns = categorical_cols + (numerical_cols)

    numerical_cols.extend(
        set(list(df.columns)) - set(categorized_columns) - set(removed_columns),
    )

    # Create a ColumnTransformer to apply different preprocessing to different columns
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numerical_cols),
            ("cat", OneHotEncoder(), categorical_cols),
        ],
    )

    # Add encoders
    X_train_preprocessed = preprocessor.fit_transform(X_train)
    X_test_preprocessed = preprocessor.transform(X_test)

    # Create a KNN classifier
    k = 5  # Number of neighbors
    knn_classifier = KNeighborsClassifier(n_neighbors=k)

    # Train the model
    knn_classifier.fit(X_train_preprocessed, y_train)

    # Make predictions on the test set
    y_pred = knn_classifier.predict(X_test_preprocessed)

    # Calculate the accuracy
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy:.2f}")

    joblib.dump(knn_classifier, "knn_model.joblib")


def use_model(df):
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

    # Define columns to be one-hot encoded and scaled
    categorical_cols = ["account", "amount_category", "amount_sign"]
    numerical_cols = [
        "amount",
        "matched_transactions_count",
        "day_of_week",
        "month_of_year",
        "description_length",
        "num_tokens",
    ]
    removed_columns = [
        "date",
        "matched_transactions",
        "description",
        "cleaned_description",
        "category",
        "pred_category",
        "correct_category",
    ]

    categorized_columns = categorical_cols + (numerical_cols)

    numerical_cols.extend(
        set(list(df.columns)) - set(categorized_columns) - set(removed_columns),
    )

    # Create a ColumnTransformer to apply different preprocessing to different columns
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numerical_cols),
            ("cat", OneHotEncoder(), categorical_cols),
        ],
    )

    # Add encoders
    X_preprocessed = preprocessor.fit_transform(df)

    loaded_knn_model = joblib.load(
        "src/plugins/categorization_model/dsci_src/knn_model.joblib",
    )

    # Now you can use 'loaded_knn_model' for making predictions
    predicted_category = loaded_knn_model.predict(X_preprocessed)

    df["category"] = predicted_category

    return df
