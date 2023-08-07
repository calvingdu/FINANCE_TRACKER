from __future__ import annotations

import pandas as pd


def transaction_matching(
    df: pd.DataFrame,
    transaction_column: str = "amount",
    date_column: str = "date",
    time_window: float = 3,
    new_matched_column: str = "matched_transactions",
) -> pd.DataFrame:
    transaction_dict = {}
    matched_result = {}

    for i in range(len(df)):
        current_amount = df.iloc[i][transaction_column]
        current_date = df.iloc[i][date_column]

        # Check if there's a matching positive transaction within the time window
        if -current_amount in transaction_dict:
            for matching_index in transaction_dict[-current_amount]:
                matching_date = df.iloc[matching_index][date_column]
                time_difference = pd.to_datetime(current_date) - pd.to_datetime(
                    matching_date,
                )

                if abs(time_difference.days) <= time_window:
                    # Matching transaction found -> process the matching pair
                    matched_result.setdefault(matching_index, set()).add(i)
                    transaction_dict.setdefault(current_amount, set()).add(i)
            else:
                # No matching transaction found, add the current negative transaction
                transaction_dict.setdefault(current_amount, set()).add(i)
        else:
            # Add the negative transaction amount to the hash table
            transaction_dict.setdefault(current_amount, set()).add(i)

    # Set matched transactions column
    df[new_matched_column] = [[] for _ in range(len(df))]

    for i in matched_result:
        df.loc[i, new_matched_column].extend(matched_result[i])
        for j in matched_result[i]:
            df.loc[j, new_matched_column].append(int(i))

    return df


def count_transactions_matching_feature(
    df: pd.DataFrame,
    matched_column: str = "matched_transactions",
):
    df[f"{matched_column}_count"] = df[matched_column].apply(len)
    return df


def get_matched_transactions(
    df: pd.DataFrame,
    matched_column: str = "matched_transactions",
    amount_column: str = "amount",
    date_column: str = "date",
) -> pd.DataFrame:
    filtered_df = df[df[matched_column].apply(lambda x: x != [])]
    return filtered_df.sort_values(date_column)
