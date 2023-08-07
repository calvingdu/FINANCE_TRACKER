from __future__ import annotations

import pandas as pd

from src.plugins.categorization_model.dsci_src.model import use_model

category_labels = [
    "housing",
    "utilities",
    "telecommunication",
    "subscriptions",
    "food_drink",
    "alcohol",
    "groceries",
    "clothing_and_accessories",
    "personal_care",
    "health",
    "shopping",
    "furniture",
    "office_supplies",
    "education",
    "transportation",
    "travel_accomodations",
    "entertainment",
    "credit_card_payment",
    "debt_payments",
    "other",
    "income_wages",
    "income_reimbursement",
    "income_government",
    "income_investments",
    "income_other",
    "transfer_in",
    "transfer_out",
    "etransfer_in",
    "etransfer_out",
]

default_category_groupings = {
    "bills": ["housing", "utilities", "telecommunication"],
    "subscriptions": ["subscriptions"],
    "food_drink": ["food_drink", "alcohol"],
    "groceries": ["groceries"],
    "clothing": ["clothing_and_accessories"],
    "health/wellbeing": ["health", "personal_care"],
    "entertainment": ["entertainment"],
    "shopping": ["shopping", "office_supplies", "furniture"],
    "education": ["education"],
    "travel": ["travel_accomodations", "transportation"],
    "money_transfer": ["credit_card_payment", "transfer_in", "transfer_out"],
    "debt_payments": ["debt_payments"],
    "other": ["other"],
    "salary": ["income_wages"],
    "other_income": ["income_government", "income_investments", "income_other"],
    "reimbursement": ["income_reimbursement"],
    "etransfer": ["etransfer_in", "etransfer_out"],
}

# print(validate_categories(default_category_groupings, category_labels))


def add_category(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["account", "date", "description", "amount"]]
    categorized_df = use_model(df)

    return categorized_df
