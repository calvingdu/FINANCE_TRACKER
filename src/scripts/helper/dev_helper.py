from __future__ import annotations


def generate_create_dataframe(df):
    output = "pd.DataFrame({"
    for col in df.columns:
        column_data = df[col].tolist()
        column_data_string = str(column_data).replace("[", "").replace("]", "")
        column_data_type = str(df[col].dtype)
        column_string = (
            f"'{col}':pd.Series([{column_data_string}], dtype='{column_data_type}'),"
        )
        output += column_string
    output = output.strip(", ")
    output += "})"

    output = output.replace("nan", "np.nan")
    output = output.replace("<NA>", "np.nan")
    output = output.replace("NaT", "pd.NaT")
    return output
