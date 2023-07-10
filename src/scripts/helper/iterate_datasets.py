from __future__ import annotations

import os
from pathlib import Path


# Looks through dataset to transform data
def get_directory_files(directory, file_name_prefix, file_type=".csv"):
    files = []
    if os.path.exists(directory + "unprocessed/"):
        unprocessed_directory = directory + "unprocessed/"
        for file in os.listdir(unprocessed_directory):
            if file.startswith(file_name_prefix) and file.endswith(file_type):
                files.append(unprocessed_directory + file)
    # iterates on the entire folder
    for file in os.listdir(directory):
        if file.startswith(file_name_prefix) and file.endswith(file_type):
            files.append(directory + file)

    return files


def move_file(file_name, old_directory, new_suffix, old_suffix="unprocessed/"):
    if not old_suffix.endswith("/"):
        old_suffix = +"/"
    if not new_suffix.endswith("/"):
        new_suffix = +"/"

    if old_directory.endswith(old_suffix):
        directory = old_directory.replace(old_suffix, "")
        Path(directory + new_suffix).mkdir(parents=True, exist_ok=True)
        os.rename(old_directory + file_name, directory + new_suffix + file_name)
        print(old_directory + file_name, directory + new_suffix + file_name)
    else:
        Path(old_directory + new_suffix).mkdir(parents=True, exist_ok=True)
        os.rename(old_directory + file_name, old_directory + new_suffix + file_name)


# changes name
def change_transaction_file_name(directory, old_name, bank_acount, df):
    earliest_date = df["date"].min().strftime("%Y%m%d")
    latest_date = df["date"].max().strftime("%Y%m%d")
    file_type = "." + old_name.split(".")[-1]
    bank_acount = bank_acount.upper()
    new_name = bank_acount + "_" + earliest_date + "_" + latest_date + file_type
    os.rename(directory + old_name, directory + new_name)
    return new_name
