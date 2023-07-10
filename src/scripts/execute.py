from __future__ import annotations

import os

from config.config import config
from src.plugins.dq_check.greatexpectations import greatexpectations
from src.plugins.mongoDB.mongodb_hook import MongoDBHook
from src.scripts.helper.etl_data import common_preload_transform
from src.scripts.helper.identify_accounts import identify_accounts
from src.scripts.helper.iterate_datasets import get_directory_files
from src.scripts.helper.iterate_datasets import move_file

config_directory = config.get("bank_directory")


def execute():
    # STAGE 1: Get Files List
    file_paths = get_directory_files(
        directory=config_directory,
        file_name_prefix="",
        file_type=".csv",
    )

    # STAGE 2: Identify Data
    for file_string in file_paths:
        directory, file = os.path.split(file_string)
        if not directory.endswith("/"):
            directory += "/"
        config, transform_func = identify_accounts(directory=directory, file=file)

        etl_execute(
            directory=directory,
            file=file,
            config=config,
            transform_func=transform_func,
        )


def etl_execute(directory, file, config, transform_func):
    try:
        account_name = config["account_name"]
        expectation_suite_name = config["expectation_suite_name"]

        # STAGE 3: Transform Data
        df = transform_func(file_name=file, file_path=directory, schema=config)

        # STAGE 4: DQ Check Data
        greatexpectations(
            df_to_validate=df,
            data_asset_name=account_name,
            expectation_suite_name=expectation_suite_name,
        )

        # STAGE 5: Categorize Data

        # STAGE 6: Load Data Into DB
        df = common_preload_transform(
            directory=directory,
            file_name=file,
            df=df,
            account_name=account_name,
        )
        mongodb_hook = MongoDBHook()
        mongodb_hook.insert_df(df=df, account_name=account_name)

        # STAGE 7: Move Data into Processed
        move_file(file_name=file, old_directory=directory, new_suffix="processed/")

    except Exception as e:
        print(f"Exception: {e}")
        move_file(file_name=file, old_directory=directory, new_suffix="unprocessed/")


execute()
