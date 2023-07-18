from __future__ import annotations

import os

import pandas as pd

from configuration.config_setup import config
from src.plugins.dq_check.gx_dq_check import gx_dq_check
from src.plugins.dq_check.gx_results_processor import print_gx_result
from src.plugins.email.email_sender import EmailSender
from src.plugins.mongoDB.mongodb_hook import MongoDBHook
from src.scripts.helper.etl_data import common_preload_transform
from src.scripts.helper.identify_accounts import identify_accounts
from src.scripts.helper.iterate_datasets import change_transaction_file_name
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

    # STAGE 2: Identify Bank Data
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
    # try:
    print(f"File: {directory+file} \nStatus: Start")
    account_name = config["account_name"]
    expectation_suite_name = config["expectation_suite_name"]

    # STAGE 3: Transform Data
    df = transform_func(file_name=file, file_path=directory, schema=config)
    df = common_preload_transform(df)

    # STAGE 4: DQ Check Data
    gx_result = gx_dq_check(
        df_to_validate=df,
        data_asset_name=account_name,
        expectation_suite_name=expectation_suite_name,
    )

    print_gx_result(directory=directory, file=file, gx_result=gx_result)

    # Stage 4.5: Send an email if GX fails
    email_sender = EmailSender()
    if not gx_result["success"]:
        dq_check_parameters = {
            "asset_name": account_name,
            "file": directory + file,
            "error_exceptions": gx_result["error_message"],
            "data_docs_site": gx_result["data_docs_site"],
            "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        email_sender.send_dq_notification_email(parameters=dq_check_parameters)

    # STAGE 5: Categorize Data

    # STAGE 6: Load Data Into DB
    mongodb_hook = MongoDBHook()
    result = mongodb_hook.insert_df(df=df, account=account_name)

    # STAGE 7: Move Data into Processed
    file_name = change_transaction_file_name(
        directory=directory,
        old_name=file,
        df=df,
        bank_acount=account_name,
    )

    move_file(file_name=file_name, old_directory=directory, new_suffix="processed/")
    print(f"File: {file_name} \nStatus: Finish \nRows Inserted: {result}")


# except Exception as e:
#     print(f"Error: {e}")


execute()
