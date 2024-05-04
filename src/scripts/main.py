from __future__ import annotations

import os

import pandas as pd
from prefect import flow
from prefect import task
from prefect.filesystems import LocalFileSystem
from prefect.task_runners import SequentialTaskRunner

from configuration.config_setup import config
from src.plugins.categorization_model.categorization import add_category
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


@task
def get_files_list(directory: str, file_name_prefix: str, file_type: str) -> list[str]:
    return get_directory_files(
        directory=directory,
        file_name_prefix=file_name_prefix,
        file_type=file_type,
    )


@task
def identify_bank_data(file_paths: list[str]) -> dict:
    bank_data = {}
    for file_string in file_paths:
        directory, file = os.path.split(file_string)
        if not directory.endswith("/"):
            directory += "/"
        config, transform_func = identify_accounts(directory=directory, file=file)
        bank_data[file_string] = {"config": config, "transform_func": transform_func}
    return bank_data


@task
def transform_data(file_data: dict) -> dict:
    transformed_data = {}
    for file_path, data in file_data.items():
        config = data["config"]
        transform_func = data["transform_func"]
        print(file_path)
        df = transform_func(file=LocalFileSystem(basepath=file_path), schema=config)
        df = common_preload_transform(df)
        transformed_data[file_path] = df
    return transformed_data


@task
def dq_check(transformed_data: dict) -> dict:
    dq_results = {}
    for file_path, df in transformed_data.items():
        config = file_data[file_path]["config"]
        account_name = config["account_name"]
        expectation_suite_name = config["expectation_suite_name"]
        gx_result = gx_dq_check(
            df_to_validate=df,
            data_asset_name=account_name,
            expectation_suite_name=expectation_suite_name,
        )
        print_gx_result(
            directory=os.path.dirname(file_path),
            file=os.path.basename(file_path),
            gx_result=gx_result,
        )
        dq_results[file_path] = gx_result
    return dq_results


# @task
# def categorize_data(transformed_data: dict) -> dict:
#     categorized_data = {}
#     for file_path, df in transformed_data.items():
#         categorized_data[file_path] = add_category(df)
#     return categorized_data


# @task
# def load_data_into_db(categorized_data: dict) -> dict:
#     load_results = {}
#     mongodb_hook = MongoDBHook()
#     for file_path, df in categorized_data.items():
#         account_name = file_data[file_path]["config"]["account_name"]
#         result = mongodb_hook.insert_df(df=df, account=account_name)
#         load_results[file_path] = result
#     return load_results


# @task
# def move_processed_files(categorized_data: dict) -> None:
#     for file_path, _ in categorized_data.items():
#         change_transaction_file_name(
#             directory=os.path.dirname(file_path),
#             old_name=os.path.basename(file_path),
#             df=categorized_data[file_path],
#             bank_acount=file_data[file_path]["config"]["account_name"],
#         )
#         move_file(file_name=file_path, old_directory=os.path.dirname(file_path), new_suffix="processed/")


@flow(
    name="Pipeline Flow",
    description="My flow using SequentialTaskRunner",
    task_runner=SequentialTaskRunner(),
)
def flow():
    directory = config_directory
    file_paths = get_files_list(directory, "", ".csv")
    file_data = identify_bank_data(file_paths)
    transformed_data = transform_data(file_data)
    dq_results = dq_check(transformed_data)
    # categorized_data = categorize_data(transformed_data)
    # load_results = load_data_into_db(categorized_data)
    # move_processed_files(categorized_data)
    return


if __name__ == "__main__":
    flow()
