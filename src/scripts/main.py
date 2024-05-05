from __future__ import annotations

import os

import pandas as pd
from prefect import flow
from prefect import get_run_logger
from prefect import task
from prefect.task_runners import ConcurrentTaskRunner

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


@task(name="Get Files List")
def get_files_list(
    directory: str,
    file_name_prefix: str,
    file_type: str,
    **kwargs,
) -> list[str]:
    return get_directory_files(
        directory=directory,
        file_name_prefix=file_name_prefix,
        file_type=file_type,
    )


@task(name="Identification")
def identify_bank_data(file_string: str, **kwargs) -> dict:
    directory, file = os.path.split(file_string)
    if not directory.endswith("/"):
        directory += "/"
    config, transform_func = identify_accounts(directory=directory, file=file)
    bank_data = {
        "config": config,
        "transform_func": transform_func,
        "directory": directory,
        "file": file,
    }
    return bank_data


@task()
def transform_data(file_path, bank_data, **kwargs) -> pd.DataFrame:
    config = bank_data["config"]
    transform_func = bank_data["transform_func"]
    df = transform_func(file_path, schema=config)
    df = common_preload_transform(df)
    return df


@task(tags=["dq_check"])
def dq_check(df, bank_data, **kwargs) -> dict:
    config = bank_data["config"]
    account_name = config["account_name"]
    expectation_suite_name = config["expectation_suite_name"]
    gx_result = gx_dq_check(
        df_to_validate=df,
        data_asset_name=account_name,
        expectation_suite_name=expectation_suite_name,
    )
    print_gx_result(
        directory=bank_data["directory"],
        file=bank_data["file"],
        gx_result=gx_result,
    )
    return gx_result


@task
def gx_email(gx_results, bank_data, **kwargs):
    email_sender = EmailSender()
    dq_check_parameters = {
        "asset_name": bank_data["config"]["account_name"],
        "file": bank_data["directory"] + bank_data["file"],
        "error_exceptions": gx_results["error_message"],
        "data_docs_site": gx_results["data_docs_site"],
        "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    email_sender.send_dq_notification_email(parameters=dq_check_parameters)


@task
def categorize_data(df, **kwargs) -> dict:
    df = add_category(df)
    return df


@task
def load_data_into_db(df, bank_data, **kwargs) -> dict:
    mongodb_hook = MongoDBHook()
    account_name = bank_data["config"]["account_name"]
    result = mongodb_hook.insert_df(df=df, account=account_name)
    return result


@task
def move_processed_files(df: pd.DataFrame, bank_data: dict, **kwargs) -> None:
    directory = bank_data["directory"]
    file = bank_data["file"]
    file_name = change_transaction_file_name(
        directory=directory,
        old_name=file,
        df=df,
        bank_acount=bank_data["config"]["account_name"],
    )
    kwargs["logger"].info(f"File Name: {file_name}")
    move_file(
        file_name=file_name,
        old_directory=bank_data["directory"],
        new_suffix="processed/",
    )


@flow(name="Pipeline Flow", description="", task_runner=ConcurrentTaskRunner())
def flow():
    logger = get_run_logger()
    file_paths = get_files_list(config_directory, "", ".csv")
    mapped_configs = identify_bank_data.map(file_paths)
    for bank_config in mapped_configs:
        bank_data = bank_config.result()
        logger.info(f"Bank Data: {bank_data}")
        file_path = bank_data["directory"] + bank_data["file"]
        transformed_data = transform_data(
            file_path,
            bank_data,
            wait_for=[identify_bank_data],
        )
        gx_results = dq_check(transformed_data, bank_data, wait_for=[transform_data])
        logger.info(f"GX Result: {gx_results['success']}")
        if not gx_results["success"]:
            logger.info(f"GX Errors: {gx_results['error_message']}")
            logger.info(f"Data Docs Site: {gx_results['data_docs_site']}")
            gx_email(gx_results=gx_results, bank_data=bank_data, wait_for=[dq_check])

        categorized_data = categorize_data(
            df=transformed_data,
            wait_for=[transform_data],
        )
        load_result = load_data_into_db(
            df=categorized_data,
            bank_data=bank_data,
            wait_for=[categorize_data],
        )
        logger.info(f"Rows Updated: {load_result}")
        # move_processed_files(categorized_data, bank_data=bank_data, logger=logger, wait_for=[load_data_into_db])
    return


if __name__ == "__main__":
    flow()
