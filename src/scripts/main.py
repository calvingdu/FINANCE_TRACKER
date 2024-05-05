from __future__ import annotations

import os

import pandas as pd
from prefect import flow
from prefect import get_run_logger
from prefect import task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.task_runners import SequentialTaskRunner

from configuration.config_setup import config
from src.plugins.categorization_model.categorization import add_category
from src.plugins.dq_check.gx_dq_check import gx_dq_check
from src.plugins.email.email_sender import EmailSender
from src.plugins.mongoDB.mongodb_hook import MongoDBHook
from src.scripts.helper.etl_data import common_preload_transform
from src.scripts.helper.identify_accounts import identify_accounts
from src.scripts.helper.iterate_datasets import change_transaction_file_name
from src.scripts.helper.iterate_datasets import get_directory_files
from src.scripts.helper.iterate_datasets import move_file

config_directory = config.get("bank_directory")


@task(name="Get_Files_List", retries=1, retry_delay_seconds=5)
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


@task(name="Bank_Identification")
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


@task(name="Extract_Transform")
def transform_data(file_path, bank_data, **kwargs) -> pd.DataFrame:
    config = bank_data["config"]
    transform_func = bank_data["transform_func"]
    df = transform_func(file_path, schema=config)
    df = common_preload_transform(df)
    return df


@task(name="DQ_Check", tags=["dq_check"])
def dq_check(df, bank_data, **kwargs) -> dict:
    config = bank_data["config"]
    account_name = config["account_name"]
    expectation_suite_name = config["expectation_suite_name"]
    gx_results = gx_dq_check(
        df_to_validate=df,
        data_asset_name=account_name,
        expectation_suite_name=expectation_suite_name,
    )
    return df, gx_results


@task(name="GX_Validate_Results", tags=["dq_check"])
def gx_validate_results(
    valid_exceptions: list[str],
    gx_results: dict,
    df: pd.DataFrame,
    **kwargs,
):
    logger = kwargs["logger"]
    if not gx_results["success"]:
        gx_error = gx_results["error_message"]
        logger.error(f"GX Errors: {gx_error}")
        logger.info(f"Data Docs Site: {gx_results['data_docs_site']}")
        if all(
            exception in gx_results["error_message"] for exception in valid_exceptions
        ):
            return df, False
        else:
            return None, False
    else:
        return df, True


@task(name="GX_Error_Email", tags=["dq_check"])
def gx_error_email(gx_results, bank_data, **kwargs):
    email_sender = EmailSender()
    dq_check_parameters = {
        "asset_name": bank_data["config"]["account_name"],
        "file": bank_data["directory"] + bank_data["file"],
        "error_exceptions": gx_results["error_message"],
        "data_docs_site": gx_results["data_docs_site"],
        "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    email_sender.send_dq_notification_email(parameters=dq_check_parameters)


@task(name="ML_Categorization")
def categorize_data(df, **kwargs) -> dict:
    df = add_category(df)
    return df


@task(name="Load_Data_MongoDB")
def load_data_into_db(df, bank_data, **kwargs) -> dict:
    mongodb_hook = MongoDBHook()
    account_name = bank_data["config"]["account_name"]
    result = mongodb_hook.insert_df(df=df, account=account_name)
    return df, result


@task(name="Move_Processed_Files")
def move_processed_files(df: pd.DataFrame, bank_data: dict, **kwargs) -> None:
    directory = bank_data["directory"]
    file = bank_data["file"]
    file_name = change_transaction_file_name(
        directory=directory,
        old_name=file,
        df=df,
        bank_acount=bank_data["config"]["account_name"],
    )
    move_file(
        file_name=file_name,
        old_directory=bank_data["directory"],
        new_suffix="processed/",
    )


@flow(
    name="File_Flow",
    description="ETL Logic for each file",
    task_runner=SequentialTaskRunner(),
)
def file_flow(bank_config: dict):
    logger = get_run_logger()
    bank_data = bank_config
    logger.info(f"Bank Data: {bank_data}")
    file_path = bank_data["directory"] + bank_data["file"]
    transformed_data = transform_data(
        file_path,
        bank_data,
        wait_for=[identify_bank_data],
    )
    dq_checked_df, gx_results = dq_check(
        transformed_data,
        bank_data,
        wait_for=[transform_data],
    )
    logger.info(f"GX Result: {gx_results['success']}")

    validated_df, gx_validate_result = gx_validate_results(
        valid_exceptions=["expect_compound_columns_to_be_unique"],
        gx_results=gx_results,
        df=dq_checked_df,
        logger=logger,
    )

    if not gx_validate_result:
        gx_error_email(
            gx_results=gx_results,
            bank_data=bank_data,
            wait_for=[gx_validate_results],
        )
        logger.warn(f"GX Email Sent for file: {bank_data['file']}")
        if validated_df is None:
            raise ValueError("Data Quality Check Failed")

    categorized_df = categorize_data(
        df=validated_df,
        wait_for=[gx_validate_results],
    )
    processed_df, load_result = load_data_into_db(
        df=categorized_df,
        bank_data=bank_data,
        wait_for=[categorize_data],
    )
    logger.info(f"Rows Updated: {load_result}")
    move_processed_files(
        processed_df,
        bank_data=bank_data,
        logger=logger,
        wait_for=[load_data_into_db],
    )


@flow(name="Main_Flow", description="", task_runner=ConcurrentTaskRunner())
def main_flow():
    logger = get_run_logger()
    file_paths = get_files_list(config_directory, "", ".csv")
    mapped_configs = identify_bank_data.map(file_paths)
    for bank_config in mapped_configs:
        try:
            file_flow(bank_config=bank_config)
        except Exception as e:
            errored_file = bank_config.result()["file"]
            logger.critical(
                f"""ETL Process Failed: {errored_file} \nError Message: {e}""",
            )


if __name__ == "__main__":
    main_flow()
