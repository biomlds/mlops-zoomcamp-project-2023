import json
import logging
from os import environ, path, remove
from typing import Literal, Optional

import boto3
import mlflow
import pandas as pd
import yfinance as yf
from botocore.exceptions import ClientError
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

S3_ENDPOINT_URL = environ.get("MINIO_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = environ.get("MINIO_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = environ.get("MINIO_SECRET_ACCESS_KEY")
S3_BUCKET = environ.get("S3_BUCKET")

s3_config = {
    "endpoint_url": S3_ENDPOINT_URL,
    "aws_access_key_id": AWS_ACCESS_KEY_ID,
    "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    "aws_session_token": None,
}

# stocks = json.loads(os.environ.get("STOCKS"))
# interval = str(environ.get("INTERVAL"))


@task
# @task(name="upload_file_s3", tags=["get_data"], retries=3, retry_delay_seconds=60)
def upload_file_s3(
    file_path: str,
    s3_bucket_name: str,
    object_name: Optional[str],
    s3_config: dict[str, Optional[str]],
) -> bool:
    """Upload a file to S3-like file storage, e.g. AWS S3 or MINIO.

    Args:
        file_path (str): full or relative path to a file
        s3_bucket_name (str): name of S3 bucket
        object_name (Optional[str]): a new name of the file object to be saved on S3.  Defaults to None
        s3_config (dict[str, Optional[str]]): a dictionary contaning configs for a S3-like file storage

    Returns:
        bool: True if upload is successfull or False
    """

    if object_name is None:
        object_name = path.basename(file_path)

    s3_client = boto3.client("s3", **s3_config)

    try:
        s3_client.upload_file(file_path, s3_bucket_name, object_name)
    except ClientError as error:
        logging.error(error)
        return False

    return True


@task
def delete_local_file(file_name):
    try:
        # Remove the file
        remove(file_name)
        print(f"The file '{file_name}' was successfully removed.")
    except OSError as error:
        # Print an error message if the removal fails
        print("Error:", error)


@task
def delete_file_s3(
    file_path: str,
    s3_bucket_name: str,
    object_name: Optional[str],
    s3_config: dict[str, Optional[str]],
) -> bool:
    """Delete a file on S3-like file storage, e.g. AWS S3 or MINIO.

    Args:
        file_path (str): full or relative path to a file
        s3_bucket_name (str): name of S3 bucket
        object_name (Optional[str]): a new name of the file object to be saved on S3.  Defaults to None
        s3_config (dict[str, Optional[str]]): a dictionary contaning configs for a S3-like file storage

    Returns:
        bool: True if sucessful or False
    """

    if object_name is None:
        object_name = path.basename(file_path)

    s3_client = boto3.client("s3", **s3_config)

    try:
        s3_client.delete_object(Bucket=s3_bucket_name, Key=object_name)
        print(
            f"The file '{object_name}' was deleted from the bucket '{s3_bucket_name}'."
        )
    except ClientError as error:
        # Print any error that occurs
        print("An error occurred:", error)
        return False

    return True


# file_path, object_name = None, s3_bucket_name = S3_BUCKET,


@task
# @task(name="download_file_s3", tags=["get_data"], retries=3, retry_delay_seconds=60)
def download_file_s3(
    file_path: str,
    s3_bucket_name: str,
    object_name: Optional[str],
    s3_config: dict[str, Optional[str]],
) -> bool:
    """Upload a file from S3-like file storage

    Args:
        file_path (str): full or relative path to a file
        s3_bucket_name (str): name of S3 bucket
        object_name (Optional[str]): a new name of the file object to be saved on S3.  Defaults to None
        s3_config (dict[str, Optional[str]]): a dictionary contaning configs for a S3-like file storage

    Returns:
        bool: True if sucessful or False
    """
    s3_client = boto3.client("s3", **s3_config)

    try:
        s3_client.download_file(
            Bucket=s3_bucket_name, Key=object_name, Filename=file_path
        )
    except ClientError as error:
        logging.error(error)
        return False

    return True


@task
def check_if_files_exist_s3(
    s3_bucket_name: str, object_name: str, s3_config: dict[str, Optional[str]]
) -> bool:
    """Checks if the `object_name` exist in the `s3_bucket_name`

    Args:
        s3_bucket_name (str): name of S3 bucket
        object_name (Optional[str]): a new name of the file object to be saved on S3.  Defaults to None
        s3_config (dict[str, Optional[str]]): a dictionary contaning configs for a S3-like file storage

    Returns:
        bool: True if sucessful or False
    """

    s3_client = boto3.client("s3", **s3_config)

    try:
        # Try to get the metadata of the object
        s3_client.head_object(Bucket=s3_bucket_name, Key=object_name)
        print(f"The file '{object_name}' exists in the bucket '{s3_bucket_name}'.")
        return True

    except ClientError as error:
        logging.error(error)
        # If the client error is a 404 (Object Not Found), the file does not exist
        if error.response["Error"]["Code"] == "404":
            print(
                f"The file '{object_name}' does not exist in the bucket '{s3_bucket_name}'. Create a new one"
            )
        else:
            # Print any other error
            print("An error occurred:", error)
        return False


@flow
def do_stock_update(
    tickers: list[str],
    interval: Literal["1m", "5m", "1h", "1d"],
    s3_bucket_name: str,
    s3_config: dict[str, Optional[str]],
    keep_local_data: bool = False,
) -> bool:
    """Upload ticker data from Yahoo Finance for a list of provided tickers

    Args:
        tickers (list[str]): List of NYSE tickers
        interval (Literal[ &quot;1m&quot;, &quot;5m&quot;, &quot;1h&quot;, ]): Timeframe
        s3_bucket_name (str): name of S3 bucket
        s3_config (dict[str, Optional[str]]): a dictionary contaning configs for a S3-like file storage
        keep_local (str): keep data for local training

    Returns:
        bool: True if sucessful or False
    """

    period = "max"
    if interval == "1m":
        period = "7d"
    elif interval == "1h":
        period = "720d"
    elif interval == "5m":
        period = "60d"

    for ticker_id in tickers:
        ticker_file_name = path.join(f"{ticker_id}_{interval}.csv.gz")
        remote_file_exist = check_if_files_exist_s3(
            object_name=ticker_file_name,
            s3_bucket_name=s3_bucket_name,
            s3_config=s3_config,
        )
        data = yf.Ticker(ticker_id).history(period=period, interval=interval)

        print(f"remote_file_exist: {remote_file_exist}")

        if remote_file_exist is False:
            data.to_csv(ticker_file_name, mode="w", compression="gzip")
            upload_file_s3(
                s3_bucket_name=s3_bucket_name,
                file_path=ticker_file_name,
                object_name=ticker_file_name,
                s3_config=s3_config,
            )
            if not keep_local_data:
                delete_local_file(ticker_file_name)

        else:
            dl_status_success = download_file_s3(
                file_path=ticker_file_name,
                s3_bucket_name=s3_bucket_name,
                object_name=ticker_file_name,
                s3_config=s3_config,
            )
            if dl_status_success and not keep_local_data:
                delete_file_s3(
                    file_path=ticker_file_name,
                    s3_bucket_name=s3_bucket_name,
                    object_name=ticker_file_name,
                    s3_config=s3_config,
                )

            data.to_csv(ticker_file_name, mode="a", header=False, compression="gzip")
            up_status_success = upload_file_s3(
                s3_bucket_name=s3_bucket_name,
                file_path=ticker_file_name,
                object_name=ticker_file_name,
                s3_config=s3_config,
            )
            if not any([dl_status_success, up_status_success]):
                return False
            if not keep_local_data:
                delete_local_file(ticker_file_name)
    return True


if __name__ == "__main__":
    do_stock_update(
        tickers=["NVDA"],
        interval="1d",
        s3_bucket_name=S3_BUCKET,
        s3_config=s3_config,
        keep_local_data=True
    )

@flow
def main():
    do_stock_update(
        tickers=["NVDA"],
        interval="1d",
        s3_bucket_name=S3_BUCKET,
        s3_config=s3_config,
        keep_local_data=True
    )