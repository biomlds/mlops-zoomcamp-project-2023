# Load the Iris dataset (for demonstration)
import warnings
from os import environ, path, remove
from typing import Literal, Optional

import joblib
import mlflow
import numpy as np
import pandas as pd
from get_data import delete_local_file, do_stock_update
from mlflow.tracking.client import MlflowClient
from prefect import flow, task
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC

# %%
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

experiment_name="will-stock-go-up"
ticker_id="NVDA"
interval="1d"
s3_bucket_name=S3_BUCKET


@flow
def run_train(
    experiment_name: str,
    ticker_id: str,
    interval: Literal["1m", "5m", "1h", "1d"],
    s3_bucket_name: str,
    s3_config: dict[str, Optional[str]],
    keep_local_data: bool = False,
    selection_metrics: str = "metrics.accuracy",
    higher_metric_is_better: bool = True,
    model_stage: str = "production",
):
    TRACKING_SERVER_HOST = environ.get("MLFLOW_TRACKING_SERVER_HOST")

    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000")

    ticker_file_name = path.join(f"{ticker_id}_{interval}.csv.gz")

    do_stock_update(
        tickers=[ticker_id],
        interval=interval,
        s3_bucket_name=s3_bucket_name,
        s3_config=s3_config,
        keep_local_data=True,
    )

    data = pd.read_csv(ticker_file_name, index_col=0)

    # mlflow.set_tracking_uri(os.environ.get('MLFLOW_TRACKING_URI'))
    model_name = f"{experiment_name}-via-api"

    mlflow.set_experiment(experiment_name)
    mlflow.autolog()

    with mlflow.start_run():
        

        # Split data into features (X) and target (y)
        X = data[["Open", "High", "Low", "Close"]]
        y = np.where(
            data["Open"].shift(-1) > data["Close"], 1, -1
        )  # Label: 1 if next day's Close > today's Close, else -1

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Create a pipeline with preprocessing and classifier
        pipeline = make_pipeline(StandardScaler(), LogisticRegression())

        # Define hyperparameters for GridSearch
        param_grid = {
            "logisticregression__C": [0.001, 0.01, 0.1, 1, 10],
        }

        # Create GridSearchCV object
        grid_search = GridSearchCV(pipeline, param_grid, cv=5, scoring="accuracy", n_jobs=-1)

        # Fit the model using GridSearchCV
        grid_search.fit(X_train, y_train)

        # pipeline.fit(X_train, y_train)
        # Print the best hyperparameters and the corresponding accuracy
        print("Best Parameters:", grid_search.best_params_)
        print("Best Cross-Validated Accuracy:", grid_search.best_score_)

        # Evaluate the model on the test set
        test_accuracy = grid_search.best_estimator_.score(X_test, y_test)
        print("Test Accuracy:", test_accuracy)
        mlflow.log_metric("Test_Accuracy", test_accuracy)
        current_run_id = mlflow.active_run().info.run_id

        selection_metrics = "metrics.mean_test_score"
        # Specify your filter criteria
        filter_string = "status = 'Active'"

    
    df = mlflow.search_runs(
        experiment_names=[experiment_name],
        order_by=[selection_metrics],
        # filter_string=filter_string,
    )

    if higher_metric_is_better is False:
        best_model_run_id = df["run_id"].iloc[0] if len(df) > 0 else current_run_id
        two_best_model_selection_metrics = (
            df[selection_metrics].loc[:1].to_numpy()
            if len(df) > 0
            else np.array([-999])
        )
    if higher_metric_is_better is True:
        best_model_run_id = df["run_id"].iloc[-1] if len(df) > 0 else current_run_id
        two_best_model_selection_metrics = (
            df[selection_metrics].loc[-1:].to_numpy()
            if len(df) > 0
            else np.array([-999])
        )
    best_model_selection_metrics = two_best_model_selection_metrics[0]

    client = MlflowClient()

    if current_run_id == best_model_run_id and (
        two_best_model_selection_metrics.min() != two_best_model_selection_metrics.max()
        or len(two_best_model_selection_metrics) == 1
    ):
        model_uri = f"runs:/{best_model_run_id}/model"
        # register model
        model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

        client.transition_model_version_stage(
            name=model_details.name,
            version=model_details.version,
            stage=model_stage,
            archive_existing_versions=True,
        )

        client.update_model_version(
            name=model_details.name,
            version=model_details.version,
            description=f"{best_model_selection_metrics}",
        )

        model_version_details = client.get_model_version(
            name=model_details.name, version=model_details.version
        )
        print(f"The current model stage is: {model_version_details.current_stage}")
    else:
        mlflow.delete_run(current_run_id)

        print(
            "This model does NOT perform beter than current one. \
                The run and the model were discarded"
        )
    delete_local_file(ticker_file_name)



if __name__ == "__main__":
    run_train(
        experiment_name="will-stock-go-up",
        ticker_id="NVDA",
        interval="1d",
        s3_bucket_name=S3_BUCKET,
        s3_config=s3_config,
    )
