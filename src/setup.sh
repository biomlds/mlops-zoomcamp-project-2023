python create_s3_bucket.py
prefect deployment build train.py:run_train -n "train model" --cron "0 12 * * 6"
prefect deployment build get_data.py:main -n "get data train" -a