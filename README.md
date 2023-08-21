# MLops ZoomcampCapstone Project

Cohort 2023

It is a simple prove of concept project to automate data retrival, model trainig and serving.
The dummy model (`logistic regression`) predicts if `NVDA` stock will close higher next day based on todays "Open", "High", "Low", "Close". 
The data is gathered from Yahoo Finance by `src/get_data.py` prefect workflow.
The model training (`src/train.py`) is deployed to run every Saturday at 12:00.

Tp build the project run `docker-compose --env-file .env -f docker-compose.yaml --profile all  up --build -d`. 