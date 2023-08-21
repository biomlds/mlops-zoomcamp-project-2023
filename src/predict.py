from os import environ

import mlflow
import pandas as pd
from flask import Flask, jsonify, request

TRACKING_SERVER_HOST = environ.get("MLFLOW_TRACKING_SERVER_HOST")
MODEL_SERVER_PORT = environ.get("MODEL_SERVER_PORT")

def get_production_model(TRACKING_SERVER_HOST=TRACKING_SERVER_HOST):

    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000")


    production_model_run_id = mlflow.search_registered_models(filter_string="name='will-stock-go-up-via-api'")[0].latest_versions[0].run_id
    production_model_uri = f'runs:/{production_model_run_id}/model'

    # Load model as a PyFuncModel.
    production_model = mlflow.pyfunc.load_model(production_model_uri)

    return production_model



def predict(data):
    X = pd.DataFrame(data, index=[0])
    model = get_production_model()
    preds = model.predict(X)
    return float(preds)

app = Flask('stock-uptrend-prediction')


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    data = request.get_json()
    print(data)
    

    # features = prepare_features(data)
    pred = predict(data)
    
    result = {
        'go_up': pred
    }

    return jsonify(result)

@app.route('/test', methods=['GET'])
def test():
    return "Hi"

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=MODEL_SERVER_PORT)