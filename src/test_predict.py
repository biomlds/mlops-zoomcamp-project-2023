import requests

data = {
    "Open": 426.,
    "High": 435.,
    "Low": 416.,
    "Close": 432.
}

url = 'http://localhost:9696/predict'
response = requests.post(url, json=data)
print("Response:", response.json())