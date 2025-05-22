import json
import requests
from datetime import datetime
from config import INPUT_PATH_DATA

# Create function to get data
def get_data():
    time_get_data = datetime.now()
    print("Fetch raw json data at", time_get_data)
    req = requests.get("https://data.sensor.community/static/v2/data.24h.json")
    data = json.loads(req.content)
    return data, time_get_data

def save_data(data, date_get_data):
    date_get_data = date_get_data.strftime("%Y%m%d_%H%M")
    with open(f"{INPUT_PATH_DATA}/data_24h_{date_get_data}.json","w") as f:
        json.dump(data,f)
        f.close()

# data, date_get_data = get_data()
# save_data(data, date_get_data)
