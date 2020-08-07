import requests
import pandas as pd
from typing import Dict

def get_temp_data(dt='2019-10-10'):
    url = f"https://api.data.gov.sg/v1/environment/air-temperature?date={dt}"
    resp = requests.get(url)
    return resp.json()

def choose_station(temp_data: Dict, station_id: str):
    output = []
    for item in temp_data['items']:
        ts = item['timestamp']
        temperature = None
        try:
            temperature = [temp['value'] for temp in item['readings'] if temp['station_id'] == station_id ][0]
        except IndexError:
            pass
        output.append({
            'time': ts,
            'temp': temperature,
        })
    return pd.DataFrame(output)

def avg_temp_by_hour(df):
    df['hour'] = df['time'].apply(lambda x: x[:13])
    return df.groupby(['hour']).mean().plot(figsize=(20, 12)).figure
