import requests
import json
import time
from datetime import datetime
from pymongo import MongoClient

API_KEY = '8f595392e9c2431cc79b082bda23c905'
CITIES = ['new york', 'delhi', 'kolkata', 'london', 'toronto']
URL = 'http://api.openweathermap.org/data/2.5/weather'
MONGO_URI = 'mongodb+srv://MyChats:Anubhob435Dey@cluster0.jmkic.mongodb.net/'
DB_NAME = 'Weather'  # Changed from 'weather' to 'Weather'
COLLECTION_NAME = 'records'

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def get_weather_data(city):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(URL, params=params)
    return response.json()

def update_weather_records():
    weather_data = []
    for city in CITIES:
        data = get_weather_data(city)
        record = {
            'city': city,
            'timestamp': datetime.now().isoformat()
        }
        if 'main' in data and 'weather' in data:
            record.update({
                'temperature': data['main']['temp'],
                'description': data['weather'][0]['description']
            })
        else:
            record['error'] = 'Could not retrieve data'
        weather_data.append(record)
    collection.insert_many(weather_data)

if __name__ == '__main__':
    while True:
        update_weather_records()
        time.sleep(900)  # Sleep for 15 minutes
