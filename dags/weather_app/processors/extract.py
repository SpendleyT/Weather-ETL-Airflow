#imports
import requests
import time
import datetime
from dateutil.relativedelta import relativedelta
import logging
from weather_app.configs.config import api_key, geoapify_key
from weather_app.common.s3 import S3BucketConfigs, s3BucketConnector


logger = logging.getLogger(__name__)


class ExtractData():
    def __init__(self):
        self.weather_url = "https://api.openweathermap.org/data/3.0/onecall/timemachine?"
        self.geoapify_url = "https://api.geoapify.com/v1/geocode/search?"

        self._city = 'city'
        self._country = 'country'
        self._cities = ['Athens', 'Madrid', 'Copenhagen', 'Dublin', 'Paris', 'London', 'Berlin', 'Geneva', 'Prague', 'Warsaw']
        self.bucket_conn = s3BucketConnector(S3BucketConfigs.ETL_BUCKET.value)


    def get_lat_long_for_city(self, city):
        geo_params = {
            'text': city,
            'lang': 'en',
            'limit': 10,
            'type': 'city',
            'apiKey': geoapify_key
        }
        try:
            response = requests.get(self.geoapify_url, params=geo_params).json()
            latitude = response['features'][0]['properties']['lat']
            longitude = response['features'][0]['properties']['lon']
        except KeyError as ke:
            logger.error(f"Error retrieving data for {city}: {ke} -- {time.ctime()}")
        return latitude, longitude
    
    def get_weather_for_city(self, lat, lon, date):
        stamp = int(time.mktime(date.timetuple()))
        #Build params
        weather_params = {
            'lat': lat,
            'lon': lon,
            'dt': stamp,
            'units': 'metric',
            'appid': api_key
        }
        response = requests.get(self.weather_url, params=weather_params).json()
        city_info = response['data'][0]
        return city_info
    
    def extract_weather_records(self):
        logger.info(f"Start extract run at {time.ctime()}.")
        get_date = datetime.datetime.now() - relativedelta(days=1)
        final_data = {}
        for city in self._cities:
            lat, lon = self.get_lat_long_for_city(city)
            weather_data = self.get_weather_for_city(lat, lon, get_date)

            history_info = {}
            for item in weather_data: 
                if item != 'weather':
                    history_info[item] = weather_data.get(item)

            final_data[city] = history_info
        key = "-".join(
            (f'data_files/weather', 
             str(get_date.year), 
             str(get_date.month), 
             str(get_date.day))
        ) + ".json"
        self.bucket_conn.write_json_to_s3(final_data, key)
        
        print(f"End run at {time.ctime()}.")