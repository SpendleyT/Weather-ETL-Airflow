#imports
import time
import logging
import pandas as pd

from configs.config import api_key, geoapify_key
from common.s3 import S3BucketConfigs, s3BucketConnector

#Create logging object
logger = logging.getLogger(__name__)


class TransformData():
    def __init__(self) -> None:
        self.bucket_conn = s3BucketConnector(S3BucketConfigs.ETL_BUCKET.value)

    
    def clean_data(self) -> pd.Dataframe:
        files_to_transform = self.bucket_conn.list_files_in_prefix('data_files/')
        full_df = pd.DataFrame()
        for json_file in files_to_transform[1:]:
            logger.info(f"Processing file {json_file} for transform: " + time.ctime())
            try:
                weather_df = self.bucket_conn.read_json_to_df(json_file)
                weather_df = weather_df.drop([
                    'feels_like', 'pressure', 'dew_point', 'clouds', 
                    'visibility', 'wind_speed', 'wind_deg', 'wind_gust'], axis=1)
                weather_df[['dt', 'sunrise', 'sunset']] = pd.to_datetime(weather_df[
                    ['dt', 'sunrise', 'sunset']
                    ].stack(), unit='s').unstack()
                weather_df['dt'] = weather_df['dt'].dt.date
                weather_df['sunrise'] = weather_df['sunrise'].dt.time
                weather_df['sunset'] = weather_df['sunset'].dt.time
                weather_df['temp_imperial'] = round((weather_df['temp'] * 1.8) + 32, 2)
                weather_df = weather_df.reset_index()
                weather_df = weather_df.rename(columns={"index": "city", "dt": "date", 
                                "temp": "temp_celsius", "uvi": "uv_index"})
                weather_df = weather_df[['city', 'date', 'sunrise', 'sunset', 
                                'temp_celsius', 'temp_imperial', 'humidity', 'uv_index']]
                full_df = pd.concat([full_df, weather_df], ignore_index=True)
            except:
                logger.error(f"Unknown Error processing {json_file} for load. " + time.ctime())
        #write files to archive folder
        self.bucket_conn.move_files_to_archive(files_to_transform)
        #return full_df for use in load method
        return full_df