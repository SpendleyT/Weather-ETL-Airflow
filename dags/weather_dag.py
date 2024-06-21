from datetime import datetime
import json
from pandas import json_normalize

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from weather_app.processors.extract import ExtractData
from weather_app.processors.transform import TransformData


def _get_clean_weather(ti):
    weather_data = TransformData()
    weather_data.to_csv('/tmp/weather_data.csv', index=None, header=False)


def _load_data():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY weather_info FROM stdin WITH DELIMITER as ','",
        filename='/tmp/weather_data.csv'
    )


with DAG('get_daily_weather', start_date=datetime(2024,6,18), 
         schedule_interval='@daily', catchup=False) as dag:
    
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS weather_info (
                city TEXT NOT NULL,
                date DATE NOT NULL,
                sunrise TIME,
                sunset TIME,
                temp_celsius NUMERIC(5, 2) NOT NULL,
                humidity INTEGER NOT NULL DEFAULT 0,
                uv_index INTEGER NOT NULL DEFAULT 0
            );
        '''
    )

    extract_weather = PythonOperator(
        task_id = 'extract_weather',
        python_callable = ExtractData
    )

    transform_weather = PythonOperator(
        task_id = 'transform_weather',
        python_callable = _get_clean_weather
    )

    load_weather = PythonOperator(
        task_id = 'load_weather',
        python_callable = _load_data
    )


    create_table >> extract_weather >> transform_weather >> load_weather