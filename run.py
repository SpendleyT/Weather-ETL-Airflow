#imports
import schedule
from common import log
from processors.extract import ExtractData
import logging
import time

logger = logging.getLogger(__name__)

def main():
    #Get logger
    logger = log.setup_custom_logger('root')

    #start ETL process
    logger.info(f"Starting ETL job -- {time.ctime()}")
    extract = ExtractData()
    extract.extract_weather_records()

    

    #End process
    logger.info(f"Starting ETL job -- {time.ctime()}")



if __name__ == "__main__":
    main()