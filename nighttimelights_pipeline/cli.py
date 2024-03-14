import datetime
import logging
import argparse
import os
import asyncio
from nighttimelights_pipeline.acore import process_nighttime_data

async def main():
    logging.basicConfig()
    azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
    azlogger.setLevel(logging.WARNING)
    logger = logging.getLogger()
    logging_stream_handler = logging.StreamHandler()
    logging_stream_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(logging_stream_handler)
    logger.name = 'nighttimelights'
    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    assert conn_str not in ['', None], f'invalid AZURE_STORAGE_CONNECTION_STRING={conn_str}'
    parser = argparse.ArgumentParser(description='Process nighttime lights data from the Earth Observation Group at Colorado School of Mines.')
    parser.add_argument('-year', '-y', type=int, help='The year of the data to download', required=False)
    parser.add_argument('-month', '-m', type=int, help='The month of the data to download', required=False)
    parser.add_argument('-day', '-d', type=int, help='The day of the data to download', required=False)
    #args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    args = parser.parse_args()

    # if no date is provided, use yesterday's date
    if not any([args.year, args.month, args.day]): # None of the arguments is provided
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        await process_nighttime_data(yesterday)
    elif not all([args.year, args.month, args.day]):
        raise ValueError('If you provide a date, you must provide the year, month, and day')
    else:
        date = datetime.date(args.year, args.month, args.day)
        await process_nighttime_data(datetime.datetime(date.year, date.month, date.day).date())

def run_pipeline():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Pipeline was cancelled by the user')

if __name__ == '__main__':
    run_pipeline()