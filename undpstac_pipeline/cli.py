import datetime
import logging
import argparse
import os
import asyncio

import sys
from undpstac_pipeline.acore import process_nighttime_data
from undpstac_pipeline.queue import fetch_message_from_queue


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
    parser = argparse.ArgumentParser(description='Process nighttime lights data from the Earth Observation Group Colorado School of Mines.')
    subparsers = parser.add_subparsers(help='main modes of operation', dest='mode', required=True)
    daily_parser = subparsers.add_parser(name='daily',
                                         help='Run the pipeline in operational/daily mode',
                                        description='Run the pipeline for a specified or previous day',
                                         usage='\npython -m undpstac_pipeline.cli -y 2024 -m 1 -d 25\n' \
                                         'python -m undpstac_pipeline.cli ')

    daily_parser.add_argument( '-y', '--year', type=int, help='The year of the data to download', required=False)
    daily_parser.add_argument('-m', '--month', type=int, help='The month of the data to download', required=False)
    daily_parser.add_argument('-d', '--day' , type=int, help='The day of the data to download', required=False)
    #lonmin=-180, latmin=-65, lonmax=180, latmax=75
    daily_parser.add_argument( '--lonmin' , type=float, help='The western bound', required=False, default=-180)
    daily_parser.add_argument( '--latmin' , type=float, help='The southern bound', required=False, default=-65)
    daily_parser.add_argument( '--lonmax' , type=float, help='The eastern bound', required=False, default=180)
    daily_parser.add_argument( '--latmax' , type=float, help='The northern bound', required=False, default=75)

    daily_parser.add_argument('-f', '--force' , type=bool, action=argparse.BooleanOptionalAction, help='Ignore exiting COG and process again', required=False)
    daily_parser.add_argument('-l', '--log-level', help='Set log level ', type=str, choices=['INFO', 'DEBUG', 'TRACE'],
                        default='INFO')


    archive_parser = subparsers.add_parser(name='archive', help='Run the pipeline in archive mode',
                                         description='Run the pipeline for every day in a given time interval defined by two dates',
                                        usage='python -m undpstac_pipeline.cli archive -s=2023-01-01 -e=2023-03-31')
    archive_parser.add_argument('-s', '--start-date', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(), required=True,
                                help='The start date from where the pipeline will start processing the VIIRS DNB mosaics')
    archive_parser.add_argument('-e', '--end-date', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(), required=True,
                                help='The end date signalizing the last day for which the VIIRS DNB mosaics will be processed')
    archive_parser.add_argument('--lonmin', type=float, help='The western bound', required=False, default=-180)
    archive_parser.add_argument('--latmin', type=float, help='The southern bound', required=False, default=-65)
    archive_parser.add_argument('--lonmax', type=float, help='The eastern bound', required=False, default=180)
    archive_parser.add_argument('--latmax', type=float, help='The northern bound', required=False, default=75)


    archive_parser.add_argument('-f', '--force', type=bool, action=argparse.BooleanOptionalAction,
                              help='Ignore exiting COG and process again', required=False)
    archive_parser.add_argument('-l', '--log-level', help='Set log level ', type=str, choices=['INFO', 'DEBUG', 'TRACE'],
                              default='INFO')

    busqueue_parser = subparsers.add_parser(
        name="queue",
        help="Run the pipeline in service bus queue mode",
        description="Pull a message from the service bus queue to ingest a day of data. \
        A message must be added into service bus queue by following the format of 'type_name,yyyyMMdd'. \
        e.g., 'nighttime,20240201'",
        usage="python -m undpstac_pipeline.cli queue"
    )
    busqueue_parser.add_argument('-f', '--force', type=bool, action=argparse.BooleanOptionalAction,
                                help='Ignore exiting COG and process again', required=False)
    busqueue_parser.add_argument('-l', '--log-level', help='Set log level ', type=str,
                                choices=['INFO', 'DEBUG', 'TRACE'],
                                default='INFO')

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    if args.log_level:
        logger.setLevel(args.log_level)
    #args = parser.parse_args()
    if args.mode == 'daily':
        # if no date is provided, use yesterday's date
        if not any([args.year, args.month, args.day]): # None of the arguments is provided
            yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
            await process_nighttime_data(yesterday)
        elif not all([args.year, args.month, args.day]):
            raise ValueError('If you provide a date, you must provide the year, month, and day')
        else:
            date = datetime.date(args.year, args.month, args.day)
            await process_nighttime_data(
                date=datetime.datetime(date.year, date.month, date.day).date(),
                lonmin=args.lonmin, latmin=args.latmin, lonmax=args.lonmax, latmax=args.latmax,
                force_processing=args.force)
    if args.mode == 'archive':
        start_date = args.start_date
        end_date = args.end_date
        delta = end_date - start_date  # returns timedelta
        for i in range(delta.days + 1):
            day = start_date + datetime.timedelta(days=i)
            await process_nighttime_data(date=day,
                                         lonmin=args.lonmin, latmin=args.latmin, lonmax=args.lonmax, latmax=args.latmax,
                                         force_processing=args.force, archive=True)
    if args.mode == 'queue':
        servicebus_conn_str = os.environ.get('AZURE_SERVICE_BUS_CONNECTION_STRING')
        servicebus_queue_name = os.environ.get('AZURE_SERVICE_BUS_QUEUE_NAME')
        messages = await fetch_message_from_queue(servicebus_queue_name, servicebus_conn_str)
        for msg in messages:
            day = msg["date"]
            date_type = msg["type"]
            logger.info(f"Start processing {str(day)} for {date_type}")

            if date_type == "nighttime":
                await process_nighttime_data(
                    date=day,
                    force_processing=args.force)


def run_pipeline():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Pipeline was cancelled by the user')


if __name__ == '__main__':
    run_pipeline()