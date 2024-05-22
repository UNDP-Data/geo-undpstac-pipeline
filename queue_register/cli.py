import argparse
import os
import sys
import datetime
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
import logging

CONNECTION_STRING = os.environ.get('AZURE_SERVICE_BUS_CONNECTION_STRING')
QUEUE_NAME = os.environ.get('AZURE_SERVICE_BUS_QUEUE_NAME')


async def send_message(data_type, days, force_processing=False):
    async with ServiceBusClient.from_connection_string(
            conn_str=CONNECTION_STRING,
            logging_enable=True) as servicebus_client:
        # get a Queue Sender object to send messages to the queue
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
        async with sender:
            batch_message = await sender.create_message_batch()
            for day in days:
                formatted_day = day.strftime('%Y%m%d')
                try:
                    message = f"{data_type},{formatted_day}"
                    if force_processing:
                        message = f"{message},force"
                    logger.info(f"Add message: {message}")
                    batch_message.add_message(ServiceBusMessage(message))
                except ValueError:
                    # ServiceBusMessageBatch object reaches max_size.
                    # New ServiceBusMessageBatch object can be created here to send more data.
                    break
            # Send the batch of messages to the queue
            await sender.send_messages(batch_message)
            logger.info(f"Sent {len(days)} messages into the queue.")

def addCommonArguments(parser):
    parser.add_argument('-t', '--type', type=str,
                              required=True,
                              help='Processed data type, e.g., nighttime',
                              default="nighttime")
    parser.add_argument('-f', '--force', type=bool, action=argparse.BooleanOptionalAction,
                              help='Ignore exiting COG and process again', required=False)
    parser.add_argument('-l', '--log-level', help='Set log level ', type=str, choices=['INFO', 'DEBUG', 'TRACE'],
                              default='INFO')

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
    logger.name = 'queue_register'

    parser = argparse.ArgumentParser(
        description='Register queue message to service bus queue')
    subparsers = parser.add_subparsers(help='main modes of operation', dest='mode', required=True)

    daily_parser = subparsers.add_parser(name='daily',
                                         help='Register a day of message into the queue',
                                         description='Run the pipeline for a specified day by registering to the queue',
                                         usage='python -m queue_register.cli daily -t=nighttime -d=2024-01-25')

    daily_parser.add_argument('-d', '--day', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(),
                              required=True,
                              help='The date where the pipeline will process')
    addCommonArguments(daily_parser)

    archive_parser = subparsers.add_parser(name='archive', help='Register a range of days into the queue',
                                           description='Run the pipeline for every day in a given time interval defined by two dates',
                                           usage='python -m queue_register.cli archive -t=nighttime -s=2023-01-01 -e=2023-03-31')
    archive_parser.add_argument('-s', '--start-date', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(),
                                required=True,
                                help='The start date from where the pipeline will start processing')
    archive_parser.add_argument('-e', '--end-date', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(),
                                required=True,
                                help='The end date signalizing the last day will be processed')
    addCommonArguments(archive_parser)

    yesterday_parser = subparsers.add_parser(name='yesterday',
                                             help='Register yesterday of message into the queue',
                                             description='Run the pipeline for yesterday',
                                             usage='python -m queue_register.cli yesterday -t=nighttime')
    addCommonArguments(yesterday_parser)

    recent_parser = subparsers.add_parser(name='recent',
                                          help='Register recent N days of message into the queue',
                                          description='Run the pipeline for the recent N days. If 5 is set to -n, '
                                                      'total 5 days til yesterday are processed.',
                                          usage='python -m queue_register.cli recent -n=4')
    recent_parser.add_argument('-n', '--number', type=int,
                                  required=True,
                                  help='The number of days before today to processed')
    addCommonArguments(recent_parser)

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    if args.log_level:
        logger.setLevel(args.log_level)

    data_type = args.type
    if args.mode == 'daily':
        day = args.day
        await send_message(data_type, [day], args.force)
    elif args.mode == 'archive':
        start_date = args.start_date
        end_date = args.end_date
        delta = end_date - start_date  # returns timedelta
        days = []
        for i in range(delta.days + 1):
            day = start_date + datetime.timedelta(days=i)
            days.append(day)
        await send_message(data_type, days, args.force)
    elif args.mode == 'yesterday':
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        await send_message(data_type, [yesterday], args.force)
    elif args.mode == 'recent':
        n_days = int(args.number)
        if n_days < 1:
            raise argparse.ArgumentTypeError("-n/--number must be greater than zero.")

        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        start_date = yesterday - datetime.timedelta(days=n_days - 1)
        delta = yesterday - start_date  # returns timedelta
        days = []
        for i in range(delta.days + 1):
            day = start_date + datetime.timedelta(days=i)
            days.append(day)
        await send_message(data_type, days, args.force)
    else:
        pass


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    asyncio.run(main())
