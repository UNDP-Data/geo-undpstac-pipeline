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

async def send_message(type, days):
    async with ServiceBusClient.from_connection_string(
            conn_str=CONNECTION_STRING,
            logging_enable=True) as servicebus_client:
        # get a Queue Sender object to send messages to the queue
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
        async with sender:
            batch_message = await sender.create_message_batch()
            for day in days:
                formattedDay = day.strftime('%Y%m%d')
                try:
                    message = f"{type},{formattedDay}"
                    logger.info(f"Add message: {message}")
                    batch_message.add_message(ServiceBusMessage(message))
                except ValueError:
                    # ServiceBusMessageBatch object reaches max_size.
                    # New ServiceBusMessageBatch object can be created here to send more data.
                    break
            # Send the batch of messages to the queue
            await sender.send_messages(batch_message)
            logger.info(f"Sent {len(days)} messages into the queue.")


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
    daily_parser.add_argument('-t', '--type', type=str,
                              required=True,
                              help='Processed data type, e.g., nighttime',
                              default="nighttime")
    daily_parser.add_argument('-d', '--day', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(),
                              required=True,
                              help='The date where the pipeline will process')

    archive_parser = subparsers.add_parser(name='archive', help='Register a range of days into the queue',
                                           description='Run the pipeline for every day in a given time interval defined by two dates',
                                           usage='python -m queue_register.cli archive -t=nighttime -s=2023-01-01 -e=2023-03-31')
    archive_parser.add_argument('-t', '--type', type=str,
                                required=True,
                                help='Processed data type, e.g., nighttime',
                                default="nighttime")
    archive_parser.add_argument('-s', '--start-date', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(),
                                required=True,
                                help='The start date from where the pipeline will start processing')
    archive_parser.add_argument('-e', '--end-date', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(),
                                required=True,
                                help='The end date signalizing the last day will be processed')

    yesterday_parser = subparsers.add_parser(name='yesterday',
                                              help='Register yesterday of message into the queue',
                                              description='Run the pipeline for yesterday',
                                              usage='python -m queue_register.cli yesterday -t=nighttime')
    yesterday_parser.add_argument('-t', '--type', type=str,
                                   required=True,
                                   help='Processed data type, e.g., nighttime',
                                   default="nighttime")

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    dataType = args.type
    if args.mode == 'daily':
        day = args.day
        await send_message(dataType, [day])
    elif args.mode == 'archive':
        start_date = args.start_date
        end_date = args.end_date
        delta = end_date - start_date  # returns timedelta
        days = []
        for i in range(delta.days + 1):
            day = start_date + datetime.timedelta(days=i)
            days.append(day)
        await send_message(dataType, days)
    elif args.mode == 'yesterday':
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        await send_message(dataType, [yesterday])
    else:
        pass

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    asyncio.run(main())
