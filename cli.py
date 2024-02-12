import argparse
import logging
from process_nighttime_data import process_nighttime_data
import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    parser = argparse.ArgumentParser(description='Process nighttime lights data from the Earth Observation Group at Colorado School of Mines.')
    parser.add_argument('-year', '-y', type=int, help='The year of the data to download', required=False)
    parser.add_argument('-month', '-m', type=int, help='The month of the data to download', required=False)
    parser.add_argument('-day', '-d', type=int, help='The day of the data to download', required=False)
    args = parser.parse_args()

    # if no date is provided, use yesterday's date
    if not any([args.year, args.month, args.day]): # None of the arguments is provided
        process_nighttime_data(datetime.datetime.now() - datetime.timedelta(days=1))
    elif not all([args.year, args.month, args.day]):
        raise ValueError('If you provide a date, you must provide the year, month, and day')
    else:
        date = datetime.date(args.year, args.month, args.day)
        process_nighttime_data(datetime.datetime(date.year, date.month, date.day))


if __name__ == '__main__':
    main()