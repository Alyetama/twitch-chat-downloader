#!/usr/bin/env python
# coding: utf-8

import argparse
import copy
import hashlib
import json
import os
import signal
import sys
import uuid
from datetime import datetime, timedelta

import pymongo
import requests
from dotenv import load_dotenv
from tqdm import tqdm


def keyboard_interrupt_handler(sig: int, _) -> None:
    print(f'KeyboardInterrupt (id: {sig}) has been caught...')
    print('Terminating the session gracefully...')
    sys.exit(1)


def opts() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        epilog='Export MongoDB connection string as '
        '`MONGODB_CONNECTION_STRING` before running the script, or add it to '
        '`.env`.')
    parser.add_argument('-d',
                        '--database-name',
                        help='MongoDB database name',
                        type=str)
    parser.add_argument('-c',
                        '--channel-name',
                        help='Channel name (MongoDB collection name)',
                        type=str)
    parser.add_argument(
        '-f',
        '--start-from-day',
        help='Start from this date (format: YYYY/M/D, example: 2021/12/31)',
        type=str)
    return parser.parse_args()


def main(database_name: str, channel_name: str, start_from_day: str) -> None:
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)

    client = pymongo.MongoClient(os.environ['MONGODB_CONNECTION_STRING'])
    db = client[database_name]
    col = db[channel_name]
    metadata_col = db[f'{channel_name}_metadata']

    first_day_obj = datetime.strptime(start_from_day, '%Y/%m/%d').date()

    existing_dates = [
        str(x['date'].date()).replace('-', '/')
        for x in list(metadata_col.find({}))
    ]

    i = 1
    dates = []

    while True:
        date = first_day_obj + timedelta(days=i)
        if date > datetime.today().date():
            break
        date_path = str(date).replace('-', '/')
        dates.append(date_path)
        i += 1

    for date_path in tqdm(dates):
        if date_path in existing_dates:
            print('Skipping...')
            continue

        resp = requests.get(
            f'https://logs.ivr.fi/channel/{channel_name}/{date_path}?json=true'
        )
        data = resp.json()
        _data = copy.deepcopy(data)

        for msg in data['messages']:
            msg.update({'_id': str(uuid.uuid4())})

        col.insert_many(data['messages'])

        md5_checksum = hashlib.md5(
            json.dumps(_data).encode('utf-8')).hexdigest()

        date_obj = datetime.strptime(date_path, '%Y/%m/%d')
        metadata_col.insert_one({
            '_id': md5_checksum,
            'date': date_obj,
            'messages': len(data['messages'])
        })


if __name__ == '__main__':
    load_dotenv()
    args = opts()
    main(database_name=args.database_name,
         channel_name=args.channel_name,
         start_from_day=args.start_from_day)
