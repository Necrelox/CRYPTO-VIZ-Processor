import os
import json
from datetime import datetime

from kafka import KafkaConsumer

from Database import Database


class BinanceCryptoDataConsumer:
    __consumer = None
    __database = None

    @staticmethod
    def _get_brokers():
        from dotenv import load_dotenv
        load_dotenv()
        brokers = os.getenv('RED_PANDA_BROKERS')
        brokers = brokers.split(',')
        return brokers

    def __init__(self):
        brokers = BinanceCryptoDataConsumer._get_brokers()
        topic = 'binance-ws-market-data'
        self.__consumer = KafkaConsumer(
            topic,
            client_id='processor',
            group_id='processor',
            bootstrap_servers=brokers,
            auto_offset_reset='earliest'
        )
        self.__database = Database.get_instance()

    @staticmethod
    def __format_data(data):
        date = datetime.fromtimestamp(float(data['E']) / 1000)
        row = [
            date,
            data['s'],
            float(data['k']['o']),  # open
            float(data['k']['c']),  # close
            float(data['k']['h']),  # high
            float(data['k']['l']),  # low
            float(data['k']['v']),  # volume
        ]
        return row

    def __callback(self, data):
        data = json.loads(data.value)
        row = BinanceCryptoDataConsumer.__format_data(data)
        print(row)
        self.__database.insert('kline_chart', [row])

    def run(self):
        if self.__consumer:
            for message in self.__consumer:
                self.__callback(message)
