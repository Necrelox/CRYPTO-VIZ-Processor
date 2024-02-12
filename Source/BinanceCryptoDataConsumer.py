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
    def __validate_data_is_crypto_data_dto(data):
        expected_keys = {
            'e': str,
            'E': int,
            's': str,
            'p': str,
            'P': str,
            'w': str,
            'x': str,
            'c': str,
            'Q': str,
            'b': str,
            'B': str,
            'a': str,
            'A': str,
            'o': str,
            'h': str,
            'l': str,
            'v': str,
            'q': str,
            'O': int,
            'C': int,
            'F': int,
            'L': int,
            'n': int
        }
        for key, value_type in expected_keys.items():
            if key not in data or not isinstance(data[key], value_type):
                return False
        return True

    @staticmethod
    def __format_data(data):
        date = datetime.fromtimestamp(float(data['E']) / 1000)
        row = [
            date.date(),
            data['s'],
            float(data['o']),
            float(data['c']),
            float(data['l']),
            float(data['h']),
            float(data['v']),
        ]
        return row

    def __callback(self, data):
        data = json.loads(data.value)

        if not BinanceCryptoDataConsumer.__validate_data_is_crypto_data_dto(data):
            return
        else:
            row = BinanceCryptoDataConsumer.__format_data(data)
            print(row)
            self.__database.insert('candlestick_chart', [row])

    def run(self):
        if self.__consumer:
            for message in self.__consumer:
                self.__callback(message)
