import os
import json
from datetime import datetime
import clickhouse_connect
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()


def get_brokers():
    brokers = os.getenv('RED_PANDA_BROKERS')
    brokers = brokers.split(',')
    return brokers


def get_consumer(brokers):
    topic = 'binance-ws-market-data'
    consumer = KafkaConsumer(
        topic,
        client_id="processor",
        group_id='processor',
        bootstrap_servers=brokers,
        auto_offset_reset='earliest',
    )
    return consumer


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST'),
        database=os.getenv('CLICKHOUSE_DB'),
        username=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )


def create_table_candlestick_chart(clickhouse_client):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS candlestick_chart (
            date Date,
            open Float64,
            close Float64,
            low Float64,
            high Float64,
            volume Float64
        ) ENGINE = MergeTree()
        ORDER BY date
    """
    clickhouse_client.command(create_table_query)


def insert_into_candlestick_chart(clickhouse_client, row):
    clickhouse_client.insert('candlestick_chart', [row])


def validate_data_is_crypto_data_dto(data):
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


def handle_message(consumer, clickhouse_client):
    for message in consumer:
        data = json.loads(message.value)
        if not validate_data_is_crypto_data_dto(data):
            continue
        else:
            print(data)
            date = datetime.fromtimestamp(float(data['E']) / 1000).date()
            row = [
                date,
                float(data['o']),
                float(data['c']),
                float(data['l']),
                float(data['h']),
                float(data['v']),
            ]
            insert_into_candlestick_chart(clickhouse_client, row)


def main():
    brokers = get_brokers()
    consumer = get_consumer(brokers)
    clickhouse_client = get_clickhouse_client()
    create_table_candlestick_chart(clickhouse_client)
    handle_message(consumer, clickhouse_client)


if __name__ == "__main__":
    main()
