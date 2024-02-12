from dotenv import load_dotenv

load_dotenv()

from Database import Database
from BinanceCryptoDataConsumer import BinanceCryptoDataConsumer

import argparse


def main():
    parser = argparse.ArgumentParser(description="Gestionnaire de script avec migrations")
    parser.add_argument(
        'migration',
        nargs='?',
        help='run migrations',
        default='start'
    )
    parser.add_argument(
        '-r',
        '--rollback',
        action='store_true',
        help='Rollback les dernières migrations'
    )

    args = parser.parse_args()

    if args.migration == 'migration':
        if args.rollback:
            rollback_migrations()
        else:
            apply_migrations()
    else:
        start_script()


def apply_migrations():
    print("Application des migrations...")
    try:
        database = Database.get_instance()
        database.run_all_migrations()
    except Exception as e:
        print(e)


def rollback_migrations():
    print("Rollback des migrations...")
    try:
        database = Database.get_instance()
        database.rollback_all_migrations()
    except Exception as e:
        print(e)


def start_script():
    print("Démarrage du script...")
    try:
        binance_data_consumer = BinanceCryptoDataConsumer()
        binance_data_consumer.run()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
