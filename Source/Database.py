import os

import clickhouse_connect
from dotenv import load_dotenv

from Migrations import CreateCandleStickTable


class Database:
    __instance = None
    __client = None
    __migrations = [
        CreateCandleStickTable,
    ]

    @staticmethod
    def get_instance():
        if Database.__instance is None:
            Database()
        return Database.__instance

    def __connect(self):
        self.__client = clickhouse_connect.get_client(
            host=self.host,
            database=self.database,
            port=8123,
            username=self.username,
            password=self.password
        )

    def __init__(self):
        if Database.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            Database.__instance = self
            load_dotenv()
            self.host = os.getenv("CLICKHOUSE_HOST")
            self.database = os.getenv("CLICKHOUSE_DB")
            self.username = os.getenv("CLICKHOUSE_USER")
            self.password = os.getenv("CLICKHOUSE_PASSWORD")
            self.__connect()

    def insert(self, table, data):
        self.__client.insert(table, data)

    def run_all_migrations(self):
        for migration in self.__migrations:
            migration.up(self.__client)
            print(f"Migration {migration.__name__} applied")

    def rollback_all_migrations(self):
        for migration in reversed(self.__migrations):
            migration.down(self.__client)
            print(f"Migration {migration.__name__} rollbacked")

    def getter_client(self):
        return self.__client
