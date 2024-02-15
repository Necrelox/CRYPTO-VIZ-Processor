from . import IMigration


class CreateDataTable(IMigration):
    @staticmethod
    def up(client):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS kline_chart (
                date DateTime, 
                tickers String,
                open Float64,
                close Float64,
                high Float64,
                low Float64,
                volume Float64
            ) ENGINE = MergeTree()
            ORDER BY date
            """
        client.command(create_table_query)

    @staticmethod
    def down(client):
        client.command("DROP TABLE IF EXISTS kline_chart")
