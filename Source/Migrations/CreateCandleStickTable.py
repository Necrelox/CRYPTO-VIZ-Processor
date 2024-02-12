from . import IMigration


class CreateCandleStickTable(IMigration):
    @staticmethod
    def up(client):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS candlestick_chart (
                date Date,
                tickers String,
                open Float64,
                close Float64,
                low Float64,
                high Float64,
                volume Float64
            ) ENGINE = MergeTree()
            ORDER BY date
            """
        client.command(create_table_query)

    @staticmethod
    def down(client):
        client.command("DROP TABLE IF EXISTS candlestick_chart")
