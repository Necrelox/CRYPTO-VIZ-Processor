class IMigration:
    @staticmethod
    def up(client):
        raise NotImplementedError()

    @staticmethod
    def down(client):
        raise NotImplementedError()
