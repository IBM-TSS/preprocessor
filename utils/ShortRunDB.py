from pymongo import MongoClient

from utils.environment import get_mongo_url


class ShortRunDB:
    mongo_client = None

    def __init__(self, **kwargs):
        self.mongo_client = MongoClient(get_mongo_url())

    def load_open_tickets(self, tickets):
        self.mongo_client['tickets']['not_assigned'].insert_many(tickets[:10])

    def __del__(self):
        if self.mongo_client is not None:
            self.mongo_client.close()

    def __exit__(self, exc_type, exc_value, traceback):
        if self.mongo_client is not None:
            self.mongo_client.close()

    def __enter__(self):
        return self
