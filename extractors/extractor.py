import pandas as pd

from utils.DBHandler import DBHandler, SANTANDER_DB_NAME


class Extractor:

    def __init__(self, client=SANTANDER_DB_NAME):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(client)

    def join_logs_bdi(self, logs_query={}, bdi_query={}):
        logs = pd.DataFrame.from_records([doc for doc in self.db.logs.find(logs_query)])
        bdi = pd.DataFrame.from_records([doc for doc in self.db.bdi.find(bdi_query)])
        bdi.rename(columns={'_id': 'atm'}, inplace=True)

        merged = pd.merge(logs, bdi, on='atm')

        merged.to_csv("merged.csv")
