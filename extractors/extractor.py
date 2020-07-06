import datetime

import pandas as pd

from utils.DBHandler import DBHandler, SANTANDER_DB_NAME


class Extractor:

    def __init__(self, client=SANTANDER_DB_NAME):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(client)

    def join_logs_bdi(self, logs_query={}, bdi_query={}):
        logs = pd.DataFrame.from_records(
            [doc for doc in self.db.logs.find(logs_query)])
        bdi = pd.DataFrame.from_records(
            [doc for doc in self.db.bdi.find(bdi_query)])
        bdi.rename(columns={'_id': 'atm'}, inplace=True)

        merged = pd.merge(logs, bdi, on='atm', how='left')

        merged.to_csv("merged.csv", encoding="latin", index=False)

        return merged

    def fun(self, tickets_df, logs):
        df_filtered = logs.drop(logs[(logs['atm'] != 'X90005')].index)
        df_filtered = df_filtered.drop(
            df_filtered[(df_filtered['start_date'] < datetime.datetime(2020, 2, 7))].index)
        return df_filtered

    def find_by_query(self, collection, query):
        df = pd.DataFrame.from_records(
            [doc for doc in self.db[collection].find(query)])
        df.to_csv("queryResult.csv")
        return df

    def join_accomplishments_bdi(self, accomplishments_query={}, bdi_query={}):
        accomplishments = pd.DataFrame.from_records(
            [doc for doc in self.db['accomplishments'].find(accomplishments_query)])
        bdi = pd.DataFrame.from_records(
            [doc for doc in self.db.bdi.find(bdi_query)])
        bdi.rename(columns={'_id': 'atm'}, inplace=True)

        merged = pd.merge(accomplishments, bdi, on='atm', how='left')

        merged.to_csv("merged.csv", encoding="latin", index=False)

        return merged

    def __exit__(self):
        self.db.close()
