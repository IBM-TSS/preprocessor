from utils.DBHandler import DBHandler, USERS_DB_NAME

from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils
import pandas as pd
from utils.FileUtils import FileUtils


class EngineersPreprocessor(Preprocessor):

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(USERS_DB_NAME)
        self.engineers = self.db['engineers']

    def parse(self):
        # DF Manipulation
        self.data = self.data.drop(
            columns=['Unnamed: 0'])
        # self.data = self.data.drop(
        #     columns=['Territorio', 'Sub Territorio', 'Unnamed: 8'])

        # Make this column boolean
        # self.data['Pasa a Cajeros'] = self.data['Pasa a Cajeros'].map(
        #     lambda x: 1 if "Y" in x else 0)

        # Translate the keys in each record for standarize porpuse.
        self.data.rename(columns=CleanUtils.standarize_keys_dict(
            self.data.columns, to_alpha=False), inplace=True)

        name_id = self.get_ids_from_tickets(["STD_Tickets_Enero.csv", "STD_Tickets_Febrero.csv",
                                   "STD_Tickets_Marzo.csv"])

        # Place the id to the engineer
        self.data['_id'] = [name_id[name]
                            if name in name_id else "" for name in self.data.name]

        # DF became a records
        self.data = self.data.to_dict('records')
        
        for data in self.data:
            if not data['_id']:
                del data['_id']

    def upload(self):
        print('Uploading...')
        self.engineers.insert(self.data)
        print("Done!")

    def remove(self):
        self.engineers.remove()

    def get_ids_from_tickets(self, tickets_file_paths=None):
        tickets = FileUtils.read(tickets_file_paths, concat=True)
        name_id = dict(zip(tickets['R_IBM employee abbreviated name'], tickets['R_No_Empleado']))
        return name_id

