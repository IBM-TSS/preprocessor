from utils.DBHandler import DBHandler, SANTANDER_DB_NAME, USERS_DB_NAME

from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils


class EngineersPreprocessor(Preprocessor):

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(USERS_DB_NAME)
        self.engineers = self.db['engineers']

    def parse(self):
        # DF Manipulation
        self.data = self.data.drop(columns=['Territorio', 'Sub Territorio'])
        
        # Make this column boolean
        self.data['Pasa a Cajeros'] = self.data['Pasa a Cajeros'].map(lambda x: 1 if "Y" in x else 0)

        # Translate the keys in each record for standarize porpuse.
        self.data.rename(columns=CleanUtils.standarize_keys_dict(self.data.columns), inplace=True)

        # DF became a records
        self.data = self.data.to_dict('records')

        self.data.to_csv("aver.csv")

    def upload(self):
        print('Uploading...')
        # self.bdi.insert(self.data)
        print("Done!")

    def remove(self):
        self.bdi.remove()
