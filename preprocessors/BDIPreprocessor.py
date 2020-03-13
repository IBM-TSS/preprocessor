from utils.DBHandler import DBHandler, SANTANDER_DB_NAME

from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils


class BDIPreprocessor(Preprocessor):

    def parse(self):
        # Pandas Manipulation
        self.data = self.data.drop(columns=['Status', 'Referencia'])
        self.data = self.data.dropna(axis=1, how='all')
        self.data['Movimiento'] = self.data['Movimiento'].str.replace('#N/D', 'ACTIVO')

        # DF became a records
        self.data = self.data.to_dict('records')
        # Translate the keys in each record for standarize porpuse.
        self.data = [CleanUtils.translate_keys(record) for record in self.data]

    def upload(self):
        handler = DBHandler()
        db = handler.get_client_db(SANTANDER_DB_NAME)
        bdi = db['bdi']
        print('Uploading...')
        bdi.insert(self.data)
        print("Done!")
