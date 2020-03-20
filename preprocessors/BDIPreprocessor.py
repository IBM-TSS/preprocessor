from utils.DBHandler import DBHandler, SANTANDER_DB_NAME

from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils


class BDIPreprocessor(Preprocessor):

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.bdi = self.db['bdi']

    def parse(self):
        # DF Manipulation
        self.data = self.data.drop(columns=['Status', 'Referencia'])
        self.data = self.data.dropna(axis=1, how='all')
        self.data['Movimiento'] = self.data['Movimiento'].str.replace('#N/D', 'ACTIVO')

        self.data['Adquisicion'] = self.data['Adquisicion'].apply(
                                    lambda x: CleanUtils.date_from_string(x))

        self.data['Alta'] = self.data['Alta'].apply(lambda x: CleanUtils.date_from_string(x))

        self.data['Fecha_Movimiento'] = self.data['Fecha_Movimiento'].apply(
                                         lambda x: CleanUtils.date_from_string(x))

        self.data[['Fecha_Movimiento']] = self.data[['Fecha_Movimiento']].astype(object).where(
                                           self.data[['Fecha_Movimiento']].notnull(), None)

        # DF became a records
        self.data = self.data.to_dict('records')
        # Translate the keys in each record for standarize porpuse.
        self.data = [CleanUtils.translate_keys(record) for record in self.data]
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.bdi = self.db['bdi']

    def upload(self):
        print('Uploading...')
        self.bdi.insert(self.data)
        print("Done!")

    def remove(self):
        self.bdi.remove()
