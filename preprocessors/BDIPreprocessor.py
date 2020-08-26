from utils.DBHandler import DBHandler, SANTANDER_DB_NAME

from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils


class BDIPreprocessor(Preprocessor):

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.bdi = self.db['bdi']
        self.atms_on_db = self.get_atms_on_db()

    def parse(self):
        print(self.data.columns)
        # DF Manipulation
        self.data = self.data.drop(columns=['Status', 'Referencia', 'G'])
        self.data = self.data.dropna(axis=1, how='all')
        self.data['Movimiento'] = self.data['Movimiento'].str.replace(
            '#N/D', 'ACTIVO')

        self.data['Adquisicion'] = self.data['Adquisicion'].apply(
            lambda x: CleanUtils.date_from_string(x) if type(x) == str else x)

        self.data['Alta'] = self.data['Alta'].apply(
            lambda x: CleanUtils.date_from_string(x) if type(x) == str else x)

        self.data['Fecha_Movimiento'] = self.data['Fecha_Movimiento'].apply(
            lambda x: CleanUtils.date_from_string(x) if type(x) == str else x)

        self.data[['Fecha_Movimiento']] = self.data[['Fecha_Movimiento']].astype(object).where(
            self.data[['Fecha_Movimiento']].notnull(), None)
        self.save(path="guardado.csv")
        # DF became a records
        self.data = self.data.to_dict('records')
        # Translate the keys in each record for standarize porpuse.
        self.data = [CleanUtils.translate_keys(record) for record in self.data]

    def upload(self):
        print('Updating...')
        ids_on_db = self.update()
        print('Uploading...')
        self.bdi.insert(
            [atm for atm in self.data if atm['_id'] not in ids_on_db])
        print("Done!")

    def update(self):
        print("--- Updating ---")

        # Update from daily data.
        atms_on_db = {atm['_id']: atm for atm in self.atms_on_db}
        atms_to_update = [atm for atm in self.data if atm['_id']
                          in atms_on_db and atm != atms_on_db[atm['_id']]]

        for atm in atms_to_update:
            _id = atm['_id']
            for atm_on_db in self.atms_on_db:
                if atm_on_db.get('_id') == _id:
                    atm_on_db.update(atm)
                    self.bdi.replace_one({'_id': _id}, atm_on_db, upsert=True)
                    print(f'Updating: {_id}')

        return atms_on_db.keys()

    def remove(self):
        self.bdi.remove()

    def get_atms_on_db(self):
        atms = [atm for atm in self.bdi.find({})]
        return atms
