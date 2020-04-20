from utils.CleanUtils import CleanUtils
from utils.DBHandler import DBHandler, SANTANDER_DB_NAME
from preprocessors.preprocessor import Preprocessor


class AccomplishmentPreprocesor(Preprocessor):

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.accomplishments = self.db['accomplishments']

    def parse(self):
        # Drop unnecesary columns
        columns_to_drop = ['Provee orig', 'COMMENT_TEXT', 'GARANTIA', 'RESP', 'AC', 'NOMBRE',
                           'MES', 'DURACION ORIGINAL', 'PROVEEDOR', 'MARCA-MODELO', 'SERVICIO']
        self.data = self.data.drop(columns=columns_to_drop)
        # We use Ticket as ID so ID should be changed
        self.data.rename(columns={'ID': 'atm'}, inplace=True)
        # Standarize column names
        self.data.rename(columns=CleanUtils.standarize_keys_dict(
            self.data.columns, to_alpha=False), inplace=True)
        # Make this column boolean
        self.data['accomplish'] = self.data['accomplish'].map(
            lambda x: "NO" not in x)
        # Make this column a integer column
        self.data['time_granted'] = self.data['time_granted'].map(
            lambda x: 0 if type(x) == str and '-' in x else x)

        self.data = self.data.to_dict('records')

    def upload(self):
        print('Uploading...')
        self.accomplishments.insert(self.data)
        print("Done!")

    def remove(self):
        self.accomplishments.remove()
