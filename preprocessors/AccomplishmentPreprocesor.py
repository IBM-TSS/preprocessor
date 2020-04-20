from utils.CleanUtils import CleanUtils
from utils.DBHandler import DBHandler, SANTANDER_DB_NAME
from preprocessors.preprocessor import Preprocessor


class AccomplishmentPreprocesor(Preprocessor):

    UNKNOW_VERSION = -1
    VERSION_1 = 1
    VERSION_2 = 2

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.accomplishments = self.db['accomplishments']

    def detec_version(self):
        columns_version_1 = [
            "DURACION", "RESP",
        ]
        columns_version_2 = [
            "DUR S/V", "ALIAS4", "SEMANA", "FUNCION"
        ]
        if all(elem in self.data.columns for elem in columns_version_1):
            return self.VERSION_1
        if all(elem in self.data.columns for elem in columns_version_2):
            return self.VERSION_2

        return self.UNKNOW_VERSION

    def parse(self):

        version = self.detec_version()

        if version == self.VERSION_1:
            self.parse_v1()
        elif version == self.VERSION_2:
            self.parse_v2()

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

    def parse_v1(self):
        # Drop unnecesary columns
        columns_to_drop = ['Provee orig', 'COMMENT_TEXT', 'GARANTIA', 'RESP', 'AC', 'NOMBRE',
                           'MES', 'DURACION ORIGINAL', 'PROVEEDOR', 'MARCA-MODELO', 'SERVICIO']
        self.data = self.data.drop(columns=columns_to_drop)

    def parse_v2(self):
        # Drop unnecesary columns
        columns_to_drop = ['COMMENT_TEXT', 'FUNCION', 'SEMANA', 'ALIAS4', 'DUR S/V', 'GARANTIA',
                           'AC', 'NOMBRE', 'MES', 'PROVEEDOR', 'MARCA-MODELO',
                           'SERVICIO']
        self.data = self.data.drop(columns=columns_to_drop)

    def upload(self):
        print('Uploading...')
        self.accomplishments.insert(self.data)
        print("Done!")

    def remove(self):
        self.accomplishments.remove()
