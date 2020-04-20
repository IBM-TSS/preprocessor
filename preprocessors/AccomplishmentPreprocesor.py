import pandas as pd

from utils.CleanUtils import CleanUtils
from utils.FileUtils import FileUtils
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

    def detec_version(self, data):
        columns_version_1 = [
            "DURACION", "RESP",
        ]
        columns_version_2 = [
            "DUR S/V", "ALIAS4", "SEMANA", "FUNCION"
        ]
        if all(elem in data.columns for elem in columns_version_1):
            return self.VERSION_1
        if all(elem in data.columns for elem in columns_version_2):
            return self.VERSION_2

        return self.UNKNOW_VERSION

    def parse(self):

        data = []
        for df in self.data:

            version = self.detec_version(df)

            if version == self.VERSION_1:
                self.parse_v1(df)
            elif version == self.VERSION_2:
                self.parse_v2(df)
            else:
                raise Exception("Possible new format")

            # We use Ticket as ID so ID should be changed
            df.rename(columns={'ID': 'atm'}, inplace=True)
            # Standarize column names
            df.rename(columns=CleanUtils.standarize_keys_dict(
                df.columns, to_alpha=False), inplace=True)
            # Make this column boolean
            df['accomplish'] = df['accomplish'].map(
                lambda x: "NO" not in x)
            # Make this column a integer column
            df['time_granted'] = df['time_granted'].map(
                lambda x: 0 if type(x) != int else x)

            data.append(df)

        self.data = pd.concat(data, sort=False, ignore_index=True)
        self.data.to_csv("aver.csv")
        self.data = self.data.to_dict('records')

    def parse_v1(self, data):
        # Drop unnecesary columns
        columns_to_drop = ['COMMENT_TEXT', 'GARANTIA', 'RESP', 'AC', 'NOMBRE',
                           'MES', 'DURACION ORIGINAL', 'PROVEEDOR', 'MARCA-MODELO', 'SERVICIO']
        if "Provee orig" in data.columns:
            columns_to_drop.append("Provee orig")
        data = data.drop(columns=columns_to_drop, inplace=True)

    def parse_v2(self, data):
        # Drop unnecesary columns
        columns_to_drop = ['COMMENT_TEXT', 'FUNCION', 'SEMANA', 'ALIAS4', 'DUR S/V', 'GARANTIA',
                           'AC', 'NOMBRE', 'MES', 'PROVEEDOR', 'MARCA-MODELO',
                           'SERVICIO']
        data = data.drop(columns=columns_to_drop, inplace=True)

    def read(self, paths, concat=True, **read_options):

        data = FileUtils.read(paths, **read_options)
        # Concat if there's more than one file
        if type(data) == pd.DataFrame:
            self.data = [data]
        else:
            self.data = data

    def upload(self):
        print('Uploading...')
        self.accomplishments.insert(self.data)
        print("Done!")

    def remove(self):
        self.accomplishments.remove()
