import pandas as pd

from utils.CleanUtils import CleanUtils
from utils.FileUtils import FileUtils
from utils.DBHandler import DBHandler, SANTANDER_DB_NAME
from preprocessors.preprocessor import Preprocessor


class AccomplishmentPreprocesor(Preprocessor):

    UNKNOW_VERSION = -1
    VERSION_1 = 1
    VERSION_2 = 2
    VERSION_3 = 3

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.accomplishments = self.db['accomplishments']
        self.accomplishments_on_db = self.get_accomplishments_on_db()

    def detec_version(self, data):
        columns_version_1 = [
            "DURACION", "RESP",
        ]
        columns_version_2 = [
            "DUR S/V", "ALIAS4", "SEMANA", "FUNCION"
        ]
        columns_version_3 = [
            "DURACION TOTAL",
        ]
        if all(elem in data.columns for elem in columns_version_1):
            return self.VERSION_1
        if all(elem in data.columns for elem in columns_version_2):
            return self.VERSION_2
        if all(elem in data.columns for elem in columns_version_3):
            return self.VERSION_3

        return self.UNKNOW_VERSION

    def parse(self):

        data = []

        if not self.data:
            return

        for df in self.data:

            version = self.detec_version(df)

            if version == self.VERSION_1:
                self.parse_v1(df)
            elif version == self.VERSION_2:
                self.parse_v2(df)
            elif version == self.VERSION_3:
                self.parse_v3(df)
            else:
                raise Exception("Possible new format")

            # We use Ticket as ID so ID should be changed
            df.rename(columns={'ID': 'atm'}, inplace=True)
            # Standarize column names
            df.rename(columns=CleanUtils.standarize_keys_dict(
                df.columns, to_alpha=False), inplace=True)
            # Make this column boolean
            df['accomplish'] = df['accomplish'].map(
                lambda x: 1 if "NO" not in x else 0)
            # Make this column a integer column
            df['time_granted'].fillna(0, inplace=True)
            df['time_granted'] = df['time_granted'].map(
                lambda x: 0 if type(x) is not int else x)

            data.append(df)

        self.data = pd.concat(data, sort=False, ignore_index=True)
        self.data.to_csv("aver.csv")
        self.data = self.data.to_dict('records')

    def parse_v1(self, data):
        # Drop unnecesary columns
        columns_to_drop = ['COMMENT_TEXT', 'REGION', 'GARANTIA', 'RESP', 'AC', 'NOMBRE',
                           'MES', 'DURACION', 'PROVEEDOR', 'MARCA-MODELO', 'SERVICIO']
        if "Provee orig" in data.columns:
            columns_to_drop.append("Provee orig")
        data = data.drop(columns=columns_to_drop, inplace=True)

    def parse_v2(self, data):
        # Drop unnecesary columns
        columns_to_drop = ['COMMENT_TEXT', 'REGION', 'FUNCION', 'SEMANA', 'ALIAS4', 'DUR S/V',
                           'AC', 'NOMBRE', 'MES', 'PROVEEDOR', 'MARCA-MODELO',
                           'SERVICIO', 'GARANTIA']
        data = data.drop(columns=columns_to_drop, inplace=True)

    def parse_v3(self, data):
        # Drop unnecesary columns
        columns_to_drop = ['FUNCION', 'REGION', 'SEMANA', 'ALIAS4', 'GARANTIA',
                           'AC', 'NOMBRE', 'MES', 'PROVEEDOR', 'MARCA-MODELO',
                           'SERVICIO', 'DURACION']
        data = data.drop(columns=columns_to_drop, inplace=True)

    def read(self, paths, concat=True, **read_options):

        data = FileUtils.read(paths, **read_options)
        # Concat if there's more than one file
        if type(data) == pd.DataFrame:
            self.data = [data]
        else:
            self.data = data

    def upload(self):
        ids_on_db = self.update()
        print('Uploading...')
        self.accomplishments.insert(
            [acc for acc in self.data if acc['_id'] not in ids_on_db])
        print("Done!")

    def update(self):
        print("--- Updating ---")

        # Update from daily data.
        accomplishments_on_db = {int(
            accomplishment['_id']): accomplishment for accomplishment in self.accomplishments_on_db}
        accomplishments_to_update = [acc for acc in self.data if acc['_id']
                                     in accomplishments_on_db and acc != accomplishments_on_db[acc['_id']]]

        for accomplishment in accomplishments_to_update:
            _id = int(accomplishment['_id'])
            for accomplishment_on_db in self.accomplishments_on_db:
                if accomplishment_on_db.get('_id') == _id:
                    accomplishment_on_db.update(accomplishment)
                    self.accomplishments.replace_one(
                        {'_id': _id}, accomplishment_on_db, upsert=True)
                    print(f'Updating: {_id}')

        return accomplishments_on_db.keys()

    def remove(self):
        self.accomplishments.remove()

    def get_accomplishments_on_db(self):
        accomplishments = [
            accomplishment for accomplishment in self.accomplishments.find({})]
        return accomplishments
