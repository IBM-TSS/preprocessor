import os
import pandas as pd

from utils.DBHandler import DBHandler, SANTANDER_DB_NAME
from preprocessors.preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils
from utils.FileUtils import FileUtils


class WarningsPreprocesor:

    DOWNLOAD_PATH = f"{os.getcwd()}/data/short_run"
    data = pd.DataFrame()

    @classmethod
    def process(cls):
        cls.read()

        # DF Manipulation
        cls.parse()

        return cls.data

    @classmethod
    def parse(cls):
        # Drop unnecesary columns
        columns_to_drop = ['DIAS_RG', 'CLOSED_TODAY', 'CREATED_YESTERDAY', 'Actual_Backlog',
                           'hrs_Backlog', 'HORAS_RG_NOW', 'CLIENTE_GLOBAL', 'CLOSED_YESTARDAY',
                           'CREATED_TODAY', 'ACTIVIDAD', 'TIME_OPEN', 'OWNER_QUEUE', 'BU_GERENTE',
                           'BU_NOMBRE', 'MES_OPN', ' FECHA_CIERRE', 'REP_TIPO',
                           'DESC_PROD', 'MODELO', 'SERIAL', 'ORIGEN', 'EQUIPO', 'CLI_TELEFONO',
                           'CLI_CONTACTO', 'CLI_NOMBRE', 'CLI_CIUDAD']

        cls.data = cls.data.drop(columns=columns_to_drop)

        # Take only the open ones
        cls.data = cls.data[cls.data.REP_STATUS == 'XA ']

        # Standarize column names
        cls.data.rename(columns={'REPORTE': '_id'}, inplace=True)
        cls.data.rename(
            columns={'ESN_NUMBER': 'esn'}, inplace=True)
        cls.data.rename(
            columns={'REP_STATUS': 'status'}, inplace=True)
        cls.data.rename(columns=CleanUtils.standarize_keys_dict(cls.data.columns),
                        inplace=True)

        # Cast date strings to Date objecs
        cls.data['start_date'] = pd.to_datetime(
            cls.data['start_date'], format='%Y-%m-%d %H:%M')
        cls.data['last_status_update_date'] = pd.to_datetime(
            cls.data['last_status_update_date'], format='%Y-%m-%d %H:%M')

        # Strip all the strings
        str_cols = cls.data.select_dtypes(['object'])
        cls.data[str_cols.columns] = str_cols.apply(lambda x: x.str.strip())

        cls.data = cls.data.to_dict('records')

    @classmethod
    def read(cls, concat=True, **read_options):

        paths = FileUtils.get_files_in_folder(
            cls.DOWNLOAD_PATH, extensions=['xlsx'])

        cls.data = FileUtils.read(paths, **read_options)
