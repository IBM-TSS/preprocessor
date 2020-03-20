import pandas as pd

from utils.DBHandler import DBHandler, SANTANDER_DB_NAME

from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils
from utils.FileUtils import FileUtils


class TicketPreprocessor(Preprocessor):

    daily_data = None
    weekly_data = None

    def __init__(self):
        pass
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.tickets = self.db['tickets']

    def parse(self):
        # DF Manipulation
        if not self.daily_data.empty:
            self.daily_report_parser()
        # Delete duplicates
        # FileUtils.delete_duplicates(self.data)

    # override
    def read(self, paths, **read_options):
        weekly_file_paths = []
        daily_file_paths = []

        # If input is a string put inside a list.
        if type(paths) == str:
            paths = [paths]

        for path in paths:
            if "history" in path.lower():
                weekly_file_paths.append(path)
            else:
                daily_file_paths.append(path)

        if daily_file_paths:
            read_options['skiprows'] = 1
            daily_data = FileUtils.read(daily_file_paths, **read_options)
            daily_data = pd.concat(daily_data, sort=False, ignore_index=True)

        if weekly_file_paths:
            weekly_data = pd.DataFrame()
            for weekly_file_path in weekly_file_paths:
                read_options['sheet_name'] = None
                read_options['skiprows'] = 2
                weekly_data_temp = FileUtils.read(weekly_file_path,  **read_options)

                if len(weekly_data_temp) > 1:
                    columns = weekly_data_temp['Ticket History IBM'].columns

                    for sheet_name in weekly_data_temp:
                        if sheet_name != "Ticket History IBM":
                            weekly_data_temp[sheet_name].columns = columns

                    weekly_data_temp = pd.concat(weekly_data_temp, sort=False, ignore_index=True)
                    weekly_data = pd.concat([weekly_data, weekly_data_temp], sort=False, ignore_index=True)

        self.weekly_data = weekly_data
        self.daily_data = daily_data

    def upload(self):
        self.tickets.insert(self.daily_data)
        print("Done!")

    def remove(self):
        self.bdi.remove()

    def daily_report_parser(self):
        columns_to_drop = ['REGIÓN', 'DURACIO', 'MES', 'AC', 'PROVEEDOR', 'MODELO', 'ATENCIÓN']
        self.daily_data = self.daily_data.drop(columns=columns_to_drop)
        self.daily_data[['FECHA_FIN']] = self.daily_data[['FECHA_FIN']].astype(object).where(self.daily_data[['FECHA_FIN']].notnull(), None)
        self.daily_data.rename(columns={'ID': 'atm'})
        FileUtils.delete_duplicates(self.daily_data)
        self.daily_data.to_csv("aver.csv")

        # DF became a records
        self.daily_data = self.daily_data.to_dict('records')
        # Translate the keys in each record for standarize porpuse.
        self.daily_data = [CleanUtils.translate_keys(record) for record in self.daily_data]
