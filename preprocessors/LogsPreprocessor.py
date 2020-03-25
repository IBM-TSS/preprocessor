import pandas as pd

from utils.DBHandler import DBHandler, SANTANDER_DB_NAME

from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils
from utils.FileUtils import FileUtils


class LogsPreprocessor(Preprocessor):

    daily_data = None
    weekly_data = None
    pre_data = None

    def __init__(self):
        pass
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.logs = self.db['logs']

    def parse(self):
        # DF Manipulation
        if self.daily_data and not self.daily_data.empty:
            self.daily_report_parser()
        if not self.weekly_data.empty:
            self.weekly_report_parser()
        # if seld.daily_data and self.weekly_data:
        #     self.pre_data = df1.id.map(df2.set_index('id1')['price'])
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

        # if daily_file_paths:
        #     read_options['skiprows'] = 1
        #     read_options['encoding'] = 'latin1'
        #     daily_data = FileUtils.read(daily_file_paths, **read_options)
        #     daily_data = pd.concat(daily_data, sort=False, ignore_index=True)

        if weekly_file_paths:
            weekly_data = pd.DataFrame()
            for weekly_file_path in weekly_file_paths:
                read_options['sheet_name'] = None
                read_options['skiprows'] = 2
                read_options['encoding'] = 'latin1'
                weekly_data_temp = FileUtils.read(weekly_file_path,  **read_options)

                if len(weekly_data_temp) > 1:
                    columns = weekly_data_temp['Ticket History IBM'].columns

                    for sheet_name in weekly_data_temp:
                        if sheet_name != "Ticket History IBM":
                            weekly_data_temp[sheet_name].columns = columns

                    weekly_data_temp = pd.concat(weekly_data_temp, sort=False, ignore_index=True)
                    weekly_data = pd.concat([weekly_data, weekly_data_temp],
                                            sort=False, ignore_index=True)

        self.weekly_data = weekly_data
        # self.daily_data = daily_data

    def upload(self):
        self.logs.insert(self.weekly_data)
        print("Done!")

    def remove(self):
        self.ticket.remove()

    def daily_report_parser(self):

        # Drop unnecesary columns
        columns_to_drop = ['REGIÓN', 'DURACION', 'MES', 'AC', 'PROVEEDOR', 'MODELO', 'ATENCIÓN']
        self.daily_data = self.daily_data.drop(columns=columns_to_drop)

        FileUtils.delete_duplicates(self.daily_data)

        # Handle Null end_date
        self.daily_data[['FECHA_FIN']] = self.daily_data[['FECHA_FIN']].astype(object).where(
                                          self.daily_data[['FECHA_FIN']].notnull(), None)

        # Group the same logs and put the status in a list.
        cols_group = [col for col in self.daily_data.columns if col != "ESTATUS"]
        self.daily_data = self.daily_data.groupby(cols_group)['ESTATUS'].apply(list).reset_index()

        self.daily_data.rename(columns={'ID': 'atm'})
        self.daily_data.rename(columns=CleanUtils.standarize_keys_dict(self.daily_data.columns))
        self.daily_data.to_csv("daily.csv", index=False, encoding='latin1')

        # DF became a records
        # self.daily_data = self.daily_data.to_dict('records')

    def weekly_report_parser(self):

        # Handle Null end_date
        self.weekly_data.to_csv("antesweekly.csv", index=False, encoding='latin1')
        self.weekly_data[['FECHA_FIN']] = self.weekly_data[['FECHA_FIN']].astype(object).where(
                                           self.weekly_data[['FECHA_FIN']].notnull(), None)

        # Group the same logs and put the status in a list.
        cols_group = [col for col in self.weekly_data.columns if col != "STATUS"]
        self.weekly_data = self.weekly_data.groupby(cols_group)['STATUS'].apply(list).reset_index()

        self.weekly_data.rename(columns={'ID': 'atm'})
        self.weekly_data.rename(columns=CleanUtils.standarize_keys_dict(self.weekly_data.columns))
