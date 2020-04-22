import pandas as pd

from utils.DBHandler import DBHandler, SANTANDER_DB_NAME
from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils
from utils.FileUtils import FileUtils


class LogsPreprocessor(Preprocessor):

    daily_data = None
    weekly_data = None
    pre_data = pd.DataFrame()

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(SANTANDER_DB_NAME)
        self.logs = self.db['logs']
        self.logs_on_db = self.get_logs_on_db()

    def parse(self):
        # DF Manipulation
        if not self.daily_data.empty:
            self.daily_report_parser()
        if not self.weekly_data.empty:
            self.weekly_report_parser()

        self.data = self.weekly_data or [] + self.daily_data or []

    # override
    def read(self, paths, **read_options):

        print("--- Reading ---")

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
            if type(daily_data) == pd.DataFrame:
                daily_data = [daily_data]
            daily_data = pd.concat(daily_data, sort=False, ignore_index=True)

        if weekly_file_paths:
            weekly_data = pd.DataFrame()
            for weekly_file_path in weekly_file_paths:
                read_options['sheet_name'] = None
                read_options['skiprows'] = 2
                read_options['encoding'] = 'latin1'
                weekly_data_temp = FileUtils.read(
                    weekly_file_path,  **read_options)

                if len(weekly_data_temp) > 1:
                    columns = weekly_data_temp['Ticket History IBM'].columns

                    for sheet_name in weekly_data_temp:
                        if sheet_name != "Ticket History IBM":
                            weekly_data_temp[sheet_name].columns = columns

                    weekly_data_temp = pd.concat(
                        weekly_data_temp, sort=False, ignore_index=True)
                    weekly_data = pd.concat([weekly_data, weekly_data_temp],
                                            sort=False, ignore_index=True)

        self.weekly_data = weekly_data
        self.daily_data = daily_data

    def upload(self):
        ids_on_db = self.update()
        self.logs.insert(
            [log for log in self.data if int(log['_id']) not in ids_on_db])
        print("Done!")

    def update(self):
        print("--- Updating ---")

        # Update from daily data.
        logs_on_db = {int(log['_id']): log for log in self.logs_on_db}
        logs_to_update = [log for log in self.data if log['_id']
                          in logs_on_db and log != logs_on_db[log['_id']]]

        for log in logs_to_update:
            _id = int(log['_id'])
            for log_on_db in self.logs_on_db:
                if log_on_db.get('_id') == _id:
                    log_on_db.update(log)
                    self.logs.replace_one({'_id': _id}, log_on_db, upsert=True)
                    print(f'Updating: {_id}')

        return logs_on_db.keys()

    def remove(self):
        self.ticket.remove()

    def daily_report_parser(self):

        # Drop unnecesary columns
        columns_to_drop = ['REGIÓN', 'DURACIO', 'MES', 'AC',
                           'PROVEEDOR', 'MODELO', 'ATENCIÓN', 'SERVICIO']
        self.daily_data = self.daily_data.drop(columns=columns_to_drop)

        # Handle Null end_date
        self.daily_data[['FECHA_FIN']] = self.daily_data[['FECHA_FIN']].astype(object).where(
            self.daily_data[['FECHA_FIN']].notnull(), None)

        # Group the same logs and put the status in a list.
        cols_group = [
            col for col in self.daily_data.columns if col != "ESTATUS"]
        # cols_group = [col for col in self.daily_data.columns if col != "ESTATUS"][14:]

        self.daily_data = self.daily_data.groupby(
            cols_group)['ESTATUS'].apply(list).reset_index()

        self.daily_data.rename(columns={'ID': 'atm'}, inplace=True)
        self.daily_data.rename(
            columns={'TIPO FALLA': 'failure_type'}, inplace=True)
        self.daily_data.rename(
            columns={'ESTATUS': 'daily_status'}, inplace=True)
        self.daily_data.rename(columns=CleanUtils.standarize_keys_dict(self.daily_data.columns),
                               inplace=True)

        # DF became a records
        self.daily_data = self.daily_data.to_dict('records')

    def weekly_report_parser(self):

        # Handle Null end_date
        self.weekly_data[['FECHA_FIN']] = self.weekly_data[['FECHA_FIN']].astype(object).where(
            self.weekly_data[['FECHA_FIN']].notnull(), None)

        # Group the same logs and put the status in a list.
        cols_group = [
            col for col in self.weekly_data.columns if col != "STATUS"]
        self.weekly_data = self.weekly_data.groupby(
            cols_group)['STATUS'].apply(list).reset_index()

        self.weekly_data.rename(columns={'ID': 'atm'}, inplace=True)
        self.weekly_data.rename(columns=CleanUtils.standarize_keys_dict(self.weekly_data.columns),
                                inplace=True)
        # self.weekly_data.to_csv("weekly.csv", index=False, encoding='latin1')

        # DF became a records
        self.weekly_data = self.weekly_data.to_dict('records')

    def get_logs_on_db(self):
        logs = [log for log in self.logs.find({})]
        return logs
