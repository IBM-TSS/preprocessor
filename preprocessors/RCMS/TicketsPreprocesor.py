import pandas as pd
from utils.DBHandler import DBHandler, RCMS_DB_NAME

from preprocessors.preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils
from utils.FileUtils import FileUtils


class TicketsPreprocessor(Preprocessor):

    def __init__(self):
        # Connecting to DB
        handler = DBHandler()
        self.db = handler.get_client_db(RCMS_DB_NAME)
        self.tickets = self.db['tickets']

    def parse(self):
        # Get only closed tickets
        self.data = self.data[self.data['R_Status of incident record'] == 'CE']
        # Sum time used.
        self.data['total_hours'] = self.data.groupby(
            ["H-RCMS incident number"])["H_Horas totales"].transform(sum)
        self.data['total_travel_hours'] = self.data.groupby(
            ["H-RCMS incident number"])["H_Horas viaje"].transform(sum)
        # Keep only necessary columns
        print(len(set(self.data['H-RCMS incident number'])))
        self.data = self.data[[
            'total_hours', 'total_travel_hours', 'H-RCMS incident number', 'H_Machine type',
            'H_Serie',
            'R_No_Empleado', 'R_Incident record last change date',

            'R_Scratch pad', 'R_Severity of incident', 'R_Incident record destination', 'R_User identity',
            'R_Original system call number', 'R_BU', 'R_Fecha Creado', 'R_Fecha Tomado',
            'R_Fecha Solucion', 'R_Fecha Cerrado', 'R_Fecha Onsite',
            'H_Call Report']]

        #self.data = self.data[self.data['H_Customer contact name'].notna()]
        #print("self ", len(self.data))

        # self.data['atm'] = self.data['H_Customer contact name'].map(
        #     lambda x: self.get_atm_id(x)
        # )

        # # Drop unnecesary columns
        # columns_to_drop = ['H-RCMS incident number', 'H_Call Report']
        # self.data = self.data.drop(columns=columns_to_drop)

        self.data.to_csv("pretick.csv")
        pre = self.data['H-RCMS incident number'].values

        # Group the same logs and put the status in a list.
        # Group the same logs and put the status in a list.
        cols_group = ['total_hours', 'total_travel_hours', 'H-RCMS incident number', 'H_Machine type',
                      'H_Serie',
                      'R_No_Empleado', 'R_Incident record last change date',

                      'R_Scratch pad', 'R_Severity of incident', 'R_Incident record destination', 'R_User identity',
                      'R_Original system call number', 'R_BU', 'R_Fecha Creado', 'R_Fecha Tomado',
                      'R_Fecha Solucion', 'R_Fecha Cerrado', 'R_Fecha Onsite']

        self.data = (self.data.groupby(cols_group)[
                     'H_Call Report'].apply(list).reset_index())

        #self.data[['H-RCMS incident number','H_Call Report']] = group

        print(
            set([el for el in pre if el not in self.data['H-RCMS incident number'].values]))
        print([k for k, v in self.data['H-RCMS incident number'].value_counts().to_dict().items() if v > 2])
        print(len(self.data))

        # self.data['H_Call Report'] = self.data['H-RCMS incident number'].map(
        #     lambda x: group.query(f'H-RCMS\ incident number=={x}')['H_Call Report'])

        # Make this column boolean
        # self.data['FLUYE A COSTOS'] = self.data['FLUYE A COSTOS'].map(
        #     lambda x: 1 if "SI" in x else 0)

        # Delete duplicate, keep first
        print(self.data['H-RCMS incident number'].isna().sum())

        self.data.to_csv("tick.csv")

        # Translate the keys in each record for standarize porpuse.
        self.data.rename(columns=CleanUtils.standarize_keys_dict(
            self.data.columns, to_alpha=False), inplace=True)

        # DF became a records
        self.data = self.data.to_dict('records')

    def upload(self):
        print('Uploading...')
        # self.tickets.insert(self.data)
        print("Done!")

    def remove(self):
        self.tickets.remove()

    # This is better than get the atm by pandas extract function
    # due to differentt clients coud has different id for their atms
    # and atm is not always bettween **
    def get_atm_id(self, data):
        if data:
            data = str(data).split("*")
            if len(data) > 1:
                return data[1]
            return data[0]
        return str(data).split()
