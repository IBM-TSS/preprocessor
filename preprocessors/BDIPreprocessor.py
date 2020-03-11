import pandas as pd
import logging

from .preprocessor import Preprocessor
from utils.CleanUtils import CleanUtils


class SantardePreprocessor(Preprocessor):

    def parse(self):
        # Pandas Manipulation
        self.data = self.data.drop(columns=['Status', 'Referencia'])
        self.data = self.data.dropna(axis=1, how='all')
        self.data['Movimiento'] = self.data['Movimiento'].str.replace('#N/D','ACTIVO')

        # DF became a records
        self.data = self.data.to_dict('records')
        # Translate the keys in each record for standarize porpuse.
        self.data = [CleanUtils.translate_keys(record) for record in self.data]

