import pandas as pd


class Preprocessor:

    def __init__(self):
        self.data = None

    def process(self, path):
        self.read(path)
        self.parse()
        #Preprocessor.save()
        self.upload()

    @NotImplementedError
    def parse(self):
        pass

    @NotImplementedError
    def upload(self):
        pass

    def save(self, path="", **save_options):

        file_name = path.split('/')[-1]
        path = path.replace(file_name, '')
        extension = file_name.split('.')

        if extension[1] == "xlsx" or extension[1] == 'xls':
            self.data.to_excel(file_name, **save_options)
        if extension[1] == "csv":
            self.data.to_csv(file_name, **save_options)

    def read(self, path, **read_options):

        file_name = path.split('/')[-1]
        path = path.replace(file_name, '')
        extension = file_name.split('.')

        if extension[-1] == "xlsx" or extension[-1] == 'xls':
            self.data = pd.read_excel(file_name)
        if extension[-1] == "csv":
            self.data = pd.read_csv(file_name, **read_options)
