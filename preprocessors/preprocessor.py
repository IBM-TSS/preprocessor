import pandas as pd

from utils.FileUtils import FileUtils


class Preprocessor:

    def __init__(self):
        self.data = None

    def process(self, paths, upload=True, ** read_options):

        self.read(paths, **read_options)

        self.parse()
        # Preprocessor.save(path="data/guardado.csv")

        if upload:
            self.upload()

    @NotImplementedError
    def parse(self):
        pass

    @NotImplementedError
    def upload(self):
        pass

    def read(self, paths, concat=True, **read_options):
        if concat:
            data = FileUtils.read(paths, **read_options)
            # Concat if there's more than one file
            if type(data) == pd.DataFrame:
                self.data = data
            else:
                self.data = pd.concat(data, sort=False, ignore_index=True)
        else:
            self.data = FileUtils.read(paths, **read_options)

    def save(self, path="", **save_options):

        file_name = path.split('/')[-1]
        path = path.replace(file_name, '')
        extension = file_name.split('.')
        print(file_name)
        print(extension)

        if extension[1] == "xlsx" or extension[1] == 'xls':
            self.data.to_excel(file_name, **save_options)
        if extension[1] == "csv":
            self.data.to_csv(file_name, **save_options)
