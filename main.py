import time
import datetime

# from preprocessors.BDIPreprocessor import BDIPreprocessor
from preprocessors.LogsPreprocessor import LogsPreprocessor
from preprocessors.EngineersPreprocessor import EngineersPreprocessor
from preprocessors.RCMS.TicketsPreprocesor import TicketsPreprocessor
from preprocessors.AccomplishmentPreprocesor import AccomplishmentPreprocesor
from extractors.extractor import Extractor
from utils.FileUtils import FileUtils
from fetchers.EngineerClosuresFetcher import EngineerClosuresFetcher
from fetchers.VerseMailFetcher import VerseMailFetcher


if __name__ == '__main__':
    # bbva_data_target = "data/bbva"
    # paths = FileUtils.get_files_in_folder(
    #     bbva_data_target, extensions=['xlsx', 'csv'])
    # # print(paths)
    # merged = FileUtils.read_parallel(paths)
    # merged.to_csv('data/bbva/data_bbva_merged2.csv',
    #               encoding="utf8", index=False)
    e = EngineerClosuresFetcher()
    s = time.perf_counter()
    e.process()
    print("time", time.perf_counter() - s)
    # e = VerseMailFetcher()
    # e.process()

    # target_dir = "C:/Users/EDUARDOLUISSANTOSDEL/Downloads/STD_TICKETS_DIARIOS"
    # target_dir2 = "C:/Users/EDUARDOLUISSANTOSDEL/Downloads/STD_TICKETS_SEMANALES"
    # target_dir3 = "C:/Users/EDUARDOLUISSANTOSDEL/Downloads/STD_ACCOMPLISMENTS"
    # paths = FileUtils.get_files_in_folder(target_dir, extensions=['xls'])
    # paths2 = FileUtils.get_files_in_folder(target_dir2, extensions=['xls'])
    # paths3 = FileUtils.get_files_in_folder(
    #     target_dir3, extensions=['xls', 'xlsx'])
    # paths4 = ["Datos IBM.xlsx"]

    # t = EngineersPreprocessor()
    # s = time.time()
    # t.process('ingenieria_con_id.csv')
    # e = time.time()

    # t = TicketsPreprocessor()
    # s = time.time()
    # t.process(paths4)
    # e = time.time()

    # t = AccomplishmentPreprocesor()
    # s = time.time()
    # t.process(paths4)
    # e = time.time()

    # print(" time: ", e - s)

    # t = LogsPreprocessor()
    # s = time.time()
    # t.process(paths + paths2)
    # e = time.time()

    # print(" time: ", e - s)

    # tickets = [
    #     'X92463', 'X92361', 'X92355', 'X93742', 'X96113', 'X98234', 'X97246', 'X97876', 'X96779',
    #     'X97862', 'X98247', 'X95324', 'X92173', 'X96005', 'X92158', 'X93644', 'X90199', 'X93177',
    #     'X93524', 'X96327', 'X96331', 'X93251', 'X95324', 'X92396', 'X95806'
    # ]
    # q = {
    #     # 'atm': {'$in': tickets},
    #     'start_date': {'$gte': datetime.datetime(2019, 12, 1)}
    # }

    # ex = Extractor()
    # s = time.time()
    # result = ex.join_accomplishments_bdi()
    # # result = ex.join_accomplishments_bdi(accomplishments_query=q)
    # e = time.time()
    # print(result)

    # print(" time: ", e - s)

    # e = Extractor()
    # options = {'parse_dates': 'start_date'}
    # tickets_df = FileUtils.read(
    #     'nivel_cumplimiento.xlsx', options)
    # # feb_q = {
    # #     'start_date': {'$gte': datetime.datetime(2020, 2, 1), '$lte': datetime.datetime(2020, 2, 29)}
    # # }
    # tickets = tickets_df.ID.tolist()
    # feb_q = {
    #     # 'atm': {'$in': tickets},
    #     'start_date': {'$gte': datetime.datetime(2020, 4, 1)},

    # }
    # s = time.time()
    # logs = e.join_logs_bdi(logs_query=feb_q)
    # # df = e.fun([], logs)
    # # df.to_csv("filtered.csv")
    # logs.to_csv("data/santander/logs/may_july13_fails.csv",
    #             encoding="latin", index=False)
    # en = time.time()
    # print(" time: ", en - s)

# s = BDIPreprocessor()
# s.process('bdi_enero.csv')
# s.remove()

    # ex = Extractor()
    # # print(ex.db.accomplishments.distinct('failure'))

    # import pandas as pd

    # pd.DataFrame((ex.db.accomplishments.distinct('failure'))).to_csv("dict_ext_std.csv")
