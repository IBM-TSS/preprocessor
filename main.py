import time
import datetime

# from preprocessors.BDIPreprocessor import BDIPreprocessor
from preprocessors.LogsPreprocessor import LogsPreprocessor
from preprocessors.EngineersPreprocessor import EngineersPreprocessor
from preprocessors.AccomplishmentPreprocesor import AccomplishmentPreprocesor
from extractors.extractor import Extractor
from utils.FileUtils import FileUtils


if __name__ == '__main__':

    # target_dir = "C:/Users/EDUARDOLUISSANTOSDEL/Downloads/STD_TICKETS_DIARIOS"
    # target_dir2 = "C:/Users/EDUARDOLUISSANTOSDEL/Downloads/STD_TICKETS_SEMANALES"
    target_dir3 = "C:/Users/EDUARDOLUISSANTOSDEL/Downloads/STD_ACCOMPLISMENTS"
    # paths = FileUtils.get_files_in_folder(target_dir, extensions=['xls'])
    # paths2 = FileUtils.get_files_in_folder(target_dir2, extensions=['xls'])
    # paths3 = FileUtils.get_files_in_folder(
    #     target_dir3, extensions=['xls', 'xlsx'])

    t = EngineersPreprocessor()
    s = time.time()
    t.process('Recursos Self Service y MVS.xlsx')
    e = time.time()

    # t = AccomplishmentPreprocesor()
    # s = time.time()
    # t.process(paths3)
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
    # result = ex.join_accomplishments_bdi(accomplishments_query=q)
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
    #     'atm': {'$in': tickets},
    #     'start_date': {'$lte': datetime.datetime(2020, 2, 29)}
    # }
    # s = time.time()
    # logs = e.join_logs_bdi(logs_query=feb_q)
    # df = e.fun([], logs)
    # df.to_csv("filtered.csv")
    # en = time.time()
    # print(" time: ", en - s)

# s = BDIPreprocessor()
# s.process('bdi_enero.csv')
# s.remove()
