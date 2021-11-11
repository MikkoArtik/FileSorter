from typing import List, Tuple, Union
from datetime import datetime
import os


TSF_EXTENSION = 'tsf'
DAT_EXTENSION = 'dat'


class TSFile:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise OSError(f'File not found - {path}')

        extension = os.path.basename(path).split('.')[-1]
        if extension != TSF_EXTENSION:
            raise OSError(f'File is not tsf-file')

        self.path = path
        self.__last_line = self.__get_last_line()

    def __get_last_line(self) -> str:
        with open(self.path) as file_ctx:
            lines = [x.rstrip() for x in file_ctx if len(x.rstrip())]
        return lines[-1]

    def __get_datetime_from_line(self, line: str):
        datetime_src = list(map(int, line.split()[:6]))
        return datetime(*datetime_src)

    @property
    def device_num_part(self) -> str:
        return os.path.basename(self.path).split('_')[0]

    @property
    def datetime_stop(self) -> datetime:
        return self.__get_datetime_from_line(self.__last_line)


class DATFile:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise OSError(f'File not found - {path}')

        extension = os.path.basename(path).split('.')[-1]
        if extension != DAT_EXTENSION:
            raise OSError(f'File is not tsf-file')

        self.path = path
        self.__last_line = self.__get_last_line()

    def __get_last_line(self) -> str:
        with open(self.path) as file_ctx:
            lines = [x.rstrip() for x in file_ctx if len(x.rstrip())]
        return lines[-1]

    def __get_datetime_from_line(self, line: str) -> datetime:
        date_src, time_src = line.split('\t')[1: 3]
        datetime_time_src = date_src + ' ' + time_src
        return datetime.strptime(datetime_time_src, '%Y-%m-%d %H:%M:%S')

    def __get_station_from_line(self, line: str) -> str:
        return line.split('\t')[0]

    @property
    def datetime_stop(self) -> datetime:
        return self.__get_datetime_from_line(self.__last_line)

    @property
    def station(self) -> str:
        return self.__get_station_from_line(self.__last_line)

    @property
    def device_full_number(self) -> str:
        with open(self.path) as f:
            need_line = f.readlines()[2].rstrip()
        return need_line.split('\t')[-1]


class PairFinder:
    def __init__(self, root: str):
        if not os.path.exists(root):
            raise OSError

        self.root = root

        self.tsf_files, self.dat_files = self.get_gravimetric_files()
        self.used_indexes = set()
        self.dat_tsf_pairs = self.create_pairs()

    def get_gravimetric_files(self) -> Tuple[List[TSFile], List[DATFile]]:
        tsf_files_list, dat_files_list = [], []
        for root, _, files in os.walk(self.root):
            for filename in files:
                path = os.path.join(root, filename)
                try:
                    tsf_file_info = TSFile(path)
                    tsf_files_list.append(tsf_file_info)
                except OSError:
                    try:
                        dat_file_info = DATFile(path)
                        dat_files_list.append(dat_file_info)
                    except OSError:
                        pass
        return tsf_files_list, dat_files_list

    def find_dat_pair(self,
                      tsf_file_index: int) -> Union[int, None]:
        tsf_file = self.tsf_files[tsf_file_index]
        dev_num_part = tsf_file.device_num_part
        for dat_file_index, dat_file in enumerate(self.dat_files):
            if dat_file_index in self.used_indexes:
                continue

            dat_dev_num_part = dat_file.device_full_number[-len(dev_num_part):]
            if dat_dev_num_part != dev_num_part:
                continue

            if dat_file.datetime_stop != tsf_file.datetime_stop:
                continue
            return dat_file_index
        else:
            return

    def create_pairs(self) -> List[Tuple[int, int]]:
        pairs = []
        self.used_indexes = set()
        for tsf_file_index in range(len(self.tsf_files)):
            dat_index = self.find_dat_pair(tsf_file_index)
            if dat_index:
                pairs.append((dat_index, tsf_file_index))
                self.used_indexes.add(dat_index)
        return pairs

    def get_pairs(self) -> Tuple[DATFile, TSFile]:
        for dat_index, tsf_index in self.dat_tsf_pairs:
            yield self.dat_files[dat_index], self.tsf_files[tsf_index]


root = '/media/michael/Data/Projects/GraviSeismicComparation/' \
       'ZapolarnoeDeposit/2021/src_data'

finder = PairFinder(root)
print(finder.create_pairs())
