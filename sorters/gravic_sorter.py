from typing import Tuple, List, Dict, NamedTuple
from datetime import datetime
from datetime import timedelta
import os


CHAIN_EXTENSION = 'txt'
TSF_EXTENSION = 'tsf'
DAT_EXTENSION = 'dat'

DAT_FIRST_LINE_INDEX = 21
TSF_FIRST_LINE_INDEX = 42

DAT_HEADER_FIRST_LINE = '/		CG-6 Survey'


class Measure(NamedTuple):
    datetime_val: datetime
    corr_grav_value: float


def is_dat_file(path: str):
    with open(path) as file_ctx:
        first_line = file_ctx.readline().rstrip()
        return first_line == DAT_HEADER_FIRST_LINE


class TSFile:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise OSError(f'File not found - {path}')

        extension = os.path.basename(path).split('.')[-1]
        if extension != TSF_EXTENSION:
            raise OSError(f'File is not tsf-file')

        self.path = path
        self.__first_line, self.__last_line = self.__get_last_lines()

    def __get_last_lines(self) -> Tuple[str, str]:
        with open(self.path) as file_ctx:
            lines = [x.rstrip() for x in file_ctx if len(x.rstrip())]
        return lines[TSF_FIRST_LINE_INDEX], lines[-1]

    def __get_datetime_from_line(self, line: str):
        datetime_src = list(map(int, line.split()[:6]))
        return datetime(*datetime_src)

    @property
    def device_num_part(self) -> str:
        return os.path.basename(self.path).split('_')[0]

    @property
    def datetime_stop(self) -> datetime:
        return self.__get_datetime_from_line(self.__last_line)

    @property
    def datetime_start(self) -> datetime:
        return self.__get_datetime_from_line(self.__first_line)


class DATFile:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise OSError(f'File not found - {path}')

        extension = os.path.basename(path).split('.')[-1]
        if extension != DAT_EXTENSION:
            raise OSError(f'File is not tsf-file')

        self.path = path
        self.__first_line, self.__last_line = self.__get_last_lines()

    def __get_last_lines(self) -> Tuple[str, str]:
        with open(self.path) as file_ctx:
            lines = [x.rstrip() for x in file_ctx if len(x.rstrip())]
        return lines[DAT_FIRST_LINE_INDEX], lines[-1]

    def __get_datetime_from_line(self, line: str) -> datetime:
        date_src, time_src = line.split('\t')[1: 3]
        datetime_time_src = date_src + ' ' + time_src
        return datetime.strptime(datetime_time_src, '%Y-%m-%d %H:%M:%S')

    def __get_station_from_line(self, line: str) -> str:
        return line.split('\t')[0]

    @property
    def datetime_start(self) -> datetime:
        line_datetime = self.__get_datetime_from_line(self.__first_line)
        return line_datetime + timedelta(minutes=-1)

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

    def extract_all_measures(self) -> List[Measure]:
        measures = []
        with open(self.path) as file_ctx:
            for _ in range(DAT_FIRST_LINE_INDEX):
                next(file_ctx)

            for line in file_ctx:
                split_line = line.rstrip().split('\t')
                if not split_line:
                    continue

                datetime_line = split_line[1] + ' ' + split_line[2]
                datetime_val = datetime.strptime(datetime_line,
                                                 '%Y-%m-%d %H:%M:%S')
                corr_grav_val = float(split_line[3])
                single_measure = Measure(datetime_val, corr_grav_val)
                measures.append(single_measure)
        return measures


class ChainFile:
    def __init__(self, path):
        if not os.path.exists(path):
            raise OSError

        extension = os.path.basename(path).split('.')[-1]
        if extension != CHAIN_EXTENSION:
            raise OSError(f'File is not {CHAIN_EXTENSION} file')

        if is_dat_file(path):
            raise RuntimeError('File is not chain file')

        self.path = path
        self.links = self.__load_links()

    def get_link_id_from_filename(self, filename: str) -> int:
        link_id = filename.split('.')[0].split('_')[-1]
        return int(link_id)

    def __load_links(self) -> Dict[str, int]:
        chain = dict()
        with open(self.path) as file_ctx:
            for line in file_ctx:
                filename = line.rstrip()
                if filename:
                    link_id = self.get_link_id_from_filename(filename)
                    chain[filename.rstrip()] = link_id
        return chain

    @property
    def sensor_part_name(self) -> str:
        return os.path.basename(self.path).split('.')[0].split('_')[1]
