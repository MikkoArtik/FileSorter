from typing import List, Tuple, Union
from datetime import datetime
from datetime import timedelta
import os


TSF_EXTENSION = 'tsf'
DAT_EXTENSION = 'dat'

DAT_FIRST_LINE_INDEX = 21
TSF_FIRST_LINE_INDEX = 42


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
        return line_datetime + timedelta(minutes=-1, seconds=1)

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
