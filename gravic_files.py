from typing import Tuple, List, Dict, NamedTuple, Union
from datetime import datetime
from datetime import timedelta
import os


CHAIN_EXTENSION = 'txt'
TSF_EXTENSION = 'tsf'
DAT_EXTENSION = 'dat'
CYCLE_EXTENSION = 'txt'

DAT_FIRST_LINE_INDEX = 21
TSF_FIRST_LINE_INDEX = 42
TSF_SIGNAL_FREQUENCY = 10

DAT_HEADER_FIRST_LINE = '/		CG-6 Survey'
CYCLE_HEADER_FIRST_LINE = 'seans\tcycle\tzabrak\tpopravka'


class Measure(NamedTuple):
    datetime_val: datetime
    corr_grav_value: float


def is_dat_file(path: str) -> bool:
    with open(path) as file_ctx:
        first_line = file_ctx.readline().rstrip()
        return first_line == DAT_HEADER_FIRST_LINE


def is_cycle_file(path: str) -> bool:
    with open(path) as file_ctx:
        first_line = file_ctx.readline().rstrip()
        return first_line == CYCLE_HEADER_FIRST_LINE


def is_good_measures_data(measures: List[Measure]) -> bool:
    for i in range(len(measures) - 1):
        top_datetime = measures[i].datetime_val
        bottom_datetime = measures[i + 1].datetime_val
        delta_sec = (bottom_datetime - top_datetime).total_seconds()
        if delta_sec != 60:
            return False
    else:
        return True


def generate_cycle_filename_by_chain_filename(filename: str) -> str:
    base_name = filename.split('.')[0]
    return base_name + '_cycles.' + CYCLE_EXTENSION


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

    def __get_signal_from_line(self, line: str) -> List[int]:
        return list(map(int, line.split()[6: 16]))

    @property
    def device_num_part(self) -> str:
        return os.path.basename(self.path).split('_')[0]

    @property
    def datetime_stop(self) -> datetime:
        return self.__get_datetime_from_line(self.__last_line)

    @property
    def datetime_start(self) -> datetime:
        return self.__get_datetime_from_line(self.__first_line) + \
               timedelta(seconds=-1 + 1 / TSF_SIGNAL_FREQUENCY)

    @property
    def src_signal(self) -> List[Measure]:
        with open(self.path) as file_ctx:
            lines = [x.rstrip() for x in file_ctx if len(x.rstrip())]

        signal = []
        current_datetime = self.datetime_start
        for line in lines[TSF_FIRST_LINE_INDEX:]:
            one_second_signal = self.__get_signal_from_line(line)
            for discrete in one_second_signal:
                measure = Measure(current_datetime, discrete)
                signal.append(measure)
                current_datetime += timedelta(
                    seconds=1 / TSF_SIGNAL_FREQUENCY)
        return signal


class DATFile:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise OSError(f'File not found - {path}')

        extension = os.path.basename(path).split('.')[-1]
        if extension != DAT_EXTENSION:
            raise OSError(f'File is not tsf-file')

        self.path = path
        self.__first_line, self.__last_line = self.__get_last_lines()
        self.measures = self.extract_all_measures()

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

    @property
    def is_good_measures_data(self) -> bool:
        return True if self.measures else False

    def extract_all_measures(self) -> Union[None, List[Measure]]:
        measures = []
        with open(self.path) as file_ctx:
            for _ in range(DAT_FIRST_LINE_INDEX):
                next(file_ctx)

            for line in file_ctx:
                line = line.rstrip()
                if not line:
                    continue

                split_line = line.split('\t')

                datetime_line = split_line[1] + ' ' + split_line[2]
                datetime_val = datetime.strptime(datetime_line,
                                                 '%Y-%m-%d %H:%M:%S')
                corr_grav_val = float(split_line[3])
                single_measure = Measure(datetime_val, corr_grav_val)
                measures.append(single_measure)

        if not is_good_measures_data(measures):
            return None
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
            for link_id, line in enumerate(file_ctx):
                filename = line.rstrip()
                if filename:
                    chain[filename.rstrip()] = link_id
        return chain

    @property
    def sensor_part_name(self) -> str:
        return os.path.basename(self.path).split('.')[0].split('_')[1]


class CycleFile:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise OSError

        extension = os.path.basename(path).split('.')[-1]
        if extension != CYCLE_EXTENSION:
            raise OSError(f'File is not {CYCLE_EXTENSION} file')

        if not is_cycle_file(path):
            raise RuntimeError('File is not chain file')

        self.path = path
        self.defects = self.__load_defect_markers()

    def __load_defect_markers(self) -> Dict[int, Dict[int, bool]]:
        defects = {}
        with open(self.path) as file_ctx:
            next(file_ctx)
            for line in file_ctx:
                tmp_arr = line.rstrip().split('\t')
                session = int(tmp_arr[0])
                cycle_index, is_bad_num = int(tmp_arr[1]), int(tmp_arr[2])
                is_bad = True if is_bad_num == 1 else False
                if session not in defects:
                    defects[session] = {cycle_index: is_bad}
                else:
                    defects[session][cycle_index] = is_bad
        return defects
