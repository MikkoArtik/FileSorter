import os
from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass
from typing import List


SKIP_LINES = 54
FREQUENCY = 6


@dataclass
class Measure:
    dt_start: datetime
    src_value: float
    duration: float
    drift_correction: float = 0

    @property
    def dt_stop(self):
        return self.dt_start + timedelta(seconds=self.duration)

    @property
    def result_value(self) -> float:
        return self.src_value + self.drift_correction


def load_file(func):
    def wrap(path):
        if not os.path.exists(path):
            raise OSError(f'Invalid file path - {path}')
        return func(path)
    return wrap


@load_file
def load_sg5_file(path: str) -> List[Measure]:
    if not os.path.exists(path):
        raise OSError('Invalid file path')

    measures = []
    with open(path) as file_ctx:
        for i, line in enumerate(file_ctx):
            if i < SKIP_LINES:
                continue

            vector = [x for x in line.split() if x]
            grav_value = float(vector[3])
            duration = int(vector[9])
            datetime_value = datetime.strptime(vector[-1] + ' ' + vector[11],
                                               '%Y/%m/%d %H:%M:%S')
            measures.append(Measure(datetime_value, grav_value, duration))
    return measures


class SG5File:
    def __init__(self, path: str):
        self.__measures = load_sg5_file(path)

    @property
    def measures(self) -> List[Measure]:
        return self.__measures

    def add_time_offset(self, offset: float):
        for measure in self.measures:
            measure.add_time_offset(offset)
