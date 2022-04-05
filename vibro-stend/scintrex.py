import os
from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass
from typing import List, Dict


SKIP_LINES = 54
FREQUENCY = 6


@dataclass
class Measure:
    dt_start: datetime
    src_value: float
    duration: float
    drift_correction: float = 0
    seis_correction: float = 0
    acceleration: float = 0

    @property
    def dt_stop(self):
        return self.dt_start + timedelta(seconds=self.duration)

    @property
    def total_correction(self) -> float:
        return self.drift_correction + self.seis_correction

    @property
    def result_value(self) -> float:
        return round(self.src_value + self.total_correction, 4)


def load_file(func):
    def wrap(path):
        if not os.path.exists(path):
            raise OSError(f'Invalid file path - {path}')
        return func(path)
    return wrap


@load_file
def load_sg5_file(path: str) -> List[Measure]:
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


@load_file
def load_drift_corrections(path: str) -> Dict[str, Dict[datetime, float]]:
    corrections = dict()
    with open(path) as file_ctx:
        for i, line in enumerate(file_ctx):
            t = line.rstrip().split('\t')
            if i == 0 or len(t) != 4:
                continue
            gravimeter = t[0]
            datetime_val = datetime.strptime(' '.join(t[1:3]),
                                             '%d/%m/%Y %H:%M:%S')
            correction_val = float(t[3])
            if gravimeter not in corrections:
                corrections[gravimeter] = dict()
            corrections[gravimeter][datetime_val] = correction_val
    return corrections


class DriftCorrectionFile:
    def __init__(self, path):
        self.corrections = load_drift_corrections(path)

    def get_value(self, gravimeter: str,
                  datetime_val: datetime) -> float:
        correction_vals = self.corrections.get(gravimeter, None)
        if not correction_vals:
            return 0

        value = correction_vals.get(datetime_val, None)
        if not value:
            return 0
        return value


class SG5File:
    def __init__(self, path: str, number: str):
        self.__measures = load_sg5_file(path)
        self.number = number

    @property
    def measures(self) -> List[Measure]:
        return self.__measures

    def add_drift_corrections(self, corrections: DriftCorrectionFile):
        for measure in self.measures:
            correction = corrections.get_value(self.number, measure.dt_start)
            measure.drift_correction = correction
