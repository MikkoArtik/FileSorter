import datetime
import os
from typing import List, Union
from dataclasses import dataclass

import yaml


@dataclass
class Limit:
    low: float
    high: float


@dataclass
class Pair:
    seismic: str
    gravimetric: str


@dataclass
class StendMeasure:
    frequency: float
    amplitude: float
    seismic_time_limit: Union[Limit, None]
    gravimetric_time_limit: Union[Limit, None]
    is_use_in_statistics: bool

    @property
    def velocity(self):
        return round(4 * self.frequency * self.amplitude, 3)

    
@dataclass
class SeismicParameters:
    frequency: int
    energy_freq: Limit


def load_yaml_file(path: str) -> dict:
    if not os.path.exists(path):
        raise OSError('Config file not found')

    with open(path) as file_ctx:
        return yaml.load(file_ctx, Loader=yaml.FullLoader)


class Config:
    def __init__(self, path: str):
        self.src_data = load_yaml_file(path)

    @property
    def measure_date(self) -> datetime.date:
        return self.src_data['general']['date']

    @property
    def seismic_root_folder(self) -> str:
        return self.src_data['seismic']['root']

    @property
    def seismic_file_path(self) -> str:
        return os.path.join(self.seismic_root_folder,
                            self.src_data['seismic']['filename'])

    @property
    def seismic_parameters(self) -> SeismicParameters:
        params = self.src_data['seismic']['processing-parameters']
        energy_freq_limits = Limit(*params['energy'].values())
        return SeismicParameters(params['frequency'], energy_freq_limits)

    @property
    def gravimetric_root_folder(self) -> str:
        return self.src_data['gravimetric']['root']

    @property
    def gravimetric_result_file_path(self) -> str:
        return os.path.join(self.gravimetric_root_folder,
                            self.src_data['gravimetric']['result-file'])

    @property
    def drift_correction_file_path(self) -> str:
        return os.path.join(self.gravimetric_root_folder,
                            self.src_data['gravimetric']['drift-correction-file'])

    @property
    def device_pair(self) -> Pair:
        seis_device = self.src_data['seismic']['device']
        grav_device = self.src_data['gravimetric']['device']
        return Pair(seis_device, grav_device)

    @property
    def measure_cycles(self) -> List[StendMeasure]:
        cycles = self.src_data['measure-cycles']
        measure_cycles = []
        for cycle in cycles:
            frequency = cycle['frequency']
            amplitude = cycle['amplitude']

            time_seismic = cycle.get('t-seismic', None)
            if time_seismic:
                time_seismic = Limit(*time_seismic.values())

            time_gravimetric = cycle.get('t-gravimetric', None)
            if time_gravimetric:
                time_gravimetric = Limit(*time_gravimetric.values())

            is_use_in_stat = cycle['use-in-statistics']

            measure = StendMeasure(frequency, amplitude, time_seismic,
                                   time_gravimetric, is_use_in_stat)
            measure_cycles.append(measure)
        return measure_cycles


class StreamConfig:
    def __init__(self, path: str):
        self.src_data = load_yaml_file(path)

    @property
    def root_folder(self) -> str:
        return self.src_data['root']

    @property
    def config_files(self) -> List[str]:
        paths = [os.path.join(self.root_folder, x)
                 for x in self.src_data['files']
                 ]
        return paths
