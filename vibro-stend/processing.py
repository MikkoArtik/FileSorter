from typing import Union, Tuple, List, Dict
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from seiscore import BinaryFile
from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy
from seiscore.functions.filter import band_pass_filter

from config import Config, StreamConfig, Limit, StendMeasure, Pair
from scintrex import SG5File, DriftCorrectionFile, Measure


ENERGY_FITTING_DEGREE = 3
AMPLITUDE_FITTING_DEGREE = 1


def get_amplitude_energy_params(signal: np.ndarray, frequency: int,
                                energy_freq_limits: Limit) -> Tuple[float,
                                                                    float,
                                                                    float]:
    spectrum_data = spectrum(signal, frequency)
    sp_e_val = spectrum_energy(spectrum_data, [energy_freq_limits.low,
                                               energy_freq_limits.high])
    filter_signal = band_pass_filter(signal, frequency,
                                     energy_freq_limits.low,
                                     energy_freq_limits.high)
    ampl_e_val = np.sum(filter_signal ** 2)

    ampl_diff = max(filter_signal) - min(filter_signal)
    return sp_e_val, ampl_e_val, ampl_diff


@dataclass
class StatInfo:
    datetime_val: datetime
    seismometer: str
    gravimeter: str
    frequency: float
    amplitude: float
    velocity: float
    spectrum_energy: float
    amplitude_energy: float
    delta_amplitude: float
    grav_measure: float

    @property
    def line(self) -> str:
        t = [self.datetime_val.strftime('%Y-%m-%d %H:%M:%S'),
             self.seismometer, self.gravimeter, str(self.frequency),
             str(self.amplitude), str(self.velocity),
             str(self.spectrum_energy), str(self.amplitude_energy),
             str(self.delta_amplitude), str(self.grav_measure)]
        return '\t'.join(t)


class Processing:
    def __init__(self, config_path: str):
        self.config = Config(config_path)

        self.corrections = self.load_corrections()
        self.result_grav_data = self.load_result_gravimetric_file()
        self.seis_data = BinaryFile(self.config.seismic_file_path)

    def load_corrections(self) -> DriftCorrectionFile:
        path = self.config.drift_correction_file_path
        return DriftCorrectionFile(path)

    def load_result_gravimetric_file(self) -> SG5File:
        path = self.config.gravimetric_result_file_path
        sg5_file = SG5File(path, self.config.device_pair.gravimetric)
        sg5_file.add_drift_corrections(self.corrections)
        return sg5_file

    def get_seis_avg_characteristics(self, dt_start: datetime,
                                     dt_stop: datetime) -> Tuple[float, float,
                                                                 float]:
        seis_params = self.config.seismic_parameters

        self.seis_data.read_date_time_start = dt_start
        self.seis_data.read_date_time_stop = dt_stop
        signal = self.seis_data.read_signal('Z')

        params = get_amplitude_energy_params(signal, seis_params.frequency,
                                             seis_params.energy_freq)
        return params

    def get_cycle_info(self, seis_dt_min: datetime,
                       seis_dt_max: datetime) -> Union[None, StendMeasure]:
        origin_start = self.seis_data.datetime_start

        dt_min_sec = (seis_dt_min - origin_start).total_seconds()
        dt_max_sec = (seis_dt_max - origin_start).total_seconds()

        cycle_indexes, amount = set(), 0
        for i, cycle in enumerate(self.config.measure_cycles):
            t_limits = cycle.seismic_time_limit
            if t_limits.low <= dt_min_sec <= t_limits.high:
                cycle_indexes.add(i)
                amount += 1
            if t_limits.low <= dt_max_sec <= t_limits.high:
                cycle_indexes.add(i)
                amount += 1
        if len(cycle_indexes) != 1 or amount != 2:
            return None
        return self.config.measure_cycles[cycle_indexes.pop()]

    def get_statistics(self) -> List[StatInfo]:
        stat_info = []
        for i, grav_measure in enumerate(self.result_grav_data.measures):
            t_min, t_max = grav_measure.dt_start, grav_measure.dt_stop
            cycle_info = self.get_cycle_info(t_min, t_max)
            if cycle_info is None or not cycle_info.is_use_in_statistics:
                continue
            try:
                avg_params = self.get_seis_avg_characteristics(t_min, t_max)
            except ValueError:
                continue

            info = StatInfo(grav_measure.dt_start,
                            self.config.device_pair.seismic,
                            self.config.device_pair.gravimetric,
                            cycle_info.frequency, cycle_info.amplitude,
                            cycle_info.velocity, *avg_params,
                            grav_measure.result_value)
            stat_info.append(info)
        return stat_info
