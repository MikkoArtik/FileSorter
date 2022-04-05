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
class EnergyRegression:
    coeffs: List[float]

    def get_value(self, x: float) -> float:
        correction = 0
        for i, coeff in enumerate(self.coeffs[1:]):
            correction += coeff * x ** (i + 1)
        return correction


@dataclass
class SeismicIntervalInfo:
    stend_measure: StendMeasure
    spectrum_energy: float
    amplitude_energy: float
    delta_amplitude: float


@dataclass
class JoinRecord:
    pair: Pair
    stend_measure: StendMeasure
    spectrum_energy: float
    amplitude_energy: float
    delta_amplitude: float
    grav_measure: Measure

    @property
    def line(self) -> str:
        t = [self.grav_measure.dt_start.strftime('%Y-%m-%d %H:%M:%S'),
             self.pair.seismic, self.pair.gravimetric]
        if self.stend_measure:
            t += [str(self.stend_measure.frequency),
             str(self.stend_measure.amplitude),
             str(self.stend_measure.velocity)]
        else:
            t += ['-9999'] * 3

        t += [str(self.spectrum_energy), str(self.amplitude_energy),
              str(self.delta_amplitude), str(self.grav_measure.src_value),
              str(self.grav_measure.result_value),
              str(self.grav_measure.acceleration)]
        return '\t'.join(t)


@dataclass
class VelocityRegression:
    coeffs: List[float]

    def get_value(self, x: float) -> float:
        correction = 0
        for i, coeff in enumerate(self.coeffs):
            correction += coeff * x ** i
        return correction


class PairProcessing:
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
        if not (self.seis_data.datetime_start <= dt_start and dt_stop <=
                self.seis_data.datetime_stop):
            raise ValueError

        seis_params = self.config.seismic_parameters

        self.seis_data.read_date_time_start = dt_start
        self.seis_data.read_date_time_stop = dt_stop
        signal = self.seis_data.read_signal('Z')

        params = get_amplitude_energy_params(signal, seis_params.frequency,
                                             seis_params.energy_freq)
        return params

    def get_stend_measure(self, seis_dt_min: datetime,
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

        stend_measure = self.config.measure_cycles[cycle_indexes.pop()]
        if not stend_measure.is_use_in_statistics:
            return None

        return stend_measure

    def get_join_data(self) -> List[JoinRecord]:
        join_records = []
        for i, grav_measure in enumerate(self.result_grav_data.measures):
            t_min, t_max = grav_measure.dt_start, grav_measure.dt_stop
            cycle_info = self.get_stend_measure(t_min, t_max)
            if cycle_info is None:
                continue
            try:
                avg_params = self.get_seis_avg_characteristics(t_min, t_max)
            except ValueError:
                continue

            info = JoinRecord(self.config.device_pair, cycle_info, *avg_params,
                              grav_measure)
            join_records.append(info)
        return join_records

    def get_energy_regression(self) -> EnergyRegression:
        join_data = self.get_join_data()
        extraction = np.zeros(shape=(0, 2))
        for record in join_data:
            amplitude_energy = record.amplitude_energy
            if amplitude_energy < 0 or record.stend_measure is None:
                continue

            extraction = np.vstack(
                (extraction,
                 [amplitude_energy, record.grav_measure.result_value]))

        coeffs = np.polyfit(extraction[:, 0], extraction[:, 1],
                            ENERGY_FITTING_DEGREE)
        return EnergyRegression(list(reversed(coeffs)))

    def add_seis_energy_correction(self):
        regression = self.get_energy_regression()
        join_data = self.get_join_data()
        for measure in join_data:
            amplitude_energy = measure.amplitude_energy
            if amplitude_energy < 0 or measure.stend_measure is None:
                continue

            regression_val = regression.get_value(amplitude_energy)
            correction = round(-regression_val, 4)
            measure.grav_measure.seis_correction = correction

    def get_seismic_intervals_info(self) -> List[SeismicIntervalInfo]:
        info = []
        seis_dt_start = self.seis_data.datetime_start
        for step in self.config.measure_cycles:
            if not step.is_use_in_statistics:
                continue

            time_limit = step.seismic_time_limit
            t_min = seis_dt_start + timedelta(seconds=time_limit.low)
            t_max = seis_dt_start + timedelta(seconds=time_limit.high)

            try:
                params = self.get_seis_avg_characteristics(t_min, t_max)
            except ValueError:
                continue
            info.append(SeismicIntervalInfo(step, *params))
        return info


class MainProcessing:
    def __init__(self, config: StreamConfig):
        self.common_config = config

        self.pairs = self.create_couples()
        self.seis_intervals_info = self.get_all_intervals_info()
        self.velocity_regressions = self.get_velocity_regressions()

    def create_couples(self) -> List[PairProcessing]:
        pairs = []
        for file_path in self.common_config.config_files:
            pairs.append(PairProcessing(file_path))
        return pairs

    def add_energy_corrections(self):
        for couple in self.pairs:
            couple.add_seis_energy_correction()

    def get_all_intervals_info(self) -> List[SeismicIntervalInfo]:
        info = []
        for couple in self.pairs:
            info += couple.get_seismic_intervals_info()
        return info

    def get_base_seismic_delta_amplitude(self) -> float:
        return min([x.delta_amplitude for x in self.seis_intervals_info
                    if x.stend_measure.frequency == 0])

    def get_table_freq_list(self) -> List[float]:
        dataset = [x.stend_measure.frequency for x in self.seis_intervals_info
                   if x.stend_measure.frequency > 0]
        return sorted(list(set(dataset) - {0}))

    def get_velocity_regressions(self) -> Dict[float, VelocityRegression]:
        regressions = dict()
        common_intervals = self.seis_intervals_info
        for freq in self.get_table_freq_list():
            x = [x.delta_amplitude for x in common_intervals
                 if x.stend_measure.frequency == freq]
            x += [self.get_base_seismic_delta_amplitude()]

            y = [y.stend_measure.velocity for y in common_intervals
                 if y.stend_measure.frequency == freq]
            y += [0]

            coeffs = list(np.polyfit(x, y, AMPLITUDE_FITTING_DEGREE))

            coeffs[1] = -coeffs[0] * self.get_base_seismic_delta_amplitude()
            coeffs.reverse()
            regressions[freq] = VelocityRegression(coeffs)
        return regressions

    def get_velocity_by_seis_signal(self, freq: float,
                                    signal: np.ndarray) -> float:
        if freq == 0:
            return 0

        regression = self.velocity_regressions.get(freq, None)
        if regression is None:
            raise ValueError('Frequency is not found')

        delta_amp = np.max(signal) - np.min(signal)
        return regression.get_value(delta_amp)

    def run(self):
        self.add_energy_corrections()
        for pair in self.pairs:
            records = pair.get_join_data()
            seis_data = pair.seis_data
            for record in records:
                freq = record.stend_measure.frequency
                t_min = record.grav_measure.dt_start
                t_max = record.grav_measure.dt_stop

                seis_data.read_date_time_start = t_min
                seis_data.read_date_time_stop = t_max
                signal = pair.seis_data.read_signal('Z')

                part_size = int(1000 / 6)
                parts_count = int(signal.shape[0] / part_size)

                velocity = np.zeros(shape=parts_count)
                for i in range(parts_count):
                    left_index = i * part_size
                    right_index = (i + 1) * part_size

                    signal_part = signal[left_index: right_index]
                    velocity_val = self.get_velocity_by_seis_signal(
                        freq, signal_part)
                    velocity[i] = velocity_val

                dv = velocity[1:] - velocity[:-1]
                acceleration = dv * 6 * 100
                record.grav_measure.acceleration = np.mean(acceleration)