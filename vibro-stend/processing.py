import os
from typing import Union

import numpy as np

from seiscore import BinaryFile
from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy
from seiscore.functions.filter import band_pass_filter

from config import Config, Limit



def get_amplitude_energy_params(signal: np.ndarray, frequency: int,
                                energy_freq_limits: Limit) -> Tuple[float,
                                                                    float,
                                                                    float]:
    min_amp, max_amp = min(signal), max(signal)

    spectrum_data = spectrum(signal, frequency)
    sp_e_val = spectrum_energy(spectrum_data, [energy_freq_limits.low,
                                               energy_freq_limits.high])
    filter_signal = band_pass_filter(signal, frequency,
                                     energy_freq_limits.low,
                                     energy_freq_limits.high)
    ampl_e_val = np.sum(filter_signal ** 2)
    ampl_diff = max_amp - min_amp
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

        self.result_grav_data = self.load_result_gravimetric_file()
        self.seis_data = BinaryFile(self.config.seismic_file_path)

    def load_result_gravimetric_file(self) -> SG5File:
        path = self.config.gravimetric_result_file_path
        sg5_file = SG5File(path)
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

    def load_avg_characteristics(self,
                                 signal_type: str) -> Union[np.ndarray, None]:
        if signal_type == 'seismic':
            params = self.config.seismic_parameters
            signal = self.seis_signal
        elif signal_type == 'gravimetric':
            params = self.config.gravimetric_parameters
            signal = self.grav_signal
        else:
            return None
        return get_amplitude_energy_params(
            signal, params.time_window, params.frequency,
            params.energy_freq)

    def save_signals(self):
        grav_signal, seis_signal = self.grav_signal, self.seis_signal

        export_path = os.path.join(self.config.seismic_root_folder,
                                   'source_signal.dat')
        np.savetxt(export_path, seis_signal, '%.3f\t%i', '\t',
                   header='Time\tAmplitude', comments='')

        export_path = os.path.join(self.config.gravimetric_root_folder,
                                   'source_signal.dat')
        np.savetxt(export_path, grav_signal, '%.3f\t%.3f', '\t',
                   header='Time\tAmplitude', comments='')

    def extract_signal_for_cycle(self, signal_type: str,
                                 cycle_index: int) -> Union[np.ndarray, None]:
        try:
            cycle = self.config.measure_cycles[cycle_index]
        except IndexError:
            return None

        if signal_type == 'seismic' and cycle.seismic_time_limit:
            t_limit = cycle.seismic_time_limit
            signal = self.seis_signal
        elif signal_type == 'gravimetric' and cycle.gravimetric_time_limit:
            t_limit = cycle.gravimetric_time_limit
            signal = self.grav_signal
        else:
            return None

        signal_part = signal[(signal[:, 0] >= t_limit.low) *
                             (signal[:, 0] < t_limit.high), 1]
        return signal_part

    def get_average_param_for_cycle(self, signal_type: str,
                                    cycle_index: int,
                                    parameter_name: str) -> Union[float, None]:
        try:
            cycle = self.config.measure_cycles[cycle_index]
        except IndexError:
            return None

        if signal_type == 'seismic' and cycle.seismic_time_limit:
            avg_data = self.seis_avg_data
            t_limit = cycle.seismic_time_limit
        elif signal_type == 'gravimetric' and cycle.gravimetric_time_limit:
            avg_data = self.grav_avg_data
            t_limit = cycle.gravimetric_time_limit
        else:
            return None

        if parameter_name == 'spectrum-energy':
            param_data = avg_data[(avg_data[:, 0] >= t_limit.low) *
                                  (avg_data[:, 0] < t_limit.high), 1]
        elif parameter_name == 'amplitude-energy':
            param_data = avg_data[(avg_data[:, 0] >= t_limit.low) *
                                  (avg_data[:, 0] < t_limit.high), 2]
        elif parameter_name == 'delta-amplitude':
            param_data = avg_data[(avg_data[:, 0] >= t_limit.low) *
                                  (avg_data[:, 0] < t_limit.high), 3]
        else:
            return None
        return np.mean(param_data)

    def get_cycle_statistics(self):
        stat_array = np.zeros(shape=(len(self.config.measure_cycles), 9))
        stat_array.fill(-9999)
        for i in range(len(self.config.measure_cycles)):
            cycle = self.config.measure_cycles[i]
            if not cycle.is_use_in_statistics:
                continue
            stat_array[i, :3] = [cycle.frequency, cycle.amplitude,
                                 cycle.velocity]
            for j, signal_type in enumerate(('seismic', 'gravimetric')):
                for k, param_name in enumerate(('spectrum-energy',
                                                'amplitude-energy',
                                                'delta-amplitude')):
                    param_val = self.get_average_param_for_cycle(
                        signal_type, i, param_name)
                    if param_val is None:
                        continue

                    cell_col_index = 3 * (j + 1) + k
                    stat_array[i, cell_col_index] = param_val
        return stat_array[stat_array[:, 3] != -9999]

    def save_spectrums(self):
        for index, cycle in enumerate(self.config.measure_cycles):
            filename = f'{index}-stand-frequency={cycle.frequency}-' \
                       f'amplitude={cycle.amplitude}.dat'
            header = 'Frequency\tAmplitude'

            seis_signal_part = self.extract_signal_for_cycle('seismic', index)
            if seis_signal_part is not None:
                freq = self.config.seismic_parameters.frequency
                spectrum_data = spectrum(seis_signal_part, freq)

                path = os.path.join(self.config.seismic_root_folder, filename)
                np.savetxt(path, spectrum_data, '%f', '\t', header=header,
                           comments='')

            grav_signal_part = self.extract_signal_for_cycle('gravimetric',
                                                             index)
            if grav_signal_part is not None:
                freq = self.config.gravimetric_parameters.frequency
                spectrum_data = spectrum(grav_signal_part, freq)

                path = os.path.join(self.config.gravimetric_root_folder,
                                    filename)
                np.savetxt(path, spectrum_data, '%f', '\t', header=header,
                           comments='')

    def save_amplitude_energy_params(self):
        if self.grav_avg_data is not None:
            export_path = os.path.join(self.config.gravimetric_root_folder,
                                       'energy-amplitude.dat')
            np.savetxt(export_path, self.grav_avg_data,
                       '%i\t%.3f\t%.3f\t%.3f',
                       header='Time,sec\tSpectrum_Energy\t'
                              'Amplitude_Energy\tAmplitude',
                       comments='')

        if self.seis_avg_data is not None:
            export_path = os.path.join(self.config.seismic_root_folder,
                                       'energy-amplitude.dat')
            np.savetxt(export_path, self.seis_avg_data, '%i\t%.3f\t%.3f\t%i',
                       header='Time,sec\tSpectrum_Energy\t'
                              'Amplitude_Energy\tAmplitude',
                       comments='')


    def run(self):
        self.save_signals()
        self.save_spectrums()
        self.save_amplitude_energy_params()
