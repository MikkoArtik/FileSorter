import os
from typing import Union

import numpy as np

from seiscore import BinaryFile
from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy
from seiscore.functions.filter import band_pass_filter

from config import Config, Limit


def get_amplitude_energy_params(signal: np.ndarray, time_window: int,
                                frequency: int,
                                energy_freq_limits: Limit) -> np.ndarray:
    parts_count = int(signal.shape[0] / (time_window * frequency))

    result = np.zeros(shape=(parts_count, 4))
    result[:, 0] = time_window / 2 + np.arange(0, parts_count) * time_window
    for i in range(parts_count):
        left_edge = int(i * time_window * frequency)
        right_edge = int((i + 1) * time_window * frequency)

        part_signal = signal[left_edge: right_edge, 1].copy()
        min_amp, max_amp = min(part_signal), max(part_signal)
        part_signal -= int(np.mean(part_signal))

        spectrum_data = spectrum(part_signal, frequency)
        sp_e_val = spectrum_energy(spectrum_data, [energy_freq_limits.low,
                                                   energy_freq_limits.high])
        result[i, 1] = sp_e_val

        filter_signal = band_pass_filter(part_signal, frequency,
                                         energy_freq_limits.low,
                                         energy_freq_limits.high)
        ampl_e_val = np.sum(filter_signal ** 2)
        result[i, 2] = ampl_e_val

        ampl_diff = max_amp - min_amp
        result[i, 3] = ampl_diff
    return result


class Processing:
    def __init__(self, config_path: str):
        self.config = Config(config_path)

        self.seis_signal = self.load_seismic_signal()
        self.seis_avg_data = self.load_avg_characteristics('seismic')

        self.grav_signal = self.load_gravimetric_signal()
        self.grav_avg_data = self.load_avg_characteristics('gravimetric')

    def load_seismic_signal(self):
        path = self.config.seismic_file_path
        bin_data = BinaryFile(path, 0, use_avg_values=False)

        signal = bin_data.read_signal('Z')
        frequency = bin_data.signal_frequency
        signal_time = np.arange(0, signal.shape[0]) / frequency
        return np.column_stack((signal_time, signal))

    def load_gravimetric_signal(self):
        path = self.config.gravimetric_file_path
        signal = np.loadtxt(path, skiprows=1)[:, 0]

        params = self.config.gravimetric_parameters
        signal *= params.g_cal / params.const

        signal_time = np.arange(0, signal.shape[0]) / params.frequency
        return np.column_stack((signal_time, signal))

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
            param_data = avg_data[:, 1]
        elif parameter_name == 'amplitude-energy':
            param_data = avg_data[:, 2]
        elif parameter_name == 'delta-amplitude':
            param_data = avg_data[:, 3]
        else:
            return None

        return np.mean(param_data[(avg_data[:, 0] >= t_limit.low) *
                                  (avg_data[:, 0] > t_limit.high)])

    def get_cycle_statistics(self):
        stat_array = np.zeros(shape=(len(self.config.measure_cycles), 9))
        stat_array.fill(-9999)
        for i in range(len(self.config.measure_cycles)):
            cycle = self.config.measure_cycles[i]
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
            np.savetxt(export_path, self.grav_avg_data, '%i\t%.3f\t%.3f\t%i',
                       header='Time,sec\tSpectrum_Energy\t'
                              'Amplitude_Energy\tAmplitude',
                       comments='')

        if self.seis_avg_data is not None:
            export_path = os.path.join(self.config.seismic_root_folder,
                                       'energy-amplitude.dat')
            np.savetxt(export_path, self.seis_avg_data,
                       '%i\t%.3f\t%.3f\t%.3f',
                       header='Time,sec\tSpectrum_Energy\t'
                              'Amplitude_Energy\tAmplitude',
                       comments='')

    def run(self):
        self.save_signals()
        self.save_spectrums()
        self.save_amplitude_energy_params()
