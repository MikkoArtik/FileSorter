import os

import numpy as np

from seiscore import BinaryFile
from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy
from seiscore.functions.filter import band_pass_filter

from config import Config, Limit


def get_amplitude_energy_params(signal: np.ndarray, time_window: int,
                                frequency: int, energy_freq_limits: Limit):
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
        self.grav_signal = self.load_gravimetric_signal()

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

    def save_signals(self):
        grav_signal, seis_signal = self.grav_signal, self.seis_signal

        export_path = os.path.join(self.config.seismic_root_folder,
                                   'source_signal.dat')
        np.savetxt(export_path, seis_signal, '%.3f\t%i', '\t',
                   header='Time\tAmplitude', comments='')

        export_path = os.path.join(self.config.gravimetric_root_folder,
                                   'source_signal.dat')
        np.savetxt(export_path, seis_signal, '%.3f\t%.3f', '\t',
                   header='Time\tAmplitude', comments='')

    def save_spectrums(self):
        grav_signal, seis_signal = self.grav_signal, self.seis_signal
        for index, cycle in enumerate(self.config.measure_cycles):
            filename = f'{index}-stand-frequency={cycle.frequency}-' \
                       f'amplitude={cycle.amplitude}.dat'
            header = 'Frequency\tAmplitude'
            if cycle.seismic_time_limit:
                t_limit = cycle.seismic_time_limit
                g_signal_part = seis_signal[
                    (seis_signal[:, 0] >= t_limit.low) *
                    (seis_signal[:, 0] < t_limit.high), 1]

                freq = self.config.seismic_parameters.frequency
                spectrum_data = spectrum(g_signal_part, freq)

                path = os.path.join(self.config.seismic_root_folder, filename)
                np.savetxt(path, spectrum_data, '%f', '\t', header=header,
                           comments='')

            if cycle.gravimetric_time_limit:
                t_limit = cycle.gravimetric_time_limit
                g_signal_part = grav_signal[
                    (grav_signal[:, 0] >= t_limit.low) *
                    (grav_signal[:, 0] < t_limit.high), 1]

                freq = self.config.gravimetric_parameters.frequency
                spectrum_data = spectrum(g_signal_part, freq)

                path = os.path.join(self.config.gravimetric_root_folder,
                                    filename)
                np.savetxt(path, spectrum_data, '%f', '\t', header=header,
                           comments='')

    def save_amplitude_energy_params(self):
        grav_signal, seis_signal = self.grav_signal, self.seis_signal

        params = self.config.gravimetric_parameters
        grav_data = get_amplitude_energy_params(grav_signal,
                                                params.time_window,
                                                params.frequency,
                                                params.energy_freq)

        export_path = os.path.join(self.config.gravimetric_root_folder,
                                   'energy-amplitude.dat')
        np.savetxt(export_path, grav_data, '%i\t%.3f\t%.3f\t%i',
                   header='Time,sec\tSpectrum_Energy\t'
                          'Amplitude_Energy\tAmplitude',
                   comments='')

        params = self.config.seismic_parameters
        seis_data = get_amplitude_energy_params(seis_signal,
                                                params.time_window,
                                                params.frequency,
                                                params.energy_freq)

        export_path = os.path.join(self.config.seismic_root_folder,
                                   'energy-amplitude.dat')
        np.savetxt(export_path, seis_data, '%i\t%.3f\t%.3f\t%.3f',
                   header='Time,sec\tSpectrum_Energy\t'
                          'Amplitude_Energy\tAmplitude',
                   comments='')

    def run(self):
        self.save_signals()
        self.save_spectrums()
        self.save_amplitude_energy_params()
