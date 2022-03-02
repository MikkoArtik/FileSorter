import os
from dataclasses import dataclass

import numpy as np

from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy


@dataclass
class StendMeasure:
    frequency: float
    amplitude: float
    t_min: int
    t_max: int


root = '/media/michael/Data/Projects/GraviSeismicComparation/2022_МОСКВА_МФТИ_ИДГ/Gravimetric/20-01-2022/458_2022_01_20'
filename = '458_2022_01_20.smp'
signal_frequency = 6
g_cal1 = 8329.075

stend_modes = [
    StendMeasure(0.1, 1.085, 888, 1426),
    StendMeasure(0.2, 1.185, 1686, 2137),
    StendMeasure(0.3, 1.245, 2394, 2874),
    StendMeasure(0.4, 1.167, 3113, 3613),
    StendMeasure(0.5, 1.083, 3900, 4414),
    StendMeasure(0.6, 1.039, 4724, 5148),
    StendMeasure(0.7, 1.081, 5376, 5851),
    StendMeasure(0.8, 1.168, 6095, 6516),
    StendMeasure(0.9, 1.160, 6829, 7253),
    StendMeasure(1.0, 1.247, 7563, 7988),
    StendMeasure(1.1, 1.362, 8266, 8659),
    StendMeasure(1.2, 1.340, 8993, 9385),
    StendMeasure(1.3, 1.297, 9679, 10072),
    StendMeasure(1.4, 1.236, 10340, 10793),
    StendMeasure(1.5, 1.225, 11050, 11498),
    StendMeasure(1.6, 1.141, 11898, 12286),

]

grav_smp_file_path = os.path.join(root, filename)
time_offset_sec = 0
time_interval = 10


grav_data = np.loadtxt(grav_smp_file_path, skiprows=1)
signal_time = np.arange(0, grav_data.shape[0]) / signal_frequency
grav_signal = np.column_stack((signal_time, grav_data[:, 0]))
grav_signal[:, 1] *= g_cal1 / 536870912

parts_count = int(grav_signal.shape[0] / (time_interval * signal_frequency))

result = np.zeros(shape=(parts_count, 3))
result[:, 0] = time_interval / 2 + np.arange(0, parts_count) * time_interval
for i in range(parts_count):
    left_edge = int(i * time_interval * signal_frequency)
    right_edge = int((i + 1) * time_interval * signal_frequency)

    signal = grav_signal[left_edge: right_edge, 1].copy()
    min_amp, max_amp = min(signal), max(signal)
    signal -= np.mean(signal)

    spectrum_data = spectrum(signal, signal_frequency)
    e_val = spectrum_energy(spectrum_data, [0, 2])
    result[i, 1] = e_val

    ampl_diff = max_amp - min_amp
    result[i, 2] = ampl_diff

export_path = os.path.join(root, 'energy-amplitude.dat')
np.savetxt(export_path, result, '%i\t%f\t%.3f',
           header='Time,sec\tEnergy\tAmplitude', comments='')

export_path = os.path.join(root, 'source_signal.dat')
np.savetxt(export_path, grav_signal, '%.3f\t%.3f', '\t',
           header='Time\tAmplitude', comments='')

for index, mode in enumerate(stend_modes):
    signal = grav_signal[(grav_signal[:, 0] >= mode.t_min) *
                         (grav_signal[:, 0] < mode.t_max), 1]
    spectrum_data = spectrum(signal, signal_frequency)

    filename = f'{index}-stand-frequency={mode.frequency}-' \
               f'amplitude={mode.amplitude}.dat'
    path = os.path.join(root, filename)
    header = 'Frequency\tAmplitude'
    np.savetxt(path, spectrum_data, '%f\t%.3f', '\t', header=header,
               comments='')
